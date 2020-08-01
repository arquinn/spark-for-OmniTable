/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, GenericInternalRow, JoinedRow, Projection, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.{LeftStackOJT, NextOJT, OrderedJoinType, StackLike}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution, Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution.{BinaryExecNode, RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics


/**
 * Performs a next join of two child relations.
 */

case class OrderedJoinExec(leftEqiKeys: Seq[Expression],
                        rightEqiKeys: Seq[Expression],
                        leftOrderKeys: Seq[Expression],
                        rightOrderKeys: Seq[Expression],
                        condition: Option[Expression],
                        joinType : OrderedJoinType,
                        left: SparkPlan,
                        right: SparkPlan) extends BinaryExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  lazy val leftKeys: Seq[Expression] = leftEqiKeys ++ leftOrderKeys
  lazy val rightKeys: Seq[Expression] = rightEqiKeys ++ rightOrderKeys

  override def output: Seq[Attribute] = (left.output ++ right.output).map(_.withNullability(true))

  override def outputPartitioning: Partitioning =
    PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = {
    // For inner join, orders of both sides keys should be kept.

    // val allLKeys = leftKeys :+ leftOKey
    // val allRKeys = rightKeys :+ rightOKey

    val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
    val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
    leftKeyOrdering.zip(rightKeyOrdering).map { case (lKey, rKey) =>
      // Also add the right key and its `sameOrderExpressions`
      SortOrder(lKey.child, Ascending, lKey.sameOrderExpressions + rKey.child ++ rKey
        .sameOrderExpressions)
    }
  }

  /**
   * The utility method to get output ordering for left or right side of the join.
   *
   * Returns the required ordering for left or right child if childOutputOrdering does not
   * satisfy the required ordering; otherwise, which means the child does not need to be sorted
   * again, returns the required ordering for this child with extra "sameOrderExpressions" from
   * the child's outputOrdering.
   */
  private def getKeyOrdering(keys: Seq[Expression], childOutputOrdering: Seq[SortOrder])
  : Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map { case (key, childOrder) =>
        SortOrder(key, Ascending, childOrder.sameOrderExpressions + childOrder.child - key)
      }
    } else {
      requiredOrdering
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  private def createLeftKeyGenerator(): Projection =
    UnsafeProjection.create(leftEqiKeys, left.output)

  private def createRightKeyGenerator(): Projection =
    UnsafeProjection.create(rightEqiKeys, right.output)

  private def createLeftOrderKeyGenerator(): Projection =
    UnsafeProjection.create(leftOrderKeys, left.output)

  private def createRightOrderKeyGenerator(): Projection =
    UnsafeProjection.create(rightOrderKeys, right.output)



  private def getSpillThreshold: Int = sqlContext.conf.sortMergeJoinExecBufferSpillThreshold

  private def getInMemoryThreshold: Int = sqlContext.conf.sortMergeJoinExecBufferInMemoryThreshold

  protected override def doExecute(): RDD[InternalRow] = {

    logInfo(s"""orderedJoinExec DoExecute Starting! ${joinType} """)
    val numOutputRows = longMetric("numOutputRows")
    val spillThreshold = getSpillThreshold
    val inMemoryThreshold = getInMemoryThreshold

    // I don't think this works in the case that right is empty?
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>

      val boundCondition: (InternalRow) => Boolean = {
        condition.map { cond =>
          newPredicate(cond, left.output ++ right.output).eval _
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      // val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
      val keyOrdering = newNaturalAscendingOrdering(leftEqiKeys.map(_.dataType))
      val orderKeyOrdering = newNaturalAscendingOrdering(leftOrderKeys.map(_.dataType))
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      new RowIterator {

        private[this] val scanner: OrderedJoinScanner = joinType match {
          case NextOJT => new NextJoinScanner(
            createLeftKeyGenerator(),
            createRightKeyGenerator(),
            keyOrdering,
            createLeftOrderKeyGenerator(),
            createRightOrderKeyGenerator(),
            orderKeyOrdering,
            RowIterator.fromScala(leftIter),
            RowIterator.fromScala(rightIter))
          case LeftStackOJT => new LeftStackJoinScanner(
            createLeftKeyGenerator(),
            createRightKeyGenerator(),
            keyOrdering,
            createLeftOrderKeyGenerator(),
            createRightOrderKeyGenerator(),
            orderKeyOrdering,
            RowIterator.fromScala(leftIter),
            RowIterator.fromScala(rightIter)
          )
          case _ => new StackJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              createLeftOrderKeyGenerator(),
              createRightOrderKeyGenerator(),
              orderKeyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter))
        }

        private[this] val joinRow = new JoinedRow
        private[this] val nulls = new GenericInternalRow(right.output.size)

        override def advanceNext(): Boolean = {
          logError("advanceNext called from OrderedJoinExec I guess?")
          val (currentLeftRow, currentRightRow) = scanner.findJoinRow()
          if (currentLeftRow != null) {
            numOutputRows += 1
            if (currentRightRow != null) {
              joinRow(currentLeftRow, currentRightRow)
            } else {
              joinRow(currentLeftRow, nulls)
            }
          }
          return currentLeftRow != null;
        }
        override def getRow: InternalRow = joinType match {
          case _ => resultProj (joinRow)
        }
      }.toScala
    }
  }
}

/**
 * Helper class that is used to implement [[OrderedJoinExec]].
 *
 * To perform an join, users of this class call [[findJoinRow()]]
 * which returns `leftrow, rightrow` if a result has been produced and `(null, null)` otherwise.
 *
 * @param lKeyGen a projection that produces join keys from the streamed input.
 * @param rKeyGen a projection that produces join keys from the buffered input.
 * @param eqiKeyOrder an ordering which can be used to compare join keys.
 * @param lIter an input whose rows will be streamed.
 * @param rIter an input whose rows will be buffered to construct sequences of rows that
 *                     have the same join key.
 */
private[joins] abstract class OrderedJoinScanner(lKeyGen: Projection,
                                     rKeyGen: Projection,
                                     eqiKeyOrder: Ordering[InternalRow],
                                     lOKeyGen : Projection,
                                     rOKeyGen: Projection,
                                     oKeyOrder: Ordering[InternalRow],
                                     lIter: RowIterator,
                                     rIter: RowIterator) extends Logging {

  protected[this] var leftRow: InternalRow = _
  protected[this] var leftRowKey: InternalRow = _
  protected[this] var leftOrderKey: InternalRow = _


  protected[this] var rightRow: InternalRow = _
  protected[this] var rightRowKey: InternalRow = _
  protected[this] var rightOrderKey: InternalRow = _


  // --- Public methods ---------------------------------------------------------------------------
  def findJoinRow() : (InternalRow, InternalRow)

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the streamed iterator and compute the new row's join key.
   * @return true if the streamed iterator returned a row and false otherwise.
   */
  protected def advancedLeft(): Boolean = {
    var foundRow: Boolean = false

    while (!foundRow && lIter.advanceNext()) {
      leftRow = lIter.getRow
      leftRowKey = lKeyGen(leftRow)
      leftOrderKey = lOKeyGen(leftRow)
      foundRow = !leftRowKey.anyNull
      // leftFinished = leftIter.advanceNext()
    }

    if (!foundRow) {
      leftRow = null
      leftRowKey = null
      false
    } else {
      true
    }
  }

  /**
   * Advance the buffered iterator until we find a row with join key that does not contain nulls.
   * @return true if the buffered iterator returned a row and false otherwise.
   */
  protected def advancedRight(): Boolean = {
    var foundRow: Boolean = false

    while (!foundRow && rIter.advanceNext()) {
      rightRow = rIter.getRow
      rightRowKey = rKeyGen(rightRow)
      rightOrderKey = rOKeyGen(rightRow)
      foundRow = !rightRowKey.anyNull
      // rightFinished = rightIter.advanceNext()
    }

    if (!foundRow) {
      rightRow = null
      rightRowKey = null
      false
    } else {
      true
    }
  }
}

private[joins] class NextJoinScanner(lKeyG: Projection,
                                     rKeyG: Projection,
                                     eqiKeyO: Ordering[InternalRow],
                                     lOKeyGen : Projection,
                                     rOKeyGen: Projection,
                                     oKeyOrder: Ordering[InternalRow],
                                     lIter: RowIterator,
                                     rIter: RowIterator)
  extends OrderedJoinScanner(lKeyG, rKeyG, eqiKeyO, lOKeyGen, rOKeyGen, oKeyOrder, lIter, rIter) {
  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   * @return left and right internal rows for a match, or (null, null) if no match exists
   */
  override  def findJoinRow() : (InternalRow, InternalRow) = {
  // is this vv correct? should I always advance? I don't think so??
    advancedLeft()
    advancedRight()

    // this behavior somewhat depends on left v. right v. inner v. outer,

    if (leftRow == null) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      rightRow = null
      (null, null)
    } else if (rightRow == null) {
      // There are no more rows to read from the buffered iterator, so there can be no more matches.
      leftRow = null
      (null, null)
    } else {
      // Advance both the streamed and buffered iterators to find the next pair of matching rows.
      var comp = 0
      do {
        // left and right are ordered first by equality -- no match means no ordering match either
        comp = eqiKeyO.compare(leftRowKey, rightRowKey)
        // logInfo(s"""comp between ${leftRow.toString} and ${rightRow.toString} is ${comp}""")
        if (comp > 0) advancedRight()
        else if (comp < 0) advancedLeft()

        // check to see that right is after the left
        else {
          val oComp = oKeyOrder.compare(leftOrderKey, rightOrderKey)
          // logInfo(s"""oComp is ${oComp}""")
          if (oComp >= 0) {
            comp = 1
            advancedRight()
          }
        }
      } while (leftRow != null && rightRow != null && comp != 0)

      if (leftRow == null || rightRow == null) {
        // We have hit the end of one of the iterators, so there can be no more matches.
        leftRow = null
        rightRow = null
        (null, null)
      } else {
        (leftRow, rightRow)
      }
    }
  }
}


private[joins] class StackJoinScanner(lKeyG: Projection,
                                      rKeyG: Projection,
                                      eqiKeyO: Ordering[InternalRow],
                                      lOKeyGen : Projection,
                                      rOKeyGen: Projection,
                                      oKeyO: Ordering[InternalRow],
                                      lIter: RowIterator,
                                      rIter: RowIterator)
  extends OrderedJoinScanner(lKeyG, rKeyG, eqiKeyO, lOKeyGen, rOKeyGen, oKeyO, lIter, rIter)
  with Logging {

  // in scala.. you can just, shove this here and it gets called on object construction (!)
  advancedLeft()

  // in this world, leftRow and all that jazz points to the row AFTER the current potential match

  protected[this] var leftStack: ListBuffer[InternalRow] = ListBuffer.empty[InternalRow]
  protected[this] var rightDone: Boolean = false;

  logInfo(s"making a stackJoinScanner (ps: can't believe this shit works:")

  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   *
   * @return left and right internal rows for a match, or (null, null) if no match exists
   *
   *         I think this works for... inner joins?
   */
  def findJoinRow(): (InternalRow, InternalRow) = {
    // on a match I *have* to advance the right iter ?
    var leftMatch: InternalRow = null

    logError(s"findJoinRow")

    if (!rightDone) {
      do {
        rightDone = !advancedRight()
        if (rightRow != null) {
          logInfo(s"""next rightRow ${rightRow.toString}""")
        }
        var innerDone = false;
        // push all equal and ordered less left rows onto the stack
        while (!innerDone && leftRow != null && rightRow != null) {

          // ordered first by equiKey, so non match means they'll never match the stack.
          // again, this depends on left v. right v. inner v. outter
          val comp = eqiKeyO.compare(leftRowKey, rightRowKey)

          logInfo(s"""equiCompare $leftRowKey vs. $rightRowKey is ${comp}""")
          if (comp == 0) {
            val oComp = oKeyO.compare(leftOrderKey, rightOrderKey)
            logInfo(s"""oComp is ${oComp}""")
            if (oComp < 0) {
              logInfo(s"""push ${leftRow.toString} to stack ${leftStack.size}""")
              // bad things happen if you don't copy (leftRow is a var after all :()
              leftStack.prepend(leftRow.copy())
              advancedLeft()
            } else {
              innerDone = true
            }
          } else if (comp < 0) {
            advancedLeft()
          } else {
            innerDone = true
            // don't I need to clear out the stack in this case...???
          }
        }
        // if there is something at the top of the stack, it's our potential match:

        leftStack.foreach((ir) => {
          val leqi = lKeyG(ir)
          logInfo(s"""stack has $leqi vs. $rightRowKey""")
          assert(eqiKeyO.compare(leqi, rightRowKey) == 0)
        })

        if (leftRow != null) {
          leftRowKey = lKeyG(leftRow)
        }
        if (!leftStack.isEmpty) {
          leftMatch = leftStack.head
          leftStack.trimStart(1)
        }
        if (leftRow != null) {
          logInfo(s"""leftRowKey $leftRowKey""")
        }
        // there should be an else here depending on left v. right v. inner v. outer
      } while (leftMatch == null && rightRow != null)
    }

    // recompute in case right done b/c of a loop:
    if (rightDone) {
      // flush out the stack and then the left iter:
      if (!leftStack.isEmpty) {
        leftMatch = leftStack.head
        leftStack.trimStart(1)
      }
      else {
        leftMatch = leftRow
        advancedLeft()
      }
    }

    if (leftMatch != null && rightRow != null) {
      logInfo(s"""return match ${leftMatch.toString} ${rightRow.toString}""")
    }
    else {
      logInfo(s"""donzo with the findJoinRow""")
    }

    if (leftRow != null) {
      logInfo(s"""leftRowKey $leftRowKey""")
    }
    (leftMatch, rightRow)
  }
}

private[joins] class LeftStackJoinScanner(lKeyG: Projection,
                                                rKeyG: Projection,
                                                eqiKeyO: Ordering[InternalRow],
                                                lOKeyGen : Projection,
                                                rOKeyGen: Projection,
                                                oKeyO: Ordering[InternalRow],
                                                lIter: RowIterator,
                                                rIter: RowIterator)
  extends OrderedJoinScanner(lKeyG, rKeyG, eqiKeyO, lOKeyGen, rOKeyGen, oKeyO, lIter, rIter)
    with Logging{

  // in scala.. you can just, shove this here and it gets called on object construction (!)
  advancedLeft()

  // in this world, leftRow and all that jazz points to the row AFTER the current potential match

  protected[this] var leftStack: ListBuffer[InternalRow] = ListBuffer.empty[InternalRow]


  protected[this] var clearStack: Boolean = false
  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   * @return left and right internal rows for a match, or (null, null) if no match exists
   *
   * I think this works for... inner joins?
   */
  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   *
   * @return left and right internal rows for a match, or (null, null) if no match exists
   *
   *         I think this works for... inner joins?
   */
  def findJoinRow(): (InternalRow, InternalRow) = {
    // on a match I *have* to advance the right iter ?
    var leftMatch: InternalRow = null
    var comp = 1

    if (clearStack) {
      leftMatch = leftStack.head
      leftStack.trimStart(1)
      clearStack = !leftStack.isEmpty
      (leftMatch, null)
    }
    else {
      do {
        advancedRight()

        if (rightRow != null) {
          logInfo(s"""rightRow ${rightRow.toString}""")
        }

        var innerDone = false;
        // push all less than left rows (both eqi and ordered) onto the stack
        while (!innerDone && leftRow != null && rightRow != null) {

          // ordered first by equiKey, so non match means they'll never match the stack.
          comp = eqiKeyO.compare(leftRowKey, rightRowKey)
          logInfo(s"""equiCompare $leftRowKey vs. $rightRowKey is ${comp}""")
          if (comp == 0) {
            val oComp = oKeyO.compare(leftOrderKey, rightOrderKey)
            logInfo(s"""oComp is ${oComp}""")
            if (oComp < 0) {
              logInfo(s"""push ${leftRow.toString} to stack ${leftStack.size}""")
              // bad things happen if you don't copy (leftRow is a var)
              leftStack.prepend(leftRow.copy())
              advancedLeft()
            } else {
              innerDone = true
            }
          } else if (comp < 0) {
            leftStack.prepend(leftRow.copy())
            advancedLeft()
          } else {
            innerDone = true
          }
        }

        // always try to pop from the stack.
        if (!leftStack.isEmpty) {
          leftMatch = leftStack.head
          leftStack.trimStart(1)
          if (rightRow != null) {
            comp = eqiKeyO.compare(lKeyG(leftMatch), rightRowKey)
          }
          else {
            comp = 1
          }
          if (leftRow != null) {
            leftRowKey = lKeyG(leftRow)
          }
        }
      } while (leftMatch == null && rightRow != null)


      if (comp == 0) {
        logInfo(s"""return match ${leftMatch.toString} ${rightRow.toString}""")
        (leftMatch, rightRow)
      } else {

        clearStack = !leftStack.isEmpty
        if (leftMatch != null) {
          logInfo(s"""return left match ${leftMatch.toString}""")
        }
        (leftMatch, null)
      }
    }
  }
}
