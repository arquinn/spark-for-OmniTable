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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, PushdownExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec
import org.apache.spark.sql.execution.metric.SQLMetrics


case class BroadcastNestedLoopPushdownJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends UnaryExecNode with Logging {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamedPlan, buildPlan) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  /** This is hacky and some complete bullshit. but it's what I've got */
  val child = buildPlan

  /** Required so that we can broadcast correctly. */
  override def requiredChildDistribution: Seq[Distribution] =
      BroadcastDistribution(IdentityBroadcastMode) :: Nil


  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      newPredicate(condition.get, streamedPlan.output ++ buildPlan.output).eval _
    } else {
      _ : InternalRow => true
    }
  }


  protected override def doExecute(): RDD[InternalRow] = {

    val broadcastRelation = buildPlan.executeBroadcast[Array[InternalRow]]()
    val streamPush = streamedPlan.asInstanceOf[PushdownExecNode]

    // Apply filter here (in case there was an issue applying it in the pushdown)
    val resultRdd = streamPush.pushdownMatch(broadcastRelation.value)
      .mapPartitionsInternal(_.filter(boundCondition))

    // not 100% sure why I need the numOutputRows (AFAIK it isn't used?)
    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>

      val resultProj = UnsafeProjection.create(
        output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))

      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1

        resultProj(r)
      }
    }
  }
}
