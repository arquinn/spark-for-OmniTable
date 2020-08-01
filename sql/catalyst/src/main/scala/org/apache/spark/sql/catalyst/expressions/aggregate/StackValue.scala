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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - stack offset based on `inc` and `dec`")
case class StackValue(inc: BinaryComparison, dec: BinaryComparison, counter: Expression)
  extends DeclarativeAggregate {

  override def children: Seq[Expression] = inc :: dec :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = LongType

  private lazy val stackOffset = AttributeReference("stackOffset", LongType)()
  // private lazy val countValue = AttributeReference("countVal", LongType)()


  override lazy val aggBufferAttributes = stackOffset :: Nil
  // override lazy val aggBufferAttributes = minSChild :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* stackOffset = */ Literal.create(0, LongType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* stackOffset = */ If(inc, Add(stackOffset, 1),
                              If(dec, Subtract(stackOffset, 1),
                                      stackOffset))
  )


  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* stackOffset = */ Add(stackOffset.left, stackOffset.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = stackOffset
}
