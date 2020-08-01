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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

@ExpressionDescription(
usage = "_FUNC_(expr) - Returns the Expression `child` from the row with minimum value of `expr`")
case class MinRowVal(sChild: Expression, child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = sChild :: child :: Nil
  // override def children: Seq[Expression] = sChild :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(sChild.dataType, "function minRowVal")

  private lazy val minSChild = AttributeReference("minSChild", sChild.dataType)()
  private lazy val minChild = AttributeReference("minChild", child.dataType)()

  override lazy val aggBufferAttributes = minSChild :: minChild :: Nil
  // override lazy val aggBufferAttributes = minSChild :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* minSChild = */ Literal.create(null, sChild.dataType),
    /* minChild  = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* minSChild = */ least(minSChild, sChild),
    /* minChild  = */ If(LessThan(minSChild, sChild), minChild, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* minSChild = */ least(minSChild.left, minSChild.right),
      /* minChild  = */ If(LessThan(minSChild.left, minSChild.right),
                           minChild.left,
                           minChild.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = minChild
}
