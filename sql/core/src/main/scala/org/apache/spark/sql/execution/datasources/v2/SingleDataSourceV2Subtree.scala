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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.columnar.InMemoryRelation


object SingleDataSourceV2Subtree extends Logging with PredicateHelper {

  /*
   * Return type is:
   *  (1) The output that we want to produce from this subtree
   *  (2) The join node.
   *  (3) The resolved data.
   *  (4) All the filters that matter
   *  (5) The reference to the DataSourceV2
   */
  type ReturnType =
    (Seq[NamedExpression],
    Join,
    InMemoryRelation,
    Seq[Expression],
    DataSourceV2Relation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {

    // We want to find single LeafNode subplans, and we only can currently handle single Join
    if (getNodes[DataSourceV2Relation](plan).size == 1 &&
      getNodes[Join](plan).size == 1) {

      val (output, join, inMem, filters, datasourceV2, _) = collectPlanInfo(plan)

      if (output.isDefined && join.isDefined && inMem.isDefined && datasourceV2.isDefined) {
        Some((output.get, join.get, inMem.get, filters, datasourceV2.get))
      }
      else {
        None
      }
    }
    else {
      None
    }
  }

  /**
   * Collects all the plan info needed for the ReturnType above. Does inlining
   * and substituting aliases a la collecteProjectsAndFilters from PhysicalOperation
   * within patterns.scala).
   *
   * @param plan The plan that we want to parse through
   * @return All the info that is interesting
   */
  def collectPlanInfo(plan: LogicalPlan):
    (Option[Seq[NamedExpression]],
      Option[Join],
      Option[InMemoryRelation],
      Seq[Expression],
      Option[DataSourceV2Relation],
      Map[ExprId, Expression]) = {
    plan match {
      case Project(fields, child) if fields.forall(_.deterministic) =>
        val (_, join, inMem, filters, datasourceV2, aliases) = collectPlanInfo(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        val als = collectAliases(substitutedFields)

        (Some(substitutedFields),
          join,
          inMem,
          filters,
          datasourceV2,
          als)

      case Filter(condition, child) if condition.deterministic =>
        val (fields, join, inMem, filters, datasourceV2, aliases) = collectPlanInfo(child)
        val substituteCondition = substitute(aliases)(condition)
        (fields,
          join,
          inMem,
          filters ++ splitConjunctivePredicates(substituteCondition),
          datasourceV2,
          aliases)

      case inMem : InMemoryRelation =>
        (None, None, Some(inMem), Nil, None, Map.empty)

      case datasource : DataSourceV2Relation =>
        (None, None, None, Nil, Some(datasource), Map.empty)

      case j : Join =>
        val (_, _, rinMem, rfilters, rdatasourceV2, raliases) = collectPlanInfo(j.right)
        val (_, _, lInMem, lfilters, ldatasourceV2, laliases) = collectPlanInfo(j.left)

        // For each of these, there should be at least one of these defined!
        val inMem = if (rinMem.isDefined) rinMem else lInMem
        val datasource = if (rdatasourceV2.isDefined) rdatasourceV2 else ldatasourceV2
        val aliases = laliases ++ raliases

        val substitutedFields = j.output.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]


        (Some(substitutedFields),
          Some(j),
          inMem,
          rfilters ++ lfilters,
          datasource,
          collectAliases(substitutedFields))

      case other =>
        logDebug(s"I don't understand how to parse: ${other.toString}")
        (None, None, None, Nil, None, Map.empty)
    }
  }

  private def collectAliases(fields: Seq[Expression]): Map[ExprId, Expression] = fields.collect {
    case a @ Alias(child, _) => a.exprId -> child
  }.toMap

  private def substitute(aliases: Map[ExprId, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref.exprId)
          .map(Alias(_, name)(a.exprId, a.qualifier))
          .getOrElse(a)


      case a: AttributeReference =>
        aliases.get(a.exprId)
          .map(Alias(_, a.name)(a.exprId, a.qualifier)).getOrElse(a)

    }
  }


  private def getNodes[T <: LogicalPlan : Manifest](plan: LogicalPlan): Seq[T] = plan match {
    case _: InMemoryRelation =>
      Nil
    case t : T =>
      t :: Nil
     case _ =>
      plan.children.foldLeft(Seq.empty[T]) { (n, child) =>
        n ++ getNodes[T](child)
      }
  }
}
