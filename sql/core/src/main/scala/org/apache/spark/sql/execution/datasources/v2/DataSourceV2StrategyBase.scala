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

import scala.collection.mutable

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSet, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownExpressions, SupportsPushDownFilterExpressions, SupportsPushDownFilters, SupportsPushDownRequiredColumns}


trait DataSourceV2StrategyBase extends Strategy with PredicateHelper {
  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
                           reader: DataSourceReader,
                           filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source filters to original catalyst filter expressions.
        val translatedFilterToExpr = mutable.HashMap.empty[Filter, Expression]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated = DataSourceStrategy.translateFilter(filterExpr)
          if (translated.isDefined) {
            translatedFilterToExpr(translated.get) = filterExpr
          } else {
            untranslatableExprs += filterExpr
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilterToExpr.keys.toArray)
          .map(translatedFilterToExpr)
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
   def pushFilterExpressions(reader: DataSourceReader,
                                    filters: Seq[Expression]):
  (Seq[Expression], Seq[Expression]) = {
    reader match {
      case r: SupportsPushDownFilterExpressions =>

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilterExpressions(filters.toArray)

        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilterExpressions()
        (pushedFilters, postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /*
  private def exprToString(sep: String, expr: Expression): String = {
    s"""${sep} Expr : ${expr.toString}, Class: ${expr.getClass.getName}
        Children: ${expr.children.map(exprToString(sep + "\t", _)).mkString("\n")}"""
  }
*/
  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return new output attributes after column pruning.
   */
  // TODO: nested column pruning.
   private def pruneColumns(reader: DataSourceReader,
                    relation: DataSourceV2Relation,
                    exprs: Seq[Expression]): Seq[AttributeReference] = {
    reader match {
      case r: SupportsPushDownRequiredColumns =>
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        if (neededOutput != relation.output) {
          r.pruneColumns(neededOutput.toStructType)

          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          r.readSchema().toAttributes.map {
            // We have to keep the attribute id during transformation.
            a => a.withExprId(nameToAttr(a.name).exprId)
          }
        } else {
          relation.output
        }

      case _ => relation.output
    }
  }

  // you might want to use transform, but then you'll get an infinite loop :/
  def substitute(in: Expression, known: Seq[NamedExpression]) : Expression = {
    known.find(a => a.semanticEquals(in) ||
               (a.isInstanceOf[Alias] && a.asInstanceOf[Alias].child.semanticEquals(in)))
         .getOrElse(in.mapChildren(substitute(_, known)))
  }


    def pruneExpressions(reader: DataSourceReader,
                       relation: DataSourceV2Relation,
                       output: Seq[Expression]): Seq[NamedExpression] = {

    // the old version of pruning
    pruneColumns(reader, relation, output)
    reader match {
      case r: SupportsPushDownExpressions =>
        r.pushExpressions(output.toArray)

      case _ => relation.output
    }
  }
}
