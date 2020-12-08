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
  import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, NamedExpression, PredicateHelper}
  import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
  import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType}
  import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DatasourceV2Pushable, Join, JoinWithPushable, LogicalPlan, Repartition}
  import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
  import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
  import org.apache.spark.sql.execution.datasources.DataSourceStrategy
  import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopPushdownJoinExec, BuildLeft, BuildRight}
  import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
  import org.apache.spark.sql.sources.Filter
  import org.apache.spark.sql.sources.v2.reader._
  import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader




  object DataSourceV2Strategy extends Strategy with PredicateHelper {

    /**
     * Pushes down filters to the data source reader
     *
     * @return pushed filter and post-scan filters.
     */
    private def pushFilters(
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
    private def pushFilterExpressions(reader: DataSourceReader,
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
    private def pruneColumns(
                              reader: DataSourceReader,
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

    private def pruneExpressions(reader: DataSourceReader,
                                 relation: DataSourceV2Relation,
                                 exprs: Seq[Expression]): Seq[AttributeReference] = {
      reader match {
        case r: SupportsPushDownExpressions =>

          r.pushExpressions(exprs.toArray)
            .map(e => {
              AttributeReference(e.name, e.dataType, e.nullable, e.metadata)().withExprId(e.exprId)
            })
        case _ => relation.output
      }
    }


    private def getPlanForJoin(stream: DataSourceV2Relation,
                               streamFilters: Seq[Expression],
                               streamProject : Seq[NamedExpression],
                               broadcast: LogicalPlan,
                               joinType: JoinType,
                               cond: Option[Expression],
                               push : Option[Expression],
                               leftBroadcast: Boolean) = {
        logDebug(
          s"""stream ${stream.toString}
             |bcast ${broadcast.toString}
             """.stripMargin)

        val reader = stream.newReader()
        reader match {
          case r: SupportsJoinPushdown =>

            val (pushedFilters, postScanFilters) = pushFilterExpressions(reader, streamFilters)
            val outputExprs = pruneExpressions(reader, stream, streamProject ++ postScanFilters)


            val joinConds = (splitConjunctivePredicates(cond.orNull) ++
                             splitConjunctivePredicates(push.orNull)).filter(_ != null)

            logDebug(s"joinPushdown... ${joinConds.mkString(", ")}")

            if (r.joinPushdown( joinConds.toArray, broadcast.output.toArray)) {
              logDebug(
                s"""
                   |Join Pushdown.
                   |broadcast: ${broadcast.toString}
                   |Pushed Filters: ${pushedFilters.mkString(", ")}
                   |Post-Scan Filters: ${postScanFilters.mkString(",")}
                   |Output: ${outputExprs.mkString(", ")}
                   """.stripMargin)

              val scan = DataSourceV2ScanExec(outputExprs,
                stream.source, stream.options, pushedFilters, reader)

              val join = if (leftBroadcast) {
                BroadcastNestedLoopPushdownJoinExec(planLater(broadcast), scan,
                  BuildLeft, joinType, cond)
              }
              else {
                BroadcastNestedLoopPushdownJoinExec(scan, planLater(broadcast),
                  BuildRight, joinType, cond)
              }

              val filterCondition = postScanFilters.reduceLeftOption(And)
              val withFilter = filterCondition.map(FilterExec(_, join)).getOrElse(join)

              logDebug(s"nlje: ${join.toString}")

              withFilter :: Nil
            }
            else {
              Nil
            }

          case _ => Nil
        }
      }



    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
      case SingleDataSourceV2Subtree(output,
                                     join,
                                     resolved,
                                     filters,
                                     relation: DataSourceV2Relation) =>

        logDebug(s"""
              | SingleDataSourceV2Sub:
              | handling: ${plan.treeString}
              | output: ${output.mkString(", ")}
              | join: $join
              | resolved: $resolved
              | filters: ${filters.mkString(", ")}
              """.stripMargin)

        val reader = relation.newReader()

        /*
         * First filter expressions and outputs like nornal:
         */

        // This is actually quite odd if I have a Projection -> Join

        val (pushedFilters, postScanFilters) = pushFilterExpressions(reader, filters)
        val outputExprs = pruneExpressions(reader, relation, output ++ postScanFilters)

        // Now, use the Join. If we cannot push it, we give up
        reader match {
          case r: SupportsNestedLoopJoin =>
            // First, actually resolve the resolved InMemoryRelation.

            val relOutput = resolved.output
            val inMemScan = InMemoryTableScanExec(resolved.output, Seq.empty, resolved )
            val resolvedRows = inMemScan.executeCollect()
            val seq =
              if (join.condition.isDefined) splitConjunctivePredicates(join.condition.get) else Nil

            if (r.nestedLoopJoin(seq.toArray,
                                 relOutput.toArray,
                                 resolvedRows)) {
              logDebug(
                s"""
                   |Join Path.
                   |Relation: ${relation.toString}
                   |Pushed Filters: ${pushedFilters.mkString(", ")}
                   |Post-Scan Filters: ${postScanFilters.mkString(",")}
                   |Output: ${outputExprs.mkString(", ")}
                """.stripMargin)

              val scan = DataSourceV2ScanExec(outputExprs,
                relation.source,
                relation.options,
                pushedFilters,
                reader)

              val filterCondition = postScanFilters.reduceLeftOption(And)
              val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
              val project = ProjectExec(output, withFilter)
              logDebug(
                s"""withFilter output ${withFilter.output.mkString(", ")}
                   |scan output ${scan.output.mkString(", ")}
                   |project output ${project.output.mkString(", ")}
                   |""".stripMargin)

              project :: Nil
            }
            else {
              Nil
            }
          case _ => Nil
        }

      case DatasourceV2Pushable(projectList, child, matchable) =>
        child match {
          case j@Join(PhysicalOperation(leftProj, leftFils, left),
                      PhysicalOperation(rightProj, rightFils, right),
                      join: InnerLike, cond) =>

            // The planner needs to help me out making these situations happen more frequently
            if (left.isInstanceOf[DataSourceV2Relation] ||
                right.isInstanceOf[DataSourceV2Relation]) {

              logDebug(
                s"""Found a d2pushable:
                   | ${projectList.mkString(", ")}
                   |join:  ${j.toString}
                   |left rel: ${left.toString}
                   |right rel: ${right.toString()}
                   |""".stripMargin)

              // decide which of these we want to stream vs. broadcast
              val (stream, streamFs, streamP, bcast, leftCast) = right match {
                case d: DataSourceV2Relation if d.source.getClass.getSimpleName == matchable =>
                  (Some(d), rightFils, rightProj ++ projectList, Some(j.left), false)
                case _ =>
                  val d2l = left.asInstanceOf[DataSourceV2Relation]
                  if (d2l.source.getClass.getSimpleName == matchable) {
                    (Some(d2l), leftFils, leftProj ++ projectList, Some(j.right), true)
                  }
                  else {
                    (None, Nil, Nil, None, false)
                  }
              }

              if (stream.isDefined) {
                getPlanForJoin(stream.get, streamFs, streamP, bcast.get, join, cond, None, leftCast)
              } else {
                Nil
              }
            }
            else {
              Nil
            }
          case j@JoinWithPushable(PhysicalOperation(leftProj, leftFils, left),
                                  PhysicalOperation(rightProj, rightFils, right),
                                  join: InnerLike, cond, push) =>
            // The planner needs to help me out making these situations happen more frequently
            if (left.isInstanceOf[DataSourceV2Relation] ||
                right.isInstanceOf[DataSourceV2Relation]) {

              // decide which of these we want to stream vs. broadcast
              val (stream, streamFs, streamP, bcast, leftCast) = right match {
                case d: DataSourceV2Relation if d.source.getClass.getSimpleName == matchable =>
                  (Some(d), rightFils, rightProj ++ projectList, Some(j.left), false)
                case _ =>
                  val d2l = left.asInstanceOf[DataSourceV2Relation]
                  if (d2l.source.getClass.getSimpleName == matchable) {
                    (Some(d2l), leftFils, leftProj ++ projectList, Some(j.right), true)
                  }
                  else {
                    (None, Nil, Nil, None, false)
                  }
              }

              logDebug(
                s"""Found a d2 joinWithPushable:
                   | ${j.toString}
                   |left rel: ${left.toString}
                   |right rel: ${right.toString()}""".stripMargin)

              if (stream.isDefined) {
                getPlanForJoin(stream.get, streamFs, streamP, bcast.get, join, cond, push, leftCast)
              } else {
                Nil
              }
            }
            else {
              Nil
            }
          case _ => Nil
        }

      case j@Join(PhysicalOperation(leftProj, leftFils, leftRel),
                  PhysicalOperation(rightProj, rightFils, rightRel),
                  joinType: InnerLike, cond) =>

        // The planner needs to help me out making these situations happen more frequently
        if (leftRel.isInstanceOf[DataSourceV2Relation] ||
          rightRel.isInstanceOf[DataSourceV2Relation]) {

          // decide which of these we want to stream vs. broadcast
          val (stream, streamFils, streamProj, bcast, leftCast) = rightRel match {
            case d: DataSourceV2Relation =>
              (d, rightFils, rightProj, j.left, false)
            case _ =>
              (leftRel.asInstanceOf[DataSourceV2Relation], leftFils, leftProj, j.right, true)
          }

          logDebug(
                s"""Found a d2 join: ${j.toString}
               |left rel: ${leftRel.toString}
               |right rel: ${rightRel.toString()}
               |""".stripMargin)

          getPlanForJoin(stream, streamFils, streamProj, bcast, joinType, cond, None, leftCast)
        }
        else {
          Nil
        }

      case j@JoinWithPushable(PhysicalOperation(leftProj, leftFils, leftRel),
                              PhysicalOperation(rightProj, rightFils, rightRel),
                              joinType: InnerLike, cond, push) =>


        // The planner needs to help me out making these situations happen more frequently
        if (leftRel.isInstanceOf[DataSourceV2Relation] ||
          rightRel.isInstanceOf[DataSourceV2Relation]) {

          // decide which of these we want to stream vs. broadcast
          val (stream, streamFils, streamProj, bcast, leftCast) = rightRel match {
            case d: DataSourceV2Relation =>
              (d, rightFils, rightProj, j.left, false)
            case _ =>
              (leftRel.asInstanceOf[DataSourceV2Relation], leftFils, leftProj, j.right, true)
          }

          logDebug(
            s"""Found a d2 joinWithPushable: ${j.toString}
               |left rel: ${leftRel.toString}
               |right rel: ${rightRel.toString()}
               |""".stripMargin)

          getPlanForJoin(stream, streamFils, streamProj, bcast, joinType, cond, push, leftCast)
        }
        else {
          Nil
        }


      case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>

        val reader = relation.newReader()
        // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
        // `postScanFilters` need to be evaluated after the scaQueryPlanner.
        // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
        var (pushedFilters, postScanFilters) = pushFilters(reader, filters)
        var output = pruneColumns(reader, relation, project ++ postScanFilters)

        /*
         * New code for datasources that require additional info about the expressions that are
         * needed
         */

        val (exprPushedFilters, exprPostScanFilters) = pushFilterExpressions(reader, filters)
        if (exprPushedFilters.nonEmpty) {
          pushedFilters = exprPushedFilters
          postScanFilters = exprPostScanFilters
        }

        val exprOutput = pruneExpressions(reader, relation, project ++ postScanFilters)

        if (exprOutput.nonEmpty) {
          output = exprOutput
        }

        logDebug(
          s"""
             |Relation: ${relation.toString}
             |Pushed Filters: ${pushedFilters.mkString(", ")}
             |Post-Scan Filters: ${postScanFilters.mkString(",")}
             |Output: ${output.mkString(", ")}
           """.stripMargin)

        val scan = DataSourceV2ScanExec(
          output, relation.source, relation.options, pushedFilters, reader)

        val filterCondition = postScanFilters.reduceLeftOption(And)
        val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

        // always add the projection, which will produce unsafe rows required by some operators
        ProjectExec(project, withFilter) :: Nil

      case r: StreamingDataSourceV2Relation =>
        // ensure there is a projection, which will produce unsafe rows required by some operators
        ProjectExec(r.output,
          DataSourceV2ScanExec(r.output, r.source, r.options, r.pushedFilters, r.reader)) :: Nil

      case WriteToDataSourceV2(writer, query) =>
        WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

      case AppendData(r: DataSourceV2Relation, query, _) =>
        WriteToDataSourceV2Exec(r.newWriter(), planLater(query)) :: Nil

      case WriteToContinuousDataSource(writer, query) =>
        WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

      case Repartition(1, false, child) =>
        val isContinuous = child.collectFirst {
          case StreamingDataSourceV2Relation(_, _, _, r: ContinuousReader) => r
        }.isDefined

        if (isContinuous) {
          ContinuousCoalesceExec(1, planLater(child)) :: Nil
        } else {
          Nil
        }

      case _ => Nil
    }
  }
}
