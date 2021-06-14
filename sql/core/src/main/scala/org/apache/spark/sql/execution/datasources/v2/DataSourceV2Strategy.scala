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

  import org.apache.spark.sql.catalyst.expressions.{And, NamedExpression}
  import org.apache.spark.sql.catalyst.planning.PhysicalOperation
  import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, Repartition}
  import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
  import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
  import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader




  object DataSourceV2Strategy extends DataSourceV2StrategyBase {

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
      case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>

        val reader = relation.newReader()
        // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
        // `postScanFilters` need to be evaluated after the scaQueryPlanner.
        // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
        var (pushedFilters, postScanFilters) = pushFilters(reader, filters)


        /*
         * New code for datasources that require additional info about the expressions that are
         * needed
         */

        val (exprPushedFilters, exprPostScanFilters) = pushFilterExpressions(reader, filters)
        if (exprPushedFilters.nonEmpty) {
          pushedFilters = exprPushedFilters
          postScanFilters = exprPostScanFilters
        }

        val output = pruneExpressions(reader, relation, project ++ postScanFilters)
        val adjustedProject = project.map(substitute(_, output).asInstanceOf[NamedExpression])
        val adjustedPostScanFilters = postScanFilters.map(substitute(_, output))

        logDebug(
          s"""
             |Relation: ${relation.toString}
             |Pushed Filters: ${pushedFilters.mkString(", ")}
             |Post-Scan Filters: ${adjustedPostScanFilters.mkString(",")}
             |Output: ${output.mkString(", ")}
           """.stripMargin)

        val scan = DataSourceV2ScanExec(output.map(_.toAttribute),
                                        relation.source,
                                        relation.options,
                                        pushedFilters,
                                        reader)

        val filterCondition = adjustedPostScanFilters.reduceLeftOption(And)
        val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

        // always add the projection, which will produce unsafe rows required by some operators
        ProjectExec(adjustedProject, withFilter) :: Nil

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
