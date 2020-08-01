package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;


/**
 * A mix-in interface for {@link DataSourceReader}. Data source readers can implement this
 * interface to claim support for Nested Join Operators
 *
 */
@InterfaceStability.Evolving
public interface SupportsNestedLoopJoin extends DataSourceReader {

    // Currently... No Function Implemented.

    Boolean nestedLoopJoin(
            Expression[] JoinExpression,
            Attribute[]  resolvedOutput,
            InternalRow[] resolvedData);
}
