package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * A mix-in interface for {@link DataSourceReader}. Data source readers can implement this
 * interface to push down expressions to the data source and only read these columns during
 * scan to reduce the size of the data to be read.
 */
@InterfaceStability.Evolving
public interface SupportsPushDownFilterExpressions extends DataSourceReader{

    /**
     * Push down the expressions required by a query.
     *
     * Implementation should try its best to prune the unnecessary columns or nested fields, but it's
     * also OK to do the pruning partially, e.g., a data source may not be able to prune nested
     * fields, and only prune top-level columns.
     *
     * Note that, data source readers should update {@link DataSourceReader#readSchema()} after
     * applying column pruning.
     */
    Expression[] pushFilterExpressions(Expression[] expressions);
    Expression[] pushedFilterExpressions();
}
