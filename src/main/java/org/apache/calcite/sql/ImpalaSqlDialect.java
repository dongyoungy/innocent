package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.HiveSqlDialect;

/** Created by Dong Young Yoon on 2019-02-02. */
public class ImpalaSqlDialect extends HiveSqlDialect {
  public ImpalaSqlDialect(Context context) {
    super(context);
  }

  @Override
  public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
    super.unparseFetchUsingLimit(writer, offset, fetch);
  }
}
