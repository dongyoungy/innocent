package dyoon.innocent.visitor;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Created by Dong Young Yoon on 10/29/18.
 */
public class AggregationRetriever extends SqlShuttle {




  @Override
  public SqlNode visit(SqlCall call) {
    return super.visit(call);
  }
}
