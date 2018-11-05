package dyoon.innocent.query;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.util.SqlShuttle;

/** Created by Dong Young Yoon on 11/4/18. */
public class AggTypeDetector extends SqlShuttle {
  boolean firstAgg;
  SqlOperator aggOp;

  public AggTypeDetector() {
    firstAgg = true;
    aggOp = null;
  }

  public SqlOperator getAggType() {
    return aggOp;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlSumAggFunction
          || op instanceof SqlAvgAggFunction
          || op instanceof SqlCountAggFunction) {
        if (firstAgg) {
          this.aggOp = op;
          this.firstAgg = false;
        }
      }
    }
    return super.visit(call);
  }
}
