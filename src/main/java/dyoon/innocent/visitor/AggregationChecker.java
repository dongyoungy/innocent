package dyoon.innocent.visitor;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;

/** Created by Dong Young Yoon on 10/29/18. */
public class AggregationChecker extends SqlShuttle {

  private boolean hasAgg;

  public AggregationChecker() {
    hasAgg = false;
  }

  public boolean hasAgg() {
    return hasAgg;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      String opString = op.getName().toLowerCase();
      if (opString.equals("sum") || opString.equals("avg")) {
        hasAgg = true;
      }
    }
    return super.visit(call);
  }
}
