package dyoon.innocent.query;

import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Created by Dong Young Yoon on 2018-11-27.
 *
 * <p>Adds a temporary alias to a select item that does not have an alias
 */
public class ExpressionNamer extends SqlShuttle {

  private static int aggCount = 0;
  private static int exprCount = 0;

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        return call;
      } else {
        // check whether the item is an aggregate
        AggregationChecker checker = new AggregationChecker();
        bc.accept(checker);

        // item is an aggregate without an alias
        if (checker.hasAgg()) {
          SqlIdentifier alias =
              new SqlIdentifier(String.format("agg%d", aggCount++), SqlParserPos.ZERO);
          return Utils.alias(bc, alias);
        } else {
          SqlIdentifier alias =
              new SqlIdentifier(String.format("expr%d", exprCount++), SqlParserPos.ZERO);
          return Utils.alias(bc, alias);
        }
      }
    }
    return super.visit(call);
  }
}
