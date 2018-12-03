package dyoon.innocent.query;

import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 10/29/18.
 *
 * <p>Simply checks whether a select list contains aggregation that uses sample table.
 */
public class AggregationChecker extends SqlShuttle {

  private boolean hasAgg;
  private List<String> sampleTablecolumns;

  public AggregationChecker(List<String> sampleTablecolumns) {
    this.sampleTablecolumns = new ArrayList<>(sampleTablecolumns);
    hasAgg = false;
  }

  public AggregationChecker() {
    this.sampleTablecolumns = null;
    this.hasAgg = false;
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
        if (bc.getOperandList().size() == 1) {
          SqlNode operand = bc.operands[0];
          if (operand instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier) operand;
            String col = id.names.get(id.names.size() - 1);
            if (sampleTablecolumns == null) {
              hasAgg = true;
            } else if (Utils.containsIgnoreCase(col, sampleTablecolumns)) {
              hasAgg = true;
            }
          }
        }
      } else if (opString.equals("count")) {
        hasAgg = true;
      }
    }
    return super.visit(call);
  }
}
