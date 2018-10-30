package dyoon.innocent.query;

import dyoon.innocent.AQPInfo;
import dyoon.innocent.ColumnType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dong Young Yoon on 10/30/18.
 *
 * <p>Detects agg and non-agg columns in the top-most select list.
 */
public class AggNonAggDetector extends SqlShuttle {

  private boolean isFirstSelect;
  private AQPInfo aqpInfo;

  private List<ColumnType> columnTypeList;

  public AggNonAggDetector(AQPInfo info) {
    this.isFirstSelect = true;
    this.aqpInfo = info;
    this.columnTypeList = new ArrayList<>();
  }

  public List<ColumnType> getColumnTypeList() {
    return columnTypeList;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      if (isFirstSelect) {
        SqlSelect select = (SqlSelect) call;

        for (int i = 0; i < select.getSelectList().size(); ++i) {
          AggDetector detector = new AggDetector(aqpInfo);
          SqlNode item = select.getSelectList().get(i);
          item.accept(detector);
          columnTypeList.add((detector.getType()));
        }
        isFirstSelect = false;
      }
    }
    return super.visit(call);
  }

  private class AggDetector extends SqlShuttle {

    private AQPInfo info;
    private ColumnType type;

    private AggDetector(AQPInfo info) {
      this.info = info;
      this.type = ColumnType.NON_AGG;
    }

    public ColumnType getType() {
      return type;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) call;
        SqlOperator op = bc.getOperator();
        if (op instanceof SqlAsOperator) {
          SqlIdentifier alias = (SqlIdentifier) bc.operands[1];
          if (alias.toString().toLowerCase().endsWith("_error")) {
            type = ColumnType.ERROR;
          }
        }
      }
      for (Pair<Integer, List<SqlNode>> pair : info.getExpressionList()) {
        for (SqlNode expr : pair.getRight()) {
          if (expr.equalsDeep(call, Litmus.IGNORE)) {
            type = ColumnType.AGG;
          }
        }
      }
      return super.visit(call);
    }
  }
}
