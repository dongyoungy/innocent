package dyoon.innocent.query;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Litmus;

/**
 * Created by Dong Young Yoon on 11/4/18.
 *
 * <p>Builds a scaled agg function for stratified sample
 */
public class ScaledAggBuilder extends SqlShuttle {

  private SqlNode scaledSource;
  private SqlIdentifier sourceColumn;
  private SqlIdentifier statTable;
  private SqlOperator aggOp;

  public ScaledAggBuilder(SqlIdentifier sourceColumn, SqlIdentifier statTable, SqlOperator aggOp) {
    this.sourceColumn = sourceColumn;
    this.statTable = statTable;
    this.aggOp = aggOp;
  }

  public SqlNode getScaledSource() {
    return scaledSource;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op.equals(aggOp)) {
        SqlNode source = bc.operands[0];
        if (source.equalsDeep(sourceColumn, Litmus.IGNORE)) {
          SqlNode scaled = constructScaledAgg(source, statTable);
          bc.setOperand(0, scaled);
          this.scaledSource = scaled;
          return bc;
        }
      }
    }
    return super.visit(call);
  }

  private SqlNode constructScaledAgg(SqlNode source, SqlIdentifier statTable) {
    SqlNode c = SqlLiteral.createExactNumeric("100000", SqlParserPos.ZERO);

    SqlBasicCall scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, new SqlNode[] {source, c}, SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            new SqlNode[] {scaled, statTable.plus("groupsize", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            new SqlNode[] {scaled, statTable.plus("actualsize", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(SqlStdOperatorTable.DIVIDE, new SqlNode[] {scaled, c}, SqlParserPos.ZERO);

    return scaled;
  }
}
