package dyoon.innocent.query;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/** Created by Dong Young Yoon on 11/18/18. */
public class TreeCloner extends SqlShuttle {
  @Override
  public SqlNode visit(SqlLiteral literal) {
    literal = literal.clone(SqlParserPos.ZERO);
    return super.visit(literal).clone(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    id = (SqlIdentifier) id.clone(SqlParserPos.ZERO);
    return id;
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    type = (SqlDataTypeSpec) type.clone(SqlParserPos.ZERO);
    return super.visit(type).clone(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlDynamicParam param) {
    param = (SqlDynamicParam) param.clone(SqlParserPos.ZERO);
    return super.visit(param).clone(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
    intervalQualifier = (SqlIntervalQualifier) intervalQualifier.clone(SqlParserPos.ZERO);
    return super.visit(intervalQualifier).clone(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call != null) call = (SqlCall) call.clone(SqlParserPos.ZERO);
    if (call instanceof SqlBasicCall) {
      SqlBasicCall orig = (SqlBasicCall) call;
      //      SqlBasicCall bc = (SqlBasicCall) call.clone(SqlParserPos.ZERO);
      SqlBasicCall bc =
          new SqlBasicCall(
              orig.getOperator(), new SqlNode[orig.operands.length], SqlParserPos.ZERO);
      for (int i = 0; i < orig.operands.length; ++i) {
        bc.setOperand(i, orig.operands[i].accept(this));
      }
      return bc;
    }
    return (call == null) ? null : super.visit(call).clone(SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlNodeList nodeList) {
    nodeList = (SqlNodeList) nodeList.clone(SqlParserPos.ZERO);
    return super.visit(nodeList).clone(SqlParserPos.ZERO);
  }
}
