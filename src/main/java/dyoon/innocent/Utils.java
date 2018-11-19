package dyoon.innocent;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

import java.util.List;

/** Created by Dong Young Yoon on 11/4/18. */
public class Utils {
  public static boolean containsIgnoreCase(String str, List<String> list) {
    for (String s : list) {
      if (s.equalsIgnoreCase(str)) return true;
    }
    return false;
  }

  public static boolean containsNode(SqlNode node, SqlNodeList list) {
    for (SqlNode n : list) {
      if (n.equalsDeep(node, Litmus.IGNORE)) {
        return true;
      }
    }
    return false;
  }

  public static SqlIdentifier getAliasIfExists(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        return (SqlIdentifier) bc.operands[1];
      }
    }
    return null;
  }

  public static SqlNode stripAliasIfExists(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        return bc.operands[0];
      }
    }
    return node;
  }

  public static SqlNode constructScaledAgg(SqlNode source, SqlIdentifier statTable) {
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

  public static SqlBasicCall alias(SqlNode source, SqlIdentifier alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, new SqlNode[] {source, alias}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall sum(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall avg(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.AVG, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall count(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.COUNT, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall sqrt(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  public static SqlBasicCall pow(SqlNode source, int e) {
    return new SqlBasicCall(
        SqlStdOperatorTable.POWER,
        new SqlNode[] {
          source, SqlLiteral.createExactNumeric(String.format("%d", e), SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
  }
}
