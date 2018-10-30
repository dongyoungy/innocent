package dyoon.innocent.query;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashMap;
import java.util.Map;

/** Created by Dong Young Yoon on 10/29/18. */
public class AliasMapBuilder extends SqlShuttle {

  private Map<SqlIdentifier, SqlIdentifier> aliasMap;

  public AliasMapBuilder() {
    this.aliasMap = new HashMap<>();
  }

  public Map<SqlIdentifier, SqlIdentifier> getAliasMap() {
    return aliasMap;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      // check 'A as B' and update alias map.
      if (op instanceof SqlAsOperator) {
        SqlNode o1 = bc.operands[0];
        SqlNode o2 = bc.operands[1];
        if (o1 instanceof SqlIdentifier && o2 instanceof SqlIdentifier) {
          SqlIdentifier id1 = (SqlIdentifier) o1;
          SqlIdentifier id2 = (SqlIdentifier) o2;
          aliasMap.put(id1, id2);
        }
      }
    }
    return super.visit(call);
  }
}
