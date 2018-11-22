package dyoon.innocent.query;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 11/20/18. */
public class SourceToErrorSubstitutor extends SqlShuttle {

  private BiMap<SqlIdentifier, SqlIdentifier> sourceToErrorMap;
  private Set<SqlNode> doNotModifySet;

  public SourceToErrorSubstitutor(Map<SqlIdentifier, SqlIdentifier> sourceToErrorMap) {
    this.sourceToErrorMap = HashBiMap.create();
    this.sourceToErrorMap.putAll(sourceToErrorMap);
  }

  public void setDoNotModifySet(Set<SqlNode> doNotModifySet) {
    this.doNotModifySet = doNotModifySet;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (!doNotModifySet.contains(id)) {
      SqlIdentifier key = Utils.matchLastName(sourceToErrorMap.keySet(), id);
      if (sourceToErrorMap.containsKey(key)) {
        return sourceToErrorMap.get(key);
      }
    }
    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (doNotModifySet.contains(call)) {
      return call;
    }
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlOverOperator) {
        bc.operands[0].accept(this);
        return bc;
      }
    }
    return super.visit(call);
  }
}
