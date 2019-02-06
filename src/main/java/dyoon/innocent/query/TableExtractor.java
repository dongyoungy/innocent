package dyoon.innocent.query;

import dyoon.innocent.Utils;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashSet;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-02-01. */
public class TableExtractor extends SqlShuttle {

  private Set<Table> allTables;
  private Set<AliasedTable> aliasedTables;
  private Set<UnorderedPair<SqlIdentifier>> joinKeySet;

  public TableExtractor() {
    this.allTables = new HashSet<>();
    this.aliasedTables = new HashSet<>();
    this.joinKeySet = new HashSet<>();
  }

  public TableExtractor(Set<Table> allTables) {
    this.allTables = allTables;
    this.aliasedTables = new HashSet<>();
    this.joinKeySet = new HashSet<>();
  }

  public Set<AliasedTable> getTables() {
    return aliasedTables;
  }

  public Set<UnorderedPair<SqlIdentifier>> getJoinKeySet() {
    return joinKeySet;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    Table t = Utils.findTableByName(allTables, id.names.get(id.names.size() - 1));
    if (t != null) {
      t.setCorrespondingNode(id);
      aliasedTables.add(new AliasedTable(t));
    }
    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        if (bc.operands[0] instanceof SqlIdentifier && bc.operands[1] instanceof SqlIdentifier) {
          SqlIdentifier tableNameId = (SqlIdentifier) bc.operands[0];
          SqlIdentifier aliasId = (SqlIdentifier) bc.operands[1];
          Table t = Utils.findTableByName(allTables, Utils.getLastName(tableNameId));
          if (t != null) {
            AliasedTable aliasedTable = new AliasedTable(t, Utils.getLastName(aliasId));
            aliasedTables.add(aliasedTable);
          }
        }
      } else if (op instanceof SqlBinaryOperator) {
        String opName = op.getName();
        if (opName.equals("=")) {
          if (bc.operands[0] instanceof SqlIdentifier && bc.operands[1] instanceof SqlIdentifier) {
            UnorderedPair<SqlIdentifier> newKeyPair =
                new UnorderedPair<>((SqlIdentifier) bc.operands[0], (SqlIdentifier) bc.operands[1]);
            joinKeySet.add(newKeyPair);
          }
        }
      } else {
        return super.visit(call);
      }
    } else {
      return super.visit(call);
    }
    return super.visit(call);
  }
}
