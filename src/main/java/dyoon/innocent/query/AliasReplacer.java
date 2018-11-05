package dyoon.innocent.query;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by Dong Young Yoon on 10/25/18. */
public class AliasReplacer extends SqlShuttle {

  SqlIdentifier singleAliasToReplace;
  Map<SqlIdentifier, SqlIdentifier> aliasToReplace;
  boolean isForOuterQuery;
  boolean isAlias;

  public AliasReplacer(Map<SqlIdentifier, SqlIdentifier> aliasToReplace) {
    this.isForOuterQuery = false;
    this.aliasToReplace = new HashMap<>(aliasToReplace);
  }

  public AliasReplacer(boolean isForOuterQuery, SqlIdentifier alias) {
    this.isForOuterQuery = isForOuterQuery;
    this.singleAliasToReplace = alias;
    this.aliasToReplace = new HashMap<>();
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (this.isForOuterQuery) {
      if (id.names.size() > 1) {
        List<String> newIds = new ArrayList<>(id.names);
        newIds.set(newIds.size() - 2, singleAliasToReplace.toString());
        //        id.setNames(newIds, new ArrayList<SqlParserPos>());
        SqlIdentifier newId = new SqlIdentifier(newIds, SqlParserPos.ZERO);
        return newId;
      } else {
        List<String> newIds = new ArrayList<>();
        newIds.add(singleAliasToReplace.toString());
        newIds.add(id.names.get(0));
        //        id.setNames(newIds, new ArrayList<SqlParserPos>());
        SqlIdentifier newId = new SqlIdentifier(newIds, SqlParserPos.ZERO);
        return newId;
      }
    }
    //    if (id.names.size() > 1) {
    List<String> names = new ArrayList<>(id.names);
    String fullName = id.toString().toLowerCase();
    for (int i = 0; i < id.names.size(); ++i) {
      String idStr = id.names.get(i).toLowerCase();
      //      String idStr = id.toString().toLowerCase();
      for (Map.Entry<SqlIdentifier, SqlIdentifier> entry : aliasToReplace.entrySet()) {
        String from = entry.getKey().toString().toLowerCase();
        String to = entry.getValue().toString();
        if (fullName.equals(from)) {
          return new SqlIdentifier(to, SqlParserPos.ZERO);
        }
        if (idStr.equals(from)) {
          names.set(i, to);
        }
      }
      return new SqlIdentifier(names, SqlParserPos.ZERO);
    }

    //    }

    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        if (!(bc.operands[0] instanceof SqlLiteral)) return bc.operands[1].accept(this);
        else return bc;
        //        if (this.isForOuterQuery) {
        //          return bc.operands[1].accept(this);
        //        } else {
        //          return super.visit(call);
        //        }
      } else {
        return super.visit(call);
      }
    } else {
      return super.visit(call);
    }
  }
}
