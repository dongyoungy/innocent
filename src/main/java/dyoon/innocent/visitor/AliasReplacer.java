package dyoon.innocent.visitor;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by Dong Young Yoon on 10/25/18. */
public class AliasReplacer extends SqlShuttle {

  SqlIdentifier singleAliasToReplace;
  Map<SqlIdentifier, SqlIdentifier> aliasToReplace;
  boolean isForOuterQuery;

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
        id.setNames(newIds, new ArrayList<SqlParserPos>());
      }
    }
    if (id.names.size() > 1) {
      String idStr = id.toString().toLowerCase();
      for (Map.Entry<SqlIdentifier, SqlIdentifier> entry : aliasToReplace.entrySet()) {
        String from = entry.getKey().toString().toLowerCase();
        String to = entry.getValue().toString().toLowerCase();
        if (idStr.contains(from)) {
          String replace = idStr.replace(from, to);
          id.setNames(Arrays.asList(replace.split("\\.")), new ArrayList<SqlParserPos>());
        }
      }
    }

    return super.visit(id);
  }
}
