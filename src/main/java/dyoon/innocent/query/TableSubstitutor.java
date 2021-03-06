package dyoon.innocent.query;

import dyoon.innocent.StratifiedSample;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 10/29/18. */
public class TableSubstitutor extends SqlShuttle {

  private int numTableSubstitutions;

  private Set<SqlIdentifier> tableSet;
  private Map<String, StratifiedSample> tableToSampleMap;
  private Map<SqlIdentifier, SqlIdentifier> aliasMap;
  private String sampleDatabase;

  public TableSubstitutor(String sampleDatabase) {
    this.sampleDatabase = sampleDatabase;
    this.numTableSubstitutions = 0;
    this.tableToSampleMap = new HashMap<>();
    this.tableSet = new HashSet<>();
  }

  public int getNumTableSubstitutions() {
    return numTableSubstitutions;
  }

  public void addTableToSample(String table, StratifiedSample stratifiedSample) {
    tableToSampleMap.put(table, stratifiedSample);
  }

  private SqlNode addTable(SqlIdentifier id) {
    SqlNode newId = this.substituteTable(id);
    tableSet.add(id);
    return newId;
  }

  private SqlNode substituteTable(SqlIdentifier id) {
    for (Map.Entry<String, StratifiedSample> entry : this.tableToSampleMap.entrySet()) {
      for (int i = 0; i < id.names.size(); ++i) {
        if (id.names.get(i).equalsIgnoreCase(entry.getKey())) {
          ++numTableSubstitutions;
          SqlIdentifier newId =
              new SqlIdentifier(
                  Arrays.asList(sampleDatabase, entry.getValue().getSampleTableName()),
                  SqlParserPos.ZERO);
          return new SqlBasicCall(
              SqlStdOperatorTable.AS,
              new SqlNode[] {
                newId,
                //                id.setName(i, entry.getValue().getSampleTableName()),
                this.getSampleAlias(entry.getValue())
              },
              SqlParserPos.ZERO);
        }
      }
    }
    return null;
  }

  public SqlIdentifier getSampleAlias(StratifiedSample s) {
    for (Map.Entry<SqlIdentifier, SqlIdentifier> entry : aliasMap.entrySet()) {
      SqlIdentifier key = entry.getKey();
      // This check needs to be revised later.
      if (key.toString().toLowerCase().contains(s.getTable().getName().toLowerCase())) {
        return entry.getValue();
      }
    }
    return new SqlIdentifier(s.getTable().getName(), SqlParserPos.ZERO);
  }

  public SqlSelect substitute(SqlSelect call) {
    AliasMapBuilder builder = new AliasMapBuilder();
    builder.visit(call);
    this.aliasMap = builder.getAliasMap();
    return (SqlSelect) this.visit(call);
  }

  @Override
  public SqlNode visit(SqlCall call) {

    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;

      SqlNode from = select.getFrom();
      if (from instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) from;
        SqlNode newId = this.addTable(id);
        if (newId != null) select.setFrom(newId);
      }
    } else if (call instanceof SqlJoin) {
      SqlJoin j = (SqlJoin) call;
      if (j.getLeft() instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) j.getLeft();
        SqlNode newId = this.addTable(id);
        if (newId != null) j.setLeft(newId);
      } else if (j.getLeft() instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) j.getLeft();
        if (bc.getOperator() instanceof SqlAsOperator && bc.operands[0] instanceof SqlIdentifier) {
          SqlNode newId = this.addTable((SqlIdentifier) bc.operands[0]);
          if (newId != null) bc.setOperand(0, newId);
        }
      }
      if (j.getRight() instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) j.getRight();
        SqlNode newId = this.addTable(id);
        if (newId != null) j.setRight(newId);
      } else if (j.getRight() instanceof SqlBasicCall) {
        SqlBasicCall bc = (SqlBasicCall) j.getRight();
        if (bc.getOperator() instanceof SqlAsOperator && bc.operands[0] instanceof SqlIdentifier) {
          this.visit(bc);
          SqlNode newId = this.addTable((SqlIdentifier) bc.operands[0]);
          if (newId != null) j.setRight(newId);
        }
      }
    }

    return super.visit(call);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (id.names.size() == 1) {
      String name = id.names.get(0);
      for (StratifiedSample stratifiedSample : tableToSampleMap.values()) {
        for (String sampleColumn : stratifiedSample.getColumnSet()) {
          if (sampleColumn.equalsIgnoreCase(name)) {
            List<String> newNames = new ArrayList<>();
            newNames.add(this.getSampleAlias(stratifiedSample).toString());
            newNames.add(name);
            id.setNames(newNames, new ArrayList<>());
          }
        }
      }
    }
    return super.visit(id);
  }
}
