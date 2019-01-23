package dyoon.innocent.query;

import dyoon.innocent.StratifiedSample;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/19/18. */
public class QueryVisitor extends SqlShuttle {

  private Map<SqlIdentifier, SqlIdentifier> aliasMap;
  private Map<SqlIdentifier, SqlNode> withQueryMap;
  private Set<SqlIdentifier> queryColumnSet;
  private Set<SqlIdentifier> tableSet;
  private Set<SqlIdentifier> groupBySet;
  private Set<Pair<SqlIdentifier, SqlIdentifier>> joinColumnSet;

  private boolean useSample;

  private int qcsDepth;
  private int selectDepth;
  private int withDepth;
  private int numTableSubstitutions;

  private SortedSet<String> sampleGroupByList;
  private Map<String, StratifiedSample> tableToSampleMap;

  public QueryVisitor() {
    this.aliasMap = new HashMap<>();
    this.withQueryMap = new HashMap<>();
    this.queryColumnSet = new HashSet<>();
    this.tableSet = new HashSet<>();
    this.joinColumnSet = new HashSet<>();
    this.groupBySet = new HashSet<>();
    this.qcsDepth = 0;
    this.selectDepth = 0;
    this.withDepth = 0;
    this.numTableSubstitutions = 0;
    this.useSample = false;
    this.sampleGroupByList = new TreeSet<>();
    this.tableToSampleMap = new HashMap<>();
  }

  public void addTableToSample(String table, StratifiedSample stratifiedSample) {
    tableToSampleMap.put(table, stratifiedSample);
  }

  public Map<SqlIdentifier, SqlNode> getWithQueryMap() {
    return withQueryMap;
  }

  public Set<SqlIdentifier> getQueryColumnSet() {
    return queryColumnSet;
  }

  public Set<SqlIdentifier> getTableSet() {
    return tableSet;
  }

  public Set<Pair<SqlIdentifier, SqlIdentifier>> getJoinColumnSet() {
    return joinColumnSet;
  }

  public void addSampleGroupBy(String groupByColumn) {
    this.sampleGroupByList.add(groupByColumn);
  }

  public boolean isUseSample() {
    return useSample;
  }

  public void setUseSample(boolean useSample) {
    this.useSample = useSample;
  }

  public int getNumTableSubstitutions() {
    return numTableSubstitutions;
  }

  public void addSampleGroupBy(Collection<String> columnSet) {
    this.sampleGroupByList.addAll(columnSet);
  }

  private SqlNode addTable(SqlIdentifier id) {
    SqlNode newId = this.substituteTable(id);
    tableSet.add(id);
    //    if (aliasMap.containsKey(id)) {
    //      aliasMap.put(newId, aliasMap.get(id));
    //    }
    return newId;
  }

  public Map<SqlIdentifier, SqlIdentifier> getAliasMap() {
    return aliasMap;
  }

  private SqlNode substituteTable(SqlIdentifier id) {
    for (Map.Entry<String, StratifiedSample> entry : this.tableToSampleMap.entrySet()) {
      for (int i = 0; i < id.names.size(); ++i) {
        if (id.names.get(i).equalsIgnoreCase(entry.getKey())) {
          ++numTableSubstitutions;
          return new SqlBasicCall(
              SqlStdOperatorTable.AS,
              new SqlNode[] {
                id.setName(i, entry.getValue().getSampleTableName()),
                this.getSampleAlias(entry.getValue())
              },
              SqlParserPos.ZERO);
        }
      }
    }
    return id;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlNodeList nodeList) {
    return super.visit(nodeList);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    boolean isSelect = false;
    boolean isWithItem = false;
    if (call instanceof SqlWithItem) {
      isWithItem = true;
      ++withDepth;
      this.handleSqlWithItem(call);
    } else if (call instanceof SqlOrderBy) {
      this.handleSqlOrderBy(call);
    } else if (call instanceof SqlSelect) {
      isSelect = true;
      ++selectDepth;
      this.handleSqlSelect(call);
    } else if (call instanceof SqlBasicCall) {
      this.handleSqlBasicCall(call);
    } else if (call instanceof SqlJoin) {
      this.handleSqlJoin(call);
    }
    SqlNode node = super.visit(call);
    if (isWithItem) --withDepth;
    if (isSelect) --selectDepth;
    return node;
  }

  private void handleSqlWithItem(SqlCall call) {
    SqlWithItem item = (SqlWithItem) call;
    withQueryMap.put(item.name, item.query);
  }

  private void handleSqlOrderBy(SqlCall call) {
    SqlOrderBy orderBy = (SqlOrderBy) call;
  }

  private void handleSqlSelect(SqlCall call) {
    SqlSelect select = (SqlSelect) call;
    if (selectDepth == 1 && select.getGroup() != null) {
      SqlBasicCall groupSize =
          new SqlBasicCall(
              new SqlAsOperator(),
              new SqlNode[] {
                new SqlBasicCall(
                    new SqlCountAggFunction("COUNT"),
                    new SqlNode[] {new SqlIdentifier("", SqlParserPos.ZERO)},
                    SqlParserPos.ZERO),
                new SqlIdentifier("groupsize", SqlParserPos.ZERO)
              },
              SqlParserPos.ZERO);
      select.getSelectList().add(groupSize);
      for (StratifiedSample stratifiedSample : tableToSampleMap.values()) {
        for (String groupBy : stratifiedSample.getColumnSet()) {
          SqlIdentifier newGroupBy =
              new SqlIdentifier(
                  Arrays.asList(stratifiedSample.getTable().getName(), groupBy), SqlParserPos.ZERO);
          select.getSelectList().add(newGroupBy);
          select.getGroup().add(newGroupBy);
        }
      }
      select.setFetch(null);
    }
    SqlNode from = select.getFrom();
    if (from instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) from;
      SqlNode newId = this.addTable(id);
      select.setFrom(newId);
    }
    if (select.getGroup() != null) {
      ++qcsDepth;
      SqlNode[] nodes = select.getGroup().toArray();
      for (SqlNode n : nodes) {
        if (n instanceof SqlIdentifier) {
          SqlIdentifier id = (SqlIdentifier) n;
          groupBySet.add(id);
          queryColumnSet.add(id);
        }
      }
      --qcsDepth;
    }
    if (select.getWhere() != null) {
      ++qcsDepth;
      select.getWhere().accept(this);
      --qcsDepth;
    }
    if (select.getHaving() != null) {
      ++qcsDepth;
      select.getHaving().accept(this);
      --qcsDepth;
    }
  }

  private void handleSqlBasicCall(SqlCall call) {
    SqlBasicCall bc = (SqlBasicCall) call;
    SqlOperator op = bc.getOperator();
    if (op.getName().toLowerCase().equals("=")) {
      // if both operands are columns then it is inner join.
      SqlNode o1 = bc.operands[0];
      SqlNode o2 = bc.operands[1];
      if (o1 instanceof SqlIdentifier && o2 instanceof SqlIdentifier) {
        SqlIdentifier id1 = (SqlIdentifier) o1;
        SqlIdentifier id2 = (SqlIdentifier) o2;
        joinColumnSet.add(ImmutablePair.of(id1, id2));
      }
    }
    for (SqlNode node : bc.operands) {
      if (qcsDepth > 0) {
        if (node instanceof SqlIdentifier) {
          SqlIdentifier id = (SqlIdentifier) node;
          queryColumnSet.add(id);
        }
      }
    }

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

  private void handleSqlJoin(SqlCall call) {
    SqlJoin j = (SqlJoin) call;
    if (j.getLeft() instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) j.getLeft();
      SqlNode newId = this.addTable(id);
      j.setLeft(newId);
    } else if (j.getLeft() instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) j.getLeft();
      if (bc.getOperator() instanceof SqlAsOperator && bc.operands[0] instanceof SqlIdentifier) {
        SqlNode newId = this.addTable((SqlIdentifier) bc.operands[0]);
        bc.setOperand(0, newId);
      }
    }
    if (j.getRight() instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) j.getRight();
      SqlNode newId = this.addTable(id);
      j.setRight(newId);
    } else if (j.getRight() instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) j.getRight();
      if (bc.getOperator() instanceof SqlAsOperator && bc.operands[0] instanceof SqlIdentifier) {
        this.visit(bc);
        SqlNode newId = this.addTable((SqlIdentifier) bc.operands[0]);
        j.setRight(newId);
        //        bc.setOperand(0, newId);
      }
    }
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
}
