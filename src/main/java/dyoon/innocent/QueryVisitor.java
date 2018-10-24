package dyoon.innocent;

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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 10/19/18. */
public class QueryVisitor extends SqlShuttle {

  private Map<SqlIdentifier, SqlNode> withQueryMap;
  private Set<SqlIdentifier> queryColumnSet;
  private Set<SqlIdentifier> tableSet;
  private Set<SqlIdentifier> groupBySet;
  private Set<Pair<SqlIdentifier, SqlIdentifier>> joinColumnSet;

  private int qcsDepth;
  private int selectDepth;
  private int withDepth;

  public QueryVisitor() {
    this.withQueryMap = new HashMap<>();
    this.queryColumnSet = new HashSet<>();
    this.tableSet = new HashSet<>();
    this.joinColumnSet = new HashSet<>();
    this.groupBySet = new HashSet<>();
    this.qcsDepth = 0;
    this.selectDepth = 0;
    this.withDepth = 0;
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
      SqlWithItem item = (SqlWithItem) call;
      withQueryMap.put(item.name, item.query);
      isWithItem = true;
      ++withDepth;
    } else if (call instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) call;
    } else if (call instanceof SqlSelect) {
      isSelect = true;
      ++selectDepth;
      SqlSelect select = (SqlSelect) call;
      if (selectDepth == 1 && select.getGroup() != null) {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        final FrameworkConfig config = configBuilder.build();
        final RelBuilder builder = RelBuilder.create(config);
        //        RelNode r =
        //            builder
        //                .project(builder.field(String.valueOf(builder.count(false, "groupsize"))))
        //                .build();
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
        select.setFetch(null);
      }
      SqlNode from = select.getFrom();
      if (from instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) from;
        tableSet.add(id);
      } else {

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
    } else if (call instanceof SqlBasicCall) {
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
    } else if (call instanceof SqlJoin) {
      SqlJoin j = (SqlJoin) call;
      if (j.getLeft() instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) j.getLeft();
      }
      if (j.getRight() instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) j.getRight();
      }
    }
    SqlNode node = super.visit(call);
    if (isWithItem) --withDepth;
    if (isSelect) --selectDepth;
    return node;
  }
}
