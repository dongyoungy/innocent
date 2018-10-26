package dyoon.innocent;

import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 10/19/18. */
public class Parser {

  private boolean isSampleUsed = false;

  public Parser() {
    this.isSampleUsed = false;
  }

  public boolean isSampleUsed() {
    return isSampleUsed;
  }

  public SqlNode parse(String sql, QueryVisitor visitor)
      throws ClassNotFoundException, SqlParseException {
    return this.parse(sql, visitor, null);
  }

  public SqlNode parse(String sql, QueryVisitor visitor, Sample s)
      throws SqlParseException, ClassNotFoundException {

    this.isSampleUsed = false;

    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode newQueryNode = sqlParser.parseQuery(); // will be used to return new query.
    sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();
    SqlNodeList gby = this.getGroupBy(node);
    SqlNodeList origGroupBy = (gby != null) ? this.cloneSqlNodeList(gby) : null;
    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);
    SqlSelect originalSelect = null;

    if (s != null) {
      visitor.setUseSample(true);
      visitor.addTableToSample(s.getTable(), s);
      visitor.addSampleGroupBy(s.getColumnSet());
    }

    originalSelect = this.getOuterMostSelect(node);

    if (originalSelect == null) {
      Logger.error("original select is null");
      return null;
    }

    SqlString sqlString = node.toSqlString(dialect);

    node.accept(visitor);

    SqlSelect modifiedSelect = this.getOuterMostSelect(node);

    // test query transformation for AQP
    if (s != null && visitor.getNumTableSubstitutions() > 0 && origGroupBy != null) {
      Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
      final FrameworkConfig config = configBuilder.build();
      final RelBuilder builder = RelBuilder.create(config);
      this.isSampleUsed = true;

      // add join with stat table
      SqlNode from = originalSelect.getFrom();
      String statTable = s.getSampleTableName() + "__stat";
      SqlIdentifier sampleAlias = visitor.getSampleAlias(s);
      if (sampleAlias == null) {
        sampleAlias = new SqlIdentifier(s.getTable(), SqlParserPos.ZERO);
      }
      SqlIdentifier statAlias = new SqlIdentifier("stat", SqlParserPos.ZERO);

      // change agg function in the select to include scaling
      SqlNodeList selectList = this.cloneSqlNodeList(modifiedSelect.getSelectList());
      SqlNodeList newSelectList = this.cloneSqlNodeList(originalSelect.getSelectList());

      int sumCount = 0, aggCount = 0;
      for (int i = 0; i < selectList.size(); ++i) {
        SqlNode item = selectList.get(i);
        // look for agg functions
        if (item instanceof SqlBasicCall) {
          SqlBasicCall bc = (SqlBasicCall) item;
          SqlOperator op = bc.getOperator();
          SqlIdentifier aggAlias = null;

          // check if there is alias
          if (op instanceof SqlAsOperator) {
            if (bc.operands[0] instanceof SqlBasicCall) {
              aggAlias = (SqlIdentifier) bc.operands[1];
              bc = (SqlBasicCall) bc.operands[0];
              op = bc.getOperator();
            } else {
              continue;
            }
          }

          if (op instanceof SqlSumAggFunction) {
            SqlNode c = SqlLiteral.createExactNumeric("100000", SqlParserPos.ZERO);
            SqlIdentifier sumAlias =
                new SqlIdentifier(String.format("sum%d", sumCount++), SqlParserPos.ZERO);
            SqlBasicCall newOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.MULTIPLY,
                    new SqlNode[] {bc.operands[0], c},
                    SqlParserPos.ZERO);
            newOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.DIVIDE,
                    new SqlNode[] {newOp, statAlias.plus("groupsize", SqlParserPos.ZERO)},
                    SqlParserPos.ZERO);
            newOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.MULTIPLY,
                    new SqlNode[] {newOp, statAlias.plus("actualsize", SqlParserPos.ZERO)},
                    SqlParserPos.ZERO);
            newOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.DIVIDE, new SqlNode[] {newOp, c}, SqlParserPos.ZERO);
            newOp =
                new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {newOp}, SqlParserPos.ZERO);
            newOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.AS, new SqlNode[] {newOp, sumAlias}, SqlParserPos.ZERO);

            SqlBasicCall newAggOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.SUM, new SqlNode[] {sumAlias}, SqlParserPos.ZERO);
            if (aggAlias != null) {
              newAggOp =
                  new SqlBasicCall(
                      SqlStdOperatorTable.AS,
                      new SqlNode[] {newAggOp, aggAlias},
                      SqlParserPos.ZERO);
            }
            for (int j = 0; j < originalSelect.getSelectList().size(); ++j) {
              SqlNode n = originalSelect.getSelectList().get(j);
              if (n.equalsDeep(item, Litmus.IGNORE)) {
                newSelectList.set(j, newAggOp);
              }
            }

            modifiedSelect.getSelectList().set(i, newOp);
          } else if (op instanceof SqlAvgAggFunction) {
            SqlIdentifier avgAlias =
                new SqlIdentifier(String.format("avg%d", sumCount++), SqlParserPos.ZERO);
            SqlBasicCall newOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.AS, new SqlNode[] {bc, avgAlias}, SqlParserPos.ZERO);
            SqlBasicCall newAggOp =
                new SqlBasicCall(
                    SqlStdOperatorTable.AVG, new SqlNode[] {avgAlias}, SqlParserPos.ZERO);
            if (aggAlias != null) {
              newAggOp =
                  new SqlBasicCall(
                      SqlStdOperatorTable.AS,
                      new SqlNode[] {newAggOp, aggAlias},
                      SqlParserPos.ZERO);
            }
            for (int j = 0; j < originalSelect.getSelectList().size(); ++j) {
              SqlNode n = originalSelect.getSelectList().get(j);
              if (n.equalsDeep(item, Litmus.IGNORE)) {
                newSelectList.set(j, newAggOp);
              }
            }
            modifiedSelect.getSelectList().set(i, newOp);
          }
        }
      }

      // construct join clause
      List<SqlBasicCall> joinOps = new ArrayList<>();
      for (String col : s.getColumnSet()) {
        SqlIdentifier col1 = sampleAlias.plus(col, SqlParserPos.ZERO);
        SqlIdentifier col2 = statAlias.plus(col, SqlParserPos.ZERO);
        SqlBasicCall op =
            new SqlBasicCall(
                SqlStdOperatorTable.EQUALS, new SqlNode[] {col1, col2}, SqlParserPos.ZERO);
        joinOps.add(op);
      }
      SqlBasicCall joinClause = null;
      if (joinOps.size() == 1) {
        joinClause = joinOps.get(0);
      } else if (joinOps.size() > 1) {
        joinClause =
            new SqlBasicCall(
                SqlStdOperatorTable.AND,
                new SqlNode[] {joinOps.get(0), joinOps.get(1)},
                SqlParserPos.ZERO);
        for (int i = 2; i < joinOps.size(); ++i) {
          joinClause =
              new SqlBasicCall(
                  SqlStdOperatorTable.AND,
                  new SqlNode[] {joinClause, joinOps.get(i)},
                  SqlParserPos.ZERO);
        }
      }

      SqlJoin newJoin =
          new SqlJoin(
              SqlParserPos.ZERO,
              from,
              SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
              JoinType.INNER.symbol(SqlParserPos.ZERO),
              new SqlBasicCall(
                  new SqlAsOperator(),
                  new SqlNode[] {new SqlIdentifier(statTable, SqlParserPos.ZERO), statAlias},
                  SqlParserPos.ZERO),
              JoinConditionType.ON.symbol(SqlParserPos.ZERO),
              joinClause);

      modifiedSelect.setFrom(newJoin);
      replaceSelectListForSampleAlias(modifiedSelect, visitor);

      // make it a subquery
      SqlBasicCall innerQuery =
          new SqlBasicCall(
              new SqlAsOperator(),
              new SqlNode[] {modifiedSelect, new SqlIdentifier("tmp", SqlParserPos.ZERO)},
              SqlParserPos.ZERO);
      //      SqlNodeList origGroupBy = originalSelect.getGroup();
      //      SqlNodeList newGroup = modifiedSelect.getGroup().clone(SqlParserPos.ZERO);
      //
      //      List<SqlNode> groupList = newGroup.getList();
      //      List<SqlNode> newGroupBy =
      //          groupList
      //              .stream()
      //              .filter(sqlNode -> IsInGroupBy(sqlNode, origGroupBy))
      //              .collect(Collectors.toList());

      this.replaceListForOuterQuery(newSelectList);
      SqlNodeList newGroupBy = this.cloneSqlNodeList(origGroupBy);
      this.replaceListForOuterQuery(newGroupBy);

      SqlSelect newSelectNode =
          new SqlSelect(
              SqlParserPos.ZERO,
              null,
              newSelectList,
              innerQuery,
              null,
              newGroupBy,
              null,
              null,
              null,
              null,
              null);

      SqlString newSql = newSelectNode.toSqlString(dialect);

      System.out.println(newSql.toString().toLowerCase());

      if (newQueryNode instanceof SqlWith) {
        SqlWith with = (SqlWith) newQueryNode;
        newQueryNode =
            new SqlWith(
                SqlParserPos.ZERO,
                new SqlNodeList(with.getOperandList(), SqlParserPos.ZERO),
                newSelectNode);
      } else if (newQueryNode instanceof SqlOrderBy) {
        SqlOrderBy orderBy = (SqlOrderBy) newQueryNode;

        // remove LIMIT
        //        newQueryNode =
        //            new SqlOrderBy(
        //                SqlParserPos.ZERO, newSelectNode, orderBy.orderList, orderBy.offset,
        // null);

        // remove order by for now.
        newQueryNode = newSelectNode;
      } else if (newQueryNode instanceof SqlSelect) {
        newQueryNode = newSelectNode;
      }
    }

    return newQueryNode;
  }

  private SqlSelect getOuterMostSelect(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      // gets rid of LIMIT
      SqlOrderBy orderBy = (SqlOrderBy) node;
      node =
          new SqlOrderBy(
              node.getParserPosition(),
              orderBy.query.clone(SqlParserPos.ZERO),
              orderBy.orderList,
              orderBy.offset,
              null);

      return this.getOuterMostSelect(orderBy.query);
    } else if (node instanceof SqlSelect) {
      return this.cloneSqlSelect((SqlSelect) node);
    } else if (node instanceof SqlWith) {
      SqlWith with = (SqlWith) node;
      return this.cloneSqlSelect((SqlSelect) with.body);
    }
    return null;
  }

  private void replaceSelectListForSampleAlias(SqlSelect modifiedSelect, QueryVisitor visitor) {
    AliasReplacer replacer = new AliasReplacer(visitor.getAliasMap());
    modifiedSelect.accept(replacer);
  }

  private void replaceListForOuterQuery(SqlNodeList selectList) {
    AliasReplacer replacer = new AliasReplacer(true);
    selectList.accept(replacer);
  }

  private SqlNodeList getGroupBy(SqlNode node) {
    if (node instanceof SqlWith) {
      SqlWith with = (SqlWith) node;
      SqlSelect select = (SqlSelect) with.body;
      return select.getGroup();
    } else if (node instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      return this.getGroupBy(orderBy.query);
      //      SqlSelect select = (SqlSelect) orderBy.query;
      //      return select.getGroup();
    } else if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;
      return select.getGroup();
    }
    return null;
  }

  private boolean IsInGroupBy(SqlNode node, SqlNodeList groupBy) {
    if (node instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) node;
      for (SqlNode groupByNode : groupBy.getList()) {
        if (groupByNode instanceof SqlIdentifier) {
          SqlIdentifier groupById = (SqlIdentifier) groupByNode;
          if (groupById.equals(id)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private SqlNodeList cloneSqlNodeList(SqlNodeList list) {
    SqlNodeList newList = new SqlNodeList(SqlParserPos.ZERO);
    for (SqlNode node : list) {
      newList.add((node != null) ? node.clone(SqlParserPos.ZERO) : null);
    }
    return newList;
  }

  private SqlSelect cloneSqlSelect(SqlSelect select) {
    SqlParserPos pos = SqlParserPos.ZERO;
    SqlNodeList selectList =
        (select.getSelectList() != null) ? this.cloneSqlNodeList(select.getSelectList()) : null;
    SqlNode from = (select.getFrom() != null) ? select.getFrom().clone(pos) : null;
    SqlNode where = (select.getWhere() != null) ? select.getWhere().clone(pos) : null;
    SqlNodeList groupBy =
        (select.getGroup() != null) ? this.cloneSqlNodeList(select.getGroup()) : null;
    SqlNode having = (select.getHaving() != null) ? select.getHaving().clone(pos) : null;
    SqlNodeList windowList =
        (select.getWindowList() != null) ? this.cloneSqlNodeList(select.getWindowList()) : null;
    SqlNodeList orderBy =
        (select.getOrderList() != null) ? this.cloneSqlNodeList(select.getOrderList()) : null;
    SqlNode offset = (select.getOffset() != null) ? select.getOffset().clone(pos) : null;
    SqlNode fetch = (select.getFetch() != null) ? select.getFetch().clone(pos) : null;

    return new SqlSelect(
        pos, null, selectList, from, where, groupBy, having, windowList, orderBy, offset, fetch);
  }
}
