package dyoon.innocent;

import dyoon.innocent.visitor.AliasReplacer;
import dyoon.innocent.visitor.QueryTransformer;
import dyoon.innocent.visitor.QueryVisitor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;

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
    sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    if (s != null) {
      QueryTransformer transformer = new QueryTransformer(s);
      SqlNode newNode = node.accept(transformer);
      System.out.println("");
      return newNode;
    }

    return node;

    //    if (s != null) {
    //      visitor.setUseSample(true);
    //      visitor.addTableToSample(s.getTable(), s);
    //      visitor.addSampleGroupBy(s.getColumnSet());
    //    }
    //
    //    originalSelect = this.getOuterMostSelect(node);
    //
    //    if (originalSelect == null) {
    //      Logger.error("original select is null");
    //      return null;
    //    }
    //
    //    SqlString sqlString = node.toSqlString(dialect);
    //
    //    node.accept(visitor);
    //
    //    SqlSelect modifiedSelect = this.getOuterMostSelect(node);
    //
    //    // test query transformation for AQP
    //    if (s != null && visitor.getNumTableSubstitutions() > 0 && origGroupBy != null) {
    //      Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    //      final FrameworkConfig config = configBuilder.build();
    //      final RelBuilder builder = RelBuilder.create(config);
    //      this.isSampleUsed = true;
    //
    //      // add join with stat table
    //      SqlNode from = originalSelect.getFrom();
    //      String statTable = s.getSampleTableName() + "__stat";
    //      SqlIdentifier sampleAlias = visitor.getSampleAlias(s);
    //      if (sampleAlias == null) {
    //        sampleAlias = new SqlIdentifier(s.getTable(), SqlParserPos.ZERO);
    //      }
    //      SqlIdentifier statAlias = new SqlIdentifier("stat", SqlParserPos.ZERO);
    //
    //      // change agg function in the select to include scaling
    //      SqlNodeList selectList = this.cloneSqlNodeList(modifiedSelect.getSelectList());
    //      SqlNodeList newSelectList = this.cloneSqlNodeList(originalSelect.getSelectList());
    //
    //      int sumCount = 0, aggCount = 0;
    //      for (int i = 0; i < selectList.size(); ++i) {
    //        SqlNode item = selectList.get(i);
    //        // look for agg functions
    //        if (item instanceof SqlBasicCall) {
    //          SqlBasicCall bc = (SqlBasicCall) item;
    //          SqlOperator op = bc.getOperator();
    //          SqlIdentifier aggAlias = null;
    //
    //          // check if there is alias
    //          if (op instanceof SqlAsOperator) {
    //            if (bc.operands[0] instanceof SqlBasicCall) {
    //              aggAlias = (SqlIdentifier) bc.operands[1];
    //              bc = (SqlBasicCall) bc.operands[0];
    //              op = bc.getOperator();
    //            } else {
    //              continue;
    //            }
    //          }
    //
    //          // x in agg(x)
    //          SqlNode aggSource = bc.operands[0];
    //
    //          if (op instanceof SqlSumAggFunction) {
    //            SqlNode c = SqlLiteral.createExactNumeric("100000", SqlParserPos.ZERO);
    //            SqlIdentifier sumAlias =
    //                new SqlIdentifier(String.format("sum%d", sumCount++), SqlParserPos.ZERO);
    //
    //            // scale the summation
    //            // x / groupsize * actualsize
    //            SqlBasicCall scaled =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.MULTIPLY,
    //                    new SqlNode[] {bc.operands[0], c},
    //                    SqlParserPos.ZERO);
    //            scaled =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.DIVIDE,
    //                    new SqlNode[] {scaled, statAlias.plus("groupsize", SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            scaled =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.MULTIPLY,
    //                    new SqlNode[] {scaled, statAlias.plus("actualsize", SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            scaled =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.DIVIDE, new SqlNode[] {scaled, c}, SqlParserPos.ZERO);
    //            // sum (x / groupsize * actualsize)
    //            SqlBasicCall newOp =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.SUM, new SqlNode[] {scaled}, SqlParserPos.ZERO);
    //            // sum (x / groupsize * actualsize) as sumAlias
    //            newOp =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS, new SqlNode[] {newOp, sumAlias},
    // SqlParserPos.ZERO);
    //
    //            SqlBasicCall newAggOp =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.SUM, new SqlNode[] {sumAlias}, SqlParserPos.ZERO);
    //            if (aggAlias != null) {
    //              newAggOp =
    //                  new SqlBasicCall(
    //                      SqlStdOperatorTable.AS,
    //                      new SqlNode[] {newAggOp, aggAlias},
    //                      SqlParserPos.ZERO);
    //            }
    //            for (int j = 0; j < originalSelect.getSelectList().size(); ++j) {
    //              SqlNode n = originalSelect.getSelectList().get(j);
    //              if (n.equalsDeep(item, Litmus.IGNORE)) {
    //                newSelectList.set(j, newAggOp);
    //              }
    //            }
    //
    //            // add window function for sample mean
    //            SqlNode sampleMean =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AVG,
    //                    new SqlNode[] {scaled.clone(SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            sampleMean =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AVG, new SqlNode[] {sampleMean}, SqlParserPos.ZERO);
    //            SqlNode sampleMeanWindow =
    //                new SqlWindow(
    //                    SqlParserPos.ZERO,
    //                    null,
    //                    null,
    //                    this.cloneSqlNodeList(origGroupBy),
    //                    new SqlNodeList(SqlParserPos.ZERO),
    //                    SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
    //                    null,
    //                    null,
    //                    SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
    //            sampleMean =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.OVER,
    //                    new SqlNode[] {sampleMean, sampleMeanWindow},
    //                    SqlParserPos.ZERO);
    //            SqlIdentifier sampleMeanAlias =
    //                new SqlIdentifier(sumAlias.toString() + "_samplemean", SqlParserPos.ZERO);
    //            sampleMean =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS,
    //                    new SqlNode[] {sampleMean, sampleMeanAlias},
    //                    SqlParserPos.ZERO);
    //
    //            // add group mean
    //            SqlNode mean =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AVG,
    //                    new SqlNode[] {scaled.clone(SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            SqlIdentifier meanAlias =
    //                new SqlIdentifier(sumAlias.toString() + "_groupmean", SqlParserPos.ZERO);
    //            mean =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS, new SqlNode[] {mean, meanAlias},
    // SqlParserPos.ZERO);
    //
    //            // add variance
    //            SqlNode variance =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.VAR_SAMP,
    //                    new SqlNode[] {scaled.clone(SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            SqlIdentifier varianceAlias =
    //                new SqlIdentifier(sumAlias.toString() + "_var", SqlParserPos.ZERO);
    //            variance =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS,
    //                    new SqlNode[] {variance, varianceAlias},
    //                    SqlParserPos.ZERO);
    //
    //            // add count
    //            SqlNode count =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.COUNT,
    //                    new SqlNode[] {new SqlIdentifier("", SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            // count - 1
    //            count =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.MINUS,
    //                    new SqlNode[] {count, SqlLiteral.createExactNumeric("1",
    // SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            SqlIdentifier countAlias =
    //                new SqlIdentifier(sumAlias.toString() + "_count", SqlParserPos.ZERO);
    //            count =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS, new SqlNode[] {count, countAlias},
    // SqlParserPos.ZERO);
    //
    //            modifiedSelect.getSelectList().set(i, newOp);
    //            modifiedSelect.getSelectList().add(variance);
    //            modifiedSelect.getSelectList().add(count);
    //            modifiedSelect.getSelectList().add(mean);
    //            modifiedSelect.getSelectList().add(sampleMean);
    //
    //            SqlNode meanDiff =
    //                ((SqlSelect)
    //                        SqlParser.create(
    //                                String.format(
    //                                    "SELECT %s - %s FROM t",
    //                                    meanAlias.toString(), sampleMeanAlias.toString()))
    //                            .parseQuery())
    //                    .getSelectList()
    //                    .get(0);
    //            meanDiff =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.POWER,
    //                    new SqlNode[] {meanDiff, SqlLiteral.createExactNumeric("2",
    // SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            SqlNode countPlusOne =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.PLUS,
    //                    new SqlNode[] {
    //                      countAlias, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
    //                    },
    //                    SqlParserPos.ZERO);
    //            meanDiff =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.MULTIPLY,
    //                    new SqlNode[] {meanDiff, countPlusOne},
    //                    SqlParserPos.ZERO);
    //
    //            SqlNode errorNumerator =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.MULTIPLY,
    //                    new SqlNode[] {varianceAlias, countAlias},
    //                    SqlParserPos.ZERO);
    //            errorNumerator =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.PLUS,
    //                    new SqlNode[] {errorNumerator, meanDiff},
    //                    SqlParserPos.ZERO);
    //            SqlNode varianceCountSum =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.SUM, new SqlNode[] {errorNumerator},
    // SqlParserPos.ZERO);
    //            SqlNode countSum =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.SUM, new SqlNode[] {countAlias}, SqlParserPos.ZERO);
    //            SqlNode div =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.DIVIDE,
    //                    new SqlNode[] {varianceCountSum, countSum},
    //                    SqlParserPos.ZERO);
    //            SqlNode stddev =
    //                new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {div},
    // SqlParserPos.ZERO);
    //
    //            SqlNode count2 =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.COUNT,
    //                    new SqlNode[] {new SqlIdentifier("", SqlParserPos.ZERO)},
    //                    SqlParserPos.ZERO);
    //            SqlNode sqrtCount =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.SQRT, new SqlNode[] {count2}, SqlParserPos.ZERO);
    //            SqlNode error =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.MULTIPLY,
    //                    new SqlNode[] {stddev, sqrtCount},
    //                    SqlParserPos.ZERO);
    //            SqlIdentifier errorAlias =
    //                new SqlIdentifier(sumAlias.toString() + "_error", SqlParserPos.ZERO);
    //            error =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS, new SqlNode[] {error, errorAlias},
    // SqlParserPos.ZERO);
    //
    //            newSelectList.add(error);
    //          } else if (op instanceof SqlAvgAggFunction) {
    //            SqlIdentifier avgAlias =
    //                new SqlIdentifier(String.format("avg%d", sumCount++), SqlParserPos.ZERO);
    //            SqlBasicCall newOp =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AS, new SqlNode[] {bc, avgAlias}, SqlParserPos.ZERO);
    //            SqlBasicCall newAggOp =
    //                new SqlBasicCall(
    //                    SqlStdOperatorTable.AVG, new SqlNode[] {avgAlias}, SqlParserPos.ZERO);
    //            if (aggAlias != null) {
    //              newAggOp =
    //                  new SqlBasicCall(
    //                      SqlStdOperatorTable.AS,
    //                      new SqlNode[] {newAggOp, aggAlias},
    //                      SqlParserPos.ZERO);
    //            }
    //            for (int j = 0; j < originalSelect.getSelectList().size(); ++j) {
    //              SqlNode n = originalSelect.getSelectList().get(j);
    //              if (n.equalsDeep(item, Litmus.IGNORE)) {
    //                newSelectList.set(j, newAggOp);
    //              }
    //            }
    //            modifiedSelect.getSelectList().set(i, newOp);
    //          }
    //        }
    //      }
    //
    //      // construct join clause
    //      List<SqlBasicCall> joinOps = new ArrayList<>();
    //      for (String col : s.getColumnSet()) {
    //        SqlIdentifier col1 = sampleAlias.plus(col, SqlParserPos.ZERO);
    //        SqlIdentifier col2 = statAlias.plus(col, SqlParserPos.ZERO);
    //        SqlBasicCall op =
    //            new SqlBasicCall(
    //                SqlStdOperatorTable.EQUALS, new SqlNode[] {col1, col2}, SqlParserPos.ZERO);
    //        joinOps.add(op);
    //      }
    //      SqlBasicCall joinClause = null;
    //      if (joinOps.size() == 1) {
    //        joinClause = joinOps.get(0);
    //      } else if (joinOps.size() > 1) {
    //        joinClause =
    //            new SqlBasicCall(
    //                SqlStdOperatorTable.AND,
    //                new SqlNode[] {joinOps.get(0), joinOps.get(1)},
    //                SqlParserPos.ZERO);
    //        for (int i = 2; i < joinOps.size(); ++i) {
    //          joinClause =
    //              new SqlBasicCall(
    //                  SqlStdOperatorTable.AND,
    //                  new SqlNode[] {joinClause, joinOps.get(i)},
    //                  SqlParserPos.ZERO);
    //        }
    //      }
    //
    //      SqlJoin newJoin =
    //          new SqlJoin(
    //              SqlParserPos.ZERO,
    //              from,
    //              SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
    //              JoinType.INNER.symbol(SqlParserPos.ZERO),
    //              new SqlBasicCall(
    //                  new SqlAsOperator(),
    //                  new SqlNode[] {new SqlIdentifier(statTable, SqlParserPos.ZERO), statAlias},
    //                  SqlParserPos.ZERO),
    //              JoinConditionType.ON.symbol(SqlParserPos.ZERO),
    //              joinClause);
    //
    //      modifiedSelect.setFrom(newJoin);
    //      replaceSelectListForSampleAlias(modifiedSelect, visitor);
    //
    //      // make it a subquery
    //      SqlBasicCall innerQuery =
    //          new SqlBasicCall(
    //              new SqlAsOperator(),
    //              new SqlNode[] {modifiedSelect, new SqlIdentifier("tmp", SqlParserPos.ZERO)},
    //              SqlParserPos.ZERO);
    //      //      SqlNodeList origGroupBy = originalSelect.getGroup();
    //      //      SqlNodeList newGroup = modifiedSelect.getGroup().clone(SqlParserPos.ZERO);
    //      //
    //      //      List<SqlNode> groupList = newGroup.getList();
    //      //      List<SqlNode> newGroupBy =
    //      //          groupList
    //      //              .stream()
    //      //              .filter(sqlNode -> IsInGroupBy(sqlNode, origGroupBy))
    //      //              .collect(Collectors.toList());
    //
    //      this.replaceListForOuterQuery(newSelectList);
    //      SqlNodeList newGroupBy = this.cloneSqlNodeList(origGroupBy);
    //      this.replaceListForOuterQuery(newGroupBy);
    //
    //      SqlSelect newSelectNode =
    //          new SqlSelect(
    //              SqlParserPos.ZERO,
    //              null,
    //              newSelectList,
    //              innerQuery,
    //              null,
    //              newGroupBy,
    //              null,
    //              null,
    //              null,
    //              null,
    //              null);
    //
    //      SqlString newSql = newSelectNode.toSqlString(dialect);
    //
    //      System.out.println(newSql.toString().toLowerCase());
    //
    //      if (newQueryNode instanceof SqlWith) {
    //        SqlWith with = (SqlWith) newQueryNode;
    //        newQueryNode =
    //            new SqlWith(
    //                SqlParserPos.ZERO,
    //                new SqlNodeList(with.getOperandList(), SqlParserPos.ZERO),
    //                newSelectNode);
    //      } else if (newQueryNode instanceof SqlOrderBy) {
    //        SqlOrderBy orderBy = (SqlOrderBy) newQueryNode;
    //
    //        // remove LIMIT
    //        //        newQueryNode =
    //        //            new SqlOrderBy(
    //        //                SqlParserPos.ZERO, newSelectNode, orderBy.orderList, orderBy.offset,
    //        // null);
    //
    //        // remove order by for now.
    //        newQueryNode = newSelectNode;
    //      } else if (newQueryNode instanceof SqlSelect) {
    //        newQueryNode = newSelectNode;
    //      }
    //    }
    //
    //    return newQueryNode;
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
    AliasReplacer replacer = new AliasReplacer(true, null);
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
