package dyoon.innocent.query;

import dyoon.innocent.Sample;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Created by Dong Young Yoon on 10/29/18. */
public class QueryTransformer extends SqlShuttle {

  int sumCount;
  int avgCount;
  int tmpCount;
  private Sample s;
  private SqlIdentifier currentAggAlias;
  private SqlOperator currentAggOp;

  private List<Pair<SqlIdentifier, SqlIdentifier>> aggAliasPairList;
  private List<SqlNode> extraColumns;

  public QueryTransformer(Sample s) {
    this.sumCount = 0;
    this.avgCount = 0;
    this.tmpCount = 0;
    this.s = s;

    this.aggAliasPairList = new ArrayList<>();
    this.extraColumns = new ArrayList<>();
  }

  public int approxCount() {
    // number of approximated columns
    return this.sumCount + this.avgCount;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      // transform SqlSelect only.
      super.visit(call);
      SqlSelect select = (SqlSelect) call;
      return transform(select);
    } else {
      return super.visit(call);
    }
  }

  private SqlNode transform(SqlSelect select) {
    // Check selectList contains aggregation
    AggregationChecker checker = new AggregationChecker();
    checker.visit(select.getSelectList());

    TableSubstitutor substitutor = new TableSubstitutor();
    substitutor.addTableToSample(s.getTable(), s);
    select = substitutor.substitute(select);

    int numSubs = substitutor.getNumTableSubstitutions();
    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);

    if (checker.hasAgg() && numSubs > 0) {
      // perform transformation
      SqlSelect innerSelect = this.cloneSqlSelect(select);
      SqlNodeList origGroupBy = select.getGroup();

      // add join with stat table
      SqlNode from = innerSelect.getFrom();
      String statTable = s.getSampleTableName() + "__stat";
      SqlIdentifier sampleAlias = substitutor.getSampleAlias(s);
      if (sampleAlias == null) {
        sampleAlias = new SqlIdentifier(s.getTable(), SqlParserPos.ZERO);
      }
      SqlIdentifier statAlias = new SqlIdentifier("stat", SqlParserPos.ZERO);

      // change agg function in the select to include scaling
      SqlNodeList origSelectList = this.cloneSqlNodeList(select.getSelectList());
      SqlNodeList outerSelectList = this.cloneSqlNodeList(select.getSelectList());
      SqlNodeList innerSelectList = innerSelect.getSelectList();

      List<SqlBasicCall> innerSelectExtraColumnList = new ArrayList<>();
      List<SqlBasicCall> outerSelectExtraColumnList = new ArrayList<>();

      // work on select list
      for (int i = 0; i < origSelectList.size(); ++i) {
        this.currentAggAlias = null;
        this.currentAggOp = null;
        SqlNode item = origSelectList.get(i);
        SqlNode aggSource = this.getAggregationSource(item);
        if (aggSource != null) {
          //          SqlBasicCall agg = (SqlBasicCall) item;
          SqlOperator op = this.currentAggOp;

          if (op instanceof SqlSumAggFunction || op instanceof SqlAvgAggFunction) {
            SqlIdentifier alias;

            SqlNode newSource = null, newAgg = null, newAggOp = null;

            if (op instanceof SqlSumAggFunction) {
              alias = new SqlIdentifier(String.format("sum%d", sumCount++), SqlParserPos.ZERO);
              if (this.currentAggAlias == null) this.currentAggAlias = alias;
              // scales the aggregation
              newSource = constructScaledAgg(aggSource, statAlias);
              newAgg = this.alias(this.sum(newSource), alias);
              newAggOp = this.alias(this.sum(alias), this.currentAggAlias);

            } else {
              alias = new SqlIdentifier(String.format("avg%d", avgCount++), SqlParserPos.ZERO);
              if (this.currentAggAlias == null) this.currentAggAlias = alias;

              // avg does not need scaling
              newSource = aggSource;
              newAgg = this.alias(this.avg(aggSource), alias);
              //              newAgg = newSource;
              newAggOp = this.alias(this.avg(alias), this.currentAggAlias);
            }
            innerSelectList.set(i, newAgg);
            outerSelectList.set(i, newAggOp);

            SqlIdentifier sampleMeanAlias =
                new SqlIdentifier(alias.toString() + "_samplemean", SqlParserPos.ZERO);
            SqlIdentifier groupMeanAlias =
                new SqlIdentifier(alias.toString() + "_groupmean", SqlParserPos.ZERO);
            SqlIdentifier varianceAlias =
                new SqlIdentifier(alias.toString() + "_var", SqlParserPos.ZERO);
            SqlIdentifier sampleCountAlias =
                new SqlIdentifier(alias.toString() + "_count", SqlParserPos.ZERO);
            SqlBasicCall sampleMean = constructSampleMean(newSource, sampleMeanAlias, origGroupBy);
            SqlBasicCall groupMean = constructGroupMean(newSource, groupMeanAlias);
            SqlBasicCall variance = constructVariance(newSource, varianceAlias);
            SqlBasicCall sampleCount = constructSampleCount(sampleCountAlias);

            innerSelectExtraColumnList.add(sampleMean);
            innerSelectExtraColumnList.add(groupMean);
            innerSelectExtraColumnList.add(variance);
            innerSelectExtraColumnList.add(sampleCount);

            SqlIdentifier errorAlias =
                new SqlIdentifier(this.currentAggAlias.toString() + "_error", SqlParserPos.ZERO);

            try {
              SqlBasicCall error =
                  (op instanceof SqlSumAggFunction)
                      ? constructSumError(
                          sampleMeanAlias,
                          groupMeanAlias,
                          varianceAlias,
                          sampleCountAlias,
                          errorAlias)
                      : constructAvgError(
                          sampleMeanAlias,
                          groupMeanAlias,
                          varianceAlias,
                          sampleCountAlias,
                          errorAlias);
              outerSelectExtraColumnList.add(error);
            } catch (SqlParseException e) {
              e.printStackTrace();
            }

            aggAliasPairList.add(ImmutablePair.of(this.currentAggAlias, errorAlias));
          }
        }
      } // end for

      for (SqlBasicCall col : innerSelectExtraColumnList) {
        innerSelectList.add(col);
      }
      for (SqlBasicCall col : outerSelectExtraColumnList) {
        outerSelectList.add(col);
      }
      extraColumns.addAll(outerSelectExtraColumnList);

      // handle FROM
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

      innerSelect.setFrom(newJoin);

      // add sample column set to select list + group by of inner query.
      for (String groupBy : s.getColumnSet()) {
        SqlIdentifier newGroupBy =
            new SqlIdentifier(Arrays.asList(s.getTable(), groupBy), SqlParserPos.ZERO);
        innerSelect.getSelectList().add(newGroupBy);
        innerSelect.getGroup().add(newGroupBy);
      }

      AliasMapBuilder mapBuilder = new AliasMapBuilder();
      mapBuilder.visit(select);
      Map<SqlIdentifier, SqlIdentifier> aliasMap = mapBuilder.getAliasMap();

      replaceSelectListForSampleAlias(innerSelect, aliasMap);

      SqlIdentifier tmpAlias =
          new SqlIdentifier(String.format("tmp%d", tmpCount++), SqlParserPos.ZERO);
      // make it a subquery
      SqlBasicCall innerQuery =
          new SqlBasicCall(
              new SqlAsOperator(), new SqlNode[] {innerSelect, tmpAlias}, SqlParserPos.ZERO);

      this.replaceListForOuterQuery(outerSelectList, tmpAlias);
      SqlNodeList newGroupBy = this.cloneSqlNodeList(origGroupBy);
      this.replaceListForOuterQuery(newGroupBy, tmpAlias);

      SqlSelect newSelectNode =
          new SqlSelect(
              SqlParserPos.ZERO,
              null,
              outerSelectList,
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

      return newSelectNode;
    } else {
      return select; // no agg -> no transformation
    }
  }

  public List<Pair<SqlIdentifier, SqlIdentifier>> getAggAliasPairList() {
    return aggAliasPairList;
  }

  private void replaceSelectListForSampleAlias(
      SqlSelect select, Map<SqlIdentifier, SqlIdentifier> aliasMap) {
    AliasReplacer replacer = new AliasReplacer(aliasMap);
    select.accept(replacer);
  }

  private void replaceListForOuterQuery(SqlNodeList selectList, SqlIdentifier alias) {
    AliasReplacer replacer = new AliasReplacer(true, alias);
    selectList.accept(replacer);
  }

  private SqlBasicCall constructAvgError(
      SqlIdentifier sampleMean,
      SqlIdentifier groupMean,
      SqlIdentifier variance,
      SqlIdentifier sampleCount,
      SqlIdentifier alias)
      throws SqlParseException {
    SqlNode meanDiff =
        ((SqlSelect)
                SqlParser.create(
                        String.format(
                            "SELECT %s - %s FROM t", sampleMean.toString(), groupMean.toString()))
                    .parseQuery())
            .getSelectList()
            .get(0);
    meanDiff =
        new SqlBasicCall(
            SqlStdOperatorTable.POWER,
            new SqlNode[] {meanDiff, SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    SqlNode countPlusOne =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            new SqlNode[] {sampleCount, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    meanDiff =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            new SqlNode[] {meanDiff, countPlusOne},
            SqlParserPos.ZERO);

    SqlNode errorNumerator =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, new SqlNode[] {variance, sampleCount}, SqlParserPos.ZERO);
    errorNumerator =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS, new SqlNode[] {errorNumerator, meanDiff}, SqlParserPos.ZERO);
    SqlNode varianceCountSum =
        new SqlBasicCall(
            SqlStdOperatorTable.SUM, new SqlNode[] {errorNumerator}, SqlParserPos.ZERO);
    SqlNode countSum =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            new SqlNode[] {sampleCount, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    countSum =
        new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {countSum}, SqlParserPos.ZERO);
    SqlNode countSumMinusOne =
        new SqlBasicCall(
            SqlStdOperatorTable.MINUS,
            new SqlNode[] {countSum, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);

    SqlNode div =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            new SqlNode[] {varianceCountSum, countSumMinusOne},
            SqlParserPos.ZERO);
    SqlNode stddev =
        new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {div}, SqlParserPos.ZERO);

    SqlNode sqrtCount =
        new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {countSum}, SqlParserPos.ZERO);
    SqlBasicCall error =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE, new SqlNode[] {stddev, sqrtCount}, SqlParserPos.ZERO);
    error =
        new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {error, alias}, SqlParserPos.ZERO);

    return error;
  }

  private SqlBasicCall constructSumError(
      SqlIdentifier sampleMean,
      SqlIdentifier groupMean,
      SqlIdentifier variance,
      SqlIdentifier sampleCount,
      SqlIdentifier alias)
      throws SqlParseException {
    SqlNode meanDiff =
        ((SqlSelect)
                SqlParser.create(
                        String.format(
                            "SELECT %s - %s FROM t", sampleMean.toString(), groupMean.toString()))
                    .parseQuery())
            .getSelectList()
            .get(0);
    meanDiff =
        new SqlBasicCall(
            SqlStdOperatorTable.POWER,
            new SqlNode[] {meanDiff, SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    SqlNode countPlusOne =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            new SqlNode[] {sampleCount, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    meanDiff =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            new SqlNode[] {meanDiff, countPlusOne},
            SqlParserPos.ZERO);

    SqlNode errorNumerator =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, new SqlNode[] {variance, sampleCount}, SqlParserPos.ZERO);
    errorNumerator =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS, new SqlNode[] {errorNumerator, meanDiff}, SqlParserPos.ZERO);
    SqlNode varianceCountSum =
        new SqlBasicCall(
            SqlStdOperatorTable.SUM, new SqlNode[] {errorNumerator}, SqlParserPos.ZERO);
    SqlNode count2 =
        new SqlBasicCall(
            SqlStdOperatorTable.COUNT,
            new SqlNode[] {new SqlIdentifier("", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    SqlNode countSum =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            new SqlNode[] {sampleCount, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    countSum =
        new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {countSum}, SqlParserPos.ZERO);
    SqlNode countSumMinusOne =
        new SqlBasicCall(
            SqlStdOperatorTable.MINUS,
            new SqlNode[] {countSum, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    //    new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {sampleCount}, SqlParserPos.ZERO);
    SqlNode div =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            new SqlNode[] {varianceCountSum, countSumMinusOne},
            SqlParserPos.ZERO);
    SqlNode stddev =
        new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {div}, SqlParserPos.ZERO);

    SqlNode sqrtCount =
        new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {countSum}, SqlParserPos.ZERO);
    SqlBasicCall error =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, new SqlNode[] {stddev, sqrtCount}, SqlParserPos.ZERO);
    error =
        new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {error, alias}, SqlParserPos.ZERO);

    return error;
  }

  private SqlBasicCall constructSampleCount(SqlIdentifier alias) {
    SqlBasicCall count =
        new SqlBasicCall(
            SqlStdOperatorTable.COUNT,
            new SqlNode[] {new SqlIdentifier("", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    // count - 1
    count =
        new SqlBasicCall(
            SqlStdOperatorTable.MINUS,
            new SqlNode[] {count, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    count =
        new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {count, alias}, SqlParserPos.ZERO);

    return count;
  }

  private SqlBasicCall constructVariance(SqlNode source, SqlIdentifier alias) {
    SqlBasicCall variance =
        new SqlBasicCall(
            SqlStdOperatorTable.VAR_SAMP,
            new SqlNode[] {source.clone(SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    variance =
        new SqlBasicCall(
            SqlStdOperatorTable.AS, new SqlNode[] {variance, alias}, SqlParserPos.ZERO);
    return variance;
  }

  private SqlBasicCall constructGroupMean(SqlNode source, SqlIdentifier alias) {
    SqlBasicCall mean =
        new SqlBasicCall(
            SqlStdOperatorTable.AVG,
            new SqlNode[] {source.clone(SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    mean = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {mean, alias}, SqlParserPos.ZERO);
    return mean;
  }

  private SqlBasicCall constructSampleMean(
      SqlNode source, SqlIdentifier alias, SqlNodeList groupBy) {

    // construct window function for sample mean
    SqlBasicCall sampleMean =
        new SqlBasicCall(
            SqlStdOperatorTable.AVG,
            new SqlNode[] {source.clone(SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    sampleMean =
        new SqlBasicCall(SqlStdOperatorTable.AVG, new SqlNode[] {sampleMean}, SqlParserPos.ZERO);
    SqlNode sampleMeanWindow =
        new SqlWindow(
            SqlParserPos.ZERO,
            null,
            null,
            this.cloneSqlNodeList(groupBy),
            new SqlNodeList(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            null,
            null,
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
    sampleMean =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            new SqlNode[] {sampleMean, sampleMeanWindow},
            SqlParserPos.ZERO);
    sampleMean =
        new SqlBasicCall(
            SqlStdOperatorTable.AS, new SqlNode[] {sampleMean, alias}, SqlParserPos.ZERO);

    return sampleMean;
  }

  private SqlBasicCall alias(SqlNode source, SqlIdentifier alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, new SqlNode[] {source, alias}, SqlParserPos.ZERO);
  }

  private SqlBasicCall sum(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  private SqlBasicCall avg(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.AVG, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  private SqlBasicCall constructScaledAgg(SqlNode aggSource, SqlIdentifier statAlias) {
    // scale the summation
    // x / groupsize * actualsize
    SqlNode c = SqlLiteral.createExactNumeric("100000", SqlParserPos.ZERO);

    SqlBasicCall scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, new SqlNode[] {aggSource, c}, SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            new SqlNode[] {scaled, statAlias.plus("groupsize", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            new SqlNode[] {scaled, statAlias.plus("actualsize", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    scaled =
        new SqlBasicCall(SqlStdOperatorTable.DIVIDE, new SqlNode[] {scaled, c}, SqlParserPos.ZERO);

    // sum (x / groupsize * actualsize)
    //    SqlBasicCall newOp =
    //        new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {scaled}, SqlParserPos.ZERO);

    // sum (x / groupsize * actualsize) as sumAlias
    //    newOp =
    //        new SqlBasicCall(
    //            SqlStdOperatorTable.AS, new SqlNode[] {newOp, aggAlias}, SqlParserPos.ZERO);

    return scaled;
  }

  private SqlNodeList cloneSqlNodeList(SqlNodeList list) {
    if (list == null) return null;
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

  private SqlNode getAggregationSource(SqlNode item) {
    // look for agg functions
    if (item instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) item;
      SqlOperator op = bc.getOperator();
      SqlIdentifier aggAlias = null;

      // check if there is alias
      if (op instanceof SqlAsOperator) {
        if (bc.operands[0] instanceof SqlBasicCall) {
          this.currentAggAlias = (SqlIdentifier) bc.operands[1];
          bc = (SqlBasicCall) bc.operands[0];
          return getAggregationSource(bc);
        }
      } else if (op instanceof SqlSumAggFunction || op instanceof SqlAvgAggFunction) {
        this.currentAggOp = op;
        return bc.operands[0];
      }
    }
    return null;
  }
}
