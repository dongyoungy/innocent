package dyoon.innocent.query;

import dyoon.innocent.Sample;
import dyoon.innocent.Utils;
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
import org.apache.calcite.sql.fun.SqlCountAggFunction;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 10/29/18. */
public class QueryTransformer extends SqlShuttle {

  int approxCount;
  int sumCount;
  int avgCount;
  int aggCount;
  int cntCount;
  int errCount;
  int tmpCount;
  private boolean isWithError;
  private Sample s;
  private SqlIdentifier currentAggAlias;
  private SqlOperator currentAggOp;

  private List<Pair<SqlIdentifier, SqlIdentifier>> aggAliasPairList;
  private List<SqlSelect> selectForError;
  private List<SqlNode> extraColumns;
  private List<String> sampleTableColumns;
  private Map<Integer, SqlOperator> nonAQPAggList;
  private Set<SqlNodeList> transformedSelectListSet;

  public QueryTransformer(Sample s, List<String> sampleTableColumns, boolean isWithError) {
    this.approxCount = 0;
    this.sumCount = 0;
    this.avgCount = 0;
    this.aggCount = 0;
    this.cntCount = 0;
    this.errCount = 0;
    this.tmpCount = 0;
    this.s = s;
    this.isWithError = isWithError;

    this.aggAliasPairList = new ArrayList<>();
    this.sampleTableColumns = new ArrayList<>(sampleTableColumns);
    this.selectForError = new ArrayList<>();
    this.extraColumns = new ArrayList<>();
    this.nonAQPAggList = new HashMap<>();

    this.transformedSelectListSet = new HashSet<>();
  }

  public int approxCount() {
    // number of approximated columns
    return this.approxCount;
  }

  public List<SqlSelect> getSelectForError() {
    return selectForError;
  }

  public Set<SqlNodeList> getTransformedSelectListSet() {
    return transformedSelectListSet;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      // transform SqlSelect only.
      call = (SqlCall) super.visit(call);
      SqlSelect select = (SqlSelect) call;
      //      select.setFrom(select.getFrom().accept(this));
      SqlSelect node = (SqlSelect) transform(select);
      node.setFrom(node.getFrom().accept(this));
      return node;
    } else if (call instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) call;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator && bc.operands[0] instanceof SqlSelect) {
        bc.setOperand(0, bc.operands[0].accept(this));
      }
      return bc;
    } else {
      return super.visit(call);
    }
  }

  private SqlNode transform(SqlSelect select) {

    // Add alias to aggregates
    ExpressionNamer namer = new ExpressionNamer();
    for (int i = 0; i < select.getSelectList().size(); ++i) {
      SqlNode node = select.getSelectList().get(i).accept(namer);
      select.getSelectList().set(i, node);
    }

    // Check selectList contains aggregation
    AggregationChecker checker = new AggregationChecker(sampleTableColumns);
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
      String statTable = s.getSampleTableName() + "___stat";
      SqlIdentifier sampleAlias = substitutor.getSampleAlias(s);
      if (sampleAlias == null) {
        sampleAlias = new SqlIdentifier(s.getTable(), SqlParserPos.ZERO);
      }
      SqlIdentifier statAlias = new SqlIdentifier("stat", SqlParserPos.ZERO);

      // change agg function in the select to include scaling
      SqlNodeList origSelectList = this.cloneSqlNodeList(select.getSelectList());
      //      SqlNodeList outerSelectList = this.cloneSqlNodeList(select.getSelectList());
      TreeCloner treeCloner = new TreeCloner();

      SqlNodeList outerSelectList = (SqlNodeList) treeCloner.visit(select.getSelectList());
      SqlNodeList innerSelectList = (SqlNodeList) treeCloner.visit(innerSelect.getSelectList());
      //      SqlNodeList innerSelectList = innerSelect.getSelectList();

      List<SqlNode> innerSelectExtraColumnList = new ArrayList<>();
      List<SqlBasicCall> outerSelectExtraColumnList = new ArrayList<>();

      SqlNodeList innerSelectRemoveColumnList = new SqlNodeList(SqlParserPos.ZERO);

      List<Integer> aggColumnIndexList = new ArrayList<>();

      // work on select list
      for (int i = 0; i < outerSelectList.size(); ++i) {
        this.currentAggAlias = null;
        this.currentAggOp = null;
        SqlNode item = outerSelectList.get(i);
        SqlNode aggSource = this.getAggregationSource(item);
        AggregationAnalyzer aggAnalyzer = new AggregationAnalyzer(sampleTableColumns);
        item.accept(aggAnalyzer);

        // use this for scaling.
        //        SqlIdentifier aggSourceColumn = aggAnalyzer.getAggSourceColumn();
        SqlBasicCall innerMostAgg = aggAnalyzer.getInnerMostAgg();
        SqlBasicCall agg = (SqlBasicCall) treeCloner.visit(innerMostAgg);

        // this is outer-most aggregation -> decides summary operation in the outer select.
        SqlOperator firstAggOp = aggAnalyzer.getFirstAggOp();

        // this is aggregation get applied directly on the sample column.
        SqlOperator firstAggWithSampleColumnOp = aggAnalyzer.getFirstAggWithSampleColumnOp();

        if (innerMostAgg != null
            && (firstAggWithSampleColumnOp instanceof SqlSumAggFunction
                || firstAggWithSampleColumnOp instanceof SqlAvgAggFunction
                || firstAggWithSampleColumnOp instanceof SqlCountAggFunction)) {
          // if AQP column exists
          boolean addAlias = false;
          SqlIdentifier alias = aggAnalyzer.getAlias();
          SqlIdentifier innerAlias;
          SqlNode newSource = null, newAgg = null, newAggOp = null;
          aggColumnIndexList.add(i);

          SqlOperator op = firstAggWithSampleColumnOp;
          SqlOperator outerOp = firstAggOp;

          if (op instanceof SqlSumAggFunction) {

            //            ScaledAggBuilder scaledAggBuilder = new ScaledAggBuilder(innerMostAgg,
            // statAlias, op);
            //            newAgg = item.accept(scaledAggBuilder);
            //            newSource = scaledAggBuilder.getScaledSource();
            innerAlias = alias;
            newSource = Utils.constructScaledAgg(agg.operands[0], statAlias);
            agg.setOperand(0, newSource);
            if (alias == null) {
              innerAlias = new SqlIdentifier(String.format("sum%d", sumCount++), SqlParserPos.ZERO);
              alias = innerAlias;
              addAlias = true;
            }
            newAgg = this.alias(agg, innerAlias);
            newAggOp = this.applySummaryForOuterOp(innerAlias, outerOp);
          } else if (op instanceof SqlAvgAggFunction) {
            innerAlias = new SqlIdentifier(String.format("avg%d", avgCount++), SqlParserPos.ZERO);
            if (alias == null) {
              alias = innerAlias;
              addAlias = true;
            }

            newSource = agg.operands[0];
            newAgg = this.alias(agg, innerAlias);
            //            newAgg = this.alias(aggAnalyzer.getInnerMostAgg(), innerAlias);
            newAggOp = this.applySummaryForOuterOp(innerAlias, outerOp);
          } else { // count
            //            ScaledAggBuilder scaledAggBuilder =
            //                new ScaledAggBuilder(aggSourceColumn, statAlias, op);
            innerAlias = new SqlIdentifier(String.format("cnt%d", cntCount++), SqlParserPos.ZERO);
            if (alias == null) {
              alias = innerAlias;
              addAlias = true;
            }

            newSource = Utils.constructScaledAgg(agg, statAlias);
            newAgg = this.alias(newSource, innerAlias);
            newAggOp = this.applySummaryForOuterOp(innerAlias, outerOp);
            innerMostAgg.setOperator(SqlStdOperatorTable.SUM);
          }
          ++approxCount;

          newAgg = this.addAliasIfNotExists(newAgg, innerAlias);
          innerSelectList.set(i, newAgg);
          //          outerSelectList.set(i, this.alias(newAggOp, alias));
          innerMostAgg.setOperand(0, innerAlias);
          if (addAlias) {
            outerSelectList.set(i, this.addAliasIfNotExists(item, alias));
          }

          if (op instanceof SqlSumAggFunction || op instanceof SqlAvgAggFunction) {
            SqlIdentifier sampleMeanAlias =
                new SqlIdentifier(innerAlias.toString() + "_samplemean", SqlParserPos.ZERO);
            SqlIdentifier groupMeanAlias =
                new SqlIdentifier(innerAlias.toString() + "_groupmean", SqlParserPos.ZERO);
            SqlIdentifier varianceAlias =
                new SqlIdentifier(innerAlias.toString() + "_var", SqlParserPos.ZERO);
            SqlIdentifier sampleCountAlias =
                new SqlIdentifier(innerAlias.toString() + "_count", SqlParserPos.ZERO);
            SqlBasicCall sampleMean = constructSampleMean(newSource, sampleMeanAlias, origGroupBy);
            SqlBasicCall groupMean = constructGroupMean(newSource, groupMeanAlias);
            SqlBasicCall variance = constructVariance(newSource, varianceAlias);
            SqlBasicCall sampleCount = constructSampleCount(sampleCountAlias);

            innerSelectExtraColumnList.add(sampleMean);
            innerSelectExtraColumnList.add(groupMean);
            innerSelectExtraColumnList.add(variance);
            innerSelectExtraColumnList.add(sampleCount);

            SqlIdentifier errorAlias =
                new SqlIdentifier(String.format("%s_error", alias.toString()), SqlParserPos.ZERO);
            //            new SqlIdentifier(this.currentAggAlias.toString() + "_error",
            // SqlParserPos.ZERO);

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
              outerSelectExtraColumnList.add(this.alias(error, errorAlias));

              // add relative error columns
              SqlIdentifier relErrorAlias =
                  new SqlIdentifier(
                      String.format("%s_rel_error", alias.toString()), SqlParserPos.ZERO);
              //              this.currentAggAlias.toString() + "_rel_error", SqlParserPos.ZERO);
              SqlBasicCall relErrorOp =
                  new SqlBasicCall(
                      SqlStdOperatorTable.DIVIDE,
                      new SqlNode[] {error, newAggOp},
                      SqlParserPos.ZERO);
              SqlBasicCall relError =
                  new SqlBasicCall(
                      SqlStdOperatorTable.AS,
                      new SqlNode[] {relErrorOp, relErrorAlias},
                      SqlParserPos.ZERO);
              outerSelectExtraColumnList.add(relError);
              ++errCount;
            } catch (SqlParseException e) {
              e.printStackTrace();
            }

            aggAliasPairList.add(ImmutablePair.of(alias, errorAlias));
            //            aggAliasPairList.add(ImmutablePair.of(this.currentAggAlias, errorAlias));
          } else if (op instanceof SqlCountAggFunction) {

            SqlIdentifier gsId = statAlias.plus("groupsize", SqlParserPos.ZERO);
            SqlBasicCall t1 =
                new SqlBasicCall(
                    SqlStdOperatorTable.DIVIDE, new SqlNode[] {agg, gsId}, SqlParserPos.ZERO);
            SqlBasicCall t2 =
                new SqlBasicCall(
                    SqlStdOperatorTable.MINUS,
                    new SqlNode[] {SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO), t1},
                    SqlParserPos.ZERO);

            SqlBasicCall t3 =
                new SqlBasicCall(
                    SqlStdOperatorTable.MULTIPLY, new SqlNode[] {t1, t2}, SqlParserPos.ZERO);
            SqlBasicCall error =
                new SqlBasicCall(
                    SqlStdOperatorTable.DIVIDE,
                    new SqlNode[] {this.sqrt(t3), this.sqrt(gsId)},
                    SqlParserPos.ZERO);
            SqlIdentifier errorAlias =
                new SqlIdentifier(String.format("%s_error", alias.toString()), SqlParserPos.ZERO);
            innerSelectExtraColumnList.add(this.alias(error, errorAlias));

            SqlBasicCall outerError =
                Utils.alias(Utils.sqrt(Utils.sum(Utils.pow(errorAlias, 2))), errorAlias);

            outerSelectExtraColumnList.add(outerError);
            aggAliasPairList.add(ImmutablePair.of(alias, errorAlias));
          }
          //        } else if (aggSourceColumn == null && firstAggOp != null) {
        } else if (innerMostAgg == null && firstAggOp != null) {
          // TODO: need to check this path...
          if (aggAnalyzer.getAggSource() != null) {
            SqlNode innerItem = item;
            SqlNode outerItem;
            SqlIdentifier alias = Utils.getAliasIfExists(item);
            if (alias == null) {
              alias = new SqlIdentifier(String.format("agg%d", aggCount++), SqlParserPos.ZERO);
              innerItem = this.alias(innerItem, alias);
            }
            if (firstAggOp instanceof SqlAvgAggFunction) {
              outerItem = this.alias(this.avg(alias), alias);
            } else {
              outerItem = this.alias(this.sum(alias), alias);
            }

            innerSelectList.set(i, innerItem);
            outerSelectList.set(i, outerItem);
            //
            //            innerSelectExtraColumnList.add(aggAnalyzer.getAggSource());
          } else {
            innerSelectRemoveColumnList.add(item);
          }
          aggColumnIndexList.add(i);
        }
      } // end for

      SqlNodeList newInnerSelectlist = new SqlNodeList(SqlParserPos.ZERO);
      for (SqlNode node : innerSelectList) {
        if (!Utils.containsNode(node, innerSelectRemoveColumnList)) {
          newInnerSelectlist.add(node);
        }
      }
      innerSelect.setSelectList(newInnerSelectlist);
      innerSelectList = innerSelect.getSelectList();

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
        SqlNodeList group = innerSelect.getGroup();
        if (group == null) {
          innerSelect.setGroupBy(new SqlNodeList(SqlParserPos.ZERO));
        }
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

      SqlBasicCall innerQueryForError =
          new SqlBasicCall(
              new SqlAsOperator(),
              new SqlNode[] {innerSelect, tmpAlias},
              //              new SqlNode[] {this.cloneSqlSelect(innerSelect), tmpAlias},
              SqlParserPos.ZERO);

      // need to add missing group by columns to select list from original query
      if (origGroupBy != null) {
        for (SqlNode node : origGroupBy) {
          SqlIdentifier groupBy = (SqlIdentifier) node;
          boolean hasGroupBy = false;
          for (SqlNode n : innerSelect.getSelectList()) {
            if (n instanceof SqlIdentifier) {
              SqlIdentifier col = (SqlIdentifier) n;
              if (col.names
                  .get(col.names.size() - 1)
                  .equalsIgnoreCase(groupBy.names.get(groupBy.names.size() - 1))) {
                hasGroupBy = true;
                break;
              }
            }
          }

          if (!hasGroupBy) {
            innerSelect.getSelectList().add(node);
          }
        }
      }

      outerSelectList =
          this.replaceListForOuterQuery(outerSelectList, tmpAlias, aggColumnIndexList);
      SqlNodeList newGroupBy = this.cloneSqlNodeList(origGroupBy);
      if (newGroupBy != null) {
        newGroupBy = replaceGroupByForSampleAlias(newGroupBy, aliasMap);
        newGroupBy = this.replaceListForOuterQuery(newGroupBy, tmpAlias);
      }

      SqlSelect newSelectNodeWithError =
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

      SqlSelect newSelectNode =
          new SqlSelect(
              SqlParserPos.ZERO,
              null,
              outerSelectList,
              innerQueryForError,
              null,
              newGroupBy,
              null,
              null,
              null,
              null,
              null);

      SqlSelect transformedSelect = this.cloneSqlSelect(newSelectNode);

      for (SqlNode col : innerSelectExtraColumnList) {
        innerSelectList.add(col);
      }
      for (SqlBasicCall col : outerSelectExtraColumnList) {
        outerSelectList.add(col);
        aggColumnIndexList.add(outerSelectList.size() - 1);
      }
      extraColumns.addAll(outerSelectExtraColumnList);

      SqlString newSql = newSelectNode.toSqlString(dialect);

      //      System.out.println(newSql.toString().toLowerCase());
      //      selectForError.add(newSelectNodeForError);

      this.transformedSelectListSet.add(outerSelectList);
      this.transformedSelectListSet.add(innerSelectList);

      return isWithError ? newSelectNodeWithError : transformedSelect;
    } else {
      return select; // no agg -> no transformation
    }
  }

  private SqlNode addAliasIfNotExists(SqlNode node, SqlIdentifier alias) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall bc = (SqlBasicCall) node;
      SqlOperator op = bc.getOperator();
      if (op instanceof SqlAsOperator) {
        return node;
      } else {
        return this.alias(node, alias);
      }
    }
    return node;
  }

  private SqlNodeList replaceListForOuterQuery(
      SqlNodeList outerSelectList, SqlIdentifier alias, List<Integer> aggColumnIndexList) {
    AliasReplacer replacer = new AliasReplacer(true, alias);
    for (int i = 0; i < outerSelectList.size(); ++i) {
      if (!aggColumnIndexList.contains(i)) {
        SqlNode node = outerSelectList.get(i);
        SqlNode newNode = node.accept(replacer);
        if (newNode instanceof SqlIdentifier && nonAQPAggList.containsKey(i)) {
          SqlIdentifier orig = (SqlIdentifier) newNode;
          SqlIdentifier newAlias =
              new SqlIdentifier(orig.names.get(orig.names.size() - 1), SqlParserPos.ZERO);
          SqlOperator op = nonAQPAggList.get(i);
          if (op instanceof SqlSumAggFunction || op instanceof SqlCountAggFunction) {
            newNode = this.alias(this.sum(newNode), newAlias);
          } else if (op instanceof SqlAvgAggFunction) {
            newNode = this.alias(this.avg(newNode), newAlias);
          }
        }
        outerSelectList.set(i, newNode);
      }
    }
    return outerSelectList;
  }

  public List<Pair<SqlIdentifier, SqlIdentifier>> getAggAliasPairList() {
    return aggAliasPairList;
  }

  private SqlNodeList replaceGroupByForSampleAlias(
      SqlNodeList list, Map<SqlIdentifier, SqlIdentifier> aliasMap) {
    AliasReplacer replacer = new AliasReplacer(aliasMap);
    SqlNodeList newGroupBy = (SqlNodeList) list.accept(replacer);
    return newGroupBy;
    //    select = (SqlSelect) select.accept(replacer);
  }

  private void replaceSelectListForSampleAlias(
      SqlSelect select, Map<SqlIdentifier, SqlIdentifier> aliasMap) {
    AliasReplacer replacer = new AliasReplacer(aliasMap);
    select = (SqlSelect) select.accept(replacer);
  }

  private SqlNodeList replaceListForOuterQuery(SqlNodeList selectList, SqlIdentifier alias) {
    AliasReplacer replacer = new AliasReplacer(true, alias);
    //    selectList.accept(replacer);
    return (SqlNodeList) selectList.accept(replacer);
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
    //    error =
    //        new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {error, alias},
    // SqlParserPos.ZERO);

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
    //    error =
    //        new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {error, alias},
    // SqlParserPos.ZERO);

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
    SqlNodeList newGroupBy = this.cloneSqlNodeList(groupBy);
    if (newGroupBy == null) newGroupBy = new SqlNodeList(SqlParserPos.ZERO);
    SqlNode sampleMeanWindow =
        new SqlWindow(
            SqlParserPos.ZERO,
            null,
            null,
            newGroupBy,
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

  private SqlBasicCall applySummaryForOuterOp(SqlNode source, SqlOperator agg) {
    if (agg instanceof SqlSumAggFunction) {
      return this.sum(source);
    } else if (agg instanceof SqlAvgAggFunction) {
      return this.avg(source);
    } else {
      return this.sum(source);
    }
  }

  private SqlBasicCall sum(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  private SqlBasicCall avg(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.AVG, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  private SqlBasicCall count(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.COUNT, new SqlNode[] {source}, SqlParserPos.ZERO);
  }

  private SqlBasicCall sqrt(SqlNode source) {
    return new SqlBasicCall(SqlStdOperatorTable.SQRT, new SqlNode[] {source}, SqlParserPos.ZERO);
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
      } else if (op instanceof SqlSumAggFunction
          || op instanceof SqlAvgAggFunction
          || op instanceof SqlCountAggFunction) {
        this.currentAggOp = op;
        return bc.operands[0];
      }
    }
    return null;
  }

  /**
   * old impl
   *
   * <p>if (aggSource != null) { SqlOperator op = this.currentAggOp;
   *
   * <p>if (op instanceof SqlSumAggFunction || op instanceof SqlAvgAggFunction || op instanceof
   * SqlCountAggFunction) { SqlIdentifier alias;
   *
   * <p>SqlNode newSource = null, newAgg = null, newAggOp = null;
   *
   * <p>aggColumnIndexList.add(i);
   *
   * <p>if (op instanceof SqlSumAggFunction) { alias = new SqlIdentifier(String.format("sum%d",
   * sumCount++), SqlParserPos.ZERO); if (this.currentAggAlias == null) this.currentAggAlias =
   * alias; // scales the aggregation newSource = constructScaledAgg(aggSource, statAlias); newAgg =
   * this.alias(this.sum(newSource), alias); newAggOp = this.sum(alias);
   *
   * <p>} else if (op instanceof SqlAvgAggFunction) { alias = new
   * SqlIdentifier(String.format("avg%d", avgCount++), SqlParserPos.ZERO); if (this.currentAggAlias
   * == null) this.currentAggAlias = alias;
   *
   * <p>// avg does not need scaling newSource = aggSource; newAgg = this.alias(this.avg(aggSource),
   * alias); // newAgg = newSource; newAggOp = this.avg(alias); } else { alias = new
   * SqlIdentifier(String.format("count%d", avgCount++), SqlParserPos.ZERO); if
   * (this.currentAggAlias == null) this.currentAggAlias = alias;
   *
   * <p>// For now, we do not handle count for aqp newSource = aggSource; newAgg =
   * this.alias(this.count(aggSource), alias); newAggOp = this.count(alias); }
   * innerSelectList.set(i, newAgg); outerSelectList.set(i, this.alias(newAggOp,
   * this.currentAggAlias));
   *
   * <p>if (op instanceof SqlSumAggFunction || op instanceof SqlAvgAggFunction) { SqlIdentifier
   * sampleMeanAlias = new SqlIdentifier(alias.toString() + "_samplemean", SqlParserPos.ZERO);
   * SqlIdentifier groupMeanAlias = new SqlIdentifier(alias.toString() + "_groupmean",
   * SqlParserPos.ZERO); SqlIdentifier varianceAlias = new SqlIdentifier(alias.toString() + "_var",
   * SqlParserPos.ZERO); SqlIdentifier sampleCountAlias = new SqlIdentifier(alias.toString() +
   * "_count", SqlParserPos.ZERO); SqlBasicCall sampleMean = constructSampleMean(newSource,
   * sampleMeanAlias, origGroupBy); SqlBasicCall groupMean = constructGroupMean(newSource,
   * groupMeanAlias); SqlBasicCall variance = constructVariance(newSource, varianceAlias);
   * SqlBasicCall sampleCount = constructSampleCount(sampleCountAlias);
   *
   * <p>innerSelectExtraColumnList.add(sampleMean); innerSelectExtraColumnList.add(groupMean);
   * innerSelectExtraColumnList.add(variance); innerSelectExtraColumnList.add(sampleCount);
   *
   * <p>SqlIdentifier errorAlias = new SqlIdentifier(this.currentAggAlias.toString() + "_error",
   * SqlParserPos.ZERO);
   *
   * <p>try { SqlBasicCall error = (op instanceof SqlSumAggFunction) ? constructSumError(
   * sampleMeanAlias, groupMeanAlias, varianceAlias, sampleCountAlias, errorAlias) :
   * constructAvgError( sampleMeanAlias, groupMeanAlias, varianceAlias, sampleCountAlias,
   * errorAlias); outerSelectExtraColumnList.add(this.alias(error, errorAlias));
   *
   * <p>// add relative error columns SqlIdentifier relErrorAlias = new SqlIdentifier(
   * this.currentAggAlias.toString() + "_rel_error", SqlParserPos.ZERO); SqlBasicCall relErrorOp =
   * new SqlBasicCall( SqlStdOperatorTable.DIVIDE, new SqlNode[] {error, newAggOp},
   * SqlParserPos.ZERO); SqlBasicCall relError = new SqlBasicCall( SqlStdOperatorTable.AS, new
   * SqlNode[] {relErrorOp, relErrorAlias}, SqlParserPos.ZERO);
   * outerSelectExtraColumnList.add(relError); } catch (SqlParseException e) { e.printStackTrace();
   * }
   *
   * <p>aggAliasPairList.add(ImmutablePair.of(this.currentAggAlias, errorAlias)); } } } else { //
   * itme may not be AQP-incompatible agg and we need to take it into account AggTypeDetector
   * typeDetector = new AggTypeDetector(); item.accept(typeDetector);
   *
   * <p>SqlOperator aggType = typeDetector.getAggType(); if (aggType != null) { nonAQPAggList.put(i,
   * aggType); } } } // end for
   */
}
