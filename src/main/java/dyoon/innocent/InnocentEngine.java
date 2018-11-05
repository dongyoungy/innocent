package dyoon.innocent;

import com.google.common.base.Joiner;
import com.google.common.math.Stats;
import dyoon.innocent.database.DatabaseImpl;
import dyoon.innocent.query.AggregationColumnResolver;
import dyoon.innocent.query.AliasReplacer;
import dyoon.innocent.query.QueryTransformer;
import dyoon.innocent.query.QueryVisitor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;
import org.pmw.tinylog.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Created by Dong Young Yoon on 10/19/18. */
public class InnocentEngine {

  private String timeCreated;
  private boolean isSampleUsed = false;
  private DatabaseImpl database;
  private Map<String, Double> origRunTimeCache;

  public InnocentEngine(DatabaseImpl database, String timestamp) {
    this.database = database;
    this.isSampleUsed = false;
    this.timeCreated = timestamp;
  }

  public boolean isSampleUsed() {
    return isSampleUsed;
  }

  public SqlNode parse(String sql, QueryVisitor visitor)
      throws ClassNotFoundException, SqlParseException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(sql);
    SqlNode node = sqlParser.parseQuery();

    return node.accept(visitor);
  }

  public AQPInfo rewriteWithSample(Query q, Sample s)
      throws SqlParseException, ClassNotFoundException, SQLException {

    this.isSampleUsed = false;

    Class.forName("org.apache.calcite.jdbc.Driver");
    SqlParser sqlParser = SqlParser.create(q.getQuery());
    SqlNode node = sqlParser.parseQuery();

    if (s != null) {
      List<String> sampleTableColumns = database.getColumns(s.getTable());
      QueryTransformer transformer = new QueryTransformer(s, sampleTableColumns);
      SqlNode newNode = node.accept(transformer);

      newNode = removeOrderBy(newNode);

      if (transformer.approxCount() > 0) {
        this.isSampleUsed = true;
        List<Pair<SqlIdentifier, SqlIdentifier>> aggAliasPairList =
            transformer.getAggAliasPairList();
        List<SqlIdentifier> aggAliasList = new ArrayList<>();
        for (Pair<SqlIdentifier, SqlIdentifier> pair : aggAliasPairList) {
          aggAliasList.add(pair.getLeft());
        }

        AggregationColumnResolver resolver = new AggregationColumnResolver(aggAliasList);
        newNode.accept(resolver);

        SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);
        q.setAqpQuery(newNode.toSqlString(dialect).toString());

        AQPInfo aqpInfo = new AQPInfo(q, s, resolver.getExpressionList(), newNode);

        return aqpInfo;
      }
    }

    return null;
  }

  private SqlNode removeOrderBy(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      return orderBy.query;
    }
    return node;
  }

  public void runAQPQueryAndCompare(Query q, AQPInfo aqpInfo, Args args) throws SQLException {
    if (q.getAqpQuery().isEmpty()) {
      Logger.error("AQP query is empty for {}. Query will not be run.", q.getId());
      return;
    }

    if (args.isMeasureTime() && args.getClearCacheScript().isEmpty()) {
      Logger.error("You must provide a script for clearing cache to measure time.");
      return;
    }

    double origTime = 0;
    double sampleTime = 0;

    try {
      // run orig query
      if (origRunTimeCache.containsKey(q.getId())
          && database.checkTableExists(q.getResultTableName())) {
        origTime = origRunTimeCache.get(q.getId());
      } else {
        origTime = database.runQueryAndSaveResult(q, args);
        origRunTimeCache.put(q.getId(), origTime);
      }
      // run AQP query
      sampleTime = database.runQueryWithSampleAndSaveResult(aqpInfo, args);
    } catch (Exception e) {
      Logger.error(e);
      return;
    }

    String originalResultTableName = q.getResultTableName();
    String aqpResultTableName = aqpInfo.getAQPResultTableName();

    List<String> originalResultColumnNames = database.getColumns(originalResultTableName);
    List<String> aqpResultColumnNames = database.getColumns(aqpResultTableName);
    List<ColumnType> aqpResultColumnTypes = aqpInfo.getColumnTypeList();

    if (aqpResultColumnNames.size() != aqpResultColumnTypes.size()) {
      // if somehow this check fails, try again with result table columns
      aqpResultColumnTypes.clear();
      List<String> aggColumns = new ArrayList<>();
      for (int i = 0; i < aqpResultColumnNames.size(); ++i) {
        String colName = aqpResultColumnNames.get(i);
        if (aqpInfo.isColumnInExpression(colName)) {
          aqpResultColumnTypes.add(ColumnType.AGG);
          aggColumns.add(colName);
        } else if (colName.contains("_rel_error")) {
          aqpResultColumnTypes.add(ColumnType.REL_ERROR);
        } else if (colName.contains("_error")) {
          aqpResultColumnTypes.add(ColumnType.ERROR);
        } else {
          aqpResultColumnTypes.add(ColumnType.NON_AGG);
        }
      }
      for (String column : aggColumns) {
        boolean sanityCheck = false;
        for (String s : aqpResultColumnNames) {
          if (s.matches(String.format("%s_\\d+_rel_error", column))) {
            sanityCheck = true;
          }
        }
        if (!sanityCheck) {
          Logger.error(
              "# of column names and types do not match: {} != {}",
              aqpResultColumnNames.size(),
              aqpResultColumnTypes.size());
          return;
        }
      }

      //      if (!sanityCheck) {
      //        Logger.error(
      //            "# of column names and types do not match: {} != {}",
      //            aqpResultColumnNames.size(),
      //            aqpResultColumnTypes.size());
      //        return;
      //      }
    }

    // get column names and types
    List<String> nonAggOrigColumns = new ArrayList<>();
    List<String> aggOrigColumns = new ArrayList<>();
    List<String> nonAggAQPColumns = new ArrayList<>();
    List<String> aggAQPColumns = new ArrayList<>();
    List<String> errorColumns = new ArrayList<>();
    List<String> relErrorColumns = new ArrayList<>();

    for (int i = 0; i < aqpResultColumnNames.size(); ++i) {
      String name = aqpResultColumnNames.get(i);
      ColumnType type = aqpResultColumnTypes.get(i);

      switch (type) {
        case NON_AGG:
          String origName =
              originalResultColumnNames.contains(name) ? name : originalResultColumnNames.get(i);
          nonAggAQPColumns.add(name);
          nonAggOrigColumns.add(origName);
          break;
        case AGG:
          origName =
              originalResultColumnNames.contains(name) ? name : originalResultColumnNames.get(i);
          aggAQPColumns.add(name);
          aggOrigColumns.add(origName);
          break;
        case ERROR:
          errorColumns.add(name);
          break;
        case REL_ERROR:
          relErrorColumns.add(name);
          break;
        default:
          Logger.error("Unknown column type: {}", type);
          return;
      }
    }

    List<String> selectItems = new ArrayList<>();
    List<String> evalItems = new ArrayList<>();
    List<String> relErrors = new ArrayList<>();
    for (int i = 0; i < aggAQPColumns.size(); ++i) {
      String origCol = aggOrigColumns.get(i);
      String aqpCol = aggAQPColumns.get(i);
      //      String errCol = errorColumns.get(i);
      evalItems.add(
          String.format(
              "abs(quotient((s.%s - o.%s) * 100000, o.%s) / 100000)", aqpCol, origCol, origCol));
      //      relErrors.add(String.format("avg(s.%s / s.%s) as %s_rel_error", errCol, aqpCol,
      // aqpCol));
    }

    for (String col : relErrorColumns) {
      relErrors.add(String.format("avg(s.%s) as %s", col, col));
    }

    String sumEval = Joiner.on(" + ").join(evalItems);
    String avgPerError =
        String.format("(avg(%s) / %d) as avg_per_error", sumEval, evalItems.size());

    selectItems.add(avgPerError);
    selectItems.addAll(relErrors);

    String selectClause = Joiner.on(",").join(selectItems);
    String fromClause =
        String.format("%s as o, %s as s", originalResultTableName, aqpResultTableName);

    List<String> joinItems = new ArrayList<>();
    for (int i = 0; i < nonAggOrigColumns.size(); ++i) {
      String origCol = nonAggOrigColumns.get(i);
      String aqpCol = nonAggAQPColumns.get(i);
      joinItems.add(String.format("o.%s = s.%s", origCol, aqpCol));
    }
    String joinClause = Joiner.on(" AND ").join(joinItems);

    String evalSql = String.format("SELECT %s FROM %s", selectClause, fromClause);
    if (!joinClause.isEmpty()) {
      evalSql += String.format(" WHERE %s", joinClause);
    }
    String origGroupCountSql =
        String.format("SELECT count(*) as groupcount from %s", originalResultTableName);
    String aqpGroupCountSql =
        String.format("SELECT count(*) as groupcount from %s s", aqpResultTableName);
    if (!joinClause.isEmpty()) {
      aqpGroupCountSql += String.format(", %s o WHERE %s", originalResultTableName, joinClause);
    }

    long origGroupCount = 0, aqpGroupCount = 0;
    double[] errors = new double[selectItems.size()];

    ResultSet rs = database.executeQuery(origGroupCountSql);
    if (rs.next()) {
      origGroupCount = rs.getLong("groupcount");
    }
    rs.close();

    rs = database.executeQuery(aqpGroupCountSql);
    if (rs.next()) {
      aqpGroupCount = rs.getLong("groupcount");
    }
    rs.close();

    rs = database.executeQuery(evalSql);
    if (rs.next()) {
      for (int i = 0; i < errors.length; ++i) {
        errors[i] = rs.getDouble(i + 1);
      }
    }

    List<String> relErrorString = new ArrayList<>();
    for (int i = 0; i < relErrorColumns.size(); ++i) {
      relErrorString.add(
          String.format("%s = %.4f %%", relErrorColumns.get(i), errors[i + 1] * 100));
    }

    double avgRelError = Stats.meanOf(errors);
    double avgPercentError = errors[0];
    double missingGroupRatio = (double) (origGroupCount - aqpGroupCount) / (double) origGroupCount;

    File resultDir = new File(String.format("./results/%s/", this.timeCreated));
    resultDir.mkdirs();

    File resultFile =
        new File(
            String.format(
                "./results/%s/%s", this.timeCreated, aqpInfo.getSample().getSampleTableName()));
    FileWriter fw = null;
    PrintWriter pw = null;
    try {
      fw = new FileWriter(resultFile, true);
      pw = new PrintWriter(fw);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (pw != null) {
      pw.println(
          String.format(
              "%s,%.4f,%.4f,%.4f,%.4f,%.4f",
              q.getId(),
              missingGroupRatio,
              avgPercentError,
              avgRelError,
              origTime / 1000,
              sampleTime / 1000));
      pw.flush();
    }

    Logger.info(
        String.format(
            "query '%s' with sample %s gives:\n\tmissing group ratio = %.4f %% (%d out of %d), "
                + "avg. percent error = %.4f %%, avg. rel error = %.4f %%, "
                + "orig time = %.4f s, sample time = %.4f s."
                + "\n\t%s",
            q.getId(),
            aqpInfo.getSample().getSampleTableName(),
            missingGroupRatio * 100,
            aqpGroupCount,
            origGroupCount,
            avgPercentError * 100,
            avgRelError * 100,
            origTime / 1000,
            sampleTime / 1000,
            Joiner.on(", ").join(relErrorString)));
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
