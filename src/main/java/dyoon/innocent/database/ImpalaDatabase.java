package dyoon.innocent.database;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import dyoon.innocent.AQPInfo;
import dyoon.innocent.Args;
import dyoon.innocent.Query;
import dyoon.innocent.Sample;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.pmw.tinylog.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 10/23/18. */
public class ImpalaDatabase extends Database implements DatabaseImpl {

  private Map<String, Object> cache = new HashMap<>();

  public ImpalaDatabase(Connection conn) {
    this.conn = conn;
    this.cache = new HashMap<>();
  }

  public ImpalaDatabase(String host, String database, String user, String password) {
    try {
      Class.forName("com.cloudera.impala.jdbc41.Driver");
      String connString = String.format("jdbc:impala://%s/%s", host, database);
      this.conn = DriverManager.getConnection(connString, user, password);
      this.cache = new HashMap<>();
    } catch (ClassNotFoundException | SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public List<String> getTables() throws SQLException {
    final List<String> tables = new ArrayList<>();
    final ResultSet rs = this.executeQuery(String.format("SHOW TABLES"));
    while (rs.next()) {
      tables.add(rs.getString(1));
    }
    return tables;
  }

  @Override
  public List<String> getColumns(String table) throws SQLException {
    final List<String> columns = new ArrayList<>();
    final ResultSet rs = this.executeQuery(String.format("DESCRIBE %s", table));
    while (rs.next()) {
      columns.add(rs.getString(1));
    }
    return columns;
  }

  @Override
  public String getColumnType(String table, String column) throws SQLException {
    final ResultSet rs = this.executeQuery(String.format("DESCRIBE %s", table));
    while (rs.next()) {
      if (rs.getString(1).equalsIgnoreCase(column)) {
        return rs.getString(2);
      }
    }
    return null;
  }

  @Override
  public void createStratifiedSample(String database, Sample s) throws SQLException {
    this.createStratifiedSample(database, database, s);
  }

  @Override
  public void createStratifiedSample(String targetDatabase, String sourceDatabase, Sample s)
      throws SQLException {
    String sampleTable = s.getSampleTableName();
    String sampleStatTable = s.getSampleTableName() + "___stat";
    String sourceTable = s.getTable();

    long maxGroupSize = this.getMaxGroupSize(s.getTable(), s.getColumnSet());
    if (maxGroupSize != -1 && maxGroupSize < s.getMinRows()) {
      Logger.warn(
          "Sample '{}' will not be created as its max group size is {} when specified size is {}",
          s.getSampleTableName(),
          maxGroupSize,
          s.getMinRows());
      return;
    }

    if (this.checkTableExists(sampleTable) && this.checkTableExists(sampleStatTable)) {
      Logger.info("Sample table '{}' already exists.", sampleTable);
      return;
    }

    this.createDatabaseIfNotExists(targetDatabase);

    final List<String> factTableColumns = this.getColumns(sourceTable);
    final SortedSet<String> sampleColumns = s.getColumnSet();
    final List<String> sampleColumnsWithFactPrefix = new ArrayList<>();
    for (final String column : sampleColumns) {
      sampleColumnsWithFactPrefix.add(String.format("fact.%s", column));
    }

    final String sampleQCSClause = Joiner.on(",").join(sampleColumnsWithFactPrefix);

    final String createSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s LIKE %s.%s STORED as parquet",
            targetDatabase, sampleTable, sourceDatabase, sourceTable);

    this.execute(createSql);

    final String insertSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s FROM "
                + "(SELECT fact.*, row_number() OVER (PARTITION BY %s ORDER BY %s) as rownum, "
                + "count(*) OVER (PARTITION BY %s ORDER BY %s) as groupsize, "
                + "%d as target_group_sample_size "
                + "FROM %s.%s as fact "
                + "ORDER BY rand()) tmp "
                + "WHERE tmp.rownum <= %d",
            targetDatabase,
            sampleTable,
            Joiner.on(",").join(factTableColumns),
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            s.getMinRows(),
            sourceDatabase,
            sourceTable,
            s.getMinRows());
    System.err.println(String.format("Executing: %s", insertSql));

    this.execute(insertSql);

    List<String> selectColumns = new ArrayList<>();
    List<String> statTableColumns = new ArrayList<>();
    List<String> joinColumns = new ArrayList<>();
    for (String column : sampleColumns) {
      String type = this.getColumnType(sourceTable, column);
      statTableColumns.add(String.format("%s %s", column, type));
      joinColumns.add(String.format("fact.%s = sample.%s", column, column));
      selectColumns.add(String.format("fact.%s as %s", column, column));
    }

    final String createStatSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s "
                + "(%s, groupsize bigint, actualsize bigint) STORED AS parquet",
            targetDatabase, sampleStatTable, Joiner.on(",").join(statTableColumns));
    System.err.println(String.format("Executing: %s", createStatSql));
    this.execute(createStatSql);

    String selectClause = Joiner.on(",").join(selectColumns);
    String groupByClause = Joiner.on(",").join(sampleColumns);
    String joinClause = Joiner.on(" AND ").join(joinColumns);
    final String insertStatSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s, sample.groupsize as samplesize, "
                + "fact.groupsize as actualsize FROM "
                + "(SELECT %s, count(*) as groupsize FROM %s.%s GROUP BY %s) sample, "
                + "(SELECT %s, count(*) as groupsize FROM %s.%s GROUP BY %s) fact "
                + "WHERE %s",
            targetDatabase,
            sampleStatTable,
            selectClause,
            groupByClause,
            targetDatabase,
            sampleTable,
            groupByClause,
            groupByClause,
            sourceDatabase,
            sourceTable,
            groupByClause,
            joinClause);
    System.err.println(String.format("Executing: %s", insertStatSql));
    this.execute(insertStatSql);

    this.execute(String.format("COMPUTE STATS %s.%s", targetDatabase, sampleTable));
    this.execute(String.format("COMPUTE STATS %s.%s", targetDatabase, sampleStatTable));
  }

  @Override
  public long getMaxGroupSize(String table, Set<String> groupBys) throws SQLException {
    String key = table + "-" + Joiner.on(",").join(groupBys) + "-maxgroupsize";
    if (cache.containsKey(key)) {
      return (Long) cache.get(key);
    }
    List<String> notNullCond = new ArrayList<>();
    for (String groupBy : groupBys) {
      notNullCond.add(String.format("%s is not null", groupBy));
    }

    String sql =
        String.format(
            "SELECT MAX(groupsize) FROM "
                + "(SELECT count(*) as groupsize FROM %s WHERE %s GROUP BY %s) tmp",
            table, Joiner.on(" AND ").join(notNullCond), Joiner.on(",").join(groupBys));
    ResultSet rs = this.executeQuery(sql);
    if (rs.next()) {
      long val = rs.getLong(1);
      cache.put(key, val);
      return val;
    }
    return -1;
  }

  @Override
  public double runQueryAndSaveResult(Query q, Args args) throws SQLException {
    String resultTable = q.getResultTableName();
    if (this.checkTableExists(resultTable) && !args.isOverwrite() && !args.isMeasureTime()) {
      return 0;
    }

    if (args.isOverwrite() || args.isMeasureTime()) {
      this.execute(String.format("DROP TABLE IF EXISTS %s", resultTable));
    }

    String sql =
        String.format("CREATE TABLE %s STORED AS parquet AS %s", resultTable, q.getQuery());
    sql = sql.replaceAll("ASYMMETRIC", "");
    sql = sql.replaceAll("LIMIT 100", "");
    sql = sql.replaceAll("limit 100", "");
    sql = sql.replaceAll("ROWS ONLY", "");
    sql = sql.replaceAll("FETCH NEXT", "LIMIT");

    if (args.isMeasureTime()) {
      this.clearCache(args.getClearCacheScript());
    }
    Stopwatch watch = Stopwatch.createStarted();
    this.execute(sql);
    watch.stop();
    return watch.elapsed(TimeUnit.MILLISECONDS);
  }

  @Override
  public double runQueryWithSampleAndSaveResult(AQPInfo info, Args args) throws SQLException {
    String resultTable = info.getAQPResultTableName();
    if (this.checkTableExists(resultTable) && !args.isOverwrite() && !args.isMeasureTime()) {
      return 0;
    }

    if (args.isOverwrite() || args.isMeasureTime()) {
      this.execute(String.format("DROP TABLE IF EXISTS %s", resultTable));
    }

    String sql = "";
    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT);
    if (info.getAqpNode() instanceof SqlWith) {
      List<String> withItems = new ArrayList<>();
      SqlWith with = (SqlWith) info.getAqpNode();
      for (SqlNode node : with.withList) {
        String withSql = node.toSqlString(dialect).toString();
        withSql = withSql.replaceAll("AS SELECT", "AS (SELECT");
        withSql += ")";
        withItems.add(withSql);
      }
      String query = with.body.toSqlString(dialect).toString();
      sql =
          String.format(
              "CREATE TABLE %s STORED AS parquet AS WITH %s %s",
              resultTable, Joiner.on(",").join(withItems), query);
    } else {
      String query = info.getAqpNode().toSqlString(dialect).toString();
      sql = String.format("CREATE TABLE %s STORED AS parquet AS %s", resultTable, query);
    }
    sql = sql.replaceAll("ASYMMETRIC", "");
    sql = sql.replaceAll("LIMIT 100", "");
    sql = sql.replaceAll("limit 100", "");
    sql = sql.replaceAll("ROWS ONLY", "");
    sql = sql.replaceAll("FETCH NEXT", "LIMIT");

    if (args.isMeasureTime()) {
      this.clearCache(args.getClearCacheScript());
    }
    Stopwatch watch = Stopwatch.createStarted();
    this.execute(sql);
    watch.stop();
    return watch.elapsed(TimeUnit.MILLISECONDS);
  }

  @Override
  public void createDatabaseIfNotExists(String database) throws SQLException {
    String sql = String.format("CREATE DATABASE IF NOT EXISTS %s", database);
    this.execute(database);
  }
}
