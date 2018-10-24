package dyoon.innocent.database;

import com.google.common.base.Joiner;
import dyoon.innocent.Sample;
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
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public List<String> getTables() throws SQLException {
    final List<String> tables = new ArrayList<>();
    final ResultSet rs = this.conn.createStatement().executeQuery(String.format("SHOW TABLES"));
    while (rs.next()) {
      tables.add(rs.getString(1));
    }
    return tables;
  }

  @Override
  public List<String> getColumns(String table) throws SQLException {
    final List<String> columns = new ArrayList<>();
    final ResultSet rs =
        this.conn.createStatement().executeQuery(String.format("DESCRIBE %s", table));
    while (rs.next()) {
      columns.add(rs.getString(1));
    }
    return columns;
  }

  @Override
  public String getColumnType(String table, String column) throws SQLException {
    final ResultSet rs =
        this.conn.createStatement().executeQuery(String.format("DESCRIBE %s", table));
    while (rs.next()) {
      if (rs.getString(1).equalsIgnoreCase(column)) {
        return rs.getString(2);
      }
    }
    return null;
  }

  @Override
  public void createStratifiedSample(String database, Sample s) throws SQLException {
    String sampleTable = s.getSampleTableName();
    String sampleStatTable = s.getSampleTableName() + "__stat";
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
            database, sampleTable, database, sourceTable);

    this.conn.createStatement().execute(createSql);

    final String insertSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s FROM "
                + "(SELECT fact.*, row_number() OVER (PARTITION BY %s ORDER BY %s) as rownum, "
                + "count(*) OVER (PARTITION BY %s ORDER BY %s) as groupsize, "
                + "%d as target_group_sample_size "
                + "FROM %s as fact "
                + "ORDER BY rand()) tmp "
                + "WHERE tmp.rownum <= %d",
            database,
            sampleTable,
            Joiner.on(",").join(factTableColumns),
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            sampleQCSClause,
            s.getMinRows(),
            sourceTable,
            s.getMinRows());
    System.err.println(String.format("Executing: %s", insertSql));

    this.conn.createStatement().execute(insertSql);

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
            database, sampleStatTable, Joiner.on(",").join(statTableColumns));
    System.err.println(String.format("Executing: %s", createStatSql));
    this.conn.createStatement().execute(createStatSql);

    String selectClause = Joiner.on(",").join(selectColumns);
    String groupByClause = Joiner.on(",").join(sampleColumns);
    String joinClause = Joiner.on(" AND ").join(joinColumns);
    final String insertStatSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s, sample.groupsize as samplesize, "
                + "fact.groupsize as actualsize FROM "
                + "(SELECT %s, count(*) as groupsize FROM %s GROUP BY %s) sample, "
                + "(SELECT %s, count(*) as groupsize FROM %s GROUP BY %s) fact "
                + "WHERE %s",
            database,
            sampleStatTable,
            selectClause,
            groupByClause,
            sampleTable,
            groupByClause,
            groupByClause,
            sourceTable,
            groupByClause,
            joinClause);
    System.err.println(String.format("Executing: %s", insertStatSql));
    this.conn.createStatement().execute(insertStatSql);

    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleTable));
    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleStatTable));
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
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) {
      long val = rs.getLong(1);
      cache.put(key, val);
      return val;
    }
    return -1;
  }
}
