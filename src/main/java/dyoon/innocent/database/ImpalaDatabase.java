package dyoon.innocent.database;

import com.google.common.base.Joiner;
import dyoon.innocent.Sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

/** Created by Dong Young Yoon on 10/23/18. */
public class ImpalaDatabase extends Database implements DatabaseImpl {

  public ImpalaDatabase(Connection conn) {
    this.conn = conn;
  }

  public ImpalaDatabase(String host, String database, String user, String password) {
    try {
      Class.forName("com.cloudera.impala.jdbc41.Driver");
      String connString = String.format("jdbc:impala://%s/%s", host, database);
      this.conn = DriverManager.getConnection(connString, user, password);
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

    List<String> statTableColumns = new ArrayList<>();
    for (String column : sampleColumns) {
      String type = this.getColumnType(sourceTable, column);
      statTableColumns.add(String.format("%s %s", column, type));
    }

    final String createStatSql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (%s, groupsize int) STORED AS parquet",
            database, sampleStatTable, Joiner.on(",").join(statTableColumns));
    System.err.println(String.format("Executing: %s", createStatSql));
    this.conn.createStatement().execute(createStatSql);

    String groupByClause = Joiner.on(",").join(sampleColumns);
    final String insertStatSql =
        String.format(
            "INSERT OVERWRITE TABLE %s.%s SELECT %s, count(*) as groupsize FROM %s GROUP BY %s",
            database, sampleStatTable, groupByClause, sampleTable, groupByClause);
    System.err.println(String.format("Executing: %s", insertStatSql));
    this.conn.createStatement().execute(insertStatSql);

    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleTable));
    this.conn
        .createStatement()
        .execute(String.format("COMPUTE STATS %s.%s", database, sampleStatTable));
  }
}
