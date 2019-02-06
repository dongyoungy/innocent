package dyoon.innocent.database;

import dyoon.innocent.AQPInfo;
import dyoon.innocent.Args;
import dyoon.innocent.Query;
import dyoon.innocent.StratifiedSample;
import dyoon.innocent.UniformSample;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.PartitionCandidate;
import dyoon.innocent.data.Prejoin;
import dyoon.innocent.data.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Created by Dong Young Yoon on 10/23/18. */
public interface DatabaseImpl {

  List<String> getTables() throws SQLException;

  List<String> getTables(String database) throws SQLException;

  List<String> getColumns(String table) throws SQLException;

  List<String> getColumns(String database, String table) throws SQLException;

  String getColumnType(String table, String column) throws SQLException;

  boolean checkTableExists(String table) throws SQLException;

  boolean checkTableExists(String database, String table) throws SQLException;

  void createStratifiedSample(String targetDatabase, String sourceDatabase, StratifiedSample s)
      throws SQLException;

  void createStratifiedSample(String database, StratifiedSample s) throws SQLException;

  void createUniformSample(String targetDatabase, String sourceDatabase, UniformSample s)
      throws SQLException;

  void createDatabaseIfNotExists(String database) throws SQLException;

  void constructPrejoinWithUniformSample(
      String targetDatabase, String sourceDatabase, Prejoin p, double ratio) throws SQLException;

  void buildPartitionTable(PartitionCandidate candidate) throws SQLException;

  long getMaxGroupSize(String table, Set<String> groupBys) throws SQLException;

  long getTableSize(Table table) throws SQLException;

  long calculateNumDistinct(Column column) throws SQLException;

  double runQueryAndSaveResult(Query q, Args args) throws SQLException;

  double runQueryWithSampleAndSaveResult(AQPInfo aqp, Args args) throws SQLException;

  ResultSet executeQuery(String sql);

  boolean execute(String sql);

  void clearCache(String script);

  String getCatalog() throws SQLException;

  String getRandomFunction();

  Set<Table> getAllTableAndColumns(String database) throws SQLException;

  String getCurrentTimestamp();

  void calculateStatsForPartitionCandidate(PartitionCandidate candidate) throws SQLException;

  Set<PartitionCandidate> getAvailablePartitionedTables() throws SQLException;
}
