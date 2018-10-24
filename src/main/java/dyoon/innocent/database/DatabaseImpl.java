package dyoon.innocent.database;

import dyoon.innocent.Sample;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Created by Dong Young Yoon on 10/23/18. */
public interface DatabaseImpl {

  List<String> getTables() throws SQLException;

  List<String> getColumns(String table) throws SQLException;

  String getColumnType(String table, String column) throws SQLException;

  boolean checkTableExists(String table) throws SQLException;

  boolean checkTableExists(String database, String table) throws SQLException;

  void createStratifiedSample(String database, Sample s) throws SQLException;

  long getMaxGroupSize(String table, Set<String> groupBys) throws SQLException;
}
