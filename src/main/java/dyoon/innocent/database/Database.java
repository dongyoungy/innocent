package dyoon.innocent.database;

import dyoon.innocent.InnocentMeta;
import org.pmw.tinylog.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Created by Dong Young Yoon on 10/23/18. */
public abstract class Database implements DatabaseImpl {

  public static final String RESULT_DATABASE_SUFFIX = "_result";
  protected Connection conn;
  protected InnocentMeta meta;
  protected String sourceDatabase;
  protected String innocentDatabase;

  @Override
  public boolean checkTableExists(String table) throws SQLException {
    final DatabaseMetaData dbm = this.conn.getMetaData();
    final ResultSet tables = dbm.getTables(null, null, table, null);
    return tables.next();
  }

  @Override
  public boolean checkTableExists(String database, String table) throws SQLException {
    final DatabaseMetaData dbm = this.conn.getMetaData();
    final ResultSet tables = dbm.getTables(null, database, table, null);
    return tables.next();
  }

  @Override
  public ResultSet executeQuery(String sql) {
    Logger.debug("Executing: {}", sql);
    ResultSet rs = null;
    try {
      rs = conn.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      Logger.error("Error while executing: {}", sql);
      e.printStackTrace();
    }
    return rs;
  }

  @Override
  public boolean execute(String sql) {
    Logger.debug("Executing: {}", sql);
    try {
      return conn.createStatement().execute(sql);
    } catch (SQLException e) {
      Logger.error("Error while executing: {}", sql);
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public String getCatalog() throws SQLException {
    return conn.getCatalog();
  }

  @Override
  public void clearCache(String script) {
    try {
      Process p = new ProcessBuilder("/bin/bash", script).start();
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public String getSourceDatabase() {
    return sourceDatabase;
  }

  public String getInnocentDatabase() {
    return innocentDatabase;
  }
}
