package dyoon.innocent.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Created by Dong Young Yoon on 10/23/18. */
public abstract class Database implements DatabaseImpl {

  protected Connection conn;

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
}
