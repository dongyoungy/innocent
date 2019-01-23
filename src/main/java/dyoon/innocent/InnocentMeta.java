package dyoon.innocent;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import dyoon.innocent.database.DatabaseImpl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Dong Young Yoon on 2019-01-16.
 *
 * <p>represents Metadata table (simple keyval with timestamp) that innocent uses
 */
public class InnocentMeta {

  private static Table<String, DatabaseImpl, InnocentMeta> instanceTable = HashBasedTable.create();
  private static final String META_TABLE_NAME = "innocentmeta";

  private Map<String, String> cache;
  private String database;
  private DatabaseImpl databaseImpl;

  private InnocentMeta(String database, DatabaseImpl databaseImpl) {
    this.cache = new HashMap<>();
    this.database = database;
    this.databaseImpl = databaseImpl;
  }

  public static synchronized InnocentMeta getInstance(String database, DatabaseImpl databaseImpl) {
    if (!instanceTable.contains(database, databaseImpl)) {
      InnocentMeta newMeta = new InnocentMeta(database, databaseImpl);
      newMeta.initialize();
      instanceTable.put(database, databaseImpl, newMeta);
    }
    return instanceTable.get(database, databaseImpl);
  }

  private void initialize() {
    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s " + "(key string, val string, ts timestamp)",
            database, META_TABLE_NAME);

    databaseImpl.execute(sql);

    // read metadata into cache in memory
    sql = String.format("SELECT * FROM %s.%s ORDER BY ts DESC", database, META_TABLE_NAME);
    ResultSet rs = databaseImpl.executeQuery(sql);
    try {
      while (rs.next()) {
        if (!cache.containsKey(rs.getString(1))) {
          cache.put(rs.getString(1), rs.getString(2));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // this overrides the old entry if exists
  public synchronized void put(String key, String val) {
    String sql =
        String.format(
            "INSERT INTO %s.%s VALUES ('%s', '%s', %s)",
            database, META_TABLE_NAME, key, val, databaseImpl.getCurrentTimestamp());

    databaseImpl.execute(sql);
    cache.put(key, val);
  }

  // returns the value associated with the key
  public synchronized String get(String key) {
    if (cache.containsKey(key)) {
      return cache.get(key);
    }

    String sql =
        String.format(
            "SELECT val FROM %s.%s WHERE key = '%s' ORDER BY ts DESC LIMIT 1",
            database, META_TABLE_NAME, key);

    try {
      ResultSet rs = databaseImpl.executeQuery(sql);
      if (rs != null) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }
}
