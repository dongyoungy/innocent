package dyoon.innocent.data;

import dyoon.innocent.database.DatabaseImpl;

import java.sql.ResultSet;
import java.sql.SQLException;

/** Created by Dong Young Yoon on 2018-12-15. */
public class Column {
  private String table;
  private String name;
  private String type;
  private long numDistinctValues;

  private Column() {
    // for JSON
  }

  public Column(Table table, String name, String type) {
    this.table = table.getName();
    this.name = name;
    this.type = type;
  }

  // copy constructor
  public Column(Column c) {
    this.table = c.table;
    this.name = c.name;
    this.type = c.type;
  }

  @Override
  public String toString() {
    return String.format("{%s, %s}", name, type);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTable() {
    return table;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public long getNumDistinctValues() {
    return numDistinctValues;
  }

  public void setNumDistinctValues(long numDistinctValues) {
    this.numDistinctValues = numDistinctValues;
  }

  public void calculateNumDistinctValue(DatabaseImpl database) throws SQLException {
    String sql = String.format("SELECT COUNT(DISTINCT %s) FROM %s", name, table);

    ResultSet rs = database.executeQuery(sql);
    if (rs.next()) {
      numDistinctValues = rs.getLong(1);
    }
  }

  @Override
  public int hashCode() {
    return table.hashCode() + name.hashCode() + type.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Column) {
      Column other = (Column) obj;
      return table.equals(other.table) && name.equals(other.name) && type.equals(other.type);
    }
    return false;
  }
}
