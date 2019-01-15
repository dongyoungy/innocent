package dyoon.innocent.data;

/** Created by Dong Young Yoon on 2019-01-13. */
public class AliasedTable {

  private Table table;
  private String alias;

  public AliasedTable(Table table, String alias) {
    this.table = table;
    this.alias = alias;
  }

  public AliasedTable(Table table) {
    this.table = table;
  }

  @Override
  public String toString() {
    return (alias != null) ? table.getName() + " AS " + alias : table.getName();
  }

  public Table getTable() {
    return table;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }
}
