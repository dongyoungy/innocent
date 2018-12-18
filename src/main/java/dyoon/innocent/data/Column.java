package dyoon.innocent.data;

/** Created by Dong Young Yoon on 2018-12-15. */
public class Column {
  private Table table;
  private String name;
  private String type;

  public Column(Table table, String name, String type) {
    this.table = table;
    this.name = name;
    this.type = type;
  }

  @Override
  public String toString() {
    return String.format("{%s, %s, %s}", table.getName(), name, type);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public int hashCode() {
    return table.hashCode() + name.hashCode() + type.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
