package dyoon.innocent.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.calcite.sql.SqlNode;

import java.util.HashSet;
import java.util.Set;

/** Created by Dong Young Yoon on 2018-12-15. */
public class Table implements Comparable<Table> {

  protected String name;
  @JsonIgnore protected Set<Column> columns;
  @JsonIgnore protected SqlNode correspondingNode;

  private Table() {
    // for JSON
  }

  public Table(String name) {
    this.name = name;
    this.columns = new HashSet<>();
  }

  public Table(String name, Set<Column> columns) {
    this.name = name;
    this.columns = columns;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Table) {
      Table other = (Table) obj;
      return other.name.equals(this.name);
    }
    return false;
  }

  public Table copy() {
    return new Table(name, columns);
  }

  public void addColumnSet(Set<Column> set) {
    columns.addAll(set);
  }

  public void addColumn(Column c) {
    columns.add(c);
  }

  public String getName() {
    return name;
  }

  public Set<Column> getColumns() {
    return columns;
  }

  public SqlNode getCorrespondingNode() {
    return correspondingNode;
  }

  public void setCorrespondingNode(SqlNode correspondingNode) {
    this.correspondingNode = correspondingNode;
  }

  @Override
  public int compareTo(Table o) {
    return name.compareTo(o.name);
  }
}
