package dyoon.innocent;

import com.google.common.base.Joiner;

import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/23/18. */
public class Sample {

  public enum Type {
    UNIFORM,
    STRATIFIED
  }

  private Type type;
  private String table;
  private SortedSet<String> columnSet;
  private long minRows;
  private int id;

  public Sample(Type type, String table, SortedSet<String> columnSet, long minRows) {
    this.type = type;
    this.table = table;
    this.columnSet = columnSet;
    this.minRows = minRows;
    this.id = 1;
  }

  public Sample(Type type, String table, List<String> columnList, long minRows) {
    this.type = type;
    this.table = table;
    this.columnSet = new TreeSet<>(columnList);
    this.minRows = minRows;
    this.id = 1;
  }

  public Sample(Type type, String table, Collection<String> columnSet, long minRows, int id) {
    this.type = type;
    this.table = table;
    this.columnSet = new TreeSet<>(columnSet);
    this.minRows = minRows;
    this.id = id;
  }

  public Type getType() {
    return type;
  }

  public String getTable() {
    return table;
  }

  public SortedSet<String> getColumnSet() {
    return columnSet;
  }

  public long getMinRows() {
    return minRows;
  }

  public String getSampleTableName() {
    return String.format(
        "%s___st___%s___%d___%d", table, Joiner.on("__").join(columnSet), minRows, id);
  }
}
