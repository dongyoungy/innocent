package dyoon.innocent;

import com.google.common.base.Joiner;

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

  public Sample(Type type, String table, SortedSet<String> columnSet, long minRows) {
    this.type = type;
    this.table = table;
    this.columnSet = columnSet;
    this.minRows = minRows;
  }

  public Sample(Type type, String table, List<String> columnList, long minRows) {
    this.type = type;
    this.table = table;
    this.columnSet = new TreeSet<>(columnList);
    this.minRows = minRows;
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
    return String.format("%s__st__%s__%d", table, Joiner.on("_").join(columnSet), minRows);
  }
}
