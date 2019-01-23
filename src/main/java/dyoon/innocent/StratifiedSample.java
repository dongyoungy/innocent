package dyoon.innocent;

import com.google.common.base.Joiner;
import dyoon.innocent.data.Table;

import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 10/23/18. */
public class StratifiedSample extends Sample {

  private SortedSet<String> columnSet;
  private long minRows;

  public StratifiedSample(Table table, SortedSet<String> columnSet, long minRows) {
    this.table = table;
    this.columnSet = columnSet;
    this.minRows = minRows;
    this.id = 1;
  }

  public StratifiedSample(Table table, List<String> columnList, long minRows) {
    this.table = table;
    this.columnSet = new TreeSet<>(columnList);
    this.minRows = minRows;
    this.id = 1;
  }

  public StratifiedSample(Table table, Collection<String> columnSet, long minRows, int id) {
    this.table = table;
    this.columnSet = new TreeSet<>(columnSet);
    this.minRows = minRows;
    this.id = id;
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
