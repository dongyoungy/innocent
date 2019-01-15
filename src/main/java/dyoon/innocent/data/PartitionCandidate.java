package dyoon.innocent.data;

import dyoon.innocent.database.DatabaseImpl;

import java.util.HashSet;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-01-08. */
public class PartitionCandidate {

  // target fact table
  private Table table;
  private Set<Column> columnSet;
  private long numDistinctValues;
  private long maxPartitionSize;
  private long minPartitionSize;

  @Override
  public int hashCode() {
    return table.hashCode() + columnSet.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PartitionCandidate) {
      PartitionCandidate other = (PartitionCandidate) obj;
      return table.equals(other.table) && columnSet.equals(other.columnSet);
    }
    return false;
  }

  public PartitionCandidate(Table table) {
    this.table = table;
    this.columnSet = new HashSet<>();
  }

  public PartitionCandidate(Table table, Set<Column> columnSet) {
    this.table = table;
    this.columnSet = columnSet;
  }

  public void calculateStats(DatabaseImpl database) {}

  public long getNumDistinctValues() {
    return numDistinctValues;
  }

  public void setNumDistinctValues(long numDistinctValues) {
    this.numDistinctValues = numDistinctValues;
  }

  public long getMaxParitionSize() {
    return maxPartitionSize;
  }

  public void setMaxParitionSize(long maxParitionSize) {
    this.maxPartitionSize = maxParitionSize;
  }

  public long getMinPartitionSize() {
    return minPartitionSize;
  }

  public void setMinPartitionSize(long minPartitionSize) {
    this.minPartitionSize = minPartitionSize;
  }
}
