package dyoon.innocent.data;

import dyoon.innocent.Query;

import java.util.HashSet;
import java.util.Set;

/** Created by Dong Young Yoon on 2019-01-08. */
public class PartitionCandidate {

  // prejoin for creating partitioned table
  private Prejoin prejoin;
  private Set<Column> columnSet;
  private long numDistinctValues;
  private long maxPartitionSize;
  private long minPartitionSize;
  private long avgPartitionSize;

  private int numQueryMeetingIOBound;
  private double totalIOReductionForOtherQueries;
  private Set<Query> queriesMeetingIOBounds;

  public PartitionCandidate(Prejoin p) {
    this.prejoin = p;
    this.columnSet = new HashSet<>();
    this.queriesMeetingIOBounds = new HashSet<>();
  }

  public PartitionCandidate(Prejoin p, Set<Column> columnSet) {
    this.prejoin = p;
    this.columnSet = columnSet;
    this.numDistinctValues = 1;
    for (Column column : columnSet) {
      this.numDistinctValues *= column.getNumDistinctValues();
    }
    this.queriesMeetingIOBounds = new HashSet<>();
  }

  @Override
  public int hashCode() {
    return prejoin.hashCode() + columnSet.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PartitionCandidate) {
      PartitionCandidate other = (PartitionCandidate) obj;
      return prejoin.equals(other.prejoin) && columnSet.equals(other.columnSet);
    }
    return false;
  }

  public Set<Column> findUsedColumns(
      Table fact,
      Table dim,
      Set<UnorderedPair<Column>> joinColumnPairs,
      Set<Predicate> predicates) {
    Set<Column> usedColumns = new HashSet<>();
    if (prejoin.contains(fact, dim, joinColumnPairs)) {
      for (Predicate pred : predicates) {
        Column col = pred.getColumn();
        if (columnSet.contains(col)) {
          usedColumns.add(col);
        }
      }
    }
    return usedColumns;
  }

  public Prejoin getPrejoin() {
    return prejoin;
  }

  public Set<Column> getColumnSet() {
    return columnSet;
  }

  public long getNumDistinctValues() {
    return numDistinctValues;
  }

  public void setNumDistinctValues(long numDistinctValues) {
    this.numDistinctValues = numDistinctValues;
  }

  public long getMaxParitionSize() {
    return maxPartitionSize;
  }

  public void setMaxPartitionSize(long maxParitionSize) {
    this.maxPartitionSize = maxParitionSize;
  }

  public long getMinPartitionSize() {
    return minPartitionSize;
  }

  public void setMinPartitionSize(long minPartitionSize) {
    this.minPartitionSize = minPartitionSize;
  }

  public long getAvgPartitionSize() {
    return avgPartitionSize;
  }

  public void setAvgPartitionSize(long avgPartitionSize) {
    this.avgPartitionSize = avgPartitionSize;
  }

  public int getNumQueryMeetingIOBound() {
    return numQueryMeetingIOBound;
  }

  public void setNumQueryMeetingIOBound(int numQueryMeetingIOBound) {
    this.numQueryMeetingIOBound = numQueryMeetingIOBound;
  }

  public double getTotalIOReductionForOtherQueries() {
    return totalIOReductionForOtherQueries;
  }

  public void setTotalIOReductionForOtherQueries(double totalIOReductionForOtherQueries) {
    this.totalIOReductionForOtherQueries = totalIOReductionForOtherQueries;
  }

  public void addQuery(Query q) {
    this.queriesMeetingIOBounds.add(q);
  }

  public void clearQueries() {
    this.queriesMeetingIOBounds.clear();
  }

  public Set<Query> getQueriesMeetingIOBounds() {
    return queriesMeetingIOBounds;
  }
}
