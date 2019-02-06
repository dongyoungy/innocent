package dyoon.innocent.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import dyoon.innocent.Query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 2019-01-08. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionCandidate {

  // prejoin for creating partitioned table
  private Prejoin prejoin;
  private Set<Column> columnSet;

  private double IOBound;
  private int rank;

  private long numDistinctValues;
  private long maxPartitionSize;
  private long minPartitionSize;
  private long avgPartitionSize;

  private int numQueryMeetingIOBound;
  private double totalIOReductionForOtherQueries;
  private Set<Query> queriesMeetingIOBounds;
  private Set<Query> effectiveQueries;

  private long queryCost;

  @JsonIgnore private com.google.common.collect.Table<Query, Table, Long> queryCostTable;

  private PartitionCandidate() {
    // default constructor for JSON deserialization
    this.columnSet = new HashSet<>();
    this.queriesMeetingIOBounds = new HashSet<>();
    this.effectiveQueries = new HashSet<>();
    this.queryCostTable = HashBasedTable.create();
  }

  public PartitionCandidate(Prejoin p) {
    this.prejoin = p;
    this.columnSet = new HashSet<>();
    this.queriesMeetingIOBounds = new HashSet<>();
    this.effectiveQueries = new HashSet<>();
    this.queryCostTable = HashBasedTable.create();
  }

  public PartitionCandidate(Prejoin p, Set<Column> columnSet) {
    this.prejoin = p;
    this.columnSet = columnSet;
    this.numDistinctValues = 1;
    for (Column column : columnSet) {
      this.numDistinctValues *= column.getNumDistinctValues();
    }
    this.queriesMeetingIOBounds = new HashSet<>();
    this.effectiveQueries = new HashSet<>();
    this.queryCostTable = HashBasedTable.create();
  }

  public String getPartitionTableName() {
    SortedSet<String> partitionColumnNames = new TreeSet<>();
    Set<String> dimTables = new HashSet<>();
    for (Column column : columnSet) {
      partitionColumnNames.add(column.getName());
      dimTables.add(column.getTable());
    }

    Set<UnorderedPair<Column>> joinColumnPairs = new HashSet<>();
    for (FactDimensionJoin join : prejoin.getJoinSet()) {
      if (dimTables.contains(join.getDimensionTable().getName())) {
        joinColumnPairs.addAll(join.getJoinPairs());
      }
    }

    return String.format(
        "%s___part___%s___%d",
        prejoin.getFactTable().getName(),
        Joiner.on("__").join(partitionColumnNames),
        Math.abs(joinColumnPairs.hashCode()));
  }

  public static String getJsonString(PartitionCandidate pc) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(pc);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static PartitionCandidate createFromJsonString(String json) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, PartitionCandidate.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
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

  public Set<PredicateColumn> findUsedColumns(FactDimensionJoin join) {
    return findUsedColumns(
        join.getFactTable(), join.getDimensionTable(), join.getJoinPairs(), join.getPredicates());
  }

  public Set<PredicateColumn> findUsedColumns(
      Table fact,
      Table dim,
      Set<UnorderedPair<Column>> joinColumnPairs,
      Set<Predicate> predicates) {
    Set<PredicateColumn> usedColumns = new HashSet<>();
    if (prejoin.contains(fact, dim, joinColumnPairs)) {
      for (Predicate pred : predicates) {
        Column col = pred.getColumn();
        int multiplier = 1;
        if (pred instanceof InPredicate) {
          InPredicate inPred = (InPredicate) pred;
          multiplier = inPred.getValues().size();
        } else if (pred instanceof RangePredicate) {
          multiplier = (int) (col.getNumDistinctValues() * 0.25);
        }
        PredicateColumn newPredCol = new PredicateColumn(col, pred, multiplier);
        if (columnSet.contains(col)) {
          usedColumns.add(newPredCol);
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

  public void addEffectiveQuery(Query q) {
    this.effectiveQueries.add(q);
  }

  public void clearQueries() {
    this.queriesMeetingIOBounds.clear();
    this.effectiveQueries.clear();
  }

  public Set<Query> getQueriesMeetingIOBounds() {
    return queriesMeetingIOBounds;
  }

  public double getIOBound() {
    return IOBound;
  }

  public void setIOBound(double IOBound) {
    this.IOBound = IOBound;
  }

  public int getRank() {
    return rank;
  }

  public void setRank(int rank) {
    this.rank = rank;
  }

  public long getQueryCost() {
    return queryCost;
  }

  public void setQueryCost(long queryCost) {
    this.queryCost = queryCost;
  }

  public void setQueryCost(Query q, Table t, long cost) {
    this.queryCostTable.put(q, t, cost);
  }

  public long getQueryCost(Query q, Table t) {
    return this.queryCostTable.get(q, t);
  }

  public com.google.common.collect.Table<Query, Table, Long> getQueryCostTable() {
    return queryCostTable;
  }

  public Set<Query> getEffectiveQueries() {
    return effectiveQueries;
  }
}
