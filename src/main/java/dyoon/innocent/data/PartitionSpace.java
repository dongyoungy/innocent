package dyoon.innocent.data;

import dyoon.innocent.Query;
import dyoon.innocent.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 2018-12-16. */
public class PartitionSpace {
  private Table factTable;
  private Column column;
  private List<Partition> partitions;
  private SortedSet<Double> boundaries;
  private Set<Query> queries;

  public PartitionSpace(Table factTable, Column column) {
    this.factTable = factTable;
    this.column = column;
    this.partitions = new ArrayList<>();
    this.boundaries = new TreeSet<>();
    this.queries = new HashSet<>();
  }

  public void addQuery(Query q) {
    this.queries.add(q);
  }

  public Set<Query> getQueries() {
    return queries;
  }

  // create partitions based on boundaries
  public void createPartitions() {
    List<Double> boundaryList = new ArrayList<>(boundaries);
    for (int i = 0; i < boundaryList.size(); ++i) {
      double value = boundaryList.get(i);
      if (i == 0) {
        Predicate lt = Utils.buildLessThanPredicate(column, value);
        partitions.add(new Partition(lt));
      }
      Predicate eq = Utils.buildEqualPredicate(column, value);
      partitions.add(new Partition(eq));
      if (i < boundaryList.size() - 1) {
        double nextVal = boundaryList.get(i + 1);
        Predicate pred = Utils.buildRangePredicate(column, value, nextVal);
        partitions.add(new Partition(pred));
      } else {
        Predicate gt = Utils.buildGreaterThanPredicate(column, value);
        partitions.add(new Partition(gt));
      }
    }
  }

  // Add a boundary defined by predicate p
  public void addBoundary(Predicate p) {
    if (p instanceof EqualPredicate) {
      EqualPredicate eq = (EqualPredicate) p;
      boundaries.add(eq.getValue());
    } else if (p instanceof RangePredicate) {
      // if p is a range predicate, this should add 2 partitions: {>, <=} or {<, >=}
      RangePredicate r = (RangePredicate) p;
      if (r.getUpperBound() != Double.POSITIVE_INFINITY) {
        boundaries.add(r.getUpperBound());
      }
      if (r.getLowerBound() != Double.NEGATIVE_INFINITY) {
        boundaries.add(r.getLowerBound());
      }
    }
  }
}
