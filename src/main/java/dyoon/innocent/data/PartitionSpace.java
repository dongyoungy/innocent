package dyoon.innocent.data;

import dyoon.innocent.Query;

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
  private List<Predicate> partitions;
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

  private void addRangePartition(RangePredicate r) {

    // check equal partition
    for (Predicate p : partitions) {
      if (r.isEqual(p)) {
        return;
      }
    }

    // check partition with same bound
    for (Predicate p : partitions) {
      if (r.hasSameBound(p)) {
        double equalVal =
            (r.getLowerBound() != Double.NEGATIVE_INFINITY) ? r.getLowerBound() : r.getUpperBound();
        EqualPredicate eq = new EqualPredicate(this.column, equalVal);
        this.addEqualPartition(eq);
        RangePredicate range = (RangePredicate) p;
        range.setLowerInclusive(false);
        range.setUpperInclusive(false);
        return;
      }
    }

    // check intersecting partition
    for (int i = 0; i < partitions.size(); ++i) {
      Predicate p = partitions.get(i);
      RangePredicate intersect = r.getIntersect(p);
      if (intersect != null && !intersect.isEqual(p)) {
        partitions.set(i, intersect);
        return;
      }
    }

    // check covering partition
    RangePredicate toAdd = null;
    for (int i = 0; i < partitions.size(); ++i) {
      Predicate p = partitions.get(i);
      if (p instanceof RangePredicate) {
        RangePredicate range = (RangePredicate) p;
        RangePredicate cover1 = r.getCover(range);
        RangePredicate cover2 = range.getCover(r);
        if (cover1 != null) {
          toAdd = cover1;
          break;
        }
        if (cover2 != null) {
          partitions.set(i, cover2);
          toAdd = r;
          break;
        }
      }
    }

    if (toAdd != null) {
      partitions.add(toAdd);
      return;
    }

    // if none of the above, just add it as-is
    partitions.add(r);
  }

  private void addEqualPartition(EqualPredicate eq) {
    for (int i = 0; i < partitions.size(); ++i) {
      Predicate p = partitions.get(i);
      if (eq.isOverlap(p)) {
        if (p instanceof EqualPredicate) {
          // already exists, so do nothing
          return;
        } else if (p instanceof RangePredicate) {
          // it overlaps with existing range predicate
          RangePredicate range = (RangePredicate) p;
          // existing range predicate is no longer inclusive
          if (range.getUpperBound() == eq.getValue()) {
            range.setUpperInclusive(false);
          } else if (range.getLowerBound() == eq.getValue()) {
            range.setLowerInclusive(false);
          }
          // add new equal predicate
          partitions.add(eq);
          return;
        }
      }
    }
    // no existing partition overlaps, so just add
    partitions.add(eq);
  }
}
