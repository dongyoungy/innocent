package dyoon.innocent.data;

import com.google.common.base.Joiner;
import dyoon.innocent.Query;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** Created by Dong Young Yoon on 2019-01-08. */
public class Prejoin {
  private Table factTable;
  private Set<FactDimensionJoin> joinSet;
  private Set<Query> queries;

  public Prejoin(Table factTable) {
    this.factTable = factTable;
    this.joinSet = new HashSet<>();
    this.queries = new HashSet<>();
  }

  public Set<Query> getQueries() {
    return queries;
  }

  public Set<FactDimensionJoin> getJoinSet() {
    return joinSet;
  }

  public void addJoin(FactDimensionJoin join) {
    joinSet.add(join);
  }

  public boolean containDimension(FactDimensionJoin j) {
    for (FactDimensionJoin join : joinSet) {
      if (join.getDimensionTable().equals(j.getDimensionTable())) {
        return true;
      }
    }
    return false;
  }

  public boolean containSameDimensionWithDiffKey(FactDimensionJoin j) {
    for (FactDimensionJoin join : joinSet) {
      if (join.getDimensionTable().equals(j.getDimensionTable())
          && !join.getJoinPairs().equals(j.getJoinPairs())) {
        return true;
      }
    }
    return false;
  }

  public void addQuery(Query q) {
    queries.add(q);
  }

  public String getPrejoinTableName() {
    SortedSet<String> tableNames = new TreeSet<>();
    tableNames.add(factTable.getName());
    for (FactDimensionJoin join : joinSet) {
      tableNames.add(join.getDimensionTable().getName());
    }
    return String.format("prejoin__%s", Joiner.on("__").join(tableNames));
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (FactDimensionJoin join : joinSet) {
      hash += join.hashCode();
    }

    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Prejoin) {
      Prejoin other = (Prejoin) obj;
      return factTable.equals(other.factTable) && joinSet.equals(other.joinSet);
    }
    return false;
  }
}
