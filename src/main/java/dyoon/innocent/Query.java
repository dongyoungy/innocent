package dyoon.innocent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.HashBasedTable;
import dyoon.innocent.data.AliasedTable;
import dyoon.innocent.data.Column;
import dyoon.innocent.data.Predicate;
import dyoon.innocent.data.Table;
import dyoon.innocent.data.UnorderedPair;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Created by Dong Young Yoon on 10/30/18. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Query implements Comparable<Query> {

  private String id;
  private String query;
  private String aqpQuery;
  private long cost;

  @JsonIgnore private Set<AliasedTable> tables;

  @JsonIgnore
  private Map<
          Table, com.google.common.collect.Table<Table, Set<UnorderedPair<Column>>, Set<Predicate>>>
      predicateTableMap;

  public Query(String id, String query) {
    this.id = id;
    this.query = query;
    this.tables = new HashSet<>();
    this.predicateTableMap = new HashMap();
  }

  public void addPredicate(
      Table fact, Table dim, Set<UnorderedPair<Column>> joinColumnSet, Predicate p) {
    if (!predicateTableMap.containsKey(fact)) {
      predicateTableMap.put(fact, HashBasedTable.create());
    }
    if (!predicateTableMap.get(fact).contains(dim, joinColumnSet)) {
      predicateTableMap.get(fact).put(dim, joinColumnSet, new HashSet<>());
    }
    predicateTableMap.get(fact).get(dim, joinColumnSet).add(p);
  }

  public void addPredicateAll(
      Table fact, Table dim, Set<UnorderedPair<Column>> joinColumnSet, Collection<Predicate> p) {
    if (!predicateTableMap.containsKey(fact)) {
      predicateTableMap.put(fact, HashBasedTable.create());
    }
    if (!predicateTableMap.get(fact).contains(dim, joinColumnSet)) {
      predicateTableMap.get(fact).put(dim, joinColumnSet, new HashSet<>());
    }
    predicateTableMap.get(fact).get(dim, joinColumnSet).addAll(p);
  }

  public Map<
          Table, com.google.common.collect.Table<Table, Set<UnorderedPair<Column>>, Set<Predicate>>>
      getPredicateTableMap() {
    return predicateTableMap;
  }

  public Set<AliasedTable> getTables() {
    return tables;
  }

  public void addTable(AliasedTable t) {
    tables.add(t);
  }

  public void addTableAll(Collection<AliasedTable> t) {
    tables.addAll(t);
  }

  @Override
  public String toString() {
    return id;
  }

  public String getId() {
    return id;
  }

  public String getQuery() {
    return query;
  }

  public String getAqpQuery() {
    return aqpQuery;
  }

  public void setAqpQuery(String aqpQuery) {
    this.aqpQuery = aqpQuery;
  }

  public String getResultTableName() {
    return id + "_orig";
  }

  public long getCost() {
    return cost;
  }

  public void setCost(long cost) {
    this.cost = cost;
  }

  @Override
  public int compareTo(Query o) {
    return id.compareTo(o.id);
  }
}
