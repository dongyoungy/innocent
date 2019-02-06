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

  @JsonIgnore private String query;
  @JsonIgnore private String aqpQuery;
  private Map<Table, Long> costMap;
  private Map<Table, Long> reducedCostMap;

  @JsonIgnore private Set<AliasedTable> tables;

  @JsonIgnore
  private Map<
          Table, com.google.common.collect.Table<Table, Set<UnorderedPair<Column>>, Set<Predicate>>>
      predicateTableMap;

  private Query() {
    // for JSON
  }

  public Query(String id, String query) {
    this.id = id;
    this.query = query;
    this.tables = new HashSet<>();
    this.predicateTableMap = new HashMap();
    this.costMap = new HashMap<>();
    this.reducedCostMap = new HashMap<>();
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

  public long getCost(Table table) {
    return costMap.get(table);
  }

  public void setCost(Table table, long cost) {
    this.costMap.put(table, cost);
  }

  public long getReducedCost(Table table) {
    return reducedCostMap.get(table);
  }

  public void setReducedCost(Table table, long cost) {
    this.reducedCostMap.put(table, cost);
  }

  public long getTotalCost() {
    long total = 0;
    for (Long value : this.costMap.values()) {
      total += value;
    }
    return total;
  }

  public long getTotalCurrentReducedCost() {
    long total = 0;
    for (Long value : this.reducedCostMap.values()) {
      total += value;
    }
    return total;
  }

  @Override
  public int compareTo(Query o) {
    return id.compareTo(o.id);
  }

  public void setCostIfNull(Table table, long cost) {
    if (!this.costMap.containsKey(table)) {
      this.costMap.put(table, cost);
    }
  }

  public void setReducedCostIfNull(Table table, long cost) {
    if (!this.reducedCostMap.containsKey(table)) {
      this.reducedCostMap.put(table, cost);
    }
  }
}
