package dyoon.innocent;

import dyoon.innocent.data.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** Created by Dong Young Yoon on 10/30/18. */
public class Query {

  private String id;
  private String query;
  private String aqpQuery;
  private Set<Predicate> predicates;

  public Query(String id, String query) {
    this.id = id;
    this.query = query;
    this.predicates = new HashSet<>();
  }

  public void addPredicate(Predicate p) {
    predicates.add(p);
  }

  public void addPredicateAll(Collection<Predicate> p) {
    predicates.addAll(p);
  }

  public Set<Predicate> getPredicates() {
    return predicates;
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
}
