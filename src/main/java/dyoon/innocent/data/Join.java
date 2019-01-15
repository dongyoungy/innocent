package dyoon.innocent.data;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2018-12-16.
 *
 * <p>Stores a set of join tables and their predicates
 */
public class Join {
  private Set<Table> tables;
  private Set<Set<SqlIdentifier>> joinKeySets;
  private Set<Predicate> predicates;

  public Join() {
    this.tables = new HashSet<>();
    this.predicates = new HashSet<>();
    this.joinKeySets = new HashSet<>();
  }

  public Join(Set<Table> tables, Set<Set<SqlIdentifier>> keys, Set<Predicate> predicates) {
    this.tables = tables;
    this.joinKeySets = keys;
    this.predicates = predicates;
  }

  public void setTables(Set<Table> tables) {
    this.tables = tables;
  }

  public void setPredicates(Set<Predicate> predicates) {
    this.predicates = predicates;
  }

  public Set<Table> getTables() {
    return tables;
  }

  public Set<Predicate> getPredicates() {
    return predicates;
  }

  public Set<Set<SqlIdentifier>> getJoinKeys() {
    return joinKeySets;
  }

  public void setJoinKeys(Set<Set<SqlIdentifier>> joinKeys) {
    this.joinKeySets = new HashSet<>(joinKeys);
  }
}
