package dyoon.innocent.data;

import dyoon.innocent.Query;
import dyoon.innocent.Utils;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Dong Young Yoon on 2019-01-10.
 *
 * <p>Depicts 1:1 join relationship between a fact and a dimension table
 */
public class FactDimensionJoin {
  private Table factTable;
  private Table dimensionTable;
  private Set<UnorderedPair<Column>> joinColumns;
  private Set<Predicate> predicates;
  private Set<Query> querySet;

  private FactDimensionJoin() {
    // for JSON
  }

  public FactDimensionJoin(
      Table factTable, Table dimensionTable, Set<UnorderedPair<Column>> joinColumns) {
    this.factTable = factTable;
    this.dimensionTable = dimensionTable;
    this.joinColumns = joinColumns;
    this.predicates = new HashSet<>();
    this.querySet = new HashSet<>();
  }

  @Override
  public int hashCode() {
    return factTable.hashCode() + dimensionTable.hashCode() + joinColumns.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FactDimensionJoin) {
      FactDimensionJoin other = (FactDimensionJoin) obj;
      return other.factTable.equals(factTable)
          && other.dimensionTable.equals(dimensionTable)
          && other.joinColumns.equals(joinColumns);
    }

    return false;
  }

  public void addQuery(Query q) {
    this.querySet.add(q);
  }

  public Set<Query> getQuerySet() {
    return querySet;
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

  public Table getFactTable() {
    return factTable;
  }

  public Table getDimensionTable() {
    return dimensionTable;
  }

  public Set<UnorderedPair<Column>> getJoinPairs() {
    return joinColumns;
  }

  public void setJoinPairs(Set<UnorderedPair<Column>> joinColumns) {
    this.joinColumns = joinColumns;
  }

  public static FactDimensionJoin create(
      AliasedTable factTable,
      AliasedTable dimensionTable,
      Set<UnorderedPair<SqlIdentifier>> keyPairSet,
      Collection<Column> allColumns) {

    Table newFactTable = factTable.getTable();
    Table newDimensionTable = dimensionTable.getTable();
    Set<UnorderedPair<Column>> newJoinKeyPairSet = new HashSet<>();

    for (UnorderedPair<SqlIdentifier> pair : keyPairSet) {
      Column leftColumn = Utils.findColumnById(allColumns, pair.getLeft());
      Column rightColumn = Utils.findColumnById(allColumns, pair.getRight());

      newJoinKeyPairSet.add(new UnorderedPair<>(leftColumn, rightColumn));
    }

    return new FactDimensionJoin(newFactTable, newDimensionTable, newJoinKeyPairSet);
  }
}
