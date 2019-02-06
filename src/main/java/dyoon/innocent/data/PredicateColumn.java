package dyoon.innocent.data;

/** Created by Dong Young Yoon on 2019-01-29. */
public class PredicateColumn {
  private Column column;
  private Predicate predicate;
  private int multiplier;

  public PredicateColumn(Column column, Predicate pred, int multiplier) {
    this.column = column;
    this.predicate = pred;
    this.multiplier = multiplier;
  }

  public Predicate getPredicate() {
    return predicate;
  }

  public int getMultiplier() {
    return multiplier;
  }

  public Column getColumn() {
    return column;
  }

  @Override
  public int hashCode() {
    return column.hashCode() + multiplier;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PredicateColumn) {
      PredicateColumn other = (PredicateColumn) obj;
      return column.equals(other.column) && multiplier == other.multiplier;
    }
    return false;
  }
}
