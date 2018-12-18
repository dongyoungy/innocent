package dyoon.innocent.data;

/** Created by Dong Young Yoon on 2018-12-16. */
public class EqualPredicate extends Predicate {
  private double value;

  public EqualPredicate(Column column, double value) {
    this.column = column;
    this.value = value;
  }

  @Override
  public String toString() {
    return column.getName() + " == " + value;
  }

  public Column getColumn() {
    return column;
  }

  public void setColumn(Column column) {
    this.column = column;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  // checks whether a given predicate shares same value (and inclusive if it is a range predicate)
  public boolean isOverlap(Predicate p) {
    if (p instanceof EqualPredicate) {
      EqualPredicate eq = (EqualPredicate) p;
      return eq.getValue() == this.value;
    } else if (p instanceof RangePredicate) {
      RangePredicate range = (RangePredicate) p;
      if (range.getLowerBound() == this.value && range.isLowerInclusive()) return true;
      else if (range.getUpperBound() == this.value && range.isUpperInclusive()) return true;
    }
    return false;
  }
}
