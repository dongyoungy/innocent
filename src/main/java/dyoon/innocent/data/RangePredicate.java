package dyoon.innocent.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.pmw.tinylog.Logger;

/** Created by Dong Young Yoon on 2018-12-16. */
public class RangePredicate extends Predicate {

  private double lowerBound;
  private double upperBound;
  private boolean isLowerInclusive;
  private boolean isUpperInclusive;

  private RangePredicate() {
    // for JSON
  }

  public RangePredicate(
      Column column,
      double lowerBound,
      double upperBound,
      boolean isLowerInclusive,
      boolean isUpperInclusive) {
    this.column = column;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.isLowerInclusive = isLowerInclusive;
    this.isUpperInclusive = isUpperInclusive;
  }

  @Override
  public int hashCode() {
    return column.hashCode()
        + Double.hashCode(lowerBound)
        + Double.hashCode(upperBound)
        + Boolean.hashCode(isLowerInclusive)
        + Boolean.hashCode(isUpperInclusive);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RangePredicate) {
      RangePredicate range = (RangePredicate) obj;
      return range.column.equals(column)
          && range.lowerBound == lowerBound
          && range.upperBound == upperBound
          && range.isLowerInclusive == isLowerInclusive
          && range.isUpperInclusive == isUpperInclusive;
    }
    return false;
  }

  @Override
  public String toString() {
    String str = "";
    if (this.lowerBound != Double.NEGATIVE_INFINITY) {
      str += this.lowerBound;
      str += (this.isLowerInclusive) ? " <= " : " < ";
    }
    str += column.getName();
    if (this.upperBound != Double.POSITIVE_INFINITY) {
      str += (this.isUpperInclusive) ? " <= " : " < ";
      str += this.upperBound;
    }
    return str;
  }

  public Column getColumn() {
    return column;
  }

  @Override
  public String toSql() {
    String sql = "";
    if (this.lowerBound != Double.NEGATIVE_INFINITY) {
      sql += column.getName();
      sql += (this.isLowerInclusive) ? " >= " : " > ";
      sql += this.lowerBound;
    }
    if (this.upperBound != Double.POSITIVE_INFINITY) {
      if (!sql.isEmpty()) {
        sql += " AND ";
      }
      sql += column.getName();
      sql += (this.isUpperInclusive) ? " <= " : " < ";
      sql += this.upperBound;
    }
    return sql;
  }

  public double getLowerBound() {
    return lowerBound;
  }

  public double getUpperBound() {
    return upperBound;
  }

  public boolean isLowerInclusive() {
    return isLowerInclusive;
  }

  public boolean isUpperInclusive() {
    return isUpperInclusive;
  }

  public void setLowerInclusive(boolean lowerInclusive) {
    this.isLowerInclusive = lowerInclusive;
  }

  public void setUpperInclusive(boolean upperInclusive) {
    this.isUpperInclusive = upperInclusive;
  }

  // if this covers 'p', returns 'this - p' predicate
  public RangePredicate getCover(RangePredicate p) {
    if (this.lowerBound == Double.NEGATIVE_INFINITY && p.lowerBound == Double.NEGATIVE_INFINITY) {
      // x < a and x < b
      double newLowerBound = (this.upperBound < p.upperBound) ? this.upperBound : p.upperBound;
      double newUpperBound = (this.upperBound > p.upperBound) ? this.upperBound : p.upperBound;
      boolean newLowerInclusive =
          (this.upperBound < p.upperBound) ? !this.isUpperInclusive : !p.isUpperInclusive;
      boolean newUpperInclusive =
          (this.upperBound > p.upperBound) ? this.isUpperInclusive : p.isUpperInclusive;
      return new RangePredicate(
          column, newLowerBound, newUpperBound, newLowerInclusive, newUpperInclusive);
    } else if (this.upperBound == Double.POSITIVE_INFINITY
        && p.upperBound == Double.POSITIVE_INFINITY) {
      // x > a and x > b
      double newLowerBound = (this.lowerBound < p.lowerBound) ? this.lowerBound : p.lowerBound;
      double newUpperBound = (this.lowerBound > p.lowerBound) ? this.lowerBound : p.lowerBound;
      boolean newLowerInclusive =
          (this.lowerBound < p.lowerBound) ? this.isLowerInclusive : p.isLowerInclusive;
      boolean newUpperInclusive =
          (this.lowerBound > p.lowerBound) ? !this.isLowerInclusive : !p.isLowerInclusive;
      return new RangePredicate(
          column, newLowerBound, newUpperBound, newLowerInclusive, newUpperInclusive);
    }
    return null;
  }

  public RangePredicate merge(RangePredicate p) {
    double newLowerBound = (p.lowerBound > this.lowerBound) ? p.lowerBound : this.lowerBound;
    double newUpperBound = (p.upperBound < this.upperBound) ? p.upperBound : this.upperBound;
    boolean newLowerInclusive =
        (p.lowerBound > this.lowerBound) ? p.isLowerInclusive : this.isLowerInclusive;
    boolean newUpperInclusive =
        (p.upperBound < this.upperBound) ? p.isUpperInclusive : this.isUpperInclusive;
    return new RangePredicate(
        this.column, newLowerBound, newUpperBound, newLowerInclusive, newUpperInclusive);
  }

  public boolean isEqual(Predicate pred) {
    if (pred instanceof RangePredicate) {
      RangePredicate p = (RangePredicate) pred;
      return this.column.equals(p.column)
          && this.lowerBound == p.lowerBound
          && this.upperBound == p.upperBound
          && this.isUpperInclusive == p.isUpperInclusive
          && this.isLowerInclusive == p.isLowerInclusive;
    }
    return false;
  }

  public boolean hasSameBound(Predicate pred) {
    if (pred instanceof RangePredicate) {
      RangePredicate p = (RangePredicate) pred;
      return this.column.equals(p.column)
          && ((this.lowerBound == p.lowerBound && this.isLowerInclusive != p.isLowerInclusive)
              || (this.upperBound == p.upperBound && this.isUpperInclusive != p.isUpperInclusive));
    }
    return false;
  }

  @JsonIgnore
  public RangePredicate getComplement() {
    double newLowerBound =
        (this.upperBound != Double.POSITIVE_INFINITY) ? this.upperBound : Double.NEGATIVE_INFINITY;
    double newUpperBound =
        (this.lowerBound != Double.NEGATIVE_INFINITY) ? this.lowerBound : Double.POSITIVE_INFINITY;
    if (newLowerBound >= newUpperBound) {
      Logger.error(
          "A complement of a range predicate has lb >= ub: {}, {}", newLowerBound, newUpperBound);
      return null;
    }
    boolean newLowerInclusive =
        this.upperBound != Double.POSITIVE_INFINITY && !this.isUpperInclusive;
    boolean newUpperInclusive =
        this.lowerBound != Double.NEGATIVE_INFINITY && !this.isLowerInclusive;
    return new RangePredicate(
        this.column, newLowerBound, newUpperBound, newLowerInclusive, newUpperInclusive);
  }

  public RangePredicate getIntersect(Predicate p) {
    if (p instanceof RangePredicate) {
      RangePredicate other = (RangePredicate) p;
      RangePredicate merged = this.merge(other);
      if (merged.getLowerBound() != Double.NEGATIVE_INFINITY
          && merged.getUpperBound() != Double.POSITIVE_INFINITY) {
        if (merged.getLowerBound() >= merged.getUpperBound()) {
          return null;
        }
        return merged;
      }
    }
    return null;
  }
}
