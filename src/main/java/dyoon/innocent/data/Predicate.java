package dyoon.innocent.data;

/** Created by Dong Young Yoon on 2018-12-16. */
public abstract class Predicate {
  protected Column column;

  public Column getColumn() {
    return column;
  }

  public abstract String toSql();
}
