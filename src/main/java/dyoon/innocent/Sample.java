package dyoon.innocent;

import dyoon.innocent.data.Table;

/** Created by Dong Young Yoon on 2019-01-15. */
public abstract class Sample {
  protected Table table;
  protected int id;

  public Table getTable() {
    return table;
  }

  public int getId() {
    return id;
  }

  public abstract String getSampleTableName();
}
