package dyoon.innocent.data;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 2019-01-15. */
public class InPredicate extends Predicate {

  private List<Object> values;

  public InPredicate(Column col) {
    this.column = col;
    this.values = new ArrayList<>();
  }

  public void addValue(Object val) {
    values.add(val);
  }

  @Override
  public String toSql() {
    return column.toString() + " IN (" + Joiner.on(",").join(values) + ")";
  }
}
