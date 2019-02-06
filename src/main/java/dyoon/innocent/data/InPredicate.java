package dyoon.innocent.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 2019-01-15. */
public class InPredicate extends Predicate {

  @JsonIgnore private List<Object> values;

  private InPredicate() {
    // for JSON
  }

  public InPredicate(Column col) {
    this.column = col;
    this.values = new ArrayList<>();
  }

  public void addValue(Object val) {
    values.add(val);
  }

  public List<Object> getValues() {
    return values;
  }

  @Override
  public String toSql() {
    return column.toString() + " IN (" + Joiner.on(",").join(values) + ")";
  }
}
