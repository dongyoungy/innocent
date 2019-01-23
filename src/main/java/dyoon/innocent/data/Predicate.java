package dyoon.innocent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Created by Dong Young Yoon on 2018-12-16. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = EqualPredicate.class, name = "equal"),
  @JsonSubTypes.Type(value = RangePredicate.class, name = "range"),
  @JsonSubTypes.Type(value = InPredicate.class, name = "in")
})
public abstract class Predicate {
  protected Column column;

  public Column getColumn() {
    return column;
  }

  public abstract String toSql();
}
