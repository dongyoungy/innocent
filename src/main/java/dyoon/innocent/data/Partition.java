package dyoon.innocent.data;

/**
 * Created by Dong Young Yoon on 2018-12-16.
 *
 * <p>A partition in partitionSpace
 */
public class Partition {
  private Predicate predicate;

  public Partition(Predicate predicate) {
    this.predicate = predicate;
  }

  public Predicate getPredicate() {
    return predicate;
  }
}
