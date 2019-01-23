package dyoon.innocent.data;

/**
 * Created by Dong Young Yoon on 2019-01-11.
 *
 * <p>an unordered pair of two objects
 */
public class UnorderedPair<T> {
  private T left;
  private T right;

  public UnorderedPair() {}

  public UnorderedPair(T left, T right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public String toString() {
    return left.toString() + " : " + right.toString();
  }

  @Override
  public int hashCode() {
    return left.hashCode() + right.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof UnorderedPair) {
      UnorderedPair other = (UnorderedPair) obj;
      if (left.equals(other.left) && right.equals(other.right)) {
        return true;
      } else return right.equals(other.left) && left.equals(other.right);
    }
    return false;
  }

  public T getLeft() {
    return left;
  }

  public T getRight() {
    return right;
  }
}
