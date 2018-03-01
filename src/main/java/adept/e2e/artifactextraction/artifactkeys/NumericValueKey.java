package adept.e2e.artifactextraction.artifactkeys;

import adept.common.NumericValue;

/**
 * Created by msrivast on 9/28/16.
 */
public final class NumericValueKey extends ItemKey {

  private static final long serialVersionUID = 3338091179672111384L;
  NumericValue numericValue;

  private NumericValueKey(NumericValue numericValue) {
    this.numericValue = numericValue;
  }

  public static NumericValueKey getKey(NumericValue numericValue) {
    return new NumericValueKey(numericValue);
  }

  @Override
  public boolean equals(Object that) {
    return (that != null && that instanceof NumericValueKey && this.numericValue.equals(
        ((NumericValueKey) that).numericValue));
  }

  @Override
  public int hashCode() {
    return this.numericValue.hashCode();
  }

  @Override
  public String toString() {
    return "{NumericValueKey: " + numericValue.asNumber().floatValue() + "}";
  }

  public NumericValue numericValue() {
    return numericValue;
  }
}
