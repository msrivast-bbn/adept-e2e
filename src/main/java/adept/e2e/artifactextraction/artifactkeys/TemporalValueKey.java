package adept.e2e.artifactextraction.artifactkeys;

import adept.common.TemporalValue;

/**
 * Created by msrivast on 9/28/16.
 */
public final class TemporalValueKey extends ItemKey {

  private static final long serialVersionUID = 8149206157506161442L;
  TemporalValue temporalValue;


  private TemporalValueKey(TemporalValue temporalValue) {
    this.temporalValue = temporalValue;
  }

  public static TemporalValueKey getKey(TemporalValue temporalValue) {
    if (null == temporalValue) {
      return null;
    }
    return new TemporalValueKey(temporalValue);
  }

  @Override
  public boolean equals(Object that) {
    return (that != null && that instanceof TemporalValueKey &&
                ((TemporalValueKey) that).temporalValue().asString() != null &&
                this.temporalValue.asString().equals(((TemporalValueKey) that).temporalValue().asString()));
  }

  @Override
  public int hashCode() {
    return temporalValue.asString().hashCode();
  }

  @Override
  public String toString() {
    return String.format("{TemporalValueKey: %s}", temporalValue.asString());
  }

  public TemporalValue temporalValue() {
    return temporalValue;
  }
}
