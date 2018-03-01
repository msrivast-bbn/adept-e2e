package adept.e2e.kbupload.uploadedartifacts;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.common.KBID;
import adept.e2e.artifactextraction.artifactkeys.GenericThingKey;
import adept.e2e.artifactextraction.artifactkeys.NumericValueKey;
import adept.e2e.artifactextraction.artifactkeys.TemporalValueKey;

/**
 * Created by msrivast on 10/12/16.
 */
public final class UploadedNonEntityArguments implements Serializable {

  private static final long serialVersionUID = -3202622256788488279L;
  final ImmutableMap<NumericValueKey, KBID> uploadedNumericValueArguments;
  final ImmutableMap<TemporalValueKey, KBID> uploadedTemporalValueArguments;
  final ImmutableMap<GenericThingKey, KBID> uploadedGenericThingArguments;

  private UploadedNonEntityArguments(
      ImmutableMap<NumericValueKey, KBID> uploadedNumericValues,
      ImmutableMap<TemporalValueKey, KBID> uploadedTemporalValues,
      ImmutableMap<GenericThingKey, KBID> uploadedGenericThings) {
    this.uploadedNumericValueArguments = uploadedNumericValues;
    this.uploadedTemporalValueArguments = uploadedTemporalValues;
    this.uploadedGenericThingArguments = uploadedGenericThings;
  }

  public static UploadedNonEntityArguments create(ImmutableMap<NumericValueKey, KBID> uploadedNumericValues,
      ImmutableMap<TemporalValueKey, KBID> uploadedTemporalValues,
      ImmutableMap<GenericThingKey, KBID> uploadedGenericThings) {
    return new UploadedNonEntityArguments(uploadedNumericValues, uploadedTemporalValues,
        uploadedGenericThings);
  }

  public Optional<ImmutableMap<NumericValueKey, KBID>> uploadedNumericValues() {
    return Optional.fromNullable(uploadedNumericValueArguments);
  }

  public Optional<ImmutableMap<TemporalValueKey, KBID>> uploadedTemporalValues() {
    return Optional.fromNullable(uploadedTemporalValueArguments);
  }

  public Optional<ImmutableMap<GenericThingKey, KBID>> uploadedGenericThings() {
    return Optional.fromNullable(uploadedGenericThingArguments);
  }

}
