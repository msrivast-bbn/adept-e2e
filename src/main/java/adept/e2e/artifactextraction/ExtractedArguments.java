package adept.e2e.artifactextraction;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.common.Chunk;
import adept.common.GenericThing;
import adept.common.NumberPhrase;
import adept.common.NumericValue;
import adept.common.TemporalValue;
import adept.common.TimePhrase;
import adept.e2e.artifactextraction.artifactkeys.GenericThingKey;
import adept.e2e.artifactextraction.artifactkeys.NumericValueKey;
import adept.e2e.artifactextraction.artifactkeys.TemporalValueKey;
import adept.e2e.artifactextraction.mergedartifacts.ItemWithProvenances;

/**
 * Created by msrivast on 10/12/16.
 */
public final class ExtractedArguments implements Serializable {

  private static final long serialVersionUID = -5432944137118823701L;
  final ImmutableMap<NumericValueKey, ItemWithProvenances<NumericValue,NumberPhrase>> mergedNumericValues;
  final ImmutableMap<TemporalValueKey, ItemWithProvenances<TemporalValue,TimePhrase>> mergedTemporalValues;
  final ImmutableMap<GenericThingKey, ItemWithProvenances<GenericThing,Chunk>> mergedGenericThings;

  private ExtractedArguments(
      ImmutableMap<NumericValueKey, ItemWithProvenances<NumericValue,NumberPhrase>> mergedNumericValues,
      ImmutableMap<TemporalValueKey, ItemWithProvenances<TemporalValue,TimePhrase>> mergedTemporalValues,
      ImmutableMap<GenericThingKey, ItemWithProvenances<GenericThing,Chunk>> mergedGenericThings) {
    this.mergedNumericValues = mergedNumericValues;
    this.mergedTemporalValues = mergedTemporalValues;
    this.mergedGenericThings = mergedGenericThings;
  }

  public static ExtractedArguments create(
      ImmutableMap<NumericValueKey, ItemWithProvenances<NumericValue,NumberPhrase>> mergedNumericValues,
      ImmutableMap<TemporalValueKey, ItemWithProvenances<TemporalValue,TimePhrase>> mergedTemporalValues,
      ImmutableMap<GenericThingKey, ItemWithProvenances<GenericThing,Chunk>> mergedGenericThings) {
    return new ExtractedArguments(mergedNumericValues, mergedTemporalValues,
        mergedGenericThings);
  }

  public Optional<ImmutableMap<NumericValueKey, ItemWithProvenances<NumericValue,NumberPhrase>>> mergedNumericValues() {
    return Optional.fromNullable(mergedNumericValues);
  }

  public Optional<ImmutableMap<TemporalValueKey, ItemWithProvenances<TemporalValue,TimePhrase>>> mergedTemporalValues() {
    return Optional.fromNullable(mergedTemporalValues);
  }

  public Optional<ImmutableMap<GenericThingKey, ItemWithProvenances<GenericThing,Chunk>>> mergedGenericThings() {
    return Optional.fromNullable(mergedGenericThings);
  }

}
