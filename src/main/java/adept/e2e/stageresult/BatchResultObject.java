package adept.e2e.stageresult;


import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import adept.e2e.driver.SerializerUtil;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * Created by msrivast on 1/24/17.
 */
public final class BatchResultObject<U> extends ResultObject<Object, U> implements Serializable {

  private static final long serialVersionUID = -3010588013860838391L;
  //A batch can comprise of multiple artifacts. For some batches, these artifacts can be
  // documents. For other batches (like KB-deduplication batches), these artifacts can be
  // entities, relations or events. In any case, these artifacts (or documents) will more often
  // than not have a key they can be identified with.
  private Optional<ImmutableList<String>> artifactIdsInvolved;
  private Optional<ImmutableList<String>> failedArtifactIds;//It is possible for a batch to fail on
  // some documents, while succeed on the rest. This list will make it easier to filter our failed
  // documents from the batch, if required.
  private Optional<ImmutableTable<String, String, ? extends Object>> artifactLevelProperties;


  private BatchResultObject(Object inputArtifact, U outputArtifact, boolean resultSuccess,
      ImmutableList<String> artifactIdsInvolved, ImmutableList<String> failedArtifactIds,
      ImmutableMap<String, Object>
          resultProperties, ImmutableTable<String, String, ? extends Object>
      artifactLevelProperties) {
    super(inputArtifact, outputArtifact, resultSuccess, resultProperties);
    checkArgument(!resultSuccess || resultSuccess && outputArtifact != null);
    this.artifactIdsInvolved = Optional.fromNullable(artifactIdsInvolved);
    this.failedArtifactIds = Optional.fromNullable(failedArtifactIds);
    this.artifactLevelProperties = Optional.fromNullable(artifactLevelProperties);
  }

  public static <U> BatchResultObject createEmpty() {
    return new BatchResultObject(null, null, false, null, null, null, null);
  }

  @Override
  public Object getInputArtifact() {
    throw new UnsupportedOperationException("This method is not "
        + "supported in this version of E2E.");
  }

//  @Override
//  public void setInputArtifact(Object inputArtifact) {
//    throw new UnsupportedOperationException("Setting inputArtifact to BatchResultObject is not "
//        + "supported in this version of E2E.");
//  }

  public void setArtifactIdsInvolved(ImmutableList<String> artifactIdsInvolved) {
    this.artifactIdsInvolved = Optional.fromNullable(artifactIdsInvolved);
  }

  public Optional<ImmutableList<String>> getArtifactIds() {
    return this.artifactIdsInvolved;
  }

  public Optional<ImmutableList<String>> getFailedArtifactIds() {
    return this.failedArtifactIds;
  }

  public void setFailedArtifactIds(ImmutableList<String> failedArtifactIds) {
    this.failedArtifactIds = Optional.fromNullable(failedArtifactIds);
  }

  public void setArtifactLevelProperties(ImmutableTable<String, String, ? extends Object>
      propertiesForArtifacts) {
    if (propertiesForArtifacts != null && !propertiesForArtifacts.isEmpty()) {
      Set<String> artifactIds = new HashSet(propertiesForArtifacts.rowKeySet());
      if (this.artifactIdsInvolved.isPresent()) {
        artifactIds.addAll(this.artifactIdsInvolved.get());
      }
      if(!this.artifactIdsInvolved.isPresent() || !this.artifactIdsInvolved.get().containsAll(artifactIds)) {
        this.artifactIdsInvolved = Optional.of(ImmutableList.copyOf(artifactIds));
      }
    }
    this.artifactLevelProperties = Optional.fromNullable(propertiesForArtifacts);
  }

  public Optional<ImmutableTable<String, String, ? extends Object>> getArtifactLevelProperties(){
    return this.artifactLevelProperties;
  }

  public Object getPropertyForArtifact(String artifactId, String propertyName) {
    if (this.artifactLevelProperties.isPresent() && this.artifactIdsInvolved.get()
        .contains(artifactId)) {
      return this.artifactLevelProperties.get().get(artifactId, propertyName);
    }
    return null;
  }

  public Optional<ImmutableMap<String, ? extends Object>> getPropertiesMapForArtifact(String
      artifactId) {
    if (this.artifactLevelProperties.isPresent() && this.artifactIdsInvolved.get()
        .contains(artifactId)) {
      return Optional.of(this.artifactLevelProperties.get().row(artifactId));
    }
    return Optional.absent();
  }

  @Override
  public String toString() {
    String retVal = super.toString();
    try {
      String artifactIds = SerializerUtil.serialize(this.artifactIdsInvolved);
      String failedArtifactIds = SerializerUtil.serialize(this.failedArtifactIds);
      String artifactProperties = SerializerUtil.serialize(this.artifactLevelProperties);
      retVal +=
          ("{artifactIds=" + artifactIds + "}\n{failedArtifactIds=" + failedArtifactIds + "}\n"
               + "{artifactLevelProperties=" + artifactLevelProperties + "}");
    } catch (Exception e) {
      retVal += ("Caught exception when trying to serialize the result-object: " + e.getMessage());
    }
    return retVal;
  }

}
