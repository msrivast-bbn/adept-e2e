package adept.e2e.artifactextraction.artifactfilters.additionalargtypefilter;

import com.google.common.collect.ImmutableMap;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import adept.common.OntType;
import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.artifactextraction.artifactkeys.ArgumentKey;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentEvent;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentRelation;
import adept.e2e.stageresult.BatchResultObject;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class AdditionalArgTypeArtifactFilter implements
    Function<BatchResultObject<ExtractedArtifacts>,BatchResultObject<ExtractedArtifacts>> {

  private static final long serialVersionUID = 1L;
  private static Logger log = LoggerFactory.getLogger(AdditionalArgTypeArtifactFilter.class);
  private final boolean throwExceptions;
  private final ImmutableMap<EntityKey,OntType> entityKeyToMajorityTypeMap;

  public AdditionalArgTypeArtifactFilter(boolean throwException, ImmutableMap<EntityKey,OntType>
      entityKeyToMajorityTypeMap) {
    this.throwExceptions = throwException;
    this.entityKeyToMajorityTypeMap = entityKeyToMajorityTypeMap;
  }

  @Override
  public BatchResultObject<ExtractedArtifacts> call(BatchResultObject<ExtractedArtifacts>
      inputBRO)
      throws Exception {

    if(!inputBRO.isSuccessful()){
      return inputBRO;
    }

    ExtractedArtifacts extractedArtifacts = inputBRO.getOutputArtifact().get();
    ImmutableMap<RelationKey, MergedDocumentRelation> mergedRelations = extractedArtifacts
        .mergedRelations();
    ImmutableMap<EventKey, MergedDocumentEvent> mergedEvents = extractedArtifacts
        .mergedEvents();
    ImmutableMap.Builder<RelationKey, MergedDocumentRelation> filteredRelations = ImmutableMap.builder();
    ImmutableMap.Builder<EventKey, MergedDocumentEvent> filteredEvents = ImmutableMap.builder();

    BatchResultObject<ExtractedArtifacts> batchResultObject = BatchResultObject.createEmpty();
    ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();
    batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    long batchStartTime = System.currentTimeMillis();
    int numRelationsDropped = 0;
    int numEventsDropped = 0;

    log.info("Filtering extractedArtifacts on additional arg type...");
     try {
      for (RelationKey key : mergedRelations.keySet()) {
        boolean addRelation = true;
        for (ArgumentKey argKey : key.getArgumentKeys()) {
          if (argKey.additionalArgType().isPresent()) {
            OntType additionalType = argKey.additionalArgType().get();
            if(!entityKeyToMajorityTypeMap.containsKey(argKey.fillerKey())){
              throw new Exception(String.format("EntityKey: %s not found in  "
                  + "entityKeyToMajorityTypeMap",argKey.fillerKey()));
            }
            OntType majorityAdditionalType = entityKeyToMajorityTypeMap.get(argKey.fillerKey());
            if(!majorityAdditionalType.equals(additionalType)){
              log.info("Dropping relation: {}",key);
              addRelation = false;
              numRelationsDropped++;
              break;
            }
         }
        }
        if(addRelation){
          filteredRelations.put(key,mergedRelations.get(key));
        }
      }
      for (EventKey key : mergedEvents.keySet()) {
        boolean addEvent = true;
        for (ArgumentKey argKey : key.getArgumentKeys()) {
          if (argKey.additionalArgType().isPresent()) {
            OntType additionalType = argKey.additionalArgType().get();
            if(!entityKeyToMajorityTypeMap.containsKey(argKey.fillerKey())){
              throw new Exception(String.format("EntityKey: %s not found in  "
                  + "entityKeyToMajorityTypeMap",argKey.fillerKey()));
            }
            OntType majorityAdditionalType = entityKeyToMajorityTypeMap.get(argKey.fillerKey());
            if(!majorityAdditionalType.equals(additionalType)){
              log.info("Dropping event: {}",key);
              addEvent = false;
              numEventsDropped++;
              break;
            }
          }
        }
        if(addEvent){
          filteredEvents.put(key,mergedEvents.get(key));
        }
      }
      ExtractedArtifacts filteredExtractedArtifacts = ExtractedArtifacts.create
          (extractedArtifacts.mergedEntities(),
          extractedArtifacts.extractedArguments(),
          filteredRelations.build(), filteredEvents.build(),
          extractedArtifacts.openIERelations());
      batchResultObject.setOutputArtifact(filteredExtractedArtifacts);
      batchResultObject.markSuccessful();
    } catch (Exception e) {
      log.error("Caught the following exception when trying to filter relations/events on "
              + "additional arg type: ",e);
      if (throwExceptions) {
        throw e;
      }
      batchResultObject.markFailed();
      batchLevelProperties.put(PROPERTY_EXCEPTION_TYPE, e.getClass()
          .getName());
      batchLevelProperties.put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage()!=null?e.getMessage():"");
      batchLevelProperties.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
    }
    long batchEndTime = System.currentTimeMillis();
    batchLevelProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
    batchLevelProperties.put("numRelationsDropped",numRelationsDropped);
    batchLevelProperties.put("numEventsDropped",numEventsDropped);
    batchResultObject.setArtifactIdsInvolved(inputBRO.getArtifactIds().orNull());
    batchResultObject.setFailedArtifactIds(inputBRO.getFailedArtifactIds().orNull());
    batchResultObject.setArtifactLevelProperties(inputBRO.getArtifactLevelProperties().orNull());
    batchResultObject.setPropertiesMap(batchLevelProperties.build());
    return batchResultObject;
  }

}


