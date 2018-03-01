package adept.e2e.artifactextraction.artifactfilters.additionalargtypefilter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import adept.common.OntType;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.stageresult.BatchResultObject;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class EntityKeyToAdditionalTypesReducer implements
    Function2<BatchResultObject<ImmutableMultimap<EntityKey,OntType>>,
            BatchResultObject<ImmutableMultimap<EntityKey,OntType>>,BatchResultObject<ImmutableMultimap<EntityKey,OntType>>> {

  private static final long serialVersionUID = 1L;
  private static Logger log = LoggerFactory.getLogger(EntityKeyToAdditionalTypesReducer.class);
  private final boolean throwExceptions;

  public EntityKeyToAdditionalTypesReducer(boolean throwExceptions) {
    this.throwExceptions = throwExceptions;
  }

  @Override
  public BatchResultObject<ImmutableMultimap<EntityKey,OntType>> call
      (BatchResultObject<ImmutableMultimap<EntityKey,OntType>> bro1,
          BatchResultObject<ImmutableMultimap<EntityKey,OntType>> bro2) throws Exception {

    log.info("Reducing EntityKey to additionalTypes map...");
    if (!bro1.isSuccessful()) {
      return bro2;
    }else if(!bro2.isSuccessful()) {
      return bro1;
    }
    ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();
    batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    long batchStartTime = System.currentTimeMillis();
    BatchResultObject<ImmutableMultimap<EntityKey, OntType>> batchResultObject = BatchResultObject
          .createEmpty();
    ImmutableMultimap.Builder reducedEntityKeyToAddnlTypesMap = ImmutableMultimap.builder();
    try {
      reducedEntityKeyToAddnlTypesMap.putAll(bro1.getOutputArtifact().get());
      reducedEntityKeyToAddnlTypesMap.putAll(bro2.getOutputArtifact().get());
      batchResultObject.setOutputArtifact(reducedEntityKeyToAddnlTypesMap.build());
      batchResultObject.markSuccessful();
    } catch (Exception e) {
        log.error("Could not reduce EntityKey to additionalTypes map", e);
        if (throwExceptions) {
          throw e;
        }
        batchResultObject.markFailed();
        batchLevelProperties.put(PROPERTY_EXCEPTION_TYPE, e.getClass()
            .getName());
        batchLevelProperties
            .put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage() != null ? e.getMessage() : "");
        batchLevelProperties.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
    }
    long batchEndTime = System.currentTimeMillis();
    batchLevelProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
    batchResultObject.setPropertiesMap(batchLevelProperties.build());
    return batchResultObject;
  }

}


