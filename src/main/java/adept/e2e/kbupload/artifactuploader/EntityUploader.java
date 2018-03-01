package adept.e2e.kbupload.artifactuploader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.KBID;
import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.mergedartifacts.ItemWithProvenances;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.ResultObject;
import adept.kbapi.KBParameters;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class EntityUploader implements
    FlatMapFunction<Iterator<BatchResultObject<ExtractedArtifacts>>,
        BatchResultObject<UploadedEntities>> {

  private static Logger log = LoggerFactory.getLogger(EntityUploader.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;

  public EntityUploader(KBParameters kbParameters, boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<UploadedEntities>> call(
      Iterator<BatchResultObject<ExtractedArtifacts>> inPart)
      throws Exception {

    List<BatchResultObject<UploadedEntities>> retVal = new ArrayList<>();
    log.info("Uploading entities from input partitions...");
    while (inPart.hasNext()) {
      BatchResultObject<UploadedEntities> batchResultObject = BatchResultObject
          .createEmpty();
      ImmutableMap.Builder batchResultProperties = ImmutableMap.builder();
      batchResultProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      long batchStartTime = System.currentTimeMillis();
      //Note: Currently, we don't care if any BatchResultObjects were successful in the previous
      // run. Support for that will only be added when implementing iterative KB-upload.
      BatchResultObject<ExtractedArtifacts> batchResultFromArtifactExtraction = inPart.next();
      if (!batchResultFromArtifactExtraction.isSuccessful() || !batchResultFromArtifactExtraction
          .getOutputArtifact().isPresent() ||
          !batchResultFromArtifactExtraction.getArtifactIds().isPresent()
          ) {
        continue;
      }

      ExtractedArtifacts extractedArtifacts = batchResultFromArtifactExtraction.getOutputArtifact
          ().get();
      log.info("Found the following docids in this RDD of the partition:");
      log.info(batchResultFromArtifactExtraction.getArtifactIds().or(ImmutableList.<String>of())
          .toString());
      batchResultObject.setArtifactIdsInvolved(batchResultFromArtifactExtraction.getArtifactIds()
          .orNull());
      ResultObject<Map<EntityKey,
          ItemWithProvenances<Entity,EntityMention>>, ImmutableMap<EntityKey,KBID>>
          entityUploadResultObject = ResultObject.create(extractedArtifacts.mergedEntities());
      try {
        log.info("Uploading entities...");
        UploaderUtils.getInstance(
            this.kbParameters, this.throwExceptions)
            .uploadEntities(entityUploadResultObject);
        batchResultObject.markSuccessful();
      } catch (Exception e) {
        log.error("Caught an exception when trying to upload entities:", e);
        if (this.throwExceptions) {
          throw e;
        }
        batchResultObject.markFailed();
        batchResultProperties.put(PROPERTY_EXCEPTION_TYPE, e.getClass()
            .getName());
        batchResultProperties.put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage() != null ? e
            .getMessage() : "");
        batchResultProperties.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
      long batchEndTime = System.currentTimeMillis();
      batchResultProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
      //set outputArtifact and count-based properties from uploadResultObject
      ImmutableMap<EntityKey,KBID> uploadedEntities =
          entityUploadResultObject.getOutputArtifact().orNull();
      batchResultObject.setOutputArtifact
          (uploadedEntities==null?null:UploadedEntities.create(uploadedEntities));
      if(entityUploadResultObject.getPropertiesMap().isPresent()){
        batchResultProperties.putAll(entityUploadResultObject.getPropertiesMap().get());
      }
      batchResultObject.setPropertiesMap(batchResultProperties.build());
      retVal.add(batchResultObject);
    }
    return retVal.iterator();
  }
}


