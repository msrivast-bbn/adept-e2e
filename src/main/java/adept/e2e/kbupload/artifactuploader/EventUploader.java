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

import adept.common.KBID;
import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.kbupload.uploadedartifacts.UploadedEvents;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.ResultObject;
import adept.kbapi.KBParameters;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class EventUploader implements
    FlatMapFunction<Iterator<BatchResultObject<ExtractedArtifacts>>,
        BatchResultObject<UploadedEvents>> {

  private static Logger log = LoggerFactory.getLogger(EventUploader.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;
  private final UploadedEntities deduplicatedEntities;
  private final UploadedNonEntityArguments uploadedNonEntityArguments;


  public EventUploader(KBParameters kbParameters, UploadedEntities
      deduplicatedEntities,
      UploadedNonEntityArguments uploadedNonEntityArguments,
      boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.deduplicatedEntities = deduplicatedEntities;
    this.uploadedNonEntityArguments = uploadedNonEntityArguments;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<UploadedEvents>> call(
      Iterator<BatchResultObject<ExtractedArtifacts>> inPart)
      throws Exception {
    List<BatchResultObject<UploadedEvents>> retVal = new ArrayList<>();
    log.info("Uploading events from input partitions...");
    while (inPart.hasNext()) {
      BatchResultObject<UploadedEvents> batchResultObject = BatchResultObject.createEmpty();
      ImmutableMap.Builder batchResultProperties = ImmutableMap.builder();
      batchResultProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      long batchStartTime = System.currentTimeMillis();
      BatchResultObject<ExtractedArtifacts> batchResultFromArtifactExtraction = inPart.next();
      if (!batchResultFromArtifactExtraction.isSuccessful() || !batchResultFromArtifactExtraction
          .getOutputArtifact().isPresent()
          || !batchResultFromArtifactExtraction.getArtifactIds().isPresent()) {
        continue;
      }
      ExtractedArtifacts extractedArtifacts = batchResultFromArtifactExtraction.getOutputArtifact
          ().get();
      log.info("Found the following docids in this RDD of the partition:");
      log.info(batchResultFromArtifactExtraction.getArtifactIds().or(ImmutableList.<String>of())
          .toString());
      batchResultObject.setArtifactIdsInvolved(batchResultFromArtifactExtraction.getArtifactIds()
          .orNull());
      ResultObject<ExtractedArtifacts,UploadedEvents> eventUploadResultObject = ResultObject
          .create(extractedArtifacts);
      try {
        log.info("Uploading events...");
        UploaderUtils.getInstance(
            this.kbParameters, this.throwExceptions).uploadEvents(eventUploadResultObject,deduplicatedEntities,
            uploadedNonEntityArguments);
        batchResultObject.markSuccessful();
      } catch (Exception e) {
        log.error("Caught an exception when trying to upload events:", e);
        if (this.throwExceptions) {
          throw e;
        }
        batchResultObject.markFailed();
        batchResultProperties.put(PROPERTY_EXCEPTION_TYPE, e.getClass()
            .getName());
        batchResultProperties.put(PROPERTY_EXCEPTION_MESSAGE,
            e.getMessage() != null ? e.getMessage() : "");
        batchResultProperties.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
      long batchEndTime = System.currentTimeMillis();
      batchResultProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
      //set outputArtifact and count-based properties from uploadResultObject
      batchResultObject.setOutputArtifact(eventUploadResultObject.getOutputArtifact().orNull());
      if(eventUploadResultObject.getPropertiesMap().isPresent()){
        batchResultProperties.putAll(eventUploadResultObject.getPropertiesMap().get());
      }
      batchResultObject.setPropertiesMap(batchResultProperties.build());
      retVal.add(batchResultObject);
    }
    return retVal.iterator();
  }
}
