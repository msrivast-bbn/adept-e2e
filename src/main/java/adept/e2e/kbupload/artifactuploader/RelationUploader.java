package adept.e2e.kbupload.artifactuploader;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.kbupload.uploadedartifacts.UploadedRelations;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.ResultObject;
import adept.kbapi.KBParameters;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class RelationUploader implements
    FlatMapFunction<Iterator<BatchResultObject<ExtractedArtifacts>>,
        BatchResultObject<UploadedRelations>> {

//  private static final long serialVersionUID = 1L;

  private static Logger log = LoggerFactory.getLogger(RelationUploader.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;
  private final UploadedEntities deduplicatedEntities;
  private final UploadedNonEntityArguments uploadedArguments;

  public RelationUploader(KBParameters kbParameters, UploadedEntities deduplicatedEntities,
      UploadedNonEntityArguments uploadedArguments,
      boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.deduplicatedEntities = deduplicatedEntities;
    this.uploadedArguments = uploadedArguments;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<UploadedRelations>> call(
      Iterator<BatchResultObject<ExtractedArtifacts>> inPart)
      throws Exception {
    List<BatchResultObject<UploadedRelations>> retVal = new ArrayList<>();
    log.info("Uploading relations from input partitions...");
    while (inPart.hasNext()) {
      BatchResultObject<UploadedRelations> batchResultObject = BatchResultObject.createEmpty();
      ImmutableMap.Builder batchResultProperties = ImmutableMap.builder();
      batchResultProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      long batchStartTime = System.currentTimeMillis();
      BatchResultObject<ExtractedArtifacts> batchResultFromArtifactExtraction = inPart.next();
      if (!batchResultFromArtifactExtraction.isSuccessful() || !batchResultFromArtifactExtraction
          .getOutputArtifact().isPresent() ||
          !batchResultFromArtifactExtraction.getArtifactIds().isPresent()) {
        continue;
      }
      ExtractedArtifacts extractedArtifacts = batchResultFromArtifactExtraction.getOutputArtifact
          ().get();
      log.info("Found the following docids in this RDD of the partition:");
      log.info(batchResultFromArtifactExtraction.getArtifactIds().or(ImmutableList.<String>of())
          .toString());
      batchResultObject.setArtifactIdsInvolved(batchResultFromArtifactExtraction.getArtifactIds()
          .orNull());
      ResultObject<ExtractedArtifacts,UploadedRelations> relationUploadResultObject = ResultObject
          .create(extractedArtifacts);
      try {
        log.info("Uploading relations...");
        UploaderUtils.getInstance(
            this.kbParameters, this.throwExceptions).uploadRelations(relationUploadResultObject,deduplicatedEntities,
            uploadedArguments);
        batchResultObject.markSuccessful();
      } catch (Exception e) {
        log.error("Caught an exception when trying to upload relations:", e);
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
      batchResultObject.setOutputArtifact(relationUploadResultObject.getOutputArtifact().orNull());
      if(relationUploadResultObject.getPropertiesMap().isPresent()){
        batchResultProperties.putAll(relationUploadResultObject.getPropertiesMap().get());
      }
      batchResultObject.setPropertiesMap(batchResultProperties.build());
      retVal.add(batchResultObject);
    }
    return retVal.iterator();
  }

}
