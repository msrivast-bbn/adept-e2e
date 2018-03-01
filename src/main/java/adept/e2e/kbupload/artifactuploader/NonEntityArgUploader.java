package adept.e2e.kbupload.artifactuploader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import adept.e2e.artifactextraction.ExtractedArguments;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.ResultObject;
import adept.kbapi.KBParameters;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class NonEntityArgUploader implements
    FlatMapFunction<Iterator<BatchResultObject<ExtractedArguments>>,
        BatchResultObject<UploadedNonEntityArguments>> {

  private static Logger log = LoggerFactory.getLogger(NonEntityArgUploader.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;

  public NonEntityArgUploader(KBParameters kbParameters, boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<UploadedNonEntityArguments>> call(
      Iterator<BatchResultObject<ExtractedArguments>> inPart)
      throws Exception {

    List<BatchResultObject<UploadedNonEntityArguments>> perPartitionUploadResults = new ArrayList<>();
    log.info("Uploading non-entity arguments from input partitions...");
    while (inPart.hasNext()) {
      BatchResultObject<UploadedNonEntityArguments> batchResultObject = BatchResultObject
          .createEmpty();
      ImmutableMap.Builder batchResultProperties = ImmutableMap.builder();
      batchResultProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      long batchStartTime = System.currentTimeMillis();
      //Note: Currently, we don't care if any BatchResultObjects were successful in the previous
      // run. Support for that will only be added when implementing iterative KB-upload.
      BatchResultObject<ExtractedArguments> batchResultFromArgumentExtraction = inPart.next();
      if (!batchResultFromArgumentExtraction.isSuccessful() || !batchResultFromArgumentExtraction
          .getOutputArtifact().isPresent() ||
          !batchResultFromArgumentExtraction.getArtifactIds().isPresent()
          ) {
        continue;
      }

      ExtractedArguments extractedArguments = batchResultFromArgumentExtraction.getOutputArtifact
          ().get();
      log.info("Found the following docids in this RDD of the partition:");
      log.info(batchResultFromArgumentExtraction.getArtifactIds().or(ImmutableList.<String>of())
          .toString());
      batchResultObject.setArtifactIdsInvolved(batchResultFromArgumentExtraction.getArtifactIds()
          .orNull());
      ResultObject<ExtractedArguments, UploadedNonEntityArguments>
          argUploadResultObject = ResultObject.create(extractedArguments);
      try {
        log.info("Uploading non-entity arguments...");
        UploaderUtils.getInstance(
            this.kbParameters, this.throwExceptions)
            .uploadNonEntityArgs(argUploadResultObject);
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
      batchResultObject.setOutputArtifact(argUploadResultObject.getOutputArtifact().orNull());
      if(argUploadResultObject.getPropertiesMap().isPresent()){
        batchResultProperties.putAll(argUploadResultObject.getPropertiesMap().get());
      }
      batchResultObject.setPropertiesMap(batchResultProperties.build());
      perPartitionUploadResults.add(batchResultObject);
    }

    return perPartitionUploadResults.iterator();
  }
}


