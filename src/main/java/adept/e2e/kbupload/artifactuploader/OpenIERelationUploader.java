package adept.e2e.kbupload.artifactuploader;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import adept.common.KBID;
import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.stageresult.BatchResultObject;
import adept.kbapi.KBParameters;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class OpenIERelationUploader implements
    FlatMapFunction<Iterator<BatchResultObject<ExtractedArtifacts>>,
        BatchResultObject<ImmutableMap<KBID,String>>> {

//  private static final long serialVersionUID = 1L;

  private static Logger log = LoggerFactory.getLogger(OpenIERelationUploader.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;

  public OpenIERelationUploader(KBParameters kbParameters, boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<ImmutableMap<KBID,String>>> call(
      Iterator<BatchResultObject<ExtractedArtifacts>> inPart)
      throws Exception {
    List<BatchResultObject<ImmutableMap<KBID,String>>> retVal = new ArrayList<>();
    log.info("Uploading openIE relations from input partitions...");
    while (inPart.hasNext()) {
      BatchResultObject<ImmutableMap<KBID,String>> batchResultObject = BatchResultObject
          .createEmpty();
      ImmutableMap.Builder batchResultProperties = ImmutableMap.builder();
      ImmutableList.Builder artifactIdsInvolved = ImmutableList.builder();
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
      StringBuilder docIds = new StringBuilder();
      for (String docId : batchResultFromArtifactExtraction.getArtifactIds().get()) {
        docIds.append(docId).append(" ");
        artifactIdsInvolved.add(docId);
      }
      log.info(docIds.toString());
      ImmutableMap<KBID,String> uploadedRelations = null;
      try {
        log.info("Uploading relations...");
        uploadedRelations = UploaderUtils.getInstance(
            this.kbParameters, this.throwExceptions).uploadOpenIERelations(extractedArtifacts);
        batchResultObject.markSuccessful();
      } catch (Exception e) {
        log.error("Caught an exception when trying to upload relations:", e);
        if (this.throwExceptions) {
          throw e;
        }
        batchResultObject.markFailed();
        batchResultProperties.put(PROPERTY_EXCEPTION_TYPE, e.getClass()
            .getName());
        batchResultProperties.put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage()!=null?e.getMessage()
                                                                                 :"");
        batchResultProperties.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
      long batchEndTime = System.currentTimeMillis();
      batchResultObject.setOutputArtifact(uploadedRelations);
      if(uploadedRelations != null){
        batchResultProperties.put("numOpenIERelationsUploaded",uploadedRelations.size());
      }
      batchResultProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
      batchResultObject.setArtifactIdsInvolved(artifactIdsInvolved.build());
      batchResultObject.setPropertiesMap(batchResultProperties.build());
      retVal.add(batchResultObject);
    }
    return retVal.iterator();
  }

}
