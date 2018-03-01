package adept.e2e.kbupload.artifactdeduplicator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import adept.common.KBID;
import adept.e2e.driver.KBSingleton;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.kbupload.uploadedartifacts.UploadedRelations;
import adept.e2e.stageresult.BatchResultObject;
import adept.kbapi.KB;
import adept.kbapi.KBParameters;
import adept.kbapi.KBRelation;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class RelationDeduplicator implements
    FlatMapFunction<Iterator<BatchResultObject<UploadedRelations>>,
        BatchResultObject<ImmutableMap<RelationKey, KBID>>> {

  private static Logger log = LoggerFactory.getLogger(RelationDeduplicator.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;

  public RelationDeduplicator(KBParameters kbParameters, boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<ImmutableMap<RelationKey, KBID>>> call(
      Iterator<BatchResultObject<UploadedRelations>> inPart)
      throws Exception {
    List<BatchResultObject<ImmutableMap<RelationKey, KBID>>> retVal = new ArrayList<>();
    BatchResultObject<ImmutableMap<RelationKey, KBID>> batchResultObject = BatchResultObject
        .createEmpty();

    ImmutableList.Builder artifactIdsInvolved = ImmutableList.builder();
    ImmutableList.Builder failedArtifactIds = ImmutableList.builder();
    ImmutableTable.Builder artifactLevelProperties = ImmutableTable.builder();
    ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();

    batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    long batchStartTime = System.currentTimeMillis();
    // Map<NumericValueKey, List<KBNumber>> kbNumberCluster = new HashMap<>();
    // Map<TemporalValueKey, List<KBTemporalSpan>> kbTemporalSpanCluster = new HashMap<>();
    // Map<GenericThingKey, List<KBGenericThing>> kbGenericThingCluster = new HashMap<>();
    Map<RelationKey, List<KBRelation>> kbRelationCluster = new HashMap<>();
    KB kb = KBSingleton.getInstance(kbParameters);
    log.info("In relation-deduplicator, de-duplicating relations for now...");
    // int globalCount = 0;
    // int globalTime = 0;
    while (inPart.hasNext()) {
      BatchResultObject<UploadedRelations> relationUploadBatchResult = inPart.next();
      if (!relationUploadBatchResult.getOutputArtifact().isPresent()) {
        continue;
      }
      UploadedRelations uploadedRelations = relationUploadBatchResult.getOutputArtifact().get();
      //      for (NumericValueKey numericValueKey : uploadedRelations.uploadedNumericValues().keySet()) {
      //        KBNumber kbNumber = kb.getNumberValueByID(uploadedRelations.uploadedNumericValues().get
      //            (numericValueKey));
      //        List<KBNumber> kbNumberList = new ArrayList<>();
      //        if (kbNumberCluster.containsKey(numericValueKey)) {
      //          kbNumberList = kbNumberCluster.get(numericValueKey);
      //        }
      //        kbNumberList.add(kbNumber);
      //        kbNumberCluster.put(numericValueKey, kbNumberList);
      //      }
      log.info("Iterating over relationKeys in this RDD of the partition...");
      //      int count=0;
      //      long start = System.currentTimeMillis();
      for (RelationKey relationKey : uploadedRelations.uploadedRelations().keySet()) {
        KBID kbId = uploadedRelations.uploadedRelations().get(relationKey);
        String artifactId = kbId.getObjectID();
        try {
          KBRelation kbRelation =
              kb.getRelationByIdWithoutArgs(kbId);
          //        count++;
          List<KBRelation> kbRelationList = new ArrayList<>();
          if (kbRelationCluster.containsKey(relationKey)) {
            kbRelationList = kbRelationCluster.get(relationKey);
          }
          kbRelationList.add(kbRelation);
          kbRelationCluster.put(relationKey, kbRelationList);
        } catch (Exception e) {
          log.error("Caught exception when fetching relation with objectId {}", artifactId, e);
          if (this.throwExceptions) {
            throw e;
          }
          failedArtifactIds.add(artifactId);
          artifactLevelProperties.put(artifactId,
              PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
          artifactLevelProperties.put(artifactId, PROPERTY_EXCEPTION_MESSAGE, e
              .getMessage()!=null?e.getMessage():"");
          artifactLevelProperties.put(artifactId, PROPERTY_EXCEPTION_TRACE, e
              .getStackTrace());
        }
        log.info("...after iteration, size of global map={}", kbRelationCluster.size());
      }
      //      long timeTaken = (System.currentTimeMillis()-start)/1000;
      //      log.info("...after iteration, size of global map={}; KBRelations fetched={}; "
      //              + "time-taken={}; average-time={}",
      //          kbRelationCluster.size(),count,timeTaken+"s",(timeTaken/count)+"s");
      //      globalCount+=count;
      //      globalTime+=timeTaken;
    }
//    log.info("After iterating over all partitions, size of global map={}; KBRelations fetched={}; "
//            + "time-taken={}; average-time={}", kbRelationCluster.size(),globalCount,
//        globalTime+"s",(globalTime/globalCount)+"s");
    log.info("After iterating over all partitions, size of global map={}",
        kbRelationCluster.size());
    log.info("Iterating over kbRelationClusters...");
    ImmutableMap.Builder<RelationKey, KBID> newKBIDMap = ImmutableMap.builder();
    for (Map.Entry<RelationKey, List<KBRelation>> entry : kbRelationCluster.entrySet()) {
      RelationKey relationKey = entry.getKey();
      log.info("Merging KBRelations with relationKey {}", relationKey);
      long dedupStartTime = System.currentTimeMillis();
      String artifactId = relationKey.toString();
      List<KBRelation> kbRelationsToMerge = entry.getValue();
      try {
        KBRelation mergedKBRelation = KBRelation.mergeKBRelations(kbRelationsToMerge, kb);
        newKBIDMap.put(relationKey, mergedKBRelation.getKBID());
      } catch (Exception e) {
        log.error("Caught exception while iterating over kbRelationCluster with RelationKey {}",
            relationKey, e);
        if (this.throwExceptions) {
          throw e;
        }
        failedArtifactIds.add(artifactId);
        artifactLevelProperties.put(artifactId,
            PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
        artifactLevelProperties.put(artifactId, PROPERTY_EXCEPTION_MESSAGE, e
            .getMessage()!=null?e.getMessage():"");
        artifactLevelProperties.put(artifactId, PROPERTY_EXCEPTION_TRACE, e
            .getStackTrace());
      }
      artifactIdsInvolved.add(artifactId);
      long dedupEndTime = System.currentTimeMillis();
      artifactLevelProperties.put(artifactId, "TOTAL_RELATIONS_FOR_DEDUP",
          kbRelationsToMerge.size());
      artifactLevelProperties.put(artifactId, PROPERTY_TIME_TAKEN,
          (dedupEndTime - dedupStartTime));
      log.info("Done de-duplicating KBRelations for above RelationKey");
    }
    long batchEndTime = System.currentTimeMillis();
    batchResultObject.markSuccessful();
    batchResultObject.setOutputArtifact(newKBIDMap.build());
    batchLevelProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
    batchResultObject.setArtifactIdsInvolved(artifactIdsInvolved.build());
    batchResultObject.setFailedArtifactIds(failedArtifactIds.build());
    batchResultObject.setArtifactLevelProperties(artifactLevelProperties.build());
    batchResultObject.setPropertiesMap(batchLevelProperties.build());
    retVal.add(batchResultObject);
    return retVal.iterator();
  }
}

