package adept.e2e.kbupload.artifactdeduplicator;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multiset;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.common.KBID;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.driver.KBSingleton;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import adept.e2e.stageresult.BatchResultObject;
import adept.kbapi.KB;
import adept.kbapi.KBEntity;
import adept.kbapi.KBGenericThing;
import adept.kbapi.KBParameters;
import adept.kbapi.KBProvenance;
import adept.kbapi.KBQueryException;
import adept.kbapi.KBTextProvenance;
import adept.kbapi.KBThing;
import adept.kbapi.KBUpdateException;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class EntityDeduplicator implements
    FlatMapFunction<Iterator<BatchResultObject<UploadedEntities>>,
        BatchResultObject<UploadedEntities>> {

  private static Logger log = LoggerFactory.getLogger(EntityDeduplicator.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;

  public EntityDeduplicator(KBParameters kbParameters, boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<UploadedEntities>> call(
      Iterator<BatchResultObject<UploadedEntities>> inPart)
      throws Exception {
    List<BatchResultObject<UploadedEntities>> retVal = new ArrayList<>();
    Map<EntityKey, List<KBThing>> kbArtifactCluster = new HashMap<>();
    BatchResultObject<UploadedEntities> batchResultObject = BatchResultObject
        .createEmpty();

    ImmutableList.Builder artifactIdsInvolved = ImmutableList.builder();
    ImmutableList.Builder failedArtifactIds = ImmutableList.builder();
    ImmutableTable.Builder artifactLevelProperties = ImmutableTable.builder();
    ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();

    batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    long batchStartTime = System.currentTimeMillis();
    KB kb = KBSingleton.getInstance(kbParameters);
    log.info("Merging inserted KBEntities from input partitions...");
    while (inPart.hasNext()) {
      //Note: Currently, we don't care if any BatchResultObjects were successful in the previous
      // run. Support for that will only be added when implementing iterative KB-upload.
      BatchResultObject<UploadedEntities> entityUploadBatchResult = inPart.next();
      if (!entityUploadBatchResult.getOutputArtifact().isPresent()) {
        continue;
      }
      UploadedEntities partitionUploadedEntities = entityUploadBatchResult.getOutputArtifact()
          .get();
      log.info("Iterating over EntityKeys in this RDD of the partition...");
      for (Map.Entry<EntityKey, KBID> entry : partitionUploadedEntities
          .uploadedEntities().entrySet()) {
        String artifactId = entry.getValue().getObjectID();
        try {
          KBThing kbEntityOrGenericThing = null;
          try {
            kbEntityOrGenericThing = kb.getEntityById(entry.getValue());
          }catch (KBQueryException ex){
            //if the artifact couldn't be retrieved as a KBEntity, it has to be a KBGenericThing
            kbEntityOrGenericThing = kb.getGenericThingByID(entry.getValue());
          }
          List<KBThing> kbArtifactList = new ArrayList<>();
          if (kbArtifactCluster.containsKey(entry.getKey())) {
              kbArtifactList = kbArtifactCluster.get(entry.getKey());
          }
          kbArtifactList.add(kbEntityOrGenericThing);
          kbArtifactCluster.put(entry.getKey(), kbArtifactList);
//          artifactIdsInvolved.add(artifactId);
        } catch (Exception e) {
          log.error("Caught exception when fetching entitiy with objectId {}"
              , artifactId, e);
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
        log.info("...after iteration, size of global map={}", kbArtifactCluster.size());
      }
    }
    log.info("Iterating over kbEntityClusters...");
    ImmutableMap.Builder<EntityKey, KBID> newKBIDMap = ImmutableMap.builder();
    for (Map.Entry<EntityKey, List<KBThing>> entry : kbArtifactCluster.entrySet()) {
      EntityKey entityKey = entry.getKey();
      log.info("Merging KBEntities with EntityKey {}", entityKey);
      long dedupStartTime = System.currentTimeMillis();
      String artifactId = entityKey.toString();
      List<KBEntity> kbEntitiesToMerge = FluentIterable.from(entry.getValue()).filter(KBEntity.class).toList();
      List<KBGenericThing> kbGenericThingsToMerge = FluentIterable.from(entry.getValue()).filter(KBGenericThing.class).toList();
      KBThing mergedKBEntityOrGenericThing = null;
      try {
        if(!kbEntitiesToMerge.isEmpty()) {//We have a KBEntity cluster to merge
          mergedKBEntityOrGenericThing = KBEntity.mergeKBEntities(kbEntitiesToMerge, kb);
        }else {//We have a KBGenericThing cluster to merge
          mergedKBEntityOrGenericThing = mergeKBGenericThings(kbGenericThingsToMerge, kb);
        }
        newKBIDMap.put(entityKey,mergedKBEntityOrGenericThing.getKBID());
      } catch (Exception e) {
        log.error("Caught exception while iterating over kbEntityCluster with entityKey {}",
            entityKey, e);
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
      artifactLevelProperties.put(artifactId, "TOTAL_ENTITIES_FOR_DEDUP",
          kbEntitiesToMerge.size());
      artifactLevelProperties.put(artifactId, PROPERTY_TIME_TAKEN,
          (dedupEndTime - dedupStartTime));
      log.info("Done de-duplicating KBEntities for above externalID");
    }



    long batchEndTime = System.currentTimeMillis();
    batchResultObject.markSuccessful();
    batchResultObject.setOutputArtifact(UploadedEntities.create(newKBIDMap
        .build()));
    batchLevelProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
    batchResultObject.setArtifactIdsInvolved(artifactIdsInvolved.build());
    batchResultObject.setFailedArtifactIds(failedArtifactIds.build());
    batchResultObject.setArtifactLevelProperties(artifactLevelProperties.build());
    batchResultObject.setPropertiesMap(batchLevelProperties.build());

    retVal.add(batchResultObject);
    return retVal.iterator();
  }

  /**
   * Merge KBGenericThings for entity-deduplication. These KBGenericThings are items created by uploading
   * adept Entities with GenericThing types. The uploaded adept Entities could still point to the same real-world
   * object. For example, two entities with GenericThing type TITLE and mentions "President" and "US President" could point to
   * the same real world entity (same external KBID). This method deduplicates such KBGenericThings by taking a union of their provenances
   * and by choosing the most frequent string-value as the string-value for the merged KBGenericThing.
   */
  private static KBGenericThing mergeKBGenericThings(List<KBGenericThing> kbGenericThingsToMerge, KB kb)
      throws KBUpdateException, KBQueryException {

    checkNotNull(kbGenericThingsToMerge);
    checkArgument(!kbGenericThingsToMerge.isEmpty(),"kbGenericThingsToMerge list cannot be empty.");
    if(kbGenericThingsToMerge.size()==1){
      return kbGenericThingsToMerge.get(0);
    }
    KBGenericThing kbGenericThingToRetain = null;
    Set<KBProvenance> allProvenances = new HashSet<>();
    Multiset<String> allCanonicalStrings = HashMultiset.create();
    for (KBGenericThing kbGenericThing : kbGenericThingsToMerge) {
      log.info("KBGenericThing to merge: {}",kbGenericThing.getKBID().getObjectID());
      if(kbGenericThingToRetain==null) {
        kbGenericThingToRetain = kbGenericThing;
      }
      //canonical string and confidence
      String canonicalString = kbGenericThing.getCanonicalString();
      allCanonicalStrings.add(canonicalString);
      //provenances
      allProvenances.addAll(kbGenericThing.getProvenances());
    }
    KBGenericThing.UpdateBuilder updateBuilder = kbGenericThingToRetain.updateBuilder();
    //provenances
    for(KBProvenance provenance : allProvenances) {
      if(!kbGenericThingToRetain.getProvenances().contains(provenance)) {
        KBTextProvenance.UpdateBuilder provenanceUpdateBuilder =
            ((KBTextProvenance)provenance).getUpdateBuilder(kbGenericThingToRetain.getKBID());
//	provenanceUpdateBuilder.setSourceEntityKBID(kbGenericThingToRetain.getKBID());
        log.info("Added provenance {}.... ",provenanceUpdateBuilder
            .getKBID().getObjectID());
        log.info("...to update with KBId: {}",provenanceUpdateBuilder.getSourceEntityKBID()
            .getObjectID());
        updateBuilder.addProvenanceToReassign(provenanceUpdateBuilder);
      }
    }
    KBGenericThing mergedKBGenericThing =  updateBuilder.update(kb);
    log.info("Deleting duplicate KBGenericThings..");
    for(KBGenericThing kbGenericThing : kbGenericThingsToMerge) {
      if(!kbGenericThing.getKBID().equals(mergedKBGenericThing.getKBID())) {
        log.info("Deleting KBGenericThing with KBID {}...",kbGenericThing.getKBID().getObjectID());
        kb.deleteDuplicateKBObject(kbGenericThing.getKBID());
      }
    }
    return mergedKBGenericThing;
  }

}

