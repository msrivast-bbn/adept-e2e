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
import java.util.Map.Entry;

import adept.common.KBID;
import adept.e2e.driver.KBSingleton;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.kbupload.uploadedartifacts.UploadedEvents;
import adept.e2e.stageresult.BatchResultObject;
import adept.kbapi.KB;
import adept.kbapi.KBEvent;
import adept.kbapi.KBParameters;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class EventDeduplicator implements
    FlatMapFunction<Iterator<BatchResultObject<UploadedEvents>>,
        BatchResultObject<ImmutableMap<EventKey, KBID>>> {

  private static Logger log = LoggerFactory.getLogger(EventDeduplicator.class);

  private final KBParameters kbParameters;
  private final boolean throwExceptions;

  public EventDeduplicator(KBParameters kbParameters, boolean throwExceptions) {
    this.kbParameters = kbParameters;
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<ImmutableMap<EventKey, KBID>>> call(
      Iterator<BatchResultObject<UploadedEvents>> inPart)
      throws Exception {
    List<BatchResultObject<ImmutableMap<EventKey, KBID>>> retVal = new ArrayList<>();
    BatchResultObject<ImmutableMap<EventKey, KBID>> batchResultObject = BatchResultObject
        .createEmpty();

    ImmutableList.Builder artifactIdsInvolved = ImmutableList.builder();
    ImmutableList.Builder failedArtifactIds = ImmutableList.builder();
    ImmutableTable.Builder artifactLevelProperties = ImmutableTable.builder();
    ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();

    batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    long batchStartTime = System.currentTimeMillis();
    Map<EventKey, List<KBEvent>> kbEventCluster = new HashMap<>();
    KB kb = KBSingleton.getInstance(kbParameters);
    log.info("In event-deduplicator, de-duplicating events for now...");
    while (inPart.hasNext()) {
      BatchResultObject<UploadedEvents> eventUploadBatchResult = inPart.next();
      if (!eventUploadBatchResult.getOutputArtifact().isPresent()) {
        continue;
      }
      UploadedEvents uploadedEvents = eventUploadBatchResult.getOutputArtifact().get();
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
      log.info("Iterating over eventKeys in this RDD of the partition...");
      for (EventKey eventKey : uploadedEvents.uploadedEvents().keySet()) {
        KBID kbId = uploadedEvents.uploadedEvents().get(eventKey);
        String artifactId = kbId.getObjectID();
        try {
          KBEvent kbEvent = kb.getEventByIdWithoutArgs(kbId);
          List<KBEvent> kbEventList = new ArrayList<>();
          if (kbEventCluster.containsKey(eventKey)) {
            kbEventList = kbEventCluster.get(eventKey);
          }
          kbEventList.add(kbEvent);
          kbEventCluster.put(eventKey, kbEventList);
        } catch (Exception e) {
          log.error("Caught exception when fetching event with objectId {}", artifactId, e);
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
        log.info("...after iteration, size of global map={}", kbEventCluster.size());
      }
    }
    log.info("Iterating over kbEventClusters...");
    ImmutableMap.Builder<EventKey, KBID> newKBIDMap = ImmutableMap.builder();
    for (Entry<EventKey, List<KBEvent>> entry : kbEventCluster.entrySet() ) {
      EventKey eventKey = entry.getKey();
      log.info("Merging KBEvents with eventKey {}", eventKey);
      long dedupStartTime = System.currentTimeMillis();
      String artifactId = eventKey.toString();
      List<KBEvent> kbEventsToMerge = entry.getValue();
      try {
        KBEvent mergedKBEvent = KBEvent.mergeKBEvents(kbEventsToMerge, kb);
        newKBIDMap.put(eventKey, mergedKBEvent.getKBID());
      } catch (Exception e) {
        log.error("Caught exception while iterating over kbEventCluster with EventKey {}",
            eventKey, e);
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
      artifactLevelProperties.put(artifactId, "TOTAL_EVENTS_FOR_DEDUP",
          kbEventsToMerge.size());
      artifactLevelProperties.put(artifactId, PROPERTY_TIME_TAKEN,
          (dedupEndTime - dedupStartTime));
      log.info("Done de-duplicating KBEvents for above eventKey");
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

