package adept.e2e.artifactextraction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimap;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.common.Chunk;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.NumberPhrase;
import adept.common.Pair;
import adept.common.Relation;
import adept.common.TimePhrase;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.GenericThingKey;
import adept.e2e.artifactextraction.artifactkeys.NumericValueKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.artifactextraction.artifactkeys.TemporalValueKey;
import adept.e2e.artifactextraction.mergedartifacts.ItemWithProvenances;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentEvent;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentRelation;
import adept.e2e.stageresult.BatchResultObject;
import scala.Tuple2;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class ArtifactExtractor implements
    FlatMapFunction<Iterator<Tuple2<String, HltContentContainer>>,
        BatchResultObject<ExtractedArtifacts>> {

  private static final long serialVersionUID = 1L;
  private static Logger log = LoggerFactory.getLogger(ArtifactExtractor.class);
  private final boolean throwExceptions;
  private final Map<String,Pair<String,String>> algorithmTypeToOntologyFilesMap;
  private final boolean useMissingEventTypesHack;

  public ArtifactExtractor(boolean throwExceptions, Map<String,Pair<String,String>>
      algorithmTypeToOntologyFilesMap, boolean
      useMissingEventTypesHack) {
    this.throwExceptions = throwExceptions;
    this.algorithmTypeToOntologyFilesMap = algorithmTypeToOntologyFilesMap;
    this.useMissingEventTypesHack = useMissingEventTypesHack;
  }

  @Override
  public Iterator<BatchResultObject<ExtractedArtifacts>> call(Iterator<Tuple2<String,
      HltContentContainer>>
      inPart)
      throws Exception {

    BatchResultObject<ExtractedArtifacts> batchResultObject = BatchResultObject.createEmpty();
    ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();
    batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    long batchStartTime = System.currentTimeMillis();

    Map<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>> mergedEntitySets =
        new HashMap<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>>();
    Map<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergedEntities = null;
    ImmutableMultimap.Builder<NumericValueKey, NumberPhrase>
        mergedNumericValues = ImmutableMultimap.builder();
    ImmutableMultimap.Builder<TemporalValueKey,TimePhrase> mergedTemporalValues =
        ImmutableMultimap.builder();
    ImmutableMultimap.Builder<GenericThingKey, Chunk> mergedGenericThings =
        ImmutableMultimap.builder();
    Map<RelationKey, MergedDocumentRelation> mergedRelations =
        new HashMap<RelationKey, MergedDocumentRelation>();
    Map<EventKey, MergedDocumentEvent> mergedEvents =
        new HashMap<EventKey, MergedDocumentEvent>();
    ImmutableList.Builder<Relation> allOpenIERelations = ImmutableList.builder();

    MergeUtils mergeUtils = new MergeUtils(throwExceptions,
        algorithmTypeToOntologyFilesMap,useMissingEventTypesHack);

    int numMergedEntities = 0;
    int numMergedDocRelations = 0;
    int numMergedDocEvents = 0;

    log.info("Processing documents in batch...");
    long maxEntityId = -1L;
    String docId = null;
    ImmutableList.Builder artifactIdsInvolved = ImmutableList.builder();
    ImmutableList.Builder failedArtifactIds = ImmutableList.builder();
    ImmutableTable.Builder artifactLevelProperties = ImmutableTable.builder();
    Map<String,Integer> countBasedProperties = new HashMap<>();
    while (inPart.hasNext()) {
      Tuple2<String, HltContentContainer> inPair = inPart.next();
      try {
        HltContentContainer hltContentContainer = inPair._2();
        docId = hltContentContainer.getDocumentId();
        log.info("Extracting artifacts from document: {}", docId);
        artifactIdsInvolved.add(docId);
        log.info("Extracting entities...");
        long newMaxEntityId =
            mergeUtils.extractEntities(hltContentContainer, mergedEntitySets);
        if (newMaxEntityId > maxEntityId) {
          maxEntityId = newMaxEntityId;
        }
        log.info("Extracting doc-relations and arguments...");
        mergeUtils.extractDocumentRelationsAndArguments(hltContentContainer,
            mergedEntitySets, mergedNumericValues, mergedTemporalValues, mergedGenericThings,
            mergedRelations,countBasedProperties);
        log.info("Extracting doc-events and arguments...");
        mergeUtils.extractDocumentEventsAndArguments(hltContentContainer,
            mergedEntitySets, mergedTemporalValues, mergedGenericThings,
            mergedEvents,countBasedProperties);
        log.info("Adding all OpenIE relations...");
        if(hltContentContainer.getRelations()!=null) {
          allOpenIERelations.addAll(hltContentContainer.getRelations());
        }
      } catch (Exception e) {
        log.error("Could not extract artifacts from document: {}", docId, e);
        if (throwExceptions) {
          throw e;
        }
        failedArtifactIds.add(docId);
        artifactLevelProperties.put(docId, PROPERTY_EXCEPTION_TYPE, e.getClass()
            .getName());
        artifactLevelProperties.put(docId, PROPERTY_EXCEPTION_MESSAGE, e.getMessage
            ()!=null?e.getMessage():"");
        artifactLevelProperties.put(docId, PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
    }
    log.info("Merging all entities...");
    List<BatchResultObject<ExtractedArtifacts>> retVal = new ArrayList<>();
    ExtractedArtifacts extractedArtifacts = null;
    try {
      mergedEntities = mergeUtils.mergeEntitySets(mergedEntitySets, maxEntityId);
      numMergedEntities+=mergedEntities.size();
      numMergedDocRelations+=mergedRelations.size();
      numMergedDocEvents+=mergedEvents.size();

      ExtractedArguments extractedArguments = ExtractedArguments.create(buildArgumentMap(mergedNumericValues.build()), buildArgumentMap(mergedTemporalValues.build()),
          buildArgumentMap(mergedGenericThings.build()));
      extractedArtifacts = ExtractedArtifacts.create(ImmutableMap.copyOf(mergedEntities),
          extractedArguments,
          ImmutableMap.copyOf(mergedRelations), ImmutableMap.copyOf(mergedEvents),
          allOpenIERelations.build());
      batchResultObject.markSuccessful();
    } catch (Exception e) {
      log.error("Caught the following exception when trying to merge artifacts for this batch: ",
          e);
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
    batchResultObject.setOutputArtifact(extractedArtifacts);
    batchLevelProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
    countBasedProperties.put("numMergedDocEvents",numMergedDocEvents);
    countBasedProperties.put("numMergedDocRelations",numMergedDocRelations);
    countBasedProperties.put("numMergedEntities",numMergedEntities);
    batchLevelProperties.putAll(countBasedProperties);
    batchResultObject.setArtifactIdsInvolved(artifactIdsInvolved.build());
    batchResultObject.setFailedArtifactIds(failedArtifactIds.build());
    batchResultObject.setArtifactLevelProperties(artifactLevelProperties.build());
    batchResultObject.setPropertiesMap(batchLevelProperties.build());
    retVal.add(batchResultObject);
    return retVal.iterator();
  }

  private <T,U,V> ImmutableMap<T,ItemWithProvenances<U,V>> buildArgumentMap(Multimap<T,V> keyProvenanceMap){
    ImmutableMap.Builder<T,ItemWithProvenances<U,V>> argMapToReturn = ImmutableMap.builder();
    for(T itemKey : keyProvenanceMap.keySet()) {
      ImmutableSet<V> provenances = ImmutableSet.copyOf(keyProvenanceMap.get(itemKey));
      if (itemKey instanceof NumericValueKey) {
        argMapToReturn
            .put(itemKey, ItemWithProvenances.create(((NumericValueKey) itemKey).numericValue(),
                provenances));

      } else if (itemKey instanceof TemporalValueKey) {
        argMapToReturn
            .put(itemKey, ItemWithProvenances.create(((TemporalValueKey) itemKey).temporalValue(),
                provenances));
      } else if (itemKey instanceof GenericThingKey) {
        argMapToReturn
            .put(itemKey, ItemWithProvenances.create(((GenericThingKey) itemKey).genericThing(),
                provenances));
      }
    }
    return argMapToReturn.build();
  }

}


