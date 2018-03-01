package adept.e2e.artifactextraction;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import adept.common.Chunk;
import adept.common.Coreference;
import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.GenericThing;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Item;
import adept.common.NumberPhrase;
import adept.common.NumericValue;
import adept.common.OntType;
import adept.common.Pair;
import adept.common.RelationMention;
import adept.common.TemporalValue;
import adept.common.TimePhrase;
import adept.common.Type;
import adept.e2e.analysis.StatsGenerator;
import adept.e2e.artifactextraction.artifactkeys.ArgumentKey;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.GenericThingKey;
import adept.e2e.artifactextraction.artifactkeys.ItemKey;
import adept.e2e.artifactextraction.artifactkeys.NumericValueKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.artifactextraction.artifactkeys.TemporalValueKey;
import adept.e2e.artifactextraction.mergedartifacts.ItemWithProvenances;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentEvent;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentRelation;
import adept.e2e.driver.E2eUtil;
import adept.kbapi.KBOntologyMap;
import adept.metadata.SourceAlgorithm;

import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_COREF;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_EVENT_EXTRACTION;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_RELATION_EXTRACTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MergeUtils {

  private Logger log = LoggerFactory.getLogger(MergeUtils.class);
  private boolean throwExceptions;
  private Map<String,Pair<String,String>> algorithmTypeToOntologyFilesMap;
  private boolean useMissingEventTypesHack;

  public MergeUtils(boolean throwExceptions, Map<String,Pair<String,String>>
      algorithmTypeToOntologyFilesMap, boolean useMissingEventTypesHack){
    this.throwExceptions = throwExceptions;
    this.algorithmTypeToOntologyFilesMap = algorithmTypeToOntologyFilesMap;
    this.useMissingEventTypesHack = useMissingEventTypesHack;
  }

  public long extractEntities(HltContentContainer hltCC,
      Map<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>> mergedEntities)
      throws Exception {
    Pair<String,String> ontologyMapFiles = algorithmTypeToOntologyFilesMap.get
        (ALGORITHM_TYPE_COREF);
    checkNotNull(ontologyMapFiles,"AlgorithmTypeToOntologyMap must have a mapping for "
        + ALGORITHM_TYPE_COREF);
    KBOntologyMap kbOntologyMap = KBOntologyMap.loadOntologyMap(ontologyMapFiles.getL(),
        ontologyMapFiles.getR());
    long maxEntityId = -1L;
    for (Coreference coreference : hltCC.getCoreferences()) {
      for (Entity entity : coreference.getEntities()) {
        if (entity.getEntityId() > maxEntityId) {
          maxEntityId = entity.getEntityId();
        }
        String entityDetails = entity.getValue() + ";" +
            entity.getCanonicalMention().getValue() + ";" + entity.getCanonicalMention().getDocId()
            + ";" +
            entity.getEntityType().getType() + ";" + entity.getCanonicalMention().getAlgorithmName()
            + ";" + entity.getIdString();
        log.info("Creating EntityKey for Entity: {}" , entityDetails);
        //if the entity is CBRNE type, use the customized kbOntologyMap for cbrne types
        //comment out if not supporting CBRNE entities or if CBRNE-type is included among other entity types and is just an OntType/IType object
//        Optional<EntityKey> entityKey = EntityKey.getKey(entity, hltCC,
//                entity.getEntityType() instanceof CBRNEType? CBRNEOntology.getInstance().getCBRNEOntologyMap():kbOntologyMap);
        Optional<EntityKey> entityKey = EntityKey.getKey(entity, hltCC, kbOntologyMap);
        if (!entityKey.isPresent()) {
          throw new Exception("No KBID found to create EntityKey for Entity: " + entityDetails);
        }
        Set<ItemWithProvenances<Entity,EntityMention>> duplicateEntities = new HashSet<>();
        if (mergedEntities.containsKey(entityKey.get())) {
          log.info("EntityKey {} already exists in map...",entityKey.get().kbID());
          duplicateEntities = mergedEntities.get(entityKey.get());
        }
        Set<EntityMention> provenances = new HashSet<EntityMention>();
        for (EntityMention mention : hltCC.getEntityMentions()) {
          if (mention.getEntityIdDistribution().containsKey(entity.getEntityId()) &&
              mention.getDocId() != null && entity.getCanonicalMention().getDocId() != null
              && mention.getDocId().equals(entity.getCanonicalMention().getDocId())
              && mention.getAlgorithmName() != null && entity.getAlgorithmName() != null
              && mention.getAlgorithmName().equals(entity.getAlgorithmName())) {
            provenances.add(mention);
          }
        }
        duplicateEntities.add(ItemWithProvenances.create(entity, ImmutableSet.copyOf(provenances)));
        mergedEntities.put(entityKey.get(), duplicateEntities);
      }
    }
    return maxEntityId;
  }

  public Map<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergeEntitySets(
      Map<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>> mergedEntitySets,
      long newEntityId) {
    Map<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergedEntities = new HashMap<>();
    for (Entry<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>> entry : mergedEntitySets.entrySet()) {
      EntityKey entityKey = entry.getKey();
      log.info("Merging entity-set with key: {}", entityKey.kbID().getObjectID());
      Map<String, Float> bestMentionToEntityConfidence = new HashMap<>();
      Set<EntityMention> provenances = new HashSet<>();
      Map<Double, Multiset<String>> canonicalStringConfidences = new HashMap<>();
      Map<String, EntityMention> valueToCanonicalMentionMap = new HashMap<String,
          EntityMention>();
      double weightTotal = 0.0;
      double maxCanonicalStringConfidence = -1.0;
      double mergedEntityConfidence = 0.0;//average of entity-confidences weighted by number of
      // provenances
      IType bestEntityType = entityKey.type();//best-type is the type in the EntityKey
      //TODO: Potentially add logic to merge entity-type confidences
      double mergedBestTypeConfidence = 0.0; //average of best-type confidences weighted by
      // number of provenances
      for (ItemWithProvenances<Entity,EntityMention> item : entry.getValue()) {
        Entity entity = (Entity) item.item();
        log.info("Entity to merge: canonicalString={} mentionConfidence={} entityType={}"
        + " entityTypeConfidence={} otherTypes={} docId={}", entity.getCanonicalMention().getValue()
        , entity.getCanonicalMentionConfidence(), entity.getEntityType().getType(), entity.getTypeConfidence()
        , entity.getAllTypes(), entity.getCanonicalMention().getDocId());

        EntityMention canonicalMention = entity.getCanonicalMention();

        String mentionValue = canonicalMention.getValue();
        Double mentionConfidence = entity.getCanonicalMentionConfidence();
        valueToCanonicalMentionMap.put(mentionValue, canonicalMention);
        Multiset<String> canonicalStrings = canonicalStringConfidences.get(mentionConfidence);
        if (canonicalStrings == null) {
          canonicalStrings = HashMultiset.create();
        }
        canonicalStrings.add(mentionValue);
        canonicalStringConfidences.put(mentionConfidence, canonicalStrings);
        //TODO: Add tie-breaking logic here to get the bestCanonicalString
        if (maxCanonicalStringConfidence < mentionConfidence) {
          maxCanonicalStringConfidence = mentionConfidence;
        }

        if (item.getProvenances() != null) {
          for (EntityMention em : item.getProvenances()) {
            float conf = em.getConfidence(entity.getEntityId());
            Float bestConf = bestMentionToEntityConfidence.get(em.getIdString());
            //TODO: Add tie-breaking logic
            if (bestConf == null || conf > bestConf) {
              bestMentionToEntityConfidence.put(em.getIdString(), conf);
            }
          }
          mergedEntityConfidence+= entity.getEntityConfidence()*item.getProvenances().size();
          provenances.addAll(item.getProvenances());
        } else {
          log.info("Entity has no mention: {} {} {} {} {}", entity.getAlgorithmName(),
              entity.getEntityType().getType(), entity.getValue(), entity
              .getCanonicalMention().getAlgorithmName(), entity.getCanonicalMention().getValue());
        }
      }
      if (provenances.isEmpty()) {
        log.info("Skipping this entityKey since the list of provenances is empty");
        continue;
      }

      int maxBestCanonicalStringCount = 0;
      String bestCanonicalString = null;
      for (String canonicalString : canonicalStringConfidences.get(maxCanonicalStringConfidence)) {
        int count = canonicalStringConfidences.get(maxCanonicalStringConfidence).count
            (canonicalString);
        //TODO: Tie breaking needed based on eval analysis or qualitative visual evaluation from KB explorer
        if (count > maxBestCanonicalStringCount) {
          maxBestCanonicalStringCount = count;
          bestCanonicalString = canonicalString;
        }
      }
      mergedEntityConfidence = mergedEntityConfidence/provenances.size();
      //If the entity's being merged have multiple types, we are discarding non primary types.
      //All we care about is that the entities being merged have the same primary type.
      Entity newEntity = new Entity(++newEntityId, entityKey.type());
      newEntity.setEntityConfidence(mergedEntityConfidence);
      newEntity.setAlgorithmName("MERGED");
      newEntity.setSourceAlgorithm(new SourceAlgorithm("MERGED", "BBN"));
//      newEntity.addTypes(entityTypeConfidences);
      EntityMention canonicalMention = valueToCanonicalMentionMap.get(bestCanonicalString);
      StringBuilder provenanceString = new StringBuilder();
      for (EntityMention mention : provenances) {
        mention.addEntityConfidencePair(newEntity.getEntityId(),
            bestMentionToEntityConfidence.get(mention.getIdString()));
        mention.setEntityType(newEntity.getEntityType());
        provenanceString.append(mention.getValue()).append(' ').append(mention.getAlgorithmName())
            .append(';');
      }
      canonicalMention.addEntityConfidencePair(newEntity.getEntityId(), (float)newEntity
          .getEntityConfidence());
      canonicalMention.setEntityType(newEntity.getEntityType());
      newEntity.setCanonicalMentions(canonicalMention);
      newEntity.setCanonicalMentionConfidence(maxCanonicalStringConfidence);
      mergedEntities.put(entityKey, ItemWithProvenances.create(newEntity,
          ImmutableSet.copyOf(provenances)));
      log.info("Created a MergedEntity: " +
          "entity-id={} type={} canonical-mention={} KBID={} provenances={}"
          , newEntityId, newEntity.getEntityType().getType(), bestCanonicalString, entityKey.kbID()
              .getObjectID(), provenanceString.toString());
     }
    return mergedEntities;
  }

  public void extractDocumentRelationsAndArguments(HltContentContainer hltCC,
      Map<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>> mergedEntitySets,
      ImmutableMultimap.Builder<NumericValueKey,NumberPhrase> numericValueArgs,
      ImmutableMultimap.Builder<TemporalValueKey,TimePhrase> temporalValueArgs,
      ImmutableMultimap.Builder<GenericThingKey,Chunk> genericThingArgs,
      Map<RelationKey, MergedDocumentRelation> mergedRelations, Map<String,Integer> countBasedPropertiesIn) throws Exception {

    if (hltCC.getDocumentRelations() == null || hltCC.getDocumentRelations().size() == 0) {
      return;
    }

    Map<String,Integer> countBasedProperties = new HashMap<>();
    int numRelationsWithoutArgs = 0;
    int numRelArgsNotPresentInOntology = 0;
    int numRelArgFillerNotPresent = 0;
    int numRelEntityArgsWithoutKey = 0;
    int numRelEntityArgsNotMappedToCoref = 0;
    int numRelGenericThingArgsWithoutKey = 0;
    int numRelationsWithoutKey = 0;
    Pair<String,String> ontologyMapFiles = algorithmTypeToOntologyFilesMap.get
        (ALGORITHM_TYPE_RELATION_EXTRACTION);
    checkNotNull(ontologyMapFiles,"AlgorithmTypeToOntologyMap must have a mapping for "
        + ALGORITHM_TYPE_RELATION_EXTRACTION);
    KBOntologyMap kbOntologyMap = KBOntologyMap.loadOntologyMap(ontologyMapFiles.getL(),
        ontologyMapFiles.getR());
    ontologyMapFiles = algorithmTypeToOntologyFilesMap.get
        (ALGORITHM_TYPE_COREF);
    checkNotNull(ontologyMapFiles,"AlgorithmTypeToOntologyMap must have a mapping for "
        + ALGORITHM_TYPE_COREF);
    KBOntologyMap entityOntologyMap = KBOntologyMap.loadOntologyMap(ontologyMapFiles.getL(),
        ontologyMapFiles.getR());
    int docRelationCount = 0;
    int docRelationSize = hltCC.getDocumentRelations().size();
    for (DocumentRelation docRelation : hltCC.getDocumentRelations()) {
      log.info("Extracting DocumentRelation {} of {} rel-type=", (++docRelationCount),
              docRelationSize, docRelation.getRelationType().getType());
      if (docRelation.getArguments() == null || docRelation.getArguments().size() == 0) {
        numRelationsWithoutArgs++;
        continue;
      }
      int docArgumentCount = 0;
      int docArgumentSize = docRelation.getArguments().size();
      ImmutableMap.Builder<ArgumentKey, DocumentRelationArgument> argKeyToArgMap
          = ImmutableMap.builder();
      Set<ArgumentKey> argumentKeys = new HashSet<>();
      boolean allArgsValid = true;
      for (DocumentRelationArgument docArgument : docRelation.getArguments()) {
        log.info("Extracting DocRelationArgument {} of {} arg-type={}", (++docArgumentCount),
                docArgumentSize, docArgument.getRole().getType());
        Optional<OntType> argRole = Optional.absent();
        try {
          argRole = RelationKey.getKBRoleForType(
              docRelation.getRelationType(), docArgument.getRole(), getRelevantKBOntologyMap(docRelation.getRelationType(),kbOntologyMap));
        } catch (Exception e) {
          String errorMsg = "Rel-Ex: No KB-kbRole found for arg-kbRole: "
              + "" + docArgument.getRole().getType();
          log.error(errorMsg, e);
          if (throwExceptions) {
            throw e;
          }
        }
        if (!argRole.isPresent()) {
          allArgsValid = false;
          numRelArgsNotPresentInOntology++;
          break;
        }
        Optional<Item> item = docArgument.getFiller().asItem();
        Set<RelationMention.Filler> argProvenances = docArgument.getProvenances();
        ItemKey itemKey = null;
        Optional<OntType> additionalTypeForEntityArg = Optional.absent();
        if (item.isPresent()) {
          if (item.get() instanceof Entity) {
            Entity entity = (Entity) item.get();
            //comment out if not supporting CBRNE entities or if CBRNE-type is included among other entity types and is just an OntType/IType object
//            itemKey = EntityKey.getKey(entity, hltCC,entity.getEntityType() instanceof CBRNEType?CBRNEOntology.getInstance().getCBRNEOntologyMap()
//                    :entityOntologyMap).orNull();
            itemKey = EntityKey.getKey(entity, hltCC,entityOntologyMap).orNull();
            if (itemKey ==null || !mergedEntitySets.containsKey((EntityKey) itemKey)) {
              String info = String.format("Relation: Entity-arg: %s;%s;%s;%s;%s;%s not found in merged-entities."
                  , entity.getValue(), entity.getCanonicalMention().getValue()
                  , entity.getCanonicalMention().getDocId(), entity.getEntityType().getType()
                  , entity.getCanonicalMention().getAlgorithmName(), entity.getIdString());
              log.info(info);
              if (throwExceptions) {
                throw new Exception(info);
              }
              allArgsValid = false;
              if(itemKey==null){
                numRelEntityArgsWithoutKey++;
              }
              if(!mergedEntitySets.containsKey((EntityKey)itemKey)){
                numRelEntityArgsNotMappedToCoref++;
              }
              break;
            }
            additionalTypeForEntityArg = RelationKey.getAdditionalTypeForEntityArg(docRelation
                .getRelationType(),docArgument.getRole(),getRelevantKBOntologyMap(docRelation.getRelationType(),kbOntologyMap));
//            log.info("Relation: DocArg: EntityKey found in map: {}"
//                , ((EntityKey) itemKey).kbID().getObjectID());
          } else if (item.get() instanceof NumericValue) {
            itemKey = NumericValueKey.getKey((NumericValue) item.get());
            for (RelationMention.Filler argProvenance : argProvenances) {
              if (argProvenance.asNumberPhrase().isPresent()) {
                numericValueArgs.put((NumericValueKey) itemKey,argProvenance.asNumberPhrase().get());
              }
            }
          } else if (item.get() instanceof TemporalValue) {
            itemKey = TemporalValueKey.getKey((TemporalValue)item.get());
            for (RelationMention.Filler argProvenance : argProvenances) {
              if (argProvenance.asTimePhrase().isPresent()) {
                temporalValueArgs.put((TemporalValueKey) itemKey,argProvenance.asTimePhrase().get());
              }
            }
          } else if (item.get() instanceof GenericThing) {
            itemKey = GenericThingKey.getKey((GenericThing)item.get()).orNull();
            if (itemKey == null) {
              String info = String.format("GenericThing arg's itemKey is null for relation: %s"
                  , docRelation.getIdString());
              log.info(info);
              if (throwExceptions) {
                throw new Exception(info);
              }
              allArgsValid = false;
              numRelGenericThingArgsWithoutKey++;
              break;
            }
            for (RelationMention.Filler argProvenance : argProvenances) {
              if (argProvenance.asGenericChunk().isPresent()) {
                genericThingArgs.put((GenericThingKey) itemKey,argProvenance.asGenericChunk().get());
              }
            }
          }
        } else {
          allArgsValid = false;
          numRelArgFillerNotPresent++;
          break;
        }
        ArgumentKey argumentKey = ArgumentKey.getKey(argRole.get(), itemKey,
            additionalTypeForEntityArg);
//        log.info("Relation Extraction: Arg-Key created: {} {}"
//            , argRole.get().getType(), (null == itemKey ? "" : itemKey.toString()));
        //Stanford relations can sometimes have duplicate arguments, therefore we need to keep track
        // of the argumentKeys in order to avoid duplicate keys in the builder
        if(!argumentKeys.contains(argumentKey)){
          argKeyToArgMap.put(argumentKey, docArgument);
        }
        argumentKeys.add(argumentKey);
      }
      if (!allArgsValid) {
        log.info("Relation Extraction: Not all args valid.");
        continue;
      }
      Optional<RelationKey> relationKeyOptional = RelationKey.getKey(docRelation.getRelationType(),
          ImmutableSet.copyOf(argumentKeys),getRelevantKBOntologyMap(docRelation.getRelationType(),kbOntologyMap));
      if (!relationKeyOptional.isPresent()) {
        String errorMsg = String.format("No relation-key could be created for relation: %s"
            , docRelation.getIdString());
        log.error(errorMsg);
        if (throwExceptions) {
          throw new Exception(errorMsg);
        }
        numRelationsWithoutKey++;
        continue;
      }
        MergedDocumentRelation mergedDocumentRelation =
            mergedRelations.get(relationKeyOptional.get());
        if (mergedDocumentRelation == null) {
          mergedDocumentRelation =
              MergedDocumentRelation.getMergedDocumentRelation(docRelation, argKeyToArgMap.build(),
                  relationKeyOptional.get(),getRelevantKBOntologyMap(docRelation.getRelationType(),kbOntologyMap));
        } else {
          mergedDocumentRelation.update(docRelation, argKeyToArgMap.build(),getRelevantKBOntologyMap(docRelation.getRelationType(),kbOntologyMap));
        }
        mergedRelations.put(relationKeyOptional.get(), mergedDocumentRelation);
        log.info("Created a MergedDocumentRelation.");
    }

    countBasedProperties.put("numRelationsWithoutArgs",numRelationsWithoutArgs);
    countBasedProperties.put("numRelArgsNotPresentInOntology",numRelArgsNotPresentInOntology);
    countBasedProperties.put("numRelEntityArgsWithoutKey",numRelEntityArgsWithoutKey);
    countBasedProperties.put("numRelEntityArgsNotMappedToCoref",numRelEntityArgsNotMappedToCoref);
    countBasedProperties.put("numRelGenericThingArgsWithoutKey",numRelGenericThingArgsWithoutKey);
    countBasedProperties.put("numRelationsWithoutKey",numRelationsWithoutKey);
    countBasedProperties.put("numRelArgFillerNotPresent",numRelArgFillerNotPresent);
    StatsGenerator.getCountBasedProperties(countBasedProperties,countBasedPropertiesIn);
  }

  private KBOntologyMap getRelevantKBOntologyMap(IType relationType, KBOntologyMap defaultKBOntologyMap) throws IOException{
    //comment out if not supporting CBRNE entities or if CBRNE-type is included among other entity types and is just an OntType/IType object
//    if(relationType instanceof CBRNEType){
//      return CBRNEOntology.getInstance().getCBRNEOntologyMap();
//    }
    return defaultKBOntologyMap;
  }

  public void extractDocumentEventsAndArguments(HltContentContainer hltCC,
      Map<EntityKey, Set<ItemWithProvenances<Entity,EntityMention>>> mergedEntitySets,
      ImmutableMultimap.Builder<TemporalValueKey,TimePhrase> temporalValueArgs,
      ImmutableMultimap.Builder<GenericThingKey,Chunk> genericThingArgs,
      Map<EventKey, MergedDocumentEvent> mergedEvents, Map<String,Integer> countBasedPropertiesIn) throws Exception {

    if (hltCC.getDocumentEvents() == null || hltCC.getDocumentEvents().size() == 0) {
      return;
    }

    Map<String,Integer> countBasedProperties = new HashMap<>();
    int numEventsWithoutArgs = 0;
    int numEventArgsNotPresentInOntology = 0;
    int numEventEntityArgsWithoutKey = 0;
    int numEventEntityArgsNotMappedToCoref = 0;
    int numEventGenericThingArgsWithoutKey = 0;
    int numEventsWithoutKey = 0;
    int numRealisExceptions=0;
    int numEventArgFillerNotPresent = 0;

    Pair<String,String> ontologyMapFiles = algorithmTypeToOntologyFilesMap.get
        (ALGORITHM_TYPE_EVENT_EXTRACTION);
    checkNotNull(ontologyMapFiles,"AlgorithmTypeToOntologyMap must have a mapping for "
        + ALGORITHM_TYPE_EVENT_EXTRACTION);
    KBOntologyMap eventOntologyMap = KBOntologyMap.loadOntologyMap(ontologyMapFiles.getL(),
        ontologyMapFiles.getR());
    ontologyMapFiles = algorithmTypeToOntologyFilesMap.get
        (ALGORITHM_TYPE_COREF);
    checkNotNull(ontologyMapFiles,"AlgorithmTypeToOntologyMap must have a mapping for "
        + ALGORITHM_TYPE_COREF);
    KBOntologyMap entityOntologyMap = KBOntologyMap.loadOntologyMap(ontologyMapFiles.getL(),
        ontologyMapFiles.getR());
    ontologyMapFiles = algorithmTypeToOntologyFilesMap.get
        (ALGORITHM_TYPE_RELATION_EXTRACTION);
    checkArgument(!useMissingEventTypesHack ||
        useMissingEventTypesHack&&ontologyMapFiles!=null,"When using missing-event-types "
        + "hack, AlgorithmTypeToOntologyMap must have a mapping for "+ALGORITHM_TYPE_RELATION_EXTRACTION);
    KBOntologyMap relationOntologyMap = ontologyMapFiles==null?null:KBOntologyMap.loadOntologyMap
        (ontologyMapFiles.getL(), ontologyMapFiles.getR());
    int docEventCount = 0;
    int docEventSize = hltCC.getDocumentEvents().size();
    for (DocumentEvent docEvent : hltCC.getDocumentEvents()) {
      KBOntologyMap kbOntologyMap = eventOntologyMap;
      log.info("Extracting DocumentEvent {} of {} event-type={}", (++docEventCount),
          docEventSize , docEvent.getEventType().getType());
      if (docEvent.getArguments() == null || docEvent.getArguments().size() == 0) {
        numEventsWithoutArgs++;
        continue;
      }
      if(useMissingEventTypesHack&& E2eUtil.missingEventTypes.contains(docEvent
          .getEventType().getType())){
        kbOntologyMap = relationOntologyMap;
      }
      int docArgumentCount = 0;
      int docArgumentSize = docEvent.getArguments().size();
      ImmutableMap.Builder<ArgumentKey, DocumentEventArgument>
          argKeyToArgMap = ImmutableMap.builder();
      Set<ArgumentKey> argumentKeys = new HashSet<>();
      boolean allArgsValid = true;
      for (DocumentEventArgument docArgument : docEvent.getArguments()) {
        log.info("Extracting DocEventArgument {} of {} arg-kbRole={}", (++docArgumentCount),
            docArgumentSize, docArgument.getRole().getType());
        Optional<OntType> argRole = Optional.absent();
        try {
          argRole = EventKey.getKBRoleForType(docEvent.getEventType(), docArgument.getRole(),
              kbOntologyMap);
        } catch (Exception e) {
          log.error("Caught exception when trying to get KB kbRole for arg", e);
          if (throwExceptions) {
            throw e;
          }
        }
        if (!argRole.isPresent()) {
          allArgsValid = false;
          break;
        }
        Optional<Item> item = docArgument.getFiller().asItem();
        ItemKey itemKey = null;
        Set<DocumentEventArgument.Provenance> argProvenances = docArgument.getProvenances();
        Optional<OntType> additionalTypeForEntityArg = Optional.absent();
        if (item.isPresent()) {
          if (item.get() instanceof Entity) {
            Entity entity = (Entity) item.get();
            itemKey = EntityKey.getKey(entity, hltCC,entityOntologyMap).orNull();
            if (itemKey == null || !mergedEntitySets.containsKey((EntityKey) itemKey)) {
              String info = String.format("Event: Entity-arg %s;%s;%s;%s;%s;%s not found in merged-entities."
                  , entity.getValue(), entity.getCanonicalMention().getValue()
                  , entity.getCanonicalMention().getDocId(),  entity.getEntityType().getType()
                  , entity.getCanonicalMention().getAlgorithmName(), entity.getIdString());
              log.info(info);
              if (throwExceptions) {
                throw new Exception(info);
              }
              allArgsValid = false;
              if(itemKey==null) {
                numEventEntityArgsWithoutKey++;
              }
              if(!mergedEntitySets.containsKey((EntityKey)itemKey)){
                numEventEntityArgsNotMappedToCoref++;
              }
              break;
            }
            additionalTypeForEntityArg = EventKey.getAdditionalTypeForEntityArg(docEvent
                .getEventType(),docArgument.getRole(),kbOntologyMap);
//            log.info("Event Extraction: DocArg: EntityKey found in map: {}", ((EntityKey) itemKey).kbID());
          } else if (item.get() instanceof TemporalValue) {
            TemporalValue temporalValue = (TemporalValue)item.get();
            itemKey = TemporalValueKey.getKey(temporalValue);
            for (DocumentEventArgument.Provenance argProvenance : argProvenances) {
              Chunk provenance = argProvenance.getEventMentionArgument().getFiller();
              OntType temporalValueType = kbOntologyMap.getKBTypeForType(new Type("Time")).get();
              temporalValueArgs.put((TemporalValueKey) itemKey,new TimePhrase(provenance.getTokenOffset(),
                  provenance.getTokenStream(),temporalValueType));
            }
          } else if (item.get() instanceof GenericThing) {
            itemKey = GenericThingKey.getKey((GenericThing)item.get()).orNull();
            if (itemKey == null) {
              String info = String.format("GenericThing arg's itemKey is null for event: %s"
                  , docEvent.getEventType().getType()+" role:"+docArgument.getRole().getType()+" type:"+((GenericThing)item.get()).getType().getType()+" docId: "+hltCC.getDocumentId()+" ");
              log.info(info);
              if (throwExceptions) {
                throw new Exception(info);
              }
              allArgsValid = false;
              numEventGenericThingArgsWithoutKey++;
              break;
            }
            for (DocumentEventArgument.Provenance argProvenance : argProvenances) {
               genericThingArgs.put((GenericThingKey) itemKey,argProvenance.getEventMentionArgument().getFiller());
            }
          }
        } else {
          allArgsValid = false;
          break;
        }
        ArgumentKey argumentKey = ArgumentKey.getKey(argRole.get(), itemKey, additionalTypeForEntityArg);
//        log.info("Event Extraction: Created Arg-key: {} {}", argRole.get().getType(), (null == itemKey ? "" : itemKey.toString()));
        //BBN_SERIF events can sometimes have duplicate arguments, therefore we need to keep track
        // of the argumentKeys in order to avoid duplicate keys in the builder
        if (!argumentKeys.contains(argumentKey)) {
          argKeyToArgMap.put(argumentKey, docArgument);
        }
        argumentKeys.add(argumentKey);
      }
      if (!allArgsValid) {
        continue;
      }
      Optional<OntType> realisType = Optional.absent();
      try{
        realisType = EventKey.getRealisKBType(docEvent,kbOntologyMap);
      } catch (Exception e) {
          log.error("Event Extraction: Could not find a KB-type for realis type.", e);
          if (throwExceptions) {
            throw e;
          }
          numRealisExceptions++;
      }

      Optional<EventKey> eventKeyOptional = EventKey.getKey(docEvent.getEventType(),
          realisType, ImmutableSet.copyOf(argumentKeys),kbOntologyMap);
      if (!eventKeyOptional.isPresent()) {
        String errorMsg = "No event-key could be created for event: " + docEvent.getIdString();
        log.error(errorMsg);
        if (throwExceptions) {
          throw new Exception(errorMsg);
        }
        numEventsWithoutKey++;
        continue;
      }
      MergedDocumentEvent mergedDocumentEvent =
          mergedEvents.get(eventKeyOptional.get());
      if (mergedDocumentEvent == null) {
        mergedDocumentEvent =
            MergedDocumentEvent.getMergedDocumentEvent(docEvent, argKeyToArgMap.build(),
                eventKeyOptional.get(),kbOntologyMap);
      } else {
        mergedDocumentEvent.update(docEvent,argKeyToArgMap.build(),kbOntologyMap);
      }
      mergedEvents.put(eventKeyOptional.get(), mergedDocumentEvent);
      log.info("Event Extraction: Created a MergedDocumentEvent.");
    }
    countBasedProperties.put("numRealisExceptions",numRealisExceptions);
    countBasedProperties.put("numEventsWithoutArgs",numEventsWithoutArgs);
    countBasedProperties.put("numEventArgsNotPresentInOntology",numEventArgsNotPresentInOntology);
    countBasedProperties.put("numEventEntityArgsWithoutKey",numEventEntityArgsWithoutKey);
    countBasedProperties.put("numEventEntityArgsNotMappedToCoref",numEventEntityArgsNotMappedToCoref);
    countBasedProperties.put("numEventGenericThingArgsWithoutKey",numEventGenericThingArgsWithoutKey);
    countBasedProperties.put("numEventsWithoutKey",numEventsWithoutKey);
    countBasedProperties.put("numEventArgFillerNotPresent",numEventArgFillerNotPresent);
    StatsGenerator.getCountBasedProperties(countBasedProperties,countBasedPropertiesIn);
  }

}


