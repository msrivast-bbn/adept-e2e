package adept.e2e.mastercontainer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import adept.common.Coreference;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.KBID;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class MasterEDLEntities {

  private static Logger log = LoggerFactory.getLogger(MasterEDLEntities.class);

  private final ImmutableList<Coreference> coreferences;
  private final ImmutableList<Entity> edlEntitiesExtracted;
  private final ImmutableSet<EntityMention> allEdlEntityMentions;
  private final ImmutableTable<Entity,KBID,Float> entityKBIDDist;
  private final ImmutableMap<String,Integer> artifactCounts;
  public static final String NIL_EDL_NOT_ALIGNED_WITH_COREF = "EDL_NOT_ALIGNED_WITH_COREF";

  private MasterEDLEntities(ImmutableList<Coreference> coreferences, ImmutableList<Entity>
      edlEntitiesExtracted,
      ImmutableSet<EntityMention> allEdlEntityMentions, ImmutableTable<Entity, KBID,Float>
      entityKBIDDist, ImmutableMap<String,Integer>
      artifactCounts){
    checkNotNull(coreferences);
    checkNotNull(edlEntitiesExtracted);
    checkNotNull(allEdlEntityMentions);
    checkNotNull(entityKBIDDist);
    checkNotNull(artifactCounts);
    this.coreferences = coreferences;
    this.edlEntitiesExtracted = edlEntitiesExtracted;
    this.allEdlEntityMentions = allEdlEntityMentions;
    this.entityKBIDDist = entityKBIDDist;
    this.artifactCounts = artifactCounts;
  }

  public static MasterEDLEntities extractEDLEntities(
      HltContentContainer entityLinkingContainer, KBOntologyMap edlOntologyMap, final String
      ENTITY_LINKING_ALGORITHM, List<Long> usedEDLEntityIds, int nextCoreferenceId, long currentMaxEntityId) throws
                                                                                    Exception{

    checkNotNull(entityLinkingContainer);
    checkNotNull(edlOntologyMap);

    int edlEntitiesWithoutKBID = 0;
    int edlEntitiesWithNILKBID = 0;
    int numTotalEDLEntities = 0;
    int numEDLEntitiesAlignedToCorefEntities = 0;
    int numNonCorefAlignedEDLEntitiesAdded = 0;
    ImmutableList.Builder<Coreference> newCoreferences = ImmutableList.builder();
    ImmutableList.Builder<Entity> allEDLEntitiesAddedBuilder = ImmutableList.builder();
    ImmutableSet.Builder<EntityMention> allEdlEntityMentionsBuilder = ImmutableSet.builder();
    ImmutableTable.Builder<Entity, KBID,Float> entityKBIDDist = ImmutableTable.builder();

    int nilSeqNo = 0;
    String docId = entityLinkingContainer.getDocumentId();
    Set<Long> edlEntityIds = MasterContainerUtils.getEntityIdToEntityMap(entityLinkingContainer).keySet();
    long maxEdlEntityId = edlEntityIds.isEmpty()?0L:Collections.max(edlEntityIds);
    long nextEntityId = Math.max(maxEdlEntityId,(long)currentMaxEntityId)+1L;
    for (Coreference coreference : entityLinkingContainer.getCoreferences()) {
      log.info("Creating a new coreference from {} "
          + "coreference...",ENTITY_LINKING_ALGORITHM );
      Coreference newCoreference = new Coreference(nextCoreferenceId++);
      List<Entity> newEntities = new ArrayList<>();
      newCoreference.setAlgorithmName(entityLinkingContainer.getAlgorithmName());
      newCoreference.setSourceAlgorithm(entityLinkingContainer.getSourceAlgorithm());
      for (Entity edlEntity : coreference.getEntities()) {
        numTotalEDLEntities++;
        if (usedEDLEntityIds.contains(edlEntity.getEntityId())) {
          numEDLEntitiesAlignedToCorefEntities++;
          continue;
        }
        if (!edlOntologyMap.getKBTypeForType(edlEntity.getEntityType())
            .isPresent()) {
          throw new Exception("EntityType " + edlEntity.getEntityType().getType() + " "
              + "not found in ontology: docId=" + docId + " algorithm="
              + ENTITY_LINKING_ALGORITHM);
        }
        if (entityLinkingContainer.
            getKBEntityMapForDocEntities().get(edlEntity) == null ||
            entityLinkingContainer.
                getKBEntityMapForDocEntities().get(edlEntity).isEmpty()) {
          log.info("No KBID found for {} entity.", ENTITY_LINKING_ALGORITHM);
          edlEntitiesWithoutKBID++;
          continue;
        }
        log.info("Creating a new {} Entity: ", ENTITY_LINKING_ALGORITHM);
        edlEntity.setAlgorithmName(entityLinkingContainer.getAlgorithmName());
        edlEntity.setSourceAlgorithm(entityLinkingContainer.getSourceAlgorithm());
        edlEntity.getCanonicalMention().setAlgorithmName(ENTITY_LINKING_ALGORITHM);
        log.info("\t entity-id={}; algorithm-name={}; value={}; entity-type={}; canonical-mention={}"
            , edlEntity.getIdString(), edlEntity.getAlgorithmName()
            , edlEntity.getValue(), edlEntity.getEntityType().getType()
            , edlEntity.getCanonicalMention().getValue());
        log.info("Setting new KBIDDist for new Entity");
        StringBuilder kbIDDistString = new StringBuilder("\t kbIDDist: ");
        Map<KBID, Float> kbIDDistribution = entityLinkingContainer.
            getKBEntityMapForDocEntities().get(edlEntity);
        KBID bestKBID = null;
        float bestConfidence = -1.0f;
        for (Map.Entry<KBID, Float> kbIDDistributionEntry : kbIDDistribution.entrySet()) {
          KBID kbID = kbIDDistributionEntry.getKey();
          kbIDDistString.append(String.format("[%s--%s:%f]  ", kbID.getKBNamespace(), kbID.getObjectID()
              , kbIDDistributionEntry.getValue()));
          if (kbIDDistributionEntry.getValue() > bestConfidence) {
            bestKBID = kbID;
            bestConfidence = kbIDDistributionEntry.getValue();
          }
        }
        log.info(kbIDDistString.toString());
        //if the bestKDID is of type NILX, ignore this entity
        if (bestKBID != null && bestKBID.getObjectID().matches(MasterContainerUtils.NIL_ID_PATTERN)) {
          log.info("Skipping this {} entity ({}) since bestKBID is NIL.", ENTITY_LINKING_ALGORITHM, edlEntity.getIdString());
          edlEntitiesWithNILKBID++;
//          continue;
        }
        Map<KBID,Float> kbIDDistMapWithNILRemoved = MasterContainerUtils.removeNILKBIDs(kbIDDistribution);
        if(kbIDDistMapWithNILRemoved.isEmpty()){
          kbIDDistMapWithNILRemoved.put(new KBID("NIL"+(nilSeqNo++)+"_"+docId,NIL_EDL_NOT_ALIGNED_WITH_COREF),1.0f);
        }
        //before adding edl entity to any MasterEDL data-structures, update its entityID
        //and cascade the updated entity ID to all linked mentions
        edlEntity = updateEntityIdInMentions(edlEntity,coreference,entityLinkingContainer,nextEntityId++);
        for(Map.Entry<KBID,Float> entry : kbIDDistMapWithNILRemoved.entrySet()) {
          KBID kbID = entry.getKey();
//          if(kbID.getObjectID().matches("NIL[0-9]+")){
//            kbID = new KBID("NIL"+(nilSeqNo++)+"_"+docId,NIL_EDL_NOT_ALIGNED_WITH_COREF);
//          }
          float confidence = entry.getValue();
          entityKBIDDist.put(edlEntity, kbID, confidence);
        }
        newEntities.add(edlEntity);
        allEdlEntityMentionsBuilder.add(edlEntity.getCanonicalMention());
        numNonCorefAlignedEDLEntitiesAdded++;
      }
      if (newEntities.isEmpty()) {//if there are no entities to add to this coref, skip this coref
        nextCoreferenceId--;
        continue;
      }
      newCoreference.setEntities(newEntities);
      List<EntityMention> remainingResolvedMentions =
          MasterContainerUtils.getRemainingEntityMentions(coreference
          .getResolvedMentions(),newEntities);
      newCoreference.setResolvedMentions(remainingResolvedMentions);
      newCoreferences.add(newCoreference);
      for (EntityMention mention : newCoreference.getResolvedMentions()) {
        mention.setAlgorithmName(ENTITY_LINKING_ALGORITHM);
      }
      allEDLEntitiesAddedBuilder.addAll(newEntities);
      allEdlEntityMentionsBuilder.addAll(newCoreference.getResolvedMentions());
    }

    ImmutableList<Entity> allEDLEntitiesAdded = allEDLEntitiesAddedBuilder.build();
    List<EntityMention> allRemainingMentions = MasterContainerUtils.getRemainingEntityMentions
        (entityLinkingContainer.getEntityMentions(),allEDLEntitiesAdded);
    //making sure that all entityMentions have algorithm-name set
    for (EntityMention mention : allRemainingMentions) {
      mention.setAlgorithmName(ENTITY_LINKING_ALGORITHM);
    }
    allEdlEntityMentionsBuilder.addAll(allRemainingMentions);

    ImmutableMap.Builder<String,Integer> artifactCounts = ImmutableMap.builder();

    artifactCounts.put("numEDLEntitiesWithoutKBID",edlEntitiesWithoutKBID);
    artifactCounts.put("numNonCorefAlignedEDLEntitiesWithNILKBID",edlEntitiesWithNILKBID);
    artifactCounts.put("numTotalEDLEntities",numTotalEDLEntities);
    artifactCounts.put("numEDLEntitiesAlignedToCorefEntities",numEDLEntitiesAlignedToCorefEntities);
    artifactCounts.put("numNonCorefAlignedEDLEntitiesAdded",numNonCorefAlignedEDLEntitiesAdded);

    ImmutableSet<EntityMention> allEdlEntityMentions = allEdlEntityMentionsBuilder.build();
    MasterContainerUtils.updateEntityIdDistInMentions(allEDLEntitiesAdded,allEdlEntityMentions.asList());

    return new MasterEDLEntities(newCoreferences.build(),allEDLEntitiesAdded,
        allEdlEntityMentions,
        entityKBIDDist.build(), artifactCounts.build());

  }

  private static Entity updateEntityIdInMentions(Entity entity, Coreference coreference, HltContentContainer hltCC, long newEntityId){
    //create new entity
    Entity newEntity = new Entity(newEntityId,entity.getEntityType());
    newEntity.setEntityConfidence(entity.getEntityConfidence());
    newEntity.setCanonicalMentionConfidence(entity.getCanonicalMentionConfidence());
    newEntity.setSourceAlgorithm(entity.getSourceAlgorithm());

    Set<EntityMention> mentionsToUpdate = new HashSet<>(hltCC.getEntityMentions());
    mentionsToUpdate.addAll(coreference.getResolvedMentions());
    mentionsToUpdate.add(entity.getCanonicalMention());

    long currentEntityId = entity.getEntityId();
    //update the new entity id in all mentions
    for(EntityMention em : mentionsToUpdate){
      if(em.getEntityIdDistribution().keySet().contains(currentEntityId)){
        Map<Long,Float> dist = new HashMap(em.getEntityIdDistribution());
        Map<Long,Float> newDist = new HashMap<>();
        for(Map.Entry<Long,Float> currentEntry : dist.entrySet()){
          if(currentEntry.getKey()==currentEntityId){
            newDist.put(newEntityId,currentEntry.getValue());
          }else{
            newDist.put(currentEntry.getKey(), currentEntry.getValue());
          }
        }
        em.setEntityIdDistribution(newDist);
      }
    }
    newEntity.setCanonicalMentions(entity.getCanonicalMention());
    return newEntity;
  }

  public List<Coreference> getCoreferences(){
    return coreferences;
  }

  public ImmutableTable<Entity,KBID,Float> getEntityKBIDDist(){
    return entityKBIDDist;
  }

  public ImmutableSet<EntityMention> getAllEDLEntityMentions(){
    return allEdlEntityMentions;
  }

  public Map<String,Integer> getArtifactCounts(){
    return artifactCounts;
  }

}


