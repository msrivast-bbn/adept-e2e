package adept.e2e.mastercontainer;

import com.google.common.base.Optional;
import com.google.common.collect.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import adept.common.Coreference;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.KBID;
import adept.common.Pair;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MasterCorefEntities {

  private static Logger log = LoggerFactory.getLogger(MasterCorefEntities.class);
  //This table will be used to store mapping between non-Coref entityids and Coref
  //Entities based on Float-type confidence which is the best confidence between the non-Coref
  //entity and a mention in the provenance-list being used for a Coref Entity.
  private final ImmutableTable<String, Long, Pair<Entity,Float>>
      nonCorefEntityIdToBestCorefEntityByAlgorithm;
  private final ImmutableList<Coreference> coreferences;
  private final ImmutableList<Entity> corefEntitiesExtracted;
  private final ImmutableSet<EntityMention> allCorefEntityMentions;
  private final ImmutableMap<Long,Entity> corefEntitiesById;
  private final ImmutableTable<Entity,KBID,Float> entityKBIDDist;

  private final ImmutableMap<String,Integer> artifactCounts;
  private final int nextCoreferenceId;
  private final long maxEntityId;
  public final static String NIL_COREF_NOT_ALIGNED_WITH_EDL = "COREF_NOT_ALIGNED_WITH_EDL";

  private MasterCorefEntities(ImmutableTable<String, Long, Pair<Entity,Float>>
      nonCorefEntityIdToBestCorefEntityByAlgorithm, ImmutableList<Coreference> coreferences,
      ImmutableList<Entity>
      corefEntitiesExtracted, ImmutableSet<EntityMention> allCorefEntityMentions,
      ImmutableMap<Long,Entity> corefEntitiesById, ImmutableTable<Entity,
      KBID,Float> entityKBIDDist, ImmutableMap<String,Integer> artifactCounts, int
      nextCoreferenceId, long maxEntityId){
    checkNotNull(nonCorefEntityIdToBestCorefEntityByAlgorithm);
    checkNotNull(coreferences);
    checkNotNull(corefEntitiesExtracted);
    checkNotNull(allCorefEntityMentions);
    checkNotNull(entityKBIDDist);
    checkNotNull(artifactCounts);
    this.nonCorefEntityIdToBestCorefEntityByAlgorithm = nonCorefEntityIdToBestCorefEntityByAlgorithm;
    this.coreferences = coreferences;
    this.corefEntitiesExtracted = corefEntitiesExtracted;
    this.allCorefEntityMentions = allCorefEntityMentions;
    this.corefEntitiesById = corefEntitiesById;
    this.entityKBIDDist = entityKBIDDist;
    this.artifactCounts = artifactCounts;
    this.nextCoreferenceId = nextCoreferenceId;
    this.maxEntityId = maxEntityId;
  }

  public static MasterCorefEntities extractCorefEntities(
      HltContentContainer corefContainer, KBOntologyMap corefOntologyMap, final String
      COREF_ALGORITHM, AlgorithmChunkMap algorithmChunkMap) throws Exception{

    checkNotNull(corefContainer);
    checkNotNull(corefOntologyMap);
    checkNotNull(algorithmChunkMap);
    checkNotNull(COREF_ALGORITHM);
    checkArgument(!COREF_ALGORITHM.isEmpty(),"Coref Algorithm Name cannot be empty");

    Table<String, Long, Pair<Entity,Float>> nonCorefEntityIdToBestCorefEntityByAlgorithm
        = HashBasedTable.create();
    ImmutableList.Builder<Coreference> newCoreferences = ImmutableList.builder();
    ImmutableList.Builder<Entity> corefEntitiesExtractedBuilder = ImmutableList.builder();
    ImmutableSet.Builder<EntityMention> allCorefEntityMentionsBuilder = ImmutableSet.builder();
    ImmutableMap.Builder<Long,Entity> addedEntitiesById = ImmutableMap.builder();
    ImmutableTable.Builder<Entity,KBID,Float> entityKBIDDist = ImmutableTable.builder();

    int nilSeqNo = 0;
    int numUnknownOtherCorefEntities = 0;
    int numCorefEntitiesWithoutAnyKBID = 0;
    int numCorefEntitiesMappedToNILKBIDs = 0;
    int numTotalCorefEntities = 0;
    int numTotalCorefEntitiesAdded = 0;

    int coreferenceId = 0;
    long maxEntityId = -1L;
    String docId = corefContainer.getDocumentId();
    for (Coreference coreference : corefContainer.getCoreferences()) {
      log.info("Creating a new coreference from {} coreference...", COREF_ALGORITHM);
      Coreference newCoreference = new Coreference(coreferenceId++);
      newCoreference.setAlgorithmName(corefContainer.getAlgorithmName());
      newCoreference.setSourceAlgorithm(corefContainer.getSourceAlgorithm());
      List<Entity> newEntities = new ArrayList<>();
      Set<EntityMention> newResolvedMentions = new HashSet<>();
      newCoreference.setAlgorithmName(corefContainer.getAlgorithmName());
      newCoreference.setSourceAlgorithm(corefContainer.getSourceAlgorithm());
//      if (coreference.getEntities().size() > 1 && coreference.getAlgorithmName() != null && ^M
//          coreference.getAlgorithmName().equals("IllinoisCoref")) {^M
//        throw new Exception(COREF_ALGORITHM + " Coreference has more than one entity.");
//      }
      for (Entity entity : coreference.getEntities()) {
        numTotalCorefEntities++;
        // skip any entities that don't have an allowable type
        if (!corefOntologyMap.getKBTypeForType(entity.getEntityType())
            .isPresent()) {
          if (!entity.getEntityType().getType().equalsIgnoreCase("UNKNOWN") &&
              !entity.getEntityType().getType().equalsIgnoreCase("OTHER")) {
            throw new Exception("EntityType " + entity.getEntityType().getType() + " "
                + "not found in ontology algorithm=" + COREF_ALGORITHM);
          } else {
            log.info("Found UNKNOWN/OTHER as Coref algorithm entity-type");
            numUnknownOtherCorefEntities++;
            continue;
          }
        }
        //The following map contains a mapping--for non-Coref algorithms--between non-Coref
        // entity and its entity-to-nonCorefMention confidence. At the end of iteration over all
        // non-Coref provenances. The confidence value will be the best/highest confidence.
        Map<String, Map<Long, Float>> nonCorefEntityIdToBestMentionConfidenceByAlgorithm
            = new HashMap<String, Map<Long, Float>>();
        List<EntityMention> provenances = algorithmChunkMap.getProvenancesByCorefEntityId
            (entity.getEntityId()).orNull();
        Map<KBID, Float> externalKBIdMap = algorithmChunkMap.getExternalKBIdsByCorefEntityId(entity
            .getEntityId()).orNull();
        // Ignore any entities that don't have provenances as they should already be accounted for by aligned pivot chunks
        if (provenances != null && !provenances.isEmpty()) {
          log.info("Getting non-{} entityIds for provenances...# of provenances={}",
              COREF_ALGORITHM, provenances.size());
          for (EntityMention entityMention : provenances) {
            //Before modifying the entityMention, if this is a non-Illinois entityMention,
            //get the list of non-Illinois entityIds mapped to it so that they can then be
            //replaced by Illinois entity when processing relations and events arguments that
            // are of type entity.
            for (Map.Entry<String, Map<String, ImmutableMap<Long, Float>>> entry : algorithmChunkMap
                .getNonCorefMentionIdToEntityIdDistByAlgorithm().rowMap().entrySet()) {
              log.info("For algorithm: {}", entry.getKey());
              Map<String, ImmutableMap<Long, Float>> mentionIdToEntityIdDistMap = entry.getValue();
              Map<Long, Float> entityIdToBestMentionConfidence =
                  nonCorefEntityIdToBestMentionConfidenceByAlgorithm.get(entry.getKey());
              if (mentionIdToEntityIdDistMap.containsKey(entityMention.getIdString())) {
                log.info("Found {} in mentionIdToEntityDistMap", entityMention.getIdString());
                Map<Long, Float> entityIdDist = mentionIdToEntityIdDistMap.get(
                    entityMention.getIdString());
                for (Entry<Long, Float> entityIdEntry : entityIdDist.entrySet()) {
                  float confidence = entityIdEntry.getValue();
                  log.info("entityId={}: confidence={}", entityIdEntry.getKey(), confidence);
                  if (entityIdToBestMentionConfidence == null) {
                    entityIdToBestMentionConfidence = new HashMap<Long, Float>();
                  }
                  Float bestConfidence =
                      entityIdToBestMentionConfidence.get(entityIdEntry.getKey());
                  if (bestConfidence == null || confidence > bestConfidence ) {
                    entityIdToBestMentionConfidence.put(entityIdEntry.getKey(), confidence);
                  }
                }
              }
              nonCorefEntityIdToBestMentionConfidenceByAlgorithm.put(
                  entry.getKey(), entityIdToBestMentionConfidence);
            }
            //Set Illinois entity's type to all aligned mentions
            entityMention.setEntityType(entity.getEntityType());
            //Set Illinois entity to entityIdDistribution of all aligned mentions
            if (!entityMention.getEntityIdDistribution().containsKey(entity.getEntityId())) {
              entityMention.getEntityIdDistribution().put(entity.getEntityId(),
                  entity.getCanonicalMention().getEntityIdDistribution()
                      .get(entity.getEntityId()));
            }
          }
        }
          log.info("Creating new {} Entity...", COREF_ALGORITHM);
          entity.setAlgorithmName(corefContainer.getAlgorithmName());
          entity.setSourceAlgorithm(corefContainer.getSourceAlgorithm());
          entity.getCanonicalMention().setAlgorithmName(COREF_ALGORITHM);
          log.info("\t entity-id={}; long-id={}; algorithm-name={}; entity-type={}; value={}; canonical-mention={}"
              , entity.getIdString(), entity.getEntityId(), entity.getAlgorithmName()
              , entity.getEntityType().getType(), entity.getValue(), entity.getCanonicalMention().getValue());
          log.info("Creating new {} Mentions...", COREF_ALGORITHM);
          StringBuilder provenanceString = new StringBuilder("\t provenance-list: ");
          for (EntityMention provenance : provenances) {
            provenanceString.append(String.format("[provenance: algorithm-name=%s; value=%s; entity-type=%s; mention-type=%s; mention-to-entity-confidence=%s] "
                , provenance.getAlgorithmName(), provenance.getValue(), provenance.getEntityType().getType()
                , (provenance.getMentionType() == null ? "NULL" : provenance.getMentionType().getType())
                , provenance.getEntityIdDistribution().get(entity.getEntityId())));
          }
          log.info(provenanceString.toString());

          Map<KBID, Float> kbIDDistribution = new HashMap(externalKBIdMap);

          log.info("Setting new KBIDDist for new Entity");
          StringBuilder kbIDDistString = new StringBuilder("\t kbIDDist: ");
          KBID bestKBID = null;
          float bestConfidence = -1.0f;
          for (Entry<KBID, Float> kbIDEntry : kbIDDistribution.entrySet()) {
            KBID kbID = kbIDEntry.getKey();
            kbIDDistString.append(String.format("[%s--%s:%s]  ", kbID.getKBNamespace()
                , kbID.getObjectID(), kbIDEntry.getValue()));
            if (kbIDDistribution.get(kbID) > bestConfidence) {
              bestKBID = kbID;
              bestConfidence = kbIDEntry.getValue();
            }
          }
          if (bestKBID != null && bestKBID.getObjectID().matches(MasterContainerUtils.NIL_ID_PATTERN)) {
//            log.info("Skipping this {} entity ({}) since bestKBID is NIL.", COREF_ALGORITHM, entity.getIdString());
              numCorefEntitiesMappedToNILKBIDs++;
//            continue;
          }

          if (kbIDDistribution.isEmpty()) {
//            log.info("Assigning a NIL KBID to this entity ({}) since kbIDDist is empty", entity.getIdString());
//            if(algorithmChunkMap.getAlgorithmNameByType(ALGORITHM_TYPE_NIL_CLUSTERING).isPresent()){
//              throw new Exception("External KBIDDist found to be empty despite using "+
//                  ALGORITHM_TYPE_NIL_CLUSTERING);
//            }
          //If the entity's canonicalName is a pronoun, drop it. We have seen entities created by IllinoisCoref that only have pronoun mentions
          //and don't resolve to any external KBID.
          if (isPronounEntity(entity)) {
            continue;
          }
            numCorefEntitiesWithoutAnyKBID++;
            String nilKBID = "NIL"+(nilSeqNo++)+"_"+docId;
            kbIDDistribution.put(new KBID(nilKBID,NIL_COREF_NOT_ALIGNED_WITH_EDL),1.0f);
          }
          log.info(kbIDDistString.toString());
          for(Map.Entry<KBID,Float> entry : kbIDDistribution.entrySet()) {
            entityKBIDDist.put(entity, entry.getKey(), entry.getValue());
          }

          //Now we will map this Coref Entity to nonCoref EntityIds that are tied to non-Coref
          // mentions in the provenances list
          for (Entry<String, Map<Long, Float>> entry1 : nonCorefEntityIdToBestMentionConfidenceByAlgorithm.entrySet()) {
            String algorithm = entry1.getKey();
            log.info("Again algorithm={}" , algorithm);
            Map<Long, Float> entityIdToBestMentionConfidence =
                entry1.getValue();
            if (entityIdToBestMentionConfidence == null) {
              log.info("No entityIdToBestMentionConfidence found");
              continue;
            }
            Map<Long,IType> entityIdToTypeMap = algorithmChunkMap.getNonCorefEntityIdToTypeByAlgorithm().row(algorithm);
            for (Entry<Long, Float> entry2 : entityIdToBestMentionConfidence.entrySet()) {
              Long entityId = entry2.getKey();
              Float confidence = entry2.getValue();
              Pair<Entity,Float> bestEntity = nonCorefEntityIdToBestCorefEntityByAlgorithm.get
                  (algorithm,entityId);
              if (bestEntity == null || bestEntity.getR() < confidence) {
                bestEntity = new Pair<Entity, Float>(entity, confidence);
              }else if(bestEntity.getR()==confidence){//if confidences are same
                  String bestEntityEntityType = bestEntity.getL().getEntityType().getType();
                  String corefEntityType = entity.getEntityType().getType();
                  String nonCorefEntityType = entityIdToTypeMap.get(entityId).getType();
                  if(!nonCorefEntityType.equals(bestEntityEntityType)&&nonCorefEntityType.equals(corefEntityType)){
                    //if this confidence is the same as bestConfidence, update
                    //nonCoref entityId only if the type of nonCoref entity matches the type of new coref entity and not that of current best
                    // coref entity
                    bestEntity = new Pair<Entity,Float>(entity,confidence);
                  }else if(nonCorefEntityType.equals(bestEntityEntityType)&&nonCorefEntityType.equals(corefEntityType)){
                    Map<KBID,Float> corefEntityKBIDMap = MasterContainerUtils.removeNILKBIDs(corefContainer.getKBEntityMapForEntity(entity));
                    Map<KBID,Float> bestEntityKBIDMap = MasterContainerUtils.removeNILKBIDs(corefContainer.getKBEntityMapForEntity(bestEntity.getL()));
                    if(bestEntityKBIDMap.isEmpty()&&!corefEntityKBIDMap.isEmpty()){
                      //if both the confidence and type are same; choose corefEntity with non-empty/non-nil KBID map
                      bestEntity = new Pair<Entity,Float>(entity,confidence);
                    }
                  }
                //entities
              }
              log.info("entityIdToBestEntity: {}: {},{}", entityId, bestEntity.getL()
                  , bestEntity.getR());
              nonCorefEntityIdToBestCorefEntityByAlgorithm.put(algorithm,entityId, bestEntity);
            }
          }
          newResolvedMentions.addAll(provenances);
          newResolvedMentions.add(entity.getCanonicalMention());
        newEntities.add(entity);
        if(entity.getEntityId()>maxEntityId){
          maxEntityId = entity.getEntityId();
        }
        numTotalCorefEntitiesAdded++;
        addedEntitiesById.put(entity.getEntityId(),entity);
      }
      List<EntityMention> resolvedMentions = new ArrayList<>();
      resolvedMentions.addAll(newResolvedMentions);
      newCoreference.setResolvedMentions(resolvedMentions);
      newCoreference.setEntities(newEntities);
      if (newEntities.isEmpty()) {//if there are no entities to add to this coref, skip this coref
        coreferenceId--;
        continue;
      }
      allCorefEntityMentionsBuilder.addAll(newCoreference.getResolvedMentions());
      corefEntitiesExtractedBuilder.addAll(newEntities);
      newCoreferences.add(newCoreference);
    }

    ImmutableList<Entity> corefEntitiesExtracted = corefEntitiesExtractedBuilder.build();
    List<EntityMention> allRemainingCorefMentions = MasterContainerUtils
        .getRemainingEntityMentions(corefContainer
            .getEntityMentions(),corefEntitiesExtracted);
    //making sure that all entityMentions have algorithm-name set
    for (EntityMention mention : allRemainingCorefMentions) {
      mention.setAlgorithmName(COREF_ALGORITHM);
    }
    allCorefEntityMentionsBuilder.addAll(allRemainingCorefMentions);

    ImmutableMap.Builder<String,Integer> artifactCounts = ImmutableMap.builder();
    artifactCounts.put("numCorefEntitiesMappedToNILKBIDs",numCorefEntitiesMappedToNILKBIDs);
//    if(!algorithmChunkMap.getAlgorithmNameByType(ALGORITHM_TYPE_NIL_CLUSTERING).isPresent()){
      artifactCounts.put("numCorefEntitiesWithoutAnyKBID", numCorefEntitiesWithoutAnyKBID);
//    }
    artifactCounts.put("numTotalCorefEntities",numTotalCorefEntities);
    artifactCounts.put("numTotalCorefEntitiesAdded",numTotalCorefEntitiesAdded);
    artifactCounts.put("numUnknownOtherCorefEntities",numUnknownOtherCorefEntities);

    ImmutableSet<EntityMention> allCorefEntityMentions = allCorefEntityMentionsBuilder.build();
    MasterContainerUtils.updateEntityIdDistInMentions(corefEntitiesExtracted,allCorefEntityMentions.asList());

    return new MasterCorefEntities(ImmutableTable.copyOf
        (nonCorefEntityIdToBestCorefEntityByAlgorithm),
        newCoreferences.build(), corefEntitiesExtracted, allCorefEntityMentions,
        addedEntitiesById.build(),entityKBIDDist.build(),artifactCounts.build(),coreferenceId, maxEntityId);

  }

  private static boolean isPronounEntity(Entity entity) throws IOException {
    String entityValue = entity.getValue().toLowerCase();
    String canonicalMention = entity.getCanonicalMention().getValue().toLowerCase();
    List<String> pronouns = MasterContainerUtils.getPronouns();
    if(pronouns.contains(entityValue)&&pronouns.contains(canonicalMention)){
      return true;
    }
    return false;
  }

  public ImmutableMap<Long,Pair<Entity,Float>> getNonCorefEntityIdToBestCorefEntityMapByAlgorithm
      (String algorithm){
    return ImmutableMap.copyOf(nonCorefEntityIdToBestCorefEntityByAlgorithm.row(algorithm));
  }

  public ImmutableList<Coreference> getCoreferences(){
    return ImmutableList.copyOf(coreferences);
  }

  public Optional<Entity> getExtractedEntityById(long entityId){
    return Optional.fromNullable(corefEntitiesById.get(entityId));
  }

  public ImmutableTable<Entity,KBID,Float> getEntityKBIDDist(){
    return entityKBIDDist;
  }

  public Map<String,Integer> getArtifactCounts(){
    return artifactCounts;
  }

  public int getNextCoreferenceId(){
    return nextCoreferenceId;
  }

  public long getMaxEntityId(){
    return maxEntityId;
  }

  public Set<EntityMention> getAllCorefEntityMentions(){
    return allCorefEntityMentions;
  }

}


