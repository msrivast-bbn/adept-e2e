package adept.e2e.mastercontainer;

import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.common.Chunk;
import adept.common.Coreference;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.KBID;
import adept.common.TokenOffset;
import adept.e2e.chunkalignment.ChunkEquivalenceClass;
import adept.e2e.chunkalignment.ChunkQuotientSet;

import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_COREF;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_ENTITY_LINKING;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_EVENT_EXTRACTION;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_NIL_CLUSTERING;
import static com.google.common.base.Preconditions.checkNotNull;

public class AlgorithmChunkMap implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(AlgorithmChunkMap.class);
  private static final long serialVersionUID = -4558093076902332332L;

  private final ImmutableMultimap<Long, EntityMention> corefEntityIdsWithProvenances;
  private final ImmutableTable<Long, KBID, Float>
      corefEntityIdsWithExternalKBIds;
  private final ImmutableTable<String, String, ImmutableMap<Long, Float>>
      nonCorefMentionIdToEntityIdDistByAlgorithm;
  private final ImmutableTable<String, Long, IType>
      nonCorefEntityIdToTypeByAlgorithm;
  private final ImmutableList<Long> edlEntitiesAlignedToCorefEntities;
  private final ImmutableMap<String,String> algorithmTypes;
  private final ImmutableMap<String,Integer> artifactCounts;
  //public static final String NIL_EDL_ALIGNED_WITH_COREF = "EDL_ALIGNED_WITH_COREF";

  private AlgorithmChunkMap(ImmutableMultimap<Long, EntityMention>
      corefEntityIdsWithProvenances,
      ImmutableTable<Long, KBID, Float>
          corefEntityIdsWithExternalKBIds,
      ImmutableTable<String, String, ImmutableMap<Long, Float>>
          nonCorefMentionIdToEntityIdDistByAlgorithm,
      ImmutableTable<String,Long,IType> nonCorefEntityIdToTypeByAlgorithm,
      ImmutableList<Long> edlEntitiesAlignedToCorefEntities, ImmutableMap<String,
      String> algorithmTypes, ImmutableMap<String,Integer> artifactCounts) {
    checkNotNull(corefEntityIdsWithProvenances);
    checkNotNull(corefEntityIdsWithExternalKBIds);
    checkNotNull(nonCorefMentionIdToEntityIdDistByAlgorithm);
    checkNotNull(edlEntitiesAlignedToCorefEntities);
    checkNotNull(algorithmTypes);
    this.corefEntityIdsWithProvenances = corefEntityIdsWithProvenances;
    this.corefEntityIdsWithExternalKBIds = corefEntityIdsWithExternalKBIds;
    this.nonCorefMentionIdToEntityIdDistByAlgorithm = nonCorefMentionIdToEntityIdDistByAlgorithm;
    this.nonCorefEntityIdToTypeByAlgorithm = nonCorefEntityIdToTypeByAlgorithm;
    this.edlEntitiesAlignedToCorefEntities = edlEntitiesAlignedToCorefEntities;
    this.algorithmTypes = algorithmTypes;
    this.artifactCounts = artifactCounts;
  }

  public static AlgorithmChunkMap createAlgorithmChunkMap(
      Map<String, HltContentContainer> algorithmOutput, ChunkQuotientSet alignments,
      Map<String, String> algorithmTypes, boolean is2017TACEvalMode)
      throws Exception {
    checkNotNull(algorithmOutput);
    checkNotNull(alignments);
    checkNotNull(algorithmTypes);

    final String COREF_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_COREF);
    final String ENTITY_LINKING_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_ENTITY_LINKING);
    final String NIL_CLUSTERING_ALGORITHM = algorithmTypes.get(
        ALGORITHM_TYPE_NIL_CLUSTERING);
    final String EVENT_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_EVENT_EXTRACTION);
    checkNotNull(COREF_ALGORITHM,"Coref Algorithm must be present in algorithmTypes map");
    checkNotNull(ENTITY_LINKING_ALGORITHM,"Entity Linking Algorithm must be present in "
        + "algorithmTypes map");
    String docId = algorithmOutput.get(COREF_ALGORITHM).getDocumentId();

    ImmutableMap.Builder<String,Integer> artifactCounts = ImmutableMap.builder();
//    int nilClusteredRPIEntitiesAlignedToCoref = 0;
//    int nilClusteredWikifierEntitiesAlignedToCoref = 0;

    Map<Long,Entity> corefEntityIdToEntityMap = MasterContainerUtils.getEntityIdToEntityMap(algorithmOutput.get(COREF_ALGORITHM));

    ImmutableMultimap.Builder<Long, EntityMention>
        corefEntityIdsWithProvenances = ImmutableMultimap.builder();
    Table<Long, KBID, Float>
        corefEntityIdsWithExternalKBIds = HashBasedTable.create();
    ImmutableTable.Builder<String, String, ImmutableMap<Long, Float>>
        mentionIdToEntityIdDistMapByNonCorefAlgorithm
        = ImmutableTable.builder();
    ImmutableTable.Builder<String, Long, IType>
        entityIdToTypeByNonCorefAlgorithm
        = ImmutableTable.builder();
    Set<String> nonCorefAlgorithms = new HashSet<>();
    Set<String> seenNonCorefAlgorithmMentionIdPairs = new HashSet<>();
    ImmutableList.Builder<Long> usedEDLEntityIds = ImmutableList.builder();
    Iterator<ChunkEquivalenceClass> equivalenceClasses = alignments.equivalenceClasses().iterator();

    while (equivalenceClasses.hasNext()) {//iterate over every bunch of aligned-chunks
      List<EntityMention> provenances = new ArrayList<EntityMention>();
      Map<KBID, Float> externalKBIDs = new HashMap<>();
      long corefEntityId = -1;//We will try to get a single corefEntity from this bunch of
      // aligned-chunks
      float bestCorefEntityConfidence = -1.0f;
      Map<Long, Float> entityIdDistribution = new HashMap<>();
      ChunkEquivalenceClass equivalenceClass = equivalenceClasses.next();
      //use Serif mention,if present to replace spans of all other mentions so that the span is only the mention head
      //for all algorithms
      EntityMention serifMention = null;
      EntityMention mentionWithShortestSpan = null; //use this if serifMention is null
      int shortestSpanLength = (int)Float.POSITIVE_INFINITY;
      for (Chunk chunk : equivalenceClass.chunks()) {//for every chunk in the alignment
        if (!(chunk instanceof EntityMention)) {
          continue;
        }
        EntityMention entityMention = (EntityMention) chunk;
        provenances.add(entityMention);
        if(EVENT_ALGORITHM!=null&&EVENT_ALGORITHM.equals("BBN_SERIF")&&entityMention.getAlgorithmName().equals(EVENT_ALGORITHM)){
          serifMention = entityMention;
        }
        if (entityMention.getAlgorithmName().equals(COREF_ALGORITHM)) {
          log.info("Found Coref entity-mention: {}", entityMention.getValue());
          long bestEntityIdForMention = entityMention.getBestEntityId();
          if(bestEntityIdForMention==-1){
            break;
          }
          float entityConfidence = entityMention.getConfidence(bestEntityIdForMention);
          boolean updateBestCorefEntity = entityConfidence>bestCorefEntityConfidence;
          if(!updateBestCorefEntity&&entityConfidence==bestCorefEntityConfidence){
            String canonicalString = corefEntityIdToEntityMap.get(bestEntityIdForMention).getValue();
            String bestCanonicalString = corefEntityIdToEntityMap.get(corefEntityId).getValue();
            if(canonicalString.length()<bestCanonicalString.length()){
              updateBestCorefEntity = true;
            }
          }
          if(updateBestCorefEntity){
            bestCorefEntityConfidence = entityConfidence;
            corefEntityId = bestEntityIdForMention;
          }
          entityIdDistribution = MasterContainerUtils.mergeMapsToRetainBestConfidence(entityIdDistribution,entityMention
              .getEntityIdDistribution());
        } else if (entityMention.getAlgorithmName()
            .equals(ENTITY_LINKING_ALGORITHM)) {
          // Find entities that this mention is linked to,
          // add them to used entity list, and add external kbids to list
          log.info("Found EDL entity-mention: {}", entityMention.getValue());
          Long entityId = entityMention.getBestEntityId();
          log.info("\tFound entityId: {}", entityId);
          for (Coreference coreference : algorithmOutput.get(ENTITY_LINKING_ALGORITHM)
                .getCoreferences()) {
          boolean entityAdded = false;
            for (Entity edlEntity : coreference.getEntities()) {
              if (edlEntity.getEntityId() == entityId) {
                  log.info("\tFound matching entityId in a coreference...");
                  usedEDLEntityIds.add(edlEntity.getEntityId());
                  Map<KBID,Float> externalKBIDMap = algorithmOutput.get(ENTITY_LINKING_ALGORITHM)
                      .getKBEntityMapForDocEntities()
                      .get(edlEntity);
                  externalKBIDMap = MasterContainerUtils.removeNILKBIDs(externalKBIDMap);
//                  if(externalKBIDMap.isEmpty()){
//                    nilClusteredRPIEntitiesAlignedToCoref++;
//                  }
                  log.info("Adding non-NIL externalKBIDs from " + ALGORITHM_TYPE_ENTITY_LINKING + " for "
                          + ALGORITHM_TYPE_COREF + " entity: {} "
                          + "KBID: {}", edlEntity.getValue(),externalKBIDMap);
                  externalKBIDs = MasterContainerUtils.mergeMapsToRetainBestConfidence(externalKBIDs,
                      externalKBIDMap);
                  entityAdded = true;
                  break;
                }
              }
              if (entityAdded) {
                break;
              }
            }
        } else if (entityMention.getAlgorithmName()
            .equals(NIL_CLUSTERING_ALGORITHM)) {
          // Find entities that this mention is linked to,
          // add them to used entity list, and add NIL kbids to list
          log.info("Found Wikification entity-mention: {}", entityMention.getValue());
          Long entityId = entityMention.getBestEntityId();
          log.info("\tFound entityId: {}", entityId);
          for (Coreference coreference : algorithmOutput.get(NIL_CLUSTERING_ALGORITHM)
              .getCoreferences()) {
            boolean entityAdded = false;
            for (Entity wikifiedEntity : coreference.getEntities()) {
              if (wikifiedEntity.getEntityId() == entityId) {
                log.info("\tFound matching entityId in a coreference...");
//                usedEDLEntityIds.add(wikifiedEntity.getEntityId());
                Map<KBID,Float> externalKBIDMap = algorithmOutput.get(NIL_CLUSTERING_ALGORITHM)
                    .getKBEntityMapForDocEntities().get(wikifiedEntity);
                //illinois-nilclusterer does not use documentEntityToKBEntityMap. It instead sets
                //KBIDs as attributes to mentions
                if(externalKBIDMap==null||externalKBIDMap.isEmpty()){
                  externalKBIDMap = new HashMap<>();
                  for(String key : entityMention.getAttributes().keySet()){
                    if(key.equals("EntityUrl")) {
                      String wikifiedUrl = entityMention.getAttributes().get(key);
                      externalKBIDMap.put(new KBID(wikifiedUrl,"IllinoisWikifier"),entityMention.getConfidence(entityId));
                    }
                  }
                }
                externalKBIDMap = MasterContainerUtils.removeNonNILKBIDs(externalKBIDMap);
//                if(!externalKBIDMap.isEmpty()){
//                  nilClusteredWikifierEntitiesAlignedToCoref++;
//                }
                log.info("Adding NIL externalKBIDs from {} "
                    + " for {} entity: {} "
                        + "KBID: {}", ALGORITHM_TYPE_NIL_CLUSTERING, ALGORITHM_TYPE_COREF, wikifiedEntity.getValue(),externalKBIDMap);
                externalKBIDs = MasterContainerUtils.mergeMapsToRetainBestConfidence(externalKBIDs,
                    externalKBIDMap);
                entityAdded = true;
                break;
              }
            }
            if (entityAdded) {
              break;
            }
          }
        } else {//handle mentions from any algorithm other than ENTITY_LINKING_ALGORITHM
          nonCorefAlgorithms.add(entityMention.getAlgorithmName());
          if(seenNonCorefAlgorithmMentionIdPairs.contains(entityMention.getAlgorithmName()+","+
              entityMention.getIdString())){
            log.info("Algorithm-mention pair: {},{} already present in the table",entityMention.getAlgorithmName(),
                entityMention.getEntityIdDistribution());
            continue;
          }
          log.info("Setting mentionId:{} to entity-distmap: {} for {}", entityMention.getIdString(),
              entityMention.getEntityIdDistribution().size(), entityMention.getAlgorithmName());
          mentionIdToEntityIdDistMapByNonCorefAlgorithm.put(entityMention.getAlgorithmName(),
              entityMention.getIdString(), ImmutableMap.copyOf(entityMention.getEntityIdDistribution
                  ()));
          seenNonCorefAlgorithmMentionIdPairs.add(entityMention.getAlgorithmName()+","+
              entityMention.getIdString());
        }
        if(!entityMention.getAlgorithmName().equals(COREF_ALGORITHM)&&!entityMention.getAlgorithmName().equals(EVENT_ALGORITHM)){
          if(entityMention.getValue().length()<shortestSpanLength){
            mentionWithShortestSpan = entityMention;
          }
        }
      }
      if(corefEntityId==-1){
        continue;
      }
      //if externalKBIDs has both a NIL and a nonNIL KBID, remove the nonNIL ones
      Map<KBID,Float> withoutNilKBIDMap = MasterContainerUtils.removeNILKBIDs(externalKBIDs);
      if(!withoutNilKBIDMap.isEmpty()){
        externalKBIDs = withoutNilKBIDMap;
      }
      // Ensure all provenances map to the coref entity ids
      for (EntityMention provenance : provenances) {
        //Replace all provenance spans with head spans, but don't do that for LIST mentions, since
        //for LIST mentions serif only produces the first element as head
        if(is2017TACEvalMode) {
          if (serifMention != null && !serifMention.getMentionType().getType().equalsIgnoreCase("LIST")) {
            TokenOffset serifOffset = serifMention.getHeadOffset().or(serifMention.getTokenOffset());
            provenance.updateOffset(serifMention.getTokenStream(), serifOffset);
            //replace provenance type with serif's provenance type
            provenance.setMentionType(serifMention.getMentionType());
          } else if (mentionWithShortestSpan != null) {
            provenance.updateOffset(mentionWithShortestSpan.getTokenStream(), mentionWithShortestSpan.getTokenOffset());
          }
        }
        //If this provenance is the canonicalMention for this entity (entity with corefEntityId), skip it
        //since the map corefEntityIdsWithProvenances is meant to map a corefEntity with provenances that
        //are not canonical-mentions
        if(provenance.getIdString().equals(corefEntityIdToEntityMap.get(corefEntityId).getCanonicalMention().getIdString())){
          continue;
        }
        provenance.setEntityIdDistribution(entityIdDistribution);
        corefEntityIdsWithProvenances.put(corefEntityId,provenance);
      }
      for(Map.Entry<KBID,Float> entry : externalKBIDs.entrySet()){
        KBID kbID = entry.getKey();
//        if(kbID.getObjectID().matches("NIL[0-9]+")){
//          String newKBID = "NIL"+(nilSeqNo++)+"_"+docId;
//          kbID = new KBID(newKBID,NIL_EDL_ALIGNED_WITH_COREF);
//        }

        float confidence = entry.getValue().floatValue();
        Float currentConfidence = corefEntityIdsWithExternalKBIds.get(corefEntityId,entry
            .getKey());
        if(currentConfidence==null || currentConfidence.floatValue()<confidence){
          corefEntityIdsWithExternalKBIds.put(corefEntityId,kbID,confidence);
        }
      }
    }
    //finally populated the entityId to Type map for nonCoref algorithms
    for(String nonCorefAlgorithm : nonCorefAlgorithms){
      for(Map.Entry<Long,Entity> entry : MasterContainerUtils.getEntityIdToEntityMap(algorithmOutput.get(nonCorefAlgorithm)).entrySet()){
        entityIdToTypeByNonCorefAlgorithm.put(nonCorefAlgorithm,entry.getKey(),entry.getValue().getEntityType());
      }
    }
//    artifactCounts.put("nilClusteredRPIEntitiesAlignedToCoref",nilClusteredRPIEntitiesAlignedToCoref);
//    artifactCounts.put("nilClusteredWikifierEntitiesAlignedToCoref",nilClusteredWikifierEntitiesAlignedToCoref);
    return new AlgorithmChunkMap(corefEntityIdsWithProvenances.build(),
        ImmutableTable.copyOf(corefEntityIdsWithExternalKBIds),
        mentionIdToEntityIdDistMapByNonCorefAlgorithm.build(),
        entityIdToTypeByNonCorefAlgorithm.build(),
        usedEDLEntityIds.build(),
        ImmutableMap.copyOf(algorithmTypes),artifactCounts.build());
  }

  public Optional<ImmutableList<EntityMention>> getProvenancesByCorefEntityId(long corefEntityId){
    return Optional.fromNullable((ImmutableList)corefEntityIdsWithProvenances.get(corefEntityId));
  }

  public Set<String> getAlgorithmNames(){
    return algorithmTypes.keySet();
  }

  public Optional<String> getAlgorithmNameByType(String algorithmType){
    return Optional.fromNullable(algorithmTypes.get(algorithmType));
  }

  public ImmutableTable<String,String,ImmutableMap<Long,Float>>
  getNonCorefMentionIdToEntityIdDistByAlgorithm(){
    return nonCorefMentionIdToEntityIdDistByAlgorithm;
  }

  public ImmutableTable<String,Long,IType>
  getNonCorefEntityIdToTypeByAlgorithm(){
    return nonCorefEntityIdToTypeByAlgorithm;
  }

  public Optional<ImmutableMap<KBID, Float>> getExternalKBIdsByCorefEntityId(long corefEntityId){
    return Optional.fromNullable(corefEntityIdsWithExternalKBIds.row(corefEntityId));
  }

  public List<Long> getUsedEDLEntityIds(){
    return edlEntitiesAlignedToCorefEntities;
  }

  public ImmutableMap<String,Integer> getArtifactCounts(){
    return artifactCounts;
  }

}
