package adept.e2e.driver;

import adept.common.*;
import adept.e2e.chunkalignment.ChunkEquivalenceClass;
import adept.e2e.chunkalignment.ChunkQuotientSet;
import adept.e2e.stageresult.DocumentResultObject;
import com.google.common.collect.*;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_COREF;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_ENTITY_LINKING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class EntityTypeReassigner implements
    Function<DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
    <HltContentContainer,
        HltContentContainer>>, ChunkQuotientSet>, ImmutableList<String>>,
        DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
            <HltContentContainer,
        HltContentContainer
        >>, ChunkQuotientSet>,ImmutableList<String>>> {

    private static final Logger log = LoggerFactory.getLogger(EntityTypeReassigner.class);
    private final List<String> algorithmsForTypeReassignment;
    private final boolean throwExceptions;


    public EntityTypeReassigner(List<String> algorithmsForTypeReassignment, boolean throwExceptions){
      checkNotNull(algorithmsForTypeReassignment);
      checkArgument(!algorithmsForTypeReassignment.isEmpty());
      this.algorithmsForTypeReassignment = algorithmsForTypeReassignment;
      this.throwExceptions = throwExceptions;
    }

    public DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer,
      HltContentContainer>>, ChunkQuotientSet>,ImmutableList<String>> call
        (DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer,HltContentContainer>>, ChunkQuotientSet>,ImmutableList<String>> input)
        throws Exception{

      Map<String,String> algorithmTypeNameMap = new HashMap<>();

      DocumentResultObject<ImmutableList<DocumentResultObject
          <HltContentContainer,
              HltContentContainer>>, ChunkQuotientSet> chunkAlignmentDRO = input.getInputArtifact();
      //if chunk-alignment is unsuccessful, or if input is already successful, return
      if(!chunkAlignmentDRO.isSuccessful() || input.isSuccessful()) {
        return input;
      }
      ImmutableMap.Builder resultProperties = ImmutableMap.builder();
      long start = System.currentTimeMillis();
      String docId = "";
      ImmutableList.Builder<String> alignedEntities = ImmutableList.builder();
      //Replace all UIUC or RPI entity-types with Stanford entity-types
      ImmutableTable.Builder<String, Long, Entity> entitiesByIdAndAlgorithmBuilder = ImmutableTable.builder();
      Table<String, Entity, Pair<String, Float>> entityToBestTypeByAlgorithm =
          HashBasedTable.create();
      Table<String,Entity,List<EntityMention>> entityToMentionsByAlgorithm = HashBasedTable.create();
      Map<String,HltContentContainer> hltContentContainerMap = new HashMap<>();
      int totalTypeReassignmentsDone = 0;

      try{
      for (DocumentResultObject<HltContentContainer, HltContentContainer>
          hltCCDRO : chunkAlignmentDRO.getInputArtifact()) {
        if (!hltCCDRO.isSuccessful()) {
          continue;
        }
        HltContentContainer hltContentContainer = hltCCDRO.getOutputArtifact().get();
        if(docId.isEmpty()) {
          docId = hltContentContainer.getDocumentId();
        }
        String algorithmType = (String)hltCCDRO.getPropertiesMap().get().get(ALGORITHM_TYPE);
        String algorithmName = hltContentContainer.getAlgorithmName();
        algorithmTypeNameMap.put(algorithmName,algorithmType);

        if(!algorithmType.equals(ALGORITHM_TYPE_COREF)&&!algorithmType.equals(ALGORITHM_TYPE_ENTITY_LINKING)&&
                !algorithmsForTypeReassignment.contains(algorithmType)){
          continue;
        }
        for (Coreference coreference : hltContentContainer.getCoreferences()) {
          for (Entity entity : coreference.getEntities()) {
             entitiesByIdAndAlgorithmBuilder.put(algorithmType, entity.getEntityId(), entity);
          }
        }
        if(algorithmType.equals(ALGORITHM_TYPE_COREF)||algorithmType.equals(ALGORITHM_TYPE_ENTITY_LINKING)){
          hltContentContainerMap.put(algorithmType,hltContentContainer);
        }
      }

      ImmutableTable<String,Long,Entity> entitiesByIdAndAlgorithm = entitiesByIdAndAlgorithmBuilder.build();

      ChunkQuotientSet alignedChunks = chunkAlignmentDRO.getOutputArtifact().get();
      for (ChunkEquivalenceClass chunkAlignment : alignedChunks.equivalenceClasses()) {
        Entity corefEntity = null;
        Entity edlEntity = null;
        Map<String,Entity> sourceEntitiesByAlgorithm = new HashMap<>();
        Map<String,Pair<String, Float>> sourceTypeConfidenceByAlgorithm = new HashMap<>();
        for (Chunk chunk : chunkAlignment) {
          if (!(chunk instanceof EntityMention)) {
            continue;
          }
          EntityMention entityMention = (EntityMention) chunk;
          String algorithmType = algorithmTypeNameMap.get(entityMention.getAlgorithmName());
          long bestEntityId = entityMention.getBestEntityId();
          if(bestEntityId==-1) {
            log.info("For Algorithm: {}, entityMention: {}:{} found bestEntityId=-1",
                algorithmType, entityMention.getValue(), entityMention.getMentionType()
                                                             != null ? entityMention
                                                             .getMentionType().getType() : "null");
            continue;
          }
          Entity destinationEntity = null;
          if (algorithmType.equals(ALGORITHM_TYPE_COREF)) {
            corefEntity = entitiesByIdAndAlgorithm.get(algorithmType, entityMention
                .getBestEntityId());
            destinationEntity = corefEntity;
          } else if (algorithmType.equals(ALGORITHM_TYPE_ENTITY_LINKING)) {
            edlEntity = entitiesByIdAndAlgorithm.get(algorithmType, entityMention
                .getBestEntityId());
            destinationEntity = edlEntity;
          } else if (algorithmsForTypeReassignment.contains(algorithmType)) {
            Entity sourceEntity = entitiesByIdAndAlgorithm.get(algorithmType, entityMention
                .getBestEntityId());
            Pair<String,Float> sourceTypeConfidence = new Pair<>(sourceEntity.getEntityType().getType(), entityMention
                .getConfidence(entityMention.getBestEntityId()));
            sourceEntitiesByAlgorithm.put(algorithmType,sourceEntity);
            sourceTypeConfidenceByAlgorithm.put(algorithmType,sourceTypeConfidence);
          }
          if(destinationEntity!=null){
            List<EntityMention> bestMentionsForEntity = entityToMentionsByAlgorithm.get
                (algorithmType,destinationEntity);
            if(bestMentionsForEntity==null){
              bestMentionsForEntity = new ArrayList<>();
            }
            bestMentionsForEntity.add(entityMention);
            entityToMentionsByAlgorithm.put(algorithmType,destinationEntity,bestMentionsForEntity);
          }
        }
        if (sourceEntitiesByAlgorithm.isEmpty() || (corefEntity == null && edlEntity == null)) {
          continue;
        }
        //Using serifEntity unconditionally for type reassignment
        Entity sourceEntityToUse = null;
        Pair<String,Float> typeConfidenceToUse = null;
        for(String sourceAlgorithm : algorithmsForTypeReassignment){
          sourceEntityToUse = sourceEntitiesByAlgorithm.get(sourceAlgorithm);
          typeConfidenceToUse = sourceTypeConfidenceByAlgorithm.get(sourceAlgorithm);
          if (sourceEntityToUse==null || typeConfidenceToUse.getL().substring(0,3).equalsIgnoreCase("UNK")
                  || typeConfidenceToUse.getL().substring(0,3).equalsIgnoreCase("OTH")) {
            continue;
          }
          break;
        }
        if (corefEntity != null) {
          if(isBadCandidateForTypeReassignment(corefEntity,sourceEntityToUse)){
            continue;
          }
          Pair<String, Float> currentBestTypeConfidence = entityToBestTypeByAlgorithm.get
              (ALGORITHM_TYPE_COREF, corefEntity);
          String currentBestType = currentBestTypeConfidence!=null?currentBestTypeConfidence.getL():"";
          float currentBestConfidence = currentBestTypeConfidence!=null?currentBestTypeConfidence
              .getR().floatValue():0.0f;
          String typeToUse = typeConfidenceToUse.getL();
          float confidenceToUse = typeConfidenceToUse.getR().floatValue();
          if (currentBestType.isEmpty() || currentBestConfidence < confidenceToUse
               || (currentBestConfidence==confidenceToUse
                   && (corefEntity.getEntityType().equals(typeToUse))//if currentBestConfidence is the same as confidenceToUse, use typeToUse if it matches illinoisType
          ) ){
            currentBestTypeConfidence = typeConfidenceToUse;
            entityToBestTypeByAlgorithm
                .put(ALGORITHM_TYPE_COREF, corefEntity, currentBestTypeConfidence);
          }
        }
        if (edlEntity != null) {
          if(isBadCandidateForTypeReassignment(edlEntity,sourceEntityToUse)){
            continue;
          }
          Pair<String, Float> currentBestTypeConfidence = entityToBestTypeByAlgorithm.get(ALGORITHM_TYPE_ENTITY_LINKING,
              edlEntity);
          String currentBestType = currentBestTypeConfidence!=null?currentBestTypeConfidence.getL():"";
          float currentBestConfidence = currentBestTypeConfidence!=null?currentBestTypeConfidence
              .getR().floatValue():0.0f;
          String typeToUse = typeConfidenceToUse.getL();
          float confidenceToUse = typeConfidenceToUse.getR().floatValue();
          if (currentBestType.isEmpty() || currentBestConfidence < confidenceToUse
              || (currentBestConfidence==confidenceToUse
                      && (edlEntity.getEntityType().equals(typeToUse))//if currentBestConfidence is the same as confidenceToUse, use typeToUse if it matches rpiType
          ) ){
            currentBestTypeConfidence = typeConfidenceToUse;
            entityToBestTypeByAlgorithm
                .put(ALGORITHM_TYPE_ENTITY_LINKING, edlEntity, currentBestTypeConfidence);
          }
        }
      }
      //now assign all current-best entity-types to the entities
      for (String algorithmName : entityToBestTypeByAlgorithm.rowKeySet()) {
        for (Map.Entry<Entity, Pair<String, Float>> entry : entityToBestTypeByAlgorithm
            .row(algorithmName)
            .entrySet()) {
          String alignmentInfo = algorithmName;
          Entity entity = entry.getKey();
          List<EntityMention> mentionsMappedToEntity = entityToMentionsByAlgorithm.get
              (algorithmName, entity);
          alignmentInfo+=(" entity:"+entity.getValue()+" before:"+entity.getEntityType().getType());
          String newEntityType = entry.getValue().getL();
          if(!newEntityType.equals(entity.getEntityType().getType())) {
            Type newType = new Type(newEntityType);
            E2eUtil
                .reAssignEntityType(entity, newType, hltContentContainerMap.get(algorithmName));
            //update entityTypes of entityMentions
            for (EntityMention mention : mentionsMappedToEntity) {
              mention.setEntityType(entity.getEntityType());
            }
            totalTypeReassignmentsDone++;
          }
          alignmentInfo+=(" after:"+entity.getEntityType().getType());
          alignedEntities.add(alignmentInfo);
        }
      }
        input.setOutputArtifact(alignedEntities.build());
        input.markSuccessful();
      } catch (Exception e) {
        log.error("Could not align chunks for document: {}", docId, e);
        if(this.throwExceptions){
          throw e;
        }
        input.setOutputArtifact(null);
        input.markFailed();
        resultProperties.put(E2eConstants.PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
        resultProperties.put(E2eConstants.PROPERTY_EXCEPTION_MESSAGE, e.getMessage()!=null?e
            .getMessage():"");
        resultProperties.put(E2eConstants.PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
      resultProperties.put(E2eConstants.PROPERTY_DOCID, docId);
      resultProperties.put(E2eConstants.PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      resultProperties.put(E2eConstants.PROPERTY_TIME_TAKEN, System.currentTimeMillis()-start);
      resultProperties.put("totalTypeReassignmentsDone",totalTypeReassignmentsDone);
      input.setPropertiesMap(resultProperties.build());
      return input;
    }

    private boolean isBadCandidateForTypeReassignment(Entity corefEntity, Entity nonCorefEntity){
      String typeToReplace = corefEntity.getEntityType().getType();
      String replacementType = nonCorefEntity.getEntityType().getType();
      if(replacementType.contains(".")){
        replacementType = replacementType.substring(0,replacementType.indexOf("."));
      }
      //Don't allow known Type to be replaced by anything that's not PER/ORG/GPE/LOC/FAC/TITLE/DATE,
      //since that's mostly erroneous (based on analysis on tac eval 6K set with Stanford-type replacement--although types URL,VEH,COM,WEA,CRIME were not present in that)
      if(!typeToReplace.equalsIgnoreCase("UNKNOWN")) {
        if (!replacementType.equalsIgnoreCase("PER") &&
            !replacementType.equalsIgnoreCase("LOC") &&
            !replacementType.equalsIgnoreCase("GPE") &&
            !replacementType.equalsIgnoreCase("ORG") &&
            !replacementType.equalsIgnoreCase("FAC") &&
            !replacementType.equalsIgnoreCase("TITLE") &&
            !replacementType.equalsIgnoreCase("DATE")) {
          return true;
        }
        if(typeToReplace.equalsIgnoreCase("GPE")&&replacementType.equalsIgnoreCase("LOC")){
            return true;//don't allow GPE->LOC reassignment
        }
        if(!typeToReplace.equalsIgnoreCase("PER")&&replacementType.equalsIgnoreCase("TITLE")){
          return true; //don't allow nonPER to TITLE reassignment
        }
        //Since serif anyway produces PER instead of TITLE, we will not allow PER->TITLE reassignment either (if TITLE comes from Stanford)
        if(typeToReplace.equalsIgnoreCase("PER")&&replacementType.equalsIgnoreCase("TITLE")){
          return true; //don't allow PER->TITLE either
        }
      }
      return false;
    }

}

