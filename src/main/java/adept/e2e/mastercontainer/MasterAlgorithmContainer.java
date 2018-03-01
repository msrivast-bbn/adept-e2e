package adept.e2e.mastercontainer;

import adept.common.*;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import javax.annotation.Nullable;

import adept.e2e.chunkalignment.ChunkQuotientSet;
import adept.e2e.driver.E2eUtil;
import adept.e2e.stageresult.DocumentResultObject;
import adept.kbapi.KBOntologyMap;
import adept.metadata.SourceAlgorithm;
import scala.Tuple2;

import static adept.e2e.driver.E2eConstants.ALGORITHM_NAME;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_COREF;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_ENTITY_LINKING;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_EVENT_EXTRACTION;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_NIL_CLUSTERING;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_RELATION_EXTRACTION;
import static adept.e2e.driver.E2eConstants.ONTOLOGY_MAPPING_FILE_PATH;
import static adept.e2e.driver.E2eConstants.PROPERTY_DOCID;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;
import static adept.e2e.driver.E2eConstants.REVERSE_ONTOLOGY_MAPPING_FILE_PATH;

public class MasterAlgorithmContainer implements PairFunction<Tuple2<String,
    DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,
        HltContentContainer>>, String,
    DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer, HltContentContainer>>,
        ChunkQuotientSet>, HltContentContainer>> {

  private static final Logger log = LoggerFactory.getLogger(MasterAlgorithmContainer.class);
  private final boolean throwExceptions;
  private final boolean useMissingEventTypesHack;
  private final boolean is2017TACEvalMode;

  public MasterAlgorithmContainer(boolean throwExceptions, boolean useMissingEventTypesHack,
      boolean is2017TACEvalMode) {
    this.throwExceptions = throwExceptions;
    this.useMissingEventTypesHack = useMissingEventTypesHack;
    this.is2017TACEvalMode = is2017TACEvalMode;
  }

  @Override
  public Tuple2<String, DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
      <HltContentContainer, HltContentContainer>>,
      ChunkQuotientSet>, HltContentContainer>> call
      (Tuple2<String,
          DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
              <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,
              HltContentContainer>>
          inPair) throws
                  Exception {

    if (!inPair._2().getInputArtifact().isSuccessful() || inPair._2().isSuccessful()) {
      //if input is not successful (or output is already successful), return failure (or return as
      // is)
      return inPair;
    }

    String docId = inPair._1().substring(
        inPair._1().lastIndexOf("/") + 1);
    log.info("Creating master-algorithm-container for document: {}" , docId);
    long timeTakenToProcess = 0L;
    ImmutableMap.Builder propertiesMap = ImmutableMap.builder();
    DocumentResultObject result = inPair._2();
    try {
      long start = System.currentTimeMillis();
      ChunkQuotientSet alignments = inPair._2().getInputArtifact().getOutputArtifact().get();

      List<DocumentResultObject<HltContentContainer, HltContentContainer>> rawAlgorithmOutput =
          inPair._2()
              .getInputArtifact().getInputArtifact();
      Map<String, HltContentContainer> algorithmOutput =
          new HashMap<String, HltContentContainer>();
      Map<String, String> algorithmTypes =
          new HashMap<String, String>();
      Map<String,KBOntologyMap> kbOntologyMaps = new HashMap<>();
      for (DocumentResultObject rawOutput : rawAlgorithmOutput) {
        log.info("Attempting to deserialize container for {} for document: {}"
            , rawOutput.getProperty(ALGORITHM_NAME) , docId);
        HltContentContainer hltcc = (HltContentContainer) rawOutput.getOutputArtifact().get();
        if (hltcc.getAlgorithmName() == null || hltcc.getSourceAlgorithm() == null) {
          throw new Exception("MasterAlgorithmContainer expects input containers to have "
              + "algorithnName and sourceAlgorithm set.");
        }
        String algorithmName = (String) rawOutput.getProperty(ALGORITHM_NAME);
        String algorithmType = (String) rawOutput.getProperty(ALGORITHM_TYPE);
        algorithmOutput.put(algorithmName, hltcc);
        algorithmTypes.put(algorithmType,
            algorithmName);
        if(rawOutput.getProperty(ONTOLOGY_MAPPING_FILE_PATH)!=null) {
          String ontologyMappingFilePath =
              (String) rawOutput.getProperty(ONTOLOGY_MAPPING_FILE_PATH);
          String reverseOntologyMappingFilePath = ontologyMappingFilePath;
          if (rawOutput.getProperty(REVERSE_ONTOLOGY_MAPPING_FILE_PATH) != null) {
            reverseOntologyMappingFilePath = (String) rawOutput.getProperty
                (REVERSE_ONTOLOGY_MAPPING_FILE_PATH);
          }
          KBOntologyMap ontologyMap = KBOntologyMap.loadOntologyMap(ontologyMappingFilePath,
              reverseOntologyMappingFilePath);
          kbOntologyMaps.put(algorithmType, ontologyMap);
        }
      }
      if (!algorithmTypes.containsKey(ALGORITHM_TYPE_COREF) ||
          !algorithmTypes.containsKey(ALGORITHM_TYPE_ENTITY_LINKING)) {
        throw new Exception("At least " + ALGORITHM_TYPE_COREF + " and " +
            ALGORITHM_TYPE_ENTITY_LINKING
            + " should be present for master-container to be created.");
      }
      final String COREF_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_COREF);
      final String ENTITY_LINKING_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_ENTITY_LINKING);
      final String RELATION_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_RELATION_EXTRACTION);
      final String EVENT_ALGORITHM = algorithmTypes.get(ALGORITHM_TYPE_EVENT_EXTRACTION);
      final String NIL_CLUSTERING_ALGORITHM = algorithmTypes.get(
          ALGORITHM_TYPE_NIL_CLUSTERING);

      //Pre-processing: getting some mappings between chunks from various algorithms
      AlgorithmChunkMap algorithmChunkMap = AlgorithmChunkMap.createAlgorithmChunkMap
          (algorithmOutput,alignments,algorithmTypes,is2017TACEvalMode);
      //Extracting coref entities
      HltContentContainer corefContainer = algorithmOutput.get(COREF_ALGORITHM);
      MasterCorefEntities masterCorefEntities = MasterCorefEntities.extractCorefEntities
          (corefContainer,kbOntologyMaps.get(ALGORITHM_TYPE_COREF),
              COREF_ALGORITHM,algorithmChunkMap);
      //Extracting edl entities
      HltContentContainer entityLinkingContainer = algorithmOutput.get(ENTITY_LINKING_ALGORITHM);
      KBOntologyMap edlOntologyMap = kbOntologyMaps.get(ALGORITHM_TYPE_ENTITY_LINKING);
      MasterEDLEntities masterEDLEntities = MasterEDLEntities.extractEDLEntities
          (entityLinkingContainer,edlOntologyMap,ENTITY_LINKING_ALGORITHM,algorithmChunkMap
              .getUsedEDLEntityIds(),masterCorefEntities.getNextCoreferenceId(), masterCorefEntities.getMaxEntityId());
      //Extracting document relations
      Optional<MasterDocRelations> masterDocRelations=Optional.absent();
      KBOntologyMap relationOntologyMap = kbOntologyMaps.get(ALGORITHM_TYPE_RELATION_EXTRACTION);
      if (RELATION_ALGORITHM != null && algorithmOutput.get(RELATION_ALGORITHM).
          getDocumentRelations() != null) {
        HltContentContainer relationContainer = algorithmOutput.get(RELATION_ALGORITHM);
        log.info("Collecting relations...no of docRelations: {}" ,
            relationContainer.getDocumentRelations().size());
        masterDocRelations = Optional.of(MasterDocRelations.extractDocRelations
            (relationContainer,relationOntologyMap,RELATION_ALGORITHM,masterCorefEntities
                .getNonCorefEntityIdToBestCorefEntityMapByAlgorithm(RELATION_ALGORITHM)));
      }
      //Extracting document events
      Optional<MasterDocEvents> masterDocEvents = Optional.absent();
      //For 2017 TAC Eval, we don't need events from Serif
      if(!is2017TACEvalMode) {
        KBOntologyMap eventOntologyMap = kbOntologyMaps.get(ALGORITHM_TYPE_EVENT_EXTRACTION);
        if (EVENT_ALGORITHM != null &&
            algorithmOutput.get(EVENT_ALGORITHM).getDocumentEvents() != null) {
          HltContentContainer eventContainer = algorithmOutput.get(EVENT_ALGORITHM);
          log.info(
              "Collecting events...no. of docEvents= {}", eventContainer.getDocumentEvents().size());
          masterDocEvents = Optional.of(MasterDocEvents.extractDocEvents(eventContainer,
              eventOntologyMap,
              EVENT_ALGORITHM, masterCorefEntities
                  .getNonCorefEntityIdToBestCorefEntityMapByAlgorithm(EVENT_ALGORITHM)));
        }
      }
      //Extracting missing document events from relation container
      Optional<MasterDocEvents> missingDocEvents = Optional.absent();
      if(useMissingEventTypesHack){
          HltContentContainer relationContainer = algorithmOutput.get(RELATION_ALGORITHM);
          //it's OK to reset DocumentEvents in relation container, since they won't be used anymore
          ImmutableList<DocumentEvent> missingEvents = ImmutableList.of();
          if (relationContainer != null) {
            missingEvents = FluentIterable.from(relationContainer.getDocumentEvents()).filter(
                new Predicate<DocumentEvent>() {
                  @Override
                  public boolean apply(@Nullable final DocumentEvent documentEvent) {
                    if(E2eUtil.missingEventTypes.contains(documentEvent.getEventType().getType())){
                      return true;
                    }
                    return false;
                  }
                }).toList();
          }
          if(!missingEvents.isEmpty()) {
            relationContainer.setDocumentEvents(missingEvents);
            log.info(
                "Collecting missing events...no. of docEvents= {}" , relationContainer
                    .getDocumentEvents().size());
            missingDocEvents = Optional.of(MasterDocEvents.extractDocEvents(relationContainer,
                relationOntologyMap,
                RELATION_ALGORITHM, masterCorefEntities
                    .getNonCorefEntityIdToBestCorefEntityMapByAlgorithm(RELATION_ALGORITHM)));
          }
      }

      //Creating the master container now...
      //Ideally all algorithms should use the same tokenization. But we have found our default event
      //algorithm to be better than our default coref algorithm for sentence tokenization
      String algorithmToUseForSentences = EVENT_ALGORITHM!=null?EVENT_ALGORITHM:COREF_ALGORITHM;
      HltContentContainer masterHltContentContainer = new HltContentContainer();
      masterHltContentContainer.setAlgorithmName("MERGED");
      masterHltContentContainer.setSourceAlgorithm(new SourceAlgorithm("MERGED", "BBN"));
      if (algorithmOutput.get(algorithmToUseForSentences).getPassages() != null) {
        masterHltContentContainer
            .setPassages(algorithmOutput.get(algorithmToUseForSentences).getPassages());
      }
      if (algorithmOutput.get(algorithmToUseForSentences).getSentences() != null) {
        masterHltContentContainer.setSentences(
            algorithmOutput.get(algorithmToUseForSentences).getSentences());
      }

      Set<EntityMention> masterEntityMentions = new HashSet<>();
      masterEntityMentions.addAll(masterCorefEntities.getAllCorefEntityMentions());
      masterEntityMentions.addAll(masterEDLEntities.getAllEDLEntityMentions());
      masterHltContentContainer.setEntityMentions(new ArrayList<>(masterEntityMentions));

      List<Coreference> masterCoreferences = new ArrayList<>();
      masterCoreferences.addAll(masterCorefEntities.getCoreferences());
      masterCoreferences.addAll(masterEDLEntities.getCoreferences());
      masterHltContentContainer.setCoreferences(masterCoreferences);
      for (Table.Cell<Entity,KBID, Float> entry : masterCorefEntities
          .getEntityKBIDDist().cellSet()) {
          masterHltContentContainer.addEntityToKBEntityMap(entry.getRowKey(),
              entry.getColumnKey(),entry.getValue());
      }
      for (Table.Cell<Entity,KBID, Float> entry : masterEDLEntities
          .getEntityKBIDDist().cellSet()) {
        masterHltContentContainer.addEntityToKBEntityMap(entry.getRowKey(),
            entry.getColumnKey(),entry.getValue());
      }

      if(masterDocRelations.isPresent()){
          ImmutableTable<Long,Pair<IType,IType>,IType> typeAssignmentDetails = updateEntityTypesBasedOnRelations(masterHltContentContainer,
                  masterDocRelations.get().getRelationTypesToEntityArgsMap());

//        for(Map.Entry<Long,Entity> entry : MasterContainerUtils.getEntityIdToEntityMap(masterHltContentContainer).entrySet()){
//          Long id = entry.getKey();
//          Entity entity = entry.getValue();
////          String details = id+" "+entity.getValue()+" "+entity.getEntityType().getType()+" "+
////              entity.getCanonicalMention().getValue()+" ["+entity.getCanonicalMention().getCharOffset().getBegin()+","+
////              entity.getCanonicalMention().getCharOffset().getEnd()+"] "+docId;
//          Map<Pair<IType,IType>,IType> typeMap = typeAssignmentDetails.row(id);
//          if(typeMap==null || typeMap.isEmpty()){
//            continue;
//          }
////          typeAssignmentLogs.add("Details for entity: "+details);
////          if(typeMap.size()>1){
////            typeAssignmentLogs.add("More than one type-assignments");
////          }
////          for(Map.Entry<Pair<IType,IType>,IType> entry1 : typeMap.entrySet()){
////             typeAssignmentLogs.add(entry1.getKey().getL().getType()+"."+entry1.getKey().getR().getType()+" "+entry1.getValue().getType());
////          }
////          typeAssignmentLogs.add("\n");
//        }
//        propertiesMap.put(Joiner.on("\n").join(typeAssignmentLogs),1);
        List<DocumentRelation> filteredDocRelations = filterRelationsWithBadArguments(masterDocRelations.get().getExtractedRelations());
        masterHltContentContainer.setDocumentRelations(filteredDocRelations);
      }
      List<DocumentEvent> masterDocumentEvents = new ArrayList<>();
      if(masterDocEvents.isPresent()){
        masterDocumentEvents.addAll(masterDocEvents.get()
            .getExtractedEvents());
      }
      //Also add the missing events, if any present
      if(missingDocEvents.isPresent()) {
        masterDocumentEvents.addAll(missingDocEvents.get().getExtractedEvents());
      }

      masterHltContentContainer.setDocumentEvents(masterDocumentEvents);

      long endTime = System.currentTimeMillis();
      timeTakenToProcess = endTime - start;
      log.info("Done creating MasterHltContentContainer for {} in {}s", docId,
          timeTakenToProcess/1000);
      result.setOutputArtifact(masterHltContentContainer);
      result.markSuccessful();
      propertiesMap.putAll(algorithmChunkMap.getArtifactCounts());
      propertiesMap.putAll(masterCorefEntities.getArtifactCounts());
      propertiesMap.putAll(masterEDLEntities.getArtifactCounts());
      if(masterDocRelations.isPresent()){
        propertiesMap.putAll(masterDocRelations.get().getArtifactCounts());
      }
      if(masterDocEvents.isPresent()){
        propertiesMap.putAll(masterDocEvents.get().getArtifactCounts());
      }
      if(missingDocEvents.isPresent()){
        propertiesMap.putAll(missingDocEvents.get().getArtifactCounts());
      }
    } catch (Exception e) {
      log.error("Could not create MasterAlgorithmContainer for the file: {}" , docId, e);
      if (throwExceptions) {
        throw e;
      }
      result.markFailed();
      propertiesMap.put(PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
      propertiesMap.put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage() != null ? e.getMessage() : "");
      propertiesMap.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
    }
    propertiesMap.put(PROPERTY_DOCID, docId);
    propertiesMap.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    propertiesMap.put(PROPERTY_TIME_TAKEN, timeTakenToProcess);
    result.setPropertiesMap(propertiesMap.build());
    return inPair;
  }

  private ImmutableTable<Long,Pair<IType,IType>,IType> updateEntityTypesBasedOnRelations(HltContentContainer masterHltContentContainer,
      ImmutableMultimap<Pair<IType,IType>,Pair<Entity,IType>> relationTypesToEntityArgs) throws IOException{
      ImmutableTable.Builder<Long,Pair<IType,IType>,IType> typeAssignmentDetails = ImmutableTable.builder();
      ImmutableMap<String,Pair<String,List<String>>> allowedArgTypes = MasterContainerUtils.getRelationArgTypes();
      for(Pair<IType,IType> relationTypes : relationTypesToEntityArgs.keySet()){
        String relType = relationTypes.getL().getType();
        String argRole = relationTypes.getR().getType();
        if(relType.equals("org:stateprovince_of_headquarters")){
          relType = "org:stateorprovince_of_headquarters";
        }
        Pair<String,List<String>> referenceArgTypes = allowedArgTypes.get(relType);
        List<String> allowedEntityTypes = ImmutableList.of(referenceArgTypes.getL());
        if(argRole.equals("arg-2")){
          allowedEntityTypes =  referenceArgTypes.getR();
        }
        Set<Long> seenEntityIds = new HashSet<>();
        for(Pair<Entity,IType> entityAndNonCorefType : relationTypesToEntityArgs.get(relationTypes)){
          Entity entity = entityAndNonCorefType.getL();
          String originalArgEntityType = entityAndNonCorefType.getR().getType();
          String entityType = entity.getEntityType().getType();
          if(!allowedEntityTypes.contains("STRING")&&!allowedEntityTypes.contains(entityType)){
            String newTypeStr = originalArgEntityType;
            if(!allowedEntityTypes.contains(originalArgEntityType)){
              newTypeStr = allowedEntityTypes.get(0);//Just take the first allowed type for now
            }
            IType newType = new Type(newTypeStr);
            E2eUtil.reAssignEntityType(entity,newType,masterHltContentContainer);
            if(!seenEntityIds.contains(entity.getEntityId())) {
              typeAssignmentDetails.put(entity.getEntityId(), relationTypes, newType);
              seenEntityIds.add(entity.getEntityId());
            }
          }
        }
      }
      return typeAssignmentDetails.build();
  }

  private List<DocumentRelation> filterRelationsWithBadArguments(List<DocumentRelation> documentRelations) throws IOException{
    ImmutableList.Builder validDocumentRelations = ImmutableList.builder();
    ImmutableMap<String,Pair<String,List<String>>> allowedArgTypes = MasterContainerUtils.getRelationArgTypes();
    for(DocumentRelation documentRelation : documentRelations){
      String relType = documentRelation.getRelationType().getType();
      String arg1Type = null;
      String arg2Type = null;
      for(DocumentRelationArgument arg : documentRelation.getArguments() ){
        String argType = null;
        if(arg.getFiller().asEntity().isPresent()){
          argType = arg.getFiller().asEntity().get().getEntityType().getType();
        }else {
          argType = "STRING";
        }
        if(arg.getRole().getType().equals("arg-1")){
          arg1Type = argType;
        }else{
          arg2Type = argType;
        }
      }
      if(arg1Type==null || arg2Type == null){
        continue;
      }
      if(relType.equals("org:stateprovince_of_headquarters")){
        relType = "org:stateorprovince_of_headquarters";
      }
      Pair<String,List<String>> referenceArgTypes = allowedArgTypes.get(relType);
      String allowedArg1Type = referenceArgTypes.getL();
      List<String> allowedArg2Types = referenceArgTypes.getR();
      if(arg1Type.equalsIgnoreCase(allowedArg1Type) && allowedArg2Types.contains(arg2Type)){
        validDocumentRelations.add(documentRelation);
      }
    }
    return validDocumentRelations.build();
  }

}
