package adept.e2e.mastercontainer;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import adept.common.Coreference;
import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.KBID;
import adept.common.OntType;
import adept.common.Pair;
import adept.common.Type;
import adept.io.Reader;
import adept.kbapi.KBOntologyMap;
import adept.kbapi.KBOntologyModel;

public class MasterContainerUtils {

  private static Logger log = LoggerFactory.getLogger(MasterContainerUtils.class);
  public static final String NIL_ID_PATTERN = ".*NIL[0-9]+";
  private static ImmutableMap<String,Pair<String,List<String>>> relationArgTypes;
  private static ImmutableList<String> pronouns;

  public static List<EntityMention> getRemainingEntityMentions(List<EntityMention> allMentions,
      List<Entity> includedEntities){
    List<EntityMention> remainingMentions = new ArrayList<>();
    for(EntityMention mention : allMentions){
      for(Entity entity : includedEntities){
        if(mention.getEntityIdDistribution().containsKey(entity.getEntityId())){
          //if the mention refers to at least one included entity, retain it
          remainingMentions.add(mention);
          break;
        }
      }
    }
    return remainingMentions;
  }

  public static void updateEntityIdDistInMentions(List<Entity> entities, List<EntityMention> mentions) {
    List<Long> extractedEntityIds = FluentIterable.from(entities).transform((Entity e) -> e.getEntityId()).toList();
    //From the entityIdDistribution of all entity-mentions, drop the entityIds that are not in the above list
    for (EntityMention mention : mentions) {
      if (!extractedEntityIds.containsAll(mention.getEntityIdDistribution().keySet())) {
        Map<Long, Float> newEntityIdDist = new HashMap<>();
        for (Map.Entry<Long, Float> entry : mention.getEntityIdDistribution().entrySet()) {
          if (extractedEntityIds.contains(entry.getKey())) {
            newEntityIdDist.put(entry.getKey(), entry.getValue());
          }
        }
        mention.setEntityIdDistribution(newEntityIdDist);
      }
    }
  }


  public static Map<Long,Entity> getEntityIdToEntityMap(HltContentContainer hltCC){
    Map<Long,Entity> idToEntityMapBuilder = new HashMap<>();
    if(hltCC.getCoreferences()==null){
      return ImmutableMap.of();
    }
    for(Coreference coreference : hltCC.getCoreferences()){
      if(coreference.getEntities()==null){
        continue;
      }
      for(Entity entity : coreference.getEntities()){
        idToEntityMapBuilder.put(entity.getEntityId(),entity);
      }
    }
    return ImmutableMap.copyOf(idToEntityMapBuilder);
  }



  public static <T> Map<T,Float> mergeMapsToRetainBestConfidence(Map<T, Float> map1, Map<T, Float> map2){
    Map<T, Float> mergedMap = new HashMap<T, Float>(map1);
    if(map2==null){
      return mergedMap;
    }
    for(Map.Entry<T,Float> entry : map2.entrySet()){
      T key = entry.getKey();
      float confidence = entry.getValue().floatValue();
      Float existingConfidence = mergedMap.get(key);
      //TODO: Based on eval analysis, this might need tie-breaking if the two confidences are equal
      //(for both coref mention-to-id mapping and coref entity to KBID mapping)
      if(existingConfidence==null||existingConfidence.floatValue()<confidence){
        mergedMap.put(key,confidence);
      }
    }
    return mergedMap;
  }

  public static Map<KBID,Float> removeNILKBIDs(Map<KBID,Float> externalKBIDMap){
    Map<KBID,Float> newKBIDMap = new HashMap<>();
    if(externalKBIDMap==null){
      return newKBIDMap;
    }
    for(Map.Entry<KBID,Float> entry : externalKBIDMap.entrySet()) {
      KBID kbID = entry.getKey();
      if (!kbID.getObjectID().matches(NIL_ID_PATTERN)) {
        newKBIDMap.put(entry.getKey(),entry.getValue());
      }
    }
    return newKBIDMap;
  }

  public static Map<KBID,Float> removeNonNILKBIDs(Map<KBID,Float> externalKBIDMap){
    Map<KBID,Float> newKBIDMap = new HashMap<>();
    if(externalKBIDMap==null){
      return newKBIDMap;
    }
    for(Map.Entry<KBID,Float> entry : externalKBIDMap.entrySet()) {
      KBID kbID = entry.getKey();
      if (kbID.getObjectID().matches(NIL_ID_PATTERN)) {
        newKBIDMap.put(entry.getKey(),entry.getValue());
      }
    }
    return newKBIDMap;
  }

  /**
   * This method was only meant to assist in some analysis. Hence, deprecating.
   * @param documentRelation
   * @param relationOntologyMap
   * @param entityOntologyMap
   * @param logPrefixIn
   */
  @Deprecated
  public static void checkRelationPreconditions(DocumentRelation documentRelation,
      KBOntologyMap relationOntologyMap, KBOntologyMap entityOntologyMap, String logPrefixIn){

      String logPrefix = "RELATION_PRECONDITIONS: "+logPrefixIn+": ";
      OntType relationOntType = relationOntologyMap.getKBTypeForType(documentRelation.getRelationType())
          .orNull();
      if(relationOntType==null) {
        log.info("{}Relation Type {} was not found in ontology",logPrefix,documentRelation
            .getRelationType()
            .getType());
        return;
      }

      String relationType = relationOntType.getType();
    KBOntologyModel kbOntologyModel = KBOntologyModel.instance();

      if (!kbOntologyModel.getLeafRelationTypes().contains(relationType)
          && !kbOntologyModel.getLeafEventTypes().contains(relationType)
          && !relationType.equals("Subsidiary")
          && !relationType.equals("FamilyRelationship")
          && !relationType.equals("MemberOriginReligionEthnicity")) {
        log.info("{}Relation Type is not a leaf type: {}",logPrefix,relationType);
      }

      Map<String, String> expectedArguments = new HashMap<String, String>(kbOntologyModel
          .getRelationArgumentTypes().get(relationType));
      Map<String, Integer> expectedArgumentOccurences = new HashMap<String, Integer>();
      for (String expectedArgument : expectedArguments.keySet()) {
        if (expectedArgumentOccurences.containsKey(expectedArgument)) {
          expectedArgumentOccurences.put(expectedArgument,
              expectedArgumentOccurences.get(expectedArgument) + 1);
        }
        expectedArgumentOccurences.put(expectedArgument, 1);
      }

      for (DocumentRelationArgument argument : documentRelation.getArguments()) {
        OntType argumentRoleOntType = relationOntologyMap.getKBRoleForType(documentRelation
                .getRelationType(),
            argument.getRole()).orNull();
        if (argumentRoleOntType == null) {
          log.info("{}Argument Role {} could not be found for type {}", logPrefix,argument.getRole()
                  .getType()
              , documentRelation.getRelationType().getType());
          continue;
        }

        String argumentRole = argumentRoleOntType.getType();

        if (!expectedArguments.containsKey(argumentRole)) {
          log.info("{}Role {} is not a valid role for relation-type {}", logPrefix, argumentRole,
              relationType);
        }

        if (argument.getFiller().asEntity().isPresent()) {
          Entity argEntity = argument.getFiller().asEntity().get();
          List<String> possibleEntityTypes = kbOntologyModel.getEntityTypeSubclasses().get(
              expectedArguments.get(argumentRole));
          OntType entityOntType = entityOntologyMap.getKBTypeForType(argEntity.getEntityType())
              .orNull();
          if (entityOntType == null) {
            log.info("{}Entity Type {} not found in ontology",logPrefix, argEntity.getEntityType()
                .getType
                ());
            continue;
          }
          String argEntityType = entityOntType.getType();

          if (possibleEntityTypes != null && !possibleEntityTypes.contains(argEntityType)) {
            log.info("{}Entity Type {} is not among possible entity-types {} for role {} "
                    + "and "
                    + "relation-type {}", logPrefix, argEntityType, possibleEntityTypes.toString(),
                argumentRole,
                relationType);
          }
          log.info("{}relation-type: {}, argRole: {}, argEntity-type: {}",logPrefix,relationType,
              argumentRole,argEntityType);
        }
        expectedArgumentOccurences.put(argumentRole,
            expectedArgumentOccurences.get(argumentRole) - 1);
      }

      // Only check for presence of all arguments if relation is not an event.
      if (!kbOntologyModel.getLeafEventTypes().contains(relationType)
          && !relationType.equals("TemporalSpan")) {
        for (Map.Entry<String, Integer> entry : expectedArgumentOccurences.entrySet()) {
          if (entry.getValue() > 0) {
            log.info("{}Did not find enough occurences of argument-role {} for relation {}",
                    logPrefix, entry.getKey(), relationType);
          }
        }
      }
    }

  /**
   * This method was only meant to assist in some analysis. Hence, deprecating.
   * @param documentEvent
   * @param eventOntologyMap
   * @param entityOntologyMap
   * @param logPrefixIn
   */
  public static void checkEventPreconditions(DocumentEvent documentEvent,
      KBOntologyMap eventOntologyMap, KBOntologyMap entityOntologyMap, String logPrefixIn){

    String logPrefix = "EVENT_PRECONDITIONS: "+logPrefixIn+": ";
    OntType eventOntType = eventOntologyMap.getKBTypeForType(documentEvent.getEventType())
        .orNull();
    if(eventOntType==null) {
      log.info("{}Event Type {} was not found in ontology",logPrefix,documentEvent
          .getEventType()
          .getType());
      return;
    }

    String eventType = eventOntType.getType();
    KBOntologyModel kbOntologyModel = KBOntologyModel.instance();

    if (!kbOntologyModel.getLeafRelationTypes().contains(eventType)
        && !kbOntologyModel.getLeafEventTypes().contains(eventType)
        && !eventType.equals("Subsidiary")
        && !eventType.equals("FamilyRelationship")
        && !eventType.equals("MemberOriginReligionEthnicity")) {
      log.info(logPrefix+"Event Type is not a leaf type: {}",eventType);
    }

    Map<String, String> expectedArguments = new HashMap<String, String>(kbOntologyModel
        .getRelationArgumentTypes().get(eventType));
    Map<String, Integer> expectedArgumentOccurences = new HashMap<String, Integer>();
    for (String expectedArgument : expectedArguments.keySet()) {
      if (expectedArgumentOccurences.containsKey(expectedArgument)) {
        expectedArgumentOccurences.put(expectedArgument,
            expectedArgumentOccurences.get(expectedArgument) + 1);
      }
      expectedArgumentOccurences.put(expectedArgument, 1);
    }

    for (DocumentEventArgument argument : documentEvent.getArguments()) {
      OntType argumentRoleOntType = eventOntologyMap.getKBRoleForType(documentEvent
              .getEventType(),
          argument.getRole()).orNull();
      if (argumentRoleOntType == null) {
        log.info("{}Argument Role {} could not be found for type {}", logPrefix,argument.getRole()
                .getType()
            , documentEvent.getEventType().getType());
        continue;
      }

      String argumentRole = argumentRoleOntType.getType();

      if (!expectedArguments.containsKey(argumentRole)) {
        log.info("{}Role {} is not a valid role for event-type {}", logPrefix, argumentRole,
            eventType);
      }

      if (argument.getFiller().asEntity().isPresent()) {
        Entity argEntity = argument.getFiller().asEntity().get();
        List<String> possibleEntityTypes = kbOntologyModel.getEntityTypeSubclasses().get(
            expectedArguments.get(argumentRole));
        String entityType = argEntity.getEntityType().getType();
        if(entityType.contains(".")){
          //Getting only the main type	
          entityType = entityType.substring(0,entityType.indexOf("."));
        }
        OntType entityOntType = entityOntologyMap.getKBTypeForType(new Type(entityType))
            .orNull();
        if (entityOntType == null) {
          log.info("{}Entity Type {} not found in ontology", logPrefix, argEntity.getEntityType()
              .getType
                  ());
          continue;
        }
        String argEntityType = entityOntType.getType();

        if (possibleEntityTypes != null && !possibleEntityTypes.contains(argEntityType)) {
          log.info("{}Entity Type {} is not among possible entity-types {} for role {} "
                  + "and "
                  + "event-type {}", logPrefix, argEntityType, possibleEntityTypes.toString(),
              argumentRole,
              eventType);
        }
        log.info("{}event-type: {}, argRole: {}, argEntity-type: {}",logPrefix,eventType,
            argumentRole,argEntityType);
      }
      expectedArgumentOccurences.put(argumentRole,
          expectedArgumentOccurences.get(argumentRole) - 1);
    }

    // Only check for presence of all arguments if relation is not an event.
//    if (!kbOntologyModel.getLeafEventTypes().contains(eventType)
//        && !eventType.equals("TemporalSpan")) {
//      for (Map.Entry<String, Integer> entry : expectedArgumentOccurences.entrySet()) {
//        if (entry.getValue() > 0) {
//          log.info(logPrefix+"Did not find enough occurences of argument-role {} for relation {}"
//              , entry.getKey(), eventType);
//        }
//      }
//    }
  }

  public static ImmutableMap<String,Pair<String,List<String>>> getRelationArgTypes() throws IOException{
    if(relationArgTypes==null){
      relationArgTypes = loadRelationArgTypes();
    }
    return relationArgTypes;
  }

  private static ImmutableMap<String,Pair<String,List<String>>> loadRelationArgTypes() throws IOException{
    Map<String,Pair<String,List<String>>> argTypesMap = new HashMap<>();
    InputStream is = Reader.findStreamInClasspathOrFileSystem("allowed_relation_arg_types.tsv");
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line = null;
    while((line = br.readLine())!=null){
      String[] tokens = line.split("\t");
      String arg1Type = tokens[0].substring(0,tokens[0].indexOf(":")).toUpperCase();
      String arg2Type = tokens[1];
      String relType = tokens[0];
      Pair<String,List<String>> argTypes = argTypesMap.get(relType);
      List<String> arg2Types = new ArrayList<>();
      arg2Types.add(arg2Type);
      if(argTypes!=null){
        arg2Types.addAll(argTypes.getR());
      }
      argTypes = new Pair<>(arg1Type,arg2Types);
      argTypesMap.put(relType,argTypes);
    }
    return ImmutableMap.copyOf(argTypesMap);
  }

  public static ImmutableList<String> getPronouns()
      throws IOException{
    if(pronouns==null){
      pronouns = loadPronouns();
    }
    return pronouns;
  }

  private static ImmutableList<String> loadPronouns() throws IOException{
    InputStream is = Reader.findStreamInClasspathOrFileSystem("pronouns.list");
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    ImmutableList.Builder list = ImmutableList.builder();
    String line = null;
    while((line = br.readLine())!=null){
      list.add(line.trim().toLowerCase());
    }
    return list.build();
  }

}


