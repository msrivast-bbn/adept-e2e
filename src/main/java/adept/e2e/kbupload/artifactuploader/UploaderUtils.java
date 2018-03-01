package adept.e2e.kbupload.artifactuploader;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.common.Argument;
import adept.common.Chunk;
import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.GenericThing;
import adept.common.IType;
import adept.common.Item;
import adept.common.KBID;
import adept.common.NumberPhrase;
import adept.common.NumericValue;
import adept.common.OntType;
import adept.common.Pair;
import adept.common.Relation;
import adept.common.RelationMention;
import adept.common.TemporalValue;
import adept.common.TimexValue;
import adept.common.XSDDate;
import adept.e2e.artifactextraction.ExtractedArguments;
import adept.e2e.artifactextraction.ExtractedArtifacts;
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
import adept.e2e.driver.KBSingleton;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import adept.e2e.kbupload.uploadedartifacts.UploadedEvents;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.kbupload.uploadedartifacts.UploadedRelations;
import adept.e2e.stageresult.ResultObject;
import adept.kbapi.KB;
import adept.kbapi.KBDate;
import adept.kbapi.KBEntity;
import adept.kbapi.KBEvent;
import adept.kbapi.KBGenericThing;
import adept.kbapi.KBNumber;
import adept.kbapi.KBOntologyMap;
import adept.kbapi.KBOntologyModel;
import adept.kbapi.KBOpenIEArgument;
import adept.kbapi.KBOpenIERelation;
import adept.kbapi.KBParameters;
import adept.kbapi.KBPredicateArgument;
import adept.kbapi.KBQueryException;
import adept.kbapi.KBRelation;
import adept.kbapi.KBTextProvenance;
import adept.kbapi.KBUpdateException;
import adept.metadata.SourceAlgorithm;

import static adept.e2e.driver.E2eUtil.getExceptionDetails;
import static adept.e2e.driver.E2eUtil.updateResultObject;
import static com.google.common.base.Preconditions.checkNotNull;


public final class UploaderUtils {

  private static Logger log = LoggerFactory.getLogger(UploaderUtils.class);

  private static UploaderUtils instance;

  private final KB kb;

  private final boolean throwExceptions;

  private UploaderUtils(KBParameters kbParameters, boolean throwExceptions) throws Exception {
    checkNotNull(kbParameters);
    kb = KBSingleton.getInstance(kbParameters);
    this.throwExceptions = throwExceptions;
  }

  public static UploaderUtils getInstance(KBParameters kbParameters, boolean throwExceptions)
      throws Exception {
    synchronized (UploaderUtils.class) {
      if (instance == null) {
        instance = new UploaderUtils(kbParameters, throwExceptions);
      }
    }
    return instance;
  }

  public void uploadEntities(ResultObject<Map<EntityKey,
        ItemWithProvenances<Entity,EntityMention>>, ImmutableMap<EntityKey,KBID>> entityUploadResultObject)
      throws Exception {
    Map<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergedEntities = entityUploadResultObject
        .getInputArtifact();
    ImmutableMap.Builder<EntityKey, KBID> entityKeyToInsertedKBIDMap = ImmutableMap.builder();
    int numEntitiesSuccessfullyUploaded = 0;
    int numEntitiesDropped = 0;
    Multiset<String> exceptionsCaught = HashMultiset.<String>create();
    //Upload entities
    log.info("Uploading entities...");
    int entityCount = 0;
    for (Map.Entry<EntityKey, ItemWithProvenances<Entity,EntityMention>> entry : mergedEntities
        .entrySet()) {
      log.info("Attempting to upload merged-entity with entity-key: {} # {} of {}",
          entry.getKey().kbID().getObjectID() , (++entityCount), mergedEntities
          .size());
      Entity entity = (Entity) entry.getValue().item();
      if (entry.getValue().getProvenances().isEmpty()) {
        log.info(
            "Entity has no mention: {} {} {} {} {} {} {}", entity.getEntityId(), entity
                .getEntityType().getType() ,
                 entity.getValue() , entity.getCanonicalMention().getValue() ,
                entry.getKey().kbID().getKBNamespace() , entry.getKey().kbID().getObjectID(), entity.getAlgorithmName());
        //this usually never happens, so no need to worry about this separate count for stats
        numEntitiesDropped++;
        continue;
      }
      log.info(
              "Inserting entity: {} {} {} {} {} {} {}", entity.getEntityId(), entity
                      .getEntityType().getType() ,
              entity.getValue() , entity.getCanonicalMention().getValue() ,
              entry.getKey().kbID().getKBNamespace() , entry.getKey().kbID().getObjectID(), entity.getAlgorithmName());
      StringBuilder provenanceString = new StringBuilder();
      for (EntityMention em : entry.getValue().getProvenances()) {
        provenanceString.append("mention: ").append(em.getValue()).append(' ').append(em.getAlgorithmName())
        .append(' ').append(em.getEntityIdDistribution().get(entity.getEntityId())).append(';');
        //}
      }
      log.info(provenanceString.toString());
      try {
        KBEntity.InsertionBuilder entityInsertionBuilder =
            KBEntity.entityInsertionBuilder(entity,
                new ArrayList(entry.getValue().getProvenances()),
                KBOntologyMap.getAdeptOntologyIdentityMap());
        entityInsertionBuilder.addExternalKBId(entry.getKey().kbID());
        KBEntity kbEntity = entityInsertionBuilder.insert(kb);
        entityKeyToInsertedKBIDMap.put(entry.getKey(), kbEntity.getKBID());
        StringBuilder insertedArtifact = new StringBuilder("InsertedEntity: ").append(entity.getEntityId()).append(
            "; ");
        insertedArtifact.append( entity.getEntityType().getType()).append("; ");
        insertedArtifact.append(entity.getValue()).append("; ");
        insertedArtifact.append(entity.getCanonicalMention().getValue()).append("; ");
        insertedArtifact.append(entry.getKey().kbID().getKBNamespace()).append("; ");
        insertedArtifact.append(entry.getKey().kbID().getObjectID()).append("; ");
        insertedArtifact.append(entity.getAlgorithmName());
        insertedArtifact.append("\nProvenances: ");
        for (EntityMention mention : entry.getValue().getProvenances()) {
          insertedArtifact.append(mention.getValue()).append("; ");
        }
        log.info(insertedArtifact.toString());
        numEntitiesSuccessfullyUploaded++;
      } catch (Exception e) {
        log.error("Entity-Upload: Caught the following exception", e);
        numEntitiesDropped++;
        if (throwExceptions) {
          Map<String,Integer> countsMap = ImmutableMap.of("numEntitiesSuccessfullyUploaded",
              numEntitiesSuccessfullyUploaded,"numEntitiesDropped",numEntitiesDropped);
          updateResultObject(entityUploadResultObject,entityKeyToInsertedKBIDMap.build()
              ,Optional.absent(),countsMap);
          throw e;
        }
        exceptionsCaught.add(getExceptionDetails(e));
      }
    }
    Map<String,Integer> countsMap = ImmutableMap.of("numEntitiesSuccessfullyUploaded",
        numEntitiesSuccessfullyUploaded,"numEntitiesDropped",numEntitiesDropped);
    updateResultObject(entityUploadResultObject,entityKeyToInsertedKBIDMap.build(),
        Optional.of(exceptionsCaught),countsMap);
  }

  public void uploadNonEntityArgs(ResultObject<ExtractedArguments,UploadedNonEntityArguments>
      argUploadResultObject) throws Exception {
    ExtractedArguments extractedArguments = argUploadResultObject.getInputArtifact();
    Multiset<String> exceptionsCaught = HashMultiset.<String>create();
    Map<String,Integer> countsMap = new HashMap<>();

    ImmutableMap<NumericValueKey,KBID> uploadedNumericValues = null;
    ImmutableMap<TemporalValueKey,KBID> uploadedTemporalValues = null;
    ImmutableMap<GenericThingKey, KBID> uploadedGenericThings = null;

    log.info("Uploading non-entity arguments...");

    if(extractedArguments.mergedNumericValues().isPresent()){
      int numNumericValuesSuccessfullyUploaded = 0;
      int numTotalNumericValues = extractedArguments.mergedNumericValues().get().size();
      uploadedNumericValues = uploadArguments(extractedArguments.mergedNumericValues().get(),exceptionsCaught);
      numNumericValuesSuccessfullyUploaded = uploadedNumericValues.size();
      countsMap.put("numNumericValuesSuccessfullyUploaded",numNumericValuesSuccessfullyUploaded);
      countsMap.put("numNumericValuesDropped",(numTotalNumericValues-numNumericValuesSuccessfullyUploaded));
    }

    if(extractedArguments.mergedTemporalValues().isPresent()){
      int numTemporalValuesSuccessfullyUploaded = 0;
      int numTotalTemporalValues = extractedArguments.mergedTemporalValues().get().size();
      uploadedTemporalValues = uploadArguments(extractedArguments.mergedTemporalValues().get(),exceptionsCaught);
      numTemporalValuesSuccessfullyUploaded = uploadedTemporalValues.size();
      countsMap.put("numTemporalValuesSuccessfullyUploaded", numTemporalValuesSuccessfullyUploaded);
      countsMap.put("numTemporalValuesDropped",(numTotalTemporalValues-numTemporalValuesSuccessfullyUploaded));
    }

    if(extractedArguments.mergedGenericThings().isPresent()) {
      int numGenericThingsSuccessfullyUploaded = 0;
      int numTotalGenericThings = extractedArguments.mergedGenericThings().get().size();
      uploadedGenericThings =
          uploadArguments(extractedArguments.mergedGenericThings().get(), exceptionsCaught);
      numGenericThingsSuccessfullyUploaded = uploadedGenericThings.size();
      countsMap.put("numGenericThingsSuccessfullyUploaded", numGenericThingsSuccessfullyUploaded);
      countsMap.put("numGenericThingsDropped",
          (numTotalGenericThings - numGenericThingsSuccessfullyUploaded));
    }

    updateResultObject(argUploadResultObject,UploadedNonEntityArguments.create(uploadedNumericValues,uploadedTemporalValues,uploadedGenericThings),
        Optional.of(exceptionsCaught),countsMap);
  }

  private <T,U,V> ImmutableMap<T,KBID> uploadArguments(ImmutableMap<T,ItemWithProvenances<U,V>> argKeyToProvenancesMap,
       Multiset<String> exceptionsCaught) throws KBUpdateException, IOException{
    ImmutableMap.Builder<T,KBID> retVal = ImmutableMap.builder();
    for(T itemKey : argKeyToProvenancesMap.keySet()){
      KBPredicateArgument kbArgument = null;
      try {
        if (itemKey instanceof NumericValueKey) {
          NumericValue numericValue = ((NumericValueKey) itemKey).numericValue();
          log.info("Inserting KBNumber {}", numericValue.asNumber());
          List<NumberPhrase> provenances = ImmutableList
              .copyOf((Set<NumberPhrase>) argKeyToProvenancesMap.get(itemKey).getProvenances());
          KBNumber.InsertionBuilder argInsertionBuilder =
              KBNumber.numberInsertionBuilder(numericValue,
                  provenances);
          kbArgument = argInsertionBuilder.insert(kb);
        } else if (itemKey instanceof TemporalValueKey) {
          TemporalValue temporalValue = ((TemporalValueKey) itemKey).temporalValue();
          log.info("Inserting KBDate {}", temporalValue.asString());
          List<Chunk> provenances = ImmutableList
              .copyOf((Set<Chunk>) argKeyToProvenancesMap.get(itemKey).getProvenances());
          List<KBTextProvenance.InsertionBuilder> provenanceInsertionBuilders =
              FluentIterable.from(provenances)
                  .transform(new Function<Chunk, KBTextProvenance.InsertionBuilder>() {
                    public KBTextProvenance.InsertionBuilder apply(Chunk c) {
                      return new KBTextProvenance.InsertionBuilder(c, 1.0f);
                    }
                  }).toList();
          KBDate.InsertionBuilder argInsertionBuilder = null;
          if (temporalValue instanceof TimexValue) {
            argInsertionBuilder = KBDate.timexInsertionBuilder(
                ((TimexValue) temporalValue).asString())
                .addProvenances(provenanceInsertionBuilders);
          } else if (temporalValue instanceof XSDDate) {
            argInsertionBuilder = KBDate.xsdDateInsertionBuilder(
                ((XSDDate) temporalValue).asString()).addProvenances(provenanceInsertionBuilders);
          }
          kbArgument = argInsertionBuilder.insert(kb);
        } else if (itemKey instanceof GenericThingKey) {
          GenericThing genericThing = ((GenericThingKey) itemKey).genericThing();
          log.info("Inserting KBGenericThing type={} value={}", genericThing.getType().getType(),
              genericThing.getValue());
          List<Chunk> provenances = ImmutableList
              .copyOf((Set<Chunk>) argKeyToProvenancesMap.get(itemKey).getProvenances());
          KBGenericThing.InsertionBuilder argInsertionBuilder =
              KBGenericThing.genericThingInsertionBuilder(genericThing,
                  provenances, KBOntologyMap.getAdeptOntologyIdentityMap());
          kbArgument = argInsertionBuilder.insert(kb);
        }
      }catch (Exception e){
        log.error("Caught the following exception while uploading an argument", e);
        if (throwExceptions) {
          throw e;
        }
        exceptionsCaught.add(getExceptionDetails(e));
      }
      if(kbArgument!=null){
        retVal.put(itemKey,kbArgument.getKBID());
      }
    }
    return retVal.build();
  }

  public void uploadRelations(ResultObject<ExtractedArtifacts,UploadedRelations>
      relationUploadResultObject, UploadedEntities deduplicatedEntities,
      UploadedNonEntityArguments uploadedArguments) throws Exception {
    ExtractedArtifacts extractedArtifacts = relationUploadResultObject.getInputArtifact();
    ImmutableMap.Builder<RelationKey, KBID> uploadedRelations = ImmutableMap.builder();

    Multiset<String> exceptionsCaught = HashMultiset.<String>create();
    int numRelationsSuccessfullyUploaded=0;
    int numRelationsDropped=0;

    log.info("Uploading relations...");
    int relationCount = 0;
    Map<RelationKey, MergedDocumentRelation> mergedRelations = extractedArtifacts.mergedRelations();
    for (Map.Entry<RelationKey, MergedDocumentRelation> entry :
        mergedRelations.entrySet()) {
      log.info("Attempting to upload merged-relation with relation-key: {} # {} of {}",
          entry.getKey().toString(), (++relationCount), mergedRelations
          .size());
      MergedDocumentRelation mergedRelation = entry.getValue();
      Map<ArgumentKey,Float> argumentConfidences = mergedRelation.argumentConfidences();
      log.info(
          "Type for relation={}" , mergedRelation.relationKBType().getType());
      DocumentRelation.Builder documentRelationBuilder = DocumentRelation.builder(
          mergedRelation.relationKBType());
      documentRelationBuilder.setConfidence(mergedRelation.confidence());
      documentRelationBuilder.addProvenances(mergedRelation.provenances());
      List<DocumentRelationArgument> docArguments = new ArrayList<>();
      Map<Item, KBPredicateArgument> insertedArgumentMap = new HashMap<>();
      Set<Pair<KBEntity,OntType>> additionalEntityTypes = new HashSet<>();
      boolean skipInsertion = false;
      for (Map.Entry<ArgumentKey, ImmutableSet<RelationMention.Filler>> relEntry :
          mergedRelation.argumentProvenances().entrySet()) {
        ArgumentKey argumentKey = relEntry.getKey();
        ItemKey itemKey = argumentKey.fillerKey();
        Set<RelationMention.Filler> argProvenances = relEntry.getValue();
        float argConfidence = argumentConfidences.get(argumentKey);
        KBPredicateArgument kbArgument = null;
        DocumentRelationArgument.Filler filler = null;
        try {
          if (itemKey instanceof EntityKey) {
            Entity entity =
                (Entity) extractedArtifacts.mergedEntities().get((EntityKey) itemKey).item();
//            kbArgument = this.kb.getKBObjectByExternalIDAndType(((EntityKey)itemKey).kbID(),(
//                (EntityKey)itemKey).type()).get();
            KBID entityKBID = deduplicatedEntities.uploadedEntities().get((EntityKey)itemKey);
            checkNotNull(entityKBID,"Could not find KBID for entity-arg with key: "+itemKey);
            try {
              kbArgument = this.kb.getEntityById(entityKBID);
            }catch (KBQueryException ex){
              kbArgument = this.kb.getGenericThingByID(entityKBID);
            }
            log.info("Rel-Upload: Found a kbArgument for entity: {},{},{},{},{}",
                kbArgument.getKBID().getObjectID(),entity.getCanonicalMention().getValue(),
                entity.getCanonicalMention().getEntityType().getType(),
                ((EntityKey) itemKey).toString(),argumentKey.kbRole().getType());

            Map<String, String> expectedArgumentTypes =
                KBOntologyModel.instance().getRelationArgumentTypes().get(mergedRelation.relationKBType().getType());
            if (expectedArgumentTypes.containsKey(argumentKey.kbRole().getType())) {
                List<String> possibleEntityTypes = KBOntologyModel.instance().getEntityTypeSubclasses()
                        .get(expectedArgumentTypes.get(argumentKey.kbRole().getType()));
                if (possibleEntityTypes != null) {
                    StringBuilder entityTypeList = new StringBuilder();
                    boolean doesCorrectEntityTypeExist = false;
                    for (IType type : entity.getAllTypes().keySet()) {
                        String entityType = type.getType();
                        entityTypeList.append(entityType).append(", ");
                        if (possibleEntityTypes.contains(entityType)) {
                                doesCorrectEntityTypeExist = true;
                                break;
                        }
                    }
                    if (entityTypeList.length() > 2) {
                      entityTypeList.setLength(entityTypeList.length() - 2);
                    }
                    if (!doesCorrectEntityTypeExist) {
                        log.info("Rel-Upload: Argument type {}'s entity is incorrect type for relation {} (Expected one of {} Actual: {}). Skipping insertion of this relation. Entity: " +
                                "{},{},{},{}", argumentKey.kbRole().getType()
                                    , mergedRelation.relationKBType().getType(),
                                    possibleEntityTypes.toString(),
                                    entityTypeList.toString(),
                                    kbArgument.getKBID().getObjectID(), entity.getCanonicalMention().getValue(),
                                    entity.getCanonicalMention().getEntityType().getType(),
                                    ((EntityKey)itemKey).toString());
                        skipInsertion = true;
                    }
                }
            }

            filler = DocumentRelationArgument.Filler.fromEntity(entity);
            if(argumentKey.additionalArgType().isPresent()){//this should be false for GenericThings
              log.info("Found additionalType {} to update for entity",argumentKey
                  .additionalArgType().get());
              additionalEntityTypes.add(new Pair<KBEntity,OntType>((KBEntity)kbArgument,argumentKey
                  .additionalArgType().get()));
            }
          } else if (itemKey instanceof NumericValueKey) {
            NumericValue numericValue = ((NumericValueKey)itemKey).numericValue();
            kbArgument = this.kb.getNumberValueByID(uploadedArguments.uploadedNumericValues().get().get(itemKey));
            filler = DocumentRelationArgument.Filler.fromNumericValue(numericValue);
          } else if (itemKey instanceof TemporalValueKey) {
            TemporalValue temporalValue = ((TemporalValueKey)itemKey).temporalValue();
            kbArgument = this.kb.getKBDateByDateId(uploadedArguments.uploadedTemporalValues().get().get(itemKey));
            filler = DocumentRelationArgument.Filler.fromTemporalValue(temporalValue);
          } else if (itemKey instanceof GenericThingKey) {
            GenericThing genericThing = ((GenericThingKey)itemKey).genericThing();
            kbArgument = this.kb.getGenericThingByID(uploadedArguments.uploadedGenericThings().get().get(itemKey));
            filler = DocumentRelationArgument.Filler.fromGenericThing(genericThing);
          } else {
            log.error("Rel-Upload: cound not recognise type of itemKey: {}", itemKey.getClass().getName());
            skipInsertion = true;
            //this usually never happens, so no need to worry about this separate count for stats
            numRelationsDropped++;
            break;
          }
        } catch (Exception e) {
          numRelationsDropped++;
          log.error(
              "Rel-Upload: Caught the following exception while uploading an argument", e);
          if (throwExceptions) {
            Map<String,Integer> countsMap = ImmutableMap.of("numRelationsSuccessfullyUploaded",
                numRelationsSuccessfullyUploaded,"numRelationsDropped",numRelationsDropped);
            updateResultObject(relationUploadResultObject,UploadedRelations.create(uploadedRelations
                    .build()),Optional.absent(),countsMap);
            throw e;
          }
          exceptionsCaught.add(getExceptionDetails(e));
          skipInsertion = true;
          break;
        }
        insertedArgumentMap.put(filler.asItem().get(), kbArgument);
        try {
          DocumentRelationArgument.Builder docArgumentBuilder
              = DocumentRelationArgument.builder(
              argumentKey.kbRole(), filler, argConfidence);
          docArgumentBuilder.addProvenances(argProvenances);
          DocumentRelationArgument docArgument = docArgumentBuilder.build();
          docArgument.setAlgorithmName("MERGED");
          docArgument.setSourceAlgorithm(new SourceAlgorithm("MERGED", "BBN"));
          docArguments.add(docArgument);
        } catch (Exception e) {
          log.error(
              "KBUploaderBkp-WARN: Rel-Upload: Caught the following exception for argument", e);
          skipInsertion = true;
          numRelationsDropped++;
          if (throwExceptions) {
            Map<String,Integer> countsMap = ImmutableMap.of("numRelationsSuccessfullyUploaded",
                numRelationsSuccessfullyUploaded,"numRelationsDropped",numRelationsDropped);
            updateResultObject(relationUploadResultObject,
                UploadedRelations.create(uploadedRelations
                        .build()), Optional.absent(), countsMap);
            throw e;
          }
          exceptionsCaught.add(getExceptionDetails(e));
          break;
        }
      }
      if (skipInsertion) {
        continue;
      }
      try {
        documentRelationBuilder.addArguments(docArguments);
        DocumentRelation documentRelation = documentRelationBuilder.build();
        documentRelation.setAlgorithmName("MERGED");
        documentRelation.setSourceAlgorithm(new SourceAlgorithm("MERGED", "BBN"));
        KBRelation.InsertionBuilder relationInsertionBuilder
            = KBRelation.relationInsertionBuilder(documentRelation,
            insertedArgumentMap, KBOntologyMap.getAdeptOntologyIdentityMap());
        for(Pair<KBEntity,OntType> updatePair : additionalEntityTypes){
          KBEntity kbEntity = updatePair.getL();
          KBEntity.UpdateBuilder entityTypeUpdateBuilder = kbEntity.updateBuilder();
          OntType type = updatePair.getR();
          if(!kbEntity.getTypes().containsKey(type)){
            entityTypeUpdateBuilder.addNewType(type,1.0f);//using a hard-coded confidence of
            // 1.0, since we don't have a mechanism to update additionalTypes in E2E
            // merging/KBUpload modules
            relationInsertionBuilder.addRequiredEntityUpdate(entityTypeUpdateBuilder);
          }
        }
        KBRelation kbRelation = relationInsertionBuilder.insert(kb);
        uploadedRelations.put(entry.getKey(), kbRelation.getKBID());
        StringBuilder insertedArtifact = new StringBuilder("KBUploaderBkp: InsertedRelation: ");
        insertedArtifact.append(documentRelation.getRelationType().getType()).append("; ");
        insertedArtifact.append(documentRelation.getValue()).append("; ");
        insertedArtifact.append(documentRelation.getAlgorithmName()).append("\nArguments: ");
        for (DocumentRelationArgument arg : documentRelation.getArguments()) {
          insertedArtifact.append(arg.getRole().getType()).append("; ");
          insertedArtifact.append(arg.getValue()).append("; ");
          insertedArtifact.append(arg.getAlgorithmName()).append("; ");
          insertedArtifact.append(arg.getFiller().asItem().get().getValue());
          if (arg.getProvenances() != null) {
            insertedArtifact.append("; Provenances: ");
            for (RelationMention.Filler prov : arg.getProvenances()) {
              if (prov.asChunk().isPresent()) {
                insertedArtifact.append(prov.asChunk().get().getValue()).append("; ");
              }
            }
          }
          insertedArtifact.append('\n');
        }
        if (documentRelation.getProvenances() != null) {
          insertedArtifact.append("RelationProvenances: ");
          for (RelationMention prov : documentRelation.getProvenances()) {
            insertedArtifact.append(prov.getValue()).append("; ");
          }
        }
        log.info(insertedArtifact.toString());
        numRelationsSuccessfullyUploaded++;
      } catch (Exception e) {
        numRelationsDropped++;
        log.error(
            "KBUploaderBkp-WARN: Rel-Upload: Caught the following exception for a relation", e);
        if (throwExceptions) {
          Map<String,Integer> countsMap = ImmutableMap.of("numRelationsSuccessfullyUploaded",
              numRelationsSuccessfullyUploaded,"numRelationsDropped",numRelationsDropped);
          updateResultObject(relationUploadResultObject,UploadedRelations.create(uploadedRelations
                  .build()),Optional.absent(),countsMap);
          //Avoid throwing exception for known cases of adept.kbapi.KBUpdateException when the failure is due to incorrect argument type for a relation/event
          // e.g. Failed to insert relation: Argument type person's entity is incorrect type for relation StartPosition (Expected one of [Male, Female, Person] Actual: GeoPoliticalEntity)
          if(e instanceof adept.kbapi.KBUpdateException && e.getMessage().contains("Failed to insert relation: Argument type")){
          }else {
            throw e;
          }
        }
        exceptionsCaught.add(getExceptionDetails(e));
      }
    }
    Map<String,Integer> countsMap = ImmutableMap.of("numRelationsSuccessfullyUploaded",
        numRelationsSuccessfullyUploaded,"numRelationsDropped",numRelationsDropped);
    updateResultObject(relationUploadResultObject,UploadedRelations.create(uploadedRelations
            .build()),Optional.of(exceptionsCaught),countsMap);
  }

  /**
   * Note that this method currently uploads all the OpenIE relations and their arguments
   * available in extractedArtifacts object to the postgres DB
   * @param extractedArtifacts
   * @return
   * @throws Exception
   */
  public ImmutableMap<KBID,String> uploadOpenIERelations(ExtractedArtifacts extractedArtifacts)
  throws Exception{
    ImmutableMap.Builder<KBID,String> uploadedRelations = ImmutableMap.builder();

    log.info("Uploading OpenIE relations...");
    int relationCount = 0;
    List<Relation> openIERelations = extractedArtifacts
        .openIERelations();
      for (Relation relation : openIERelations) {
        try {
          String argumentsAsString = "";
          for (Argument arg : relation.getArguments()) {
            argumentsAsString +=
                "{Argument: " + arg.getArgumentType() + ";" + arg.getValue() + ";" + arg
                    .getBestArgument().getValue() + "}";
          }
          String relationAsString =
              "[Relation: " + relation.getType() + ";" + argumentsAsString + "]";
          log.info("Attempting to upload openIE relation: {}",
              relationAsString);
          log.info("First attempting to upload  arguments...");
          KBOpenIEArgument kbArg1 = null;
          KBOpenIEArgument kbArg2 = null;
          for (Argument arg : relation.getArguments()) {
            KBOpenIEArgument.InsertionBuilder argInsertionBuilder =
                KBOpenIEArgument.insertionBuilder
                    (arg.getValue()!=null?arg.getValue():arg.getBestArgument().getValue());
            argInsertionBuilder.setConfidence(arg.getConfidence());
            for (Pair<Chunk, Float> element : arg.getArgumentDistribution()) {
              KBTextProvenance.InsertionBuilder provenanceInsertionBuilder =
                  KBTextProvenance.builder
                      (element.getL(), element.getR());
              argInsertionBuilder.addProvenance(provenanceInsertionBuilder);
            }
            log.info("Inserting argument: type={}, value={}, bestChunk={}", arg.getArgumentType(),
                arg.getValue(),arg.getBestArgument().getValue());
            KBOpenIEArgument kbArg = argInsertionBuilder.insert(kb);
            if (arg.getArgumentType().equalsIgnoreCase("Argument1")) {
              kbArg1 = kbArg;
            } else if (arg.getArgumentType().equalsIgnoreCase("Argument2")) {
              kbArg2 = kbArg;
            }
          }
          KBOpenIERelation.InsertionBuilder relationInsertionBuilder = KBOpenIERelation
              .insertionBuilder(relation.getType());
          relationInsertionBuilder.addArg1(kbArg1);
          relationInsertionBuilder.addArg2(kbArg2);
          relationInsertionBuilder.setConfidence(relation.getConfidence());
          Chunk context = relation.getContext();
          float contextConfidence = 1.0f;//hard-coding for now
          KBTextProvenance.InsertionBuilder relationProvenance = KBTextProvenance.builder(context,
              contextConfidence);
          relationInsertionBuilder.addProvenance(relationProvenance);
          KBOpenIERelation kbRelation = relationInsertionBuilder.insert(kb);

          uploadedRelations.put(kbRelation.getKBID(), relationAsString);
        }catch (Exception e){
          if(this.throwExceptions){
            throw e;
          }
          log.error("Caught exception when trying to upload the relation",e);
        }
      }

    return uploadedRelations.build();
  }

  //Upload events
  public void uploadEvents(ResultObject<ExtractedArtifacts,UploadedEvents>
      eventUploadResultObject, UploadedEntities deduplicatedEntities,
      UploadedNonEntityArguments uploadedArguments) throws Exception {
    log.info("KBUploaderBkp: Uploading events...");
    ExtractedArtifacts extractedArtifacts = eventUploadResultObject.getInputArtifact();
    ImmutableMap.Builder<EventKey, KBID> uploadedEvents = ImmutableMap.builder();
    int numEventsSuccessfullyUploaded = 0;
    int numEventsDropped = 0;
    Multiset<String> exceptionsCaught = HashMultiset.<String>create();

    int eventCount = 0;
    Map<EventKey, MergedDocumentEvent> mergedEvents = extractedArtifacts.mergedEvents();
    for (Map.Entry<EventKey, MergedDocumentEvent> entry :
        mergedEvents.entrySet()) {
      log.info("KBUploaderBkp: Attempting to upload merged-event with event-key: {} # {} of {}" +
          entry.getKey().toString(), (++eventCount), mergedEvents.size());
      MergedDocumentEvent mergedEvent = entry.getValue();
      Map<ArgumentKey,Float> argConfidences = mergedEvent.argumentConfidences();
      DocumentEvent.Builder documentEventBuilder = DocumentEvent.builder(
          mergedEvent.eventKBType());
      documentEventBuilder.setScore(mergedEvent.confidence());
      documentEventBuilder.addProvenances(mergedEvent.provenances());
      boolean skipInsertion = false;
      if (mergedEvent.realisKBType().isPresent()) {
        Map<OntType, Float> unaryAttribute = new HashMap<>();
        unaryAttribute.put(mergedEvent.realisKBType().get(), 1.0f);//hard-coding the score for now
        documentEventBuilder.setAttributes(unaryAttribute);
      }
      List<DocumentEventArgument> docArguments = new ArrayList<>();
      Map<Item, KBPredicateArgument> insertedArgumentMap = new HashMap<>();
      Set<Pair<KBEntity,OntType>> additionalEntityTypes = new HashSet<>();
      for (Map.Entry<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>> relEntry :
          mergedEvent.argumentProvenances().entrySet()) {
        ArgumentKey argumentKey = relEntry.getKey();
        ItemKey itemKey = argumentKey.fillerKey();
        Set<DocumentEventArgument.Provenance> argProvenances = relEntry.getValue();
        KBPredicateArgument kbArgument = null;
        DocumentEventArgument.Filler filler = null;
        try {
          if (itemKey instanceof EntityKey) {
            Entity entity =
                (Entity) extractedArtifacts.mergedEntities().get((EntityKey) itemKey).item();
//            kbArgument = this.kb.getKBObjectByExternalIDAndType(((EntityKey)itemKey).kbID(),(
//                (EntityKey)itemKey).type()).get();
            KBID entityKBID = deduplicatedEntities.uploadedEntities().get((EntityKey)itemKey);
            checkNotNull(entityKBID,"Could not find KBID for an entity-arg");
            try {
              kbArgument = this.kb.getEntityById(entityKBID);
            }catch (KBQueryException ex){
              kbArgument = this.kb.getGenericThingByID(entityKBID);
            }
            log.info("Event-Upload: Found a kbArgument for entity: {} {} {} {} {}",
                kbArgument.getKBID().getObjectID(),entity.getCanonicalMention().getValue(),
                 entity.getCanonicalMention().getEntityType(),
                ((EntityKey) itemKey).toString(), argumentKey.kbRole().getType());
            filler = DocumentEventArgument.Filler.fromEntity(entity);
            if(argumentKey.additionalArgType().isPresent()){//this should be false for GenericThings
              log.info("Found additionalType {} to update for entity",argumentKey
                  .additionalArgType().get());
              additionalEntityTypes.add(new Pair<KBEntity,OntType>((KBEntity)kbArgument,argumentKey
                  .additionalArgType().get()));
            }
          }else if (itemKey instanceof TemporalValueKey) {
            TemporalValue temporalValue = ((TemporalValueKey)itemKey).temporalValue();
            kbArgument = this.kb.getKBDateByDateId(uploadedArguments.uploadedTemporalValues().get().get(itemKey));
            filler = DocumentEventArgument.Filler.fromTemporalValue(temporalValue);
          } else if (itemKey instanceof GenericThingKey) {
            GenericThing genericThing = ((GenericThingKey)itemKey).genericThing();
            kbArgument = this.kb.getGenericThingByID(uploadedArguments.uploadedGenericThings().get().get(itemKey));
            filler = DocumentEventArgument.Filler.fromGenericThing(genericThing);
          } else {
            //usually never happens
            log.error("KBUploaderBkp: Event-Upload: Could not upload an argument due to unknown type: {}", itemKey.getClass().getName());
            skipInsertion = true;
            numEventsDropped++;
            break;
          }
        } catch (Exception e) {
          numEventsDropped++;
          log.error("KBUploaderBkp: Event-Upload: Could not upload an argument", e);
          if (this.throwExceptions) {
            Map<String,Integer> countsMap = ImmutableMap.of
                ("numEventsSuccessfullyUploaded",numEventsSuccessfullyUploaded,"numEventsDropped",
                    numEventsDropped);
            updateResultObject(eventUploadResultObject,UploadedEvents.create(uploadedEvents.build()),Optional.absent(),
                countsMap);
            throw e;
          }
          exceptionsCaught.add(getExceptionDetails(e));
          skipInsertion = true;
          break;
        }

        insertedArgumentMap.put(filler.asItem().get(), kbArgument);
        try {
//          log.println("KB kbRole for event argument=" + argumentKey.kbRole().getType());
//          log.println("Role for docEventArgument=" + kbOntologyMap.getTypeForKBType(
//              argumentKey.kbRole()).get().getType());
          DocumentEventArgument.Builder docArgumentBuilder
              = DocumentEventArgument.builder(mergedEvent.eventKBType(),
              argumentKey.kbRole(), filler);
          docArgumentBuilder.setScore(argConfidences.get(argumentKey));
          docArgumentBuilder.addProvenances(argProvenances);
          DocumentEventArgument docArgument = docArgumentBuilder.build();
          docArgument.setAlgorithmName("MERGED");
          docArgument.setSourceAlgorithm(new SourceAlgorithm("MERGED", "BBN"));
          docArguments.add(docArgument);
        } catch (Exception e) {
          numEventsDropped++;
          log.error(
              "KBUploaderBkp-WARN: Event-Upload: Caught exception while creating docEventArgument",
              e);
          if (this.throwExceptions) {
            Map<String,Integer> countsMap = ImmutableMap.of
                ("numEventsSuccessfullyUploaded",numEventsSuccessfullyUploaded,"numEventsDropped",
                    numEventsDropped);
            updateResultObject(eventUploadResultObject,UploadedEvents.create(
                    uploadedEvents.build()),Optional.absent(),
                countsMap);
            throw e;
          }
          exceptionsCaught.add(getExceptionDetails(e));
          skipInsertion = true;
          break;
        }
      }
      if (skipInsertion) {
        continue;
      }
      try {
        documentEventBuilder.addArguments(docArguments);
        DocumentEvent documentEvent = documentEventBuilder.build();
        documentEvent.setAlgorithmName("MERGED");
        documentEvent.setSourceAlgorithm(new SourceAlgorithm("MERGED", "BBN"));
        KBEvent.InsertionBuilder eventInsertionBuilder
            = KBEvent.eventInsertionBuilder(documentEvent,
            insertedArgumentMap, KBOntologyMap.getAdeptOntologyIdentityMap());
        for(Pair<KBEntity,OntType> updatePair : additionalEntityTypes){
          KBEntity kbEntity = updatePair.getL();
          KBEntity.UpdateBuilder entityTypeUpdateBuilder = kbEntity.updateBuilder();
          OntType type = updatePair.getR();
          if(!kbEntity.getTypes().containsKey(type)){
            entityTypeUpdateBuilder.addNewType(type,1.0f);//using a hard-coded confidence of
            // 1.0, since we don't have a mechanism to update additionalTypes in E2E
            // merging/KBUpload modules
            eventInsertionBuilder.addRequiredEntityUpdate(entityTypeUpdateBuilder);
          }
        }
        KBEvent kbEvent = eventInsertionBuilder.insert(kb);
        uploadedEvents.put(entry.getKey(), kbEvent.getKBID());
        StringBuffer insertedArtifact = new StringBuffer("InsertedEvent: ").append(documentEvent.getEventType().getType())
            .append("; ").append(documentEvent.getValue()).append("; ").append(documentEvent.getAlgorithmName());
        insertedArtifact.append("\nArguments: ");
        for (DocumentEventArgument arg : documentEvent.getArguments()) {
          insertedArtifact.append(arg.getRole().getType()).append("; ");
          insertedArtifact.append(arg.getValue()).append("; ");
          insertedArtifact.append(arg.getAlgorithmName()).append("; ");
          insertedArtifact.append(arg.getFiller().asItem().get().getValue());
          if (arg.getProvenances() != null) {
            insertedArtifact.append("; Provenances: ");
            for (DocumentEventArgument.Provenance prov : arg.getProvenances()) {
              try {
                insertedArtifact.append(prov.getEventMentionArgument().getFiller().getValue()).append("; ");
              } catch (NullPointerException e) {
                insertedArtifact.append("null; ");
              }
            }
          }
          insertedArtifact.append("\n");
        }
        if (documentEvent.getProvenances() != null) {
          insertedArtifact.append("EventProvenances: ");
          for (DocumentEvent.Provenance prov : documentEvent.getProvenances()) {
            try {
              for (Chunk c : prov.getEventText().getProvenanceChunks()) {
                insertedArtifact.append(c.getValue()).append("; ");
              }
            } catch (NullPointerException e) {
              insertedArtifact.append("null; ");
            }
          }
        }
        log.info(insertedArtifact.toString());
        numEventsSuccessfullyUploaded++;
      } catch (Exception e) {
        numEventsDropped++;
        log.error(
            "KBUploaderBkp-WARN: Event-Upload: Caught the following exception while inserting an "
                + "event", e);
        if (this.throwExceptions) {
          Map<String,Integer> countsMap = ImmutableMap.of
              ("numEventsSuccessfullyUploaded",numEventsSuccessfullyUploaded,"numEventsDropped",
                  numEventsDropped);
          updateResultObject(eventUploadResultObject,UploadedEvents.create(uploadedEvents.build()),Optional.absent(),
              countsMap);
          //Avoid throwing exception for known cases of adept.kbapi.KBUpdateException when the failure is due to incorrect argument type for a relation/event
          // e.g. Failed to insert relation: Argument type person's entity is incorrect type for relation StartPosition (Expected one of [Male, Female, Person] Actual: GeoPoliticalEntity)
          if(e instanceof adept.kbapi.KBUpdateException && e.getMessage().contains("Failed to insert relation: Argument type")){
          }else {
            throw e;
          }
        }
        exceptionsCaught.add(getExceptionDetails(e));
      }
    }
    log.info("KBUploaderBkp: Done inserting artifacts.");
    Map<String,Integer> countsMap = ImmutableMap.of
        ("numEventsSuccessfullyUploaded", numEventsSuccessfullyUploaded, "numEventsDropped",
            numEventsDropped);
    updateResultObject(eventUploadResultObject,
        UploadedEvents.create(uploadedEvents.build()), Optional.of(exceptionsCaught),
        countsMap);
  }


}


