package adept.e2e.mastercontainer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.GenericThing;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Item;
import adept.common.NumericValue;
import adept.common.OntType;
import adept.common.Pair;
import adept.common.RelationMention;
import adept.common.TemporalValue;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MasterDocRelations {

  private static Logger log = LoggerFactory.getLogger(MasterDocRelations.class);

  private final ImmutableList<DocumentRelation> documentRelationsExtracted;
  private final ImmutableMap<String,Integer> artifactCounts;
  private final ImmutableMultimap<Pair<IType,IType>,Pair<Entity,IType>> relationTypesToEntityArgs;

  private MasterDocRelations(ImmutableList<DocumentRelation> documentRelationsExtracted,
      ImmutableMap<String,Integer> artifactCounts, ImmutableMultimap<Pair<IType,IType>,Pair<Entity,IType>> relationTypesToEntityArgs){
    checkNotNull(documentRelationsExtracted);
    checkNotNull(artifactCounts);
    checkNotNull(relationTypesToEntityArgs);
    this.documentRelationsExtracted = documentRelationsExtracted;
    this.artifactCounts = artifactCounts;
    this.relationTypesToEntityArgs = relationTypesToEntityArgs;
  }

  public static MasterDocRelations extractDocRelations(
      HltContentContainer relationContainer, KBOntologyMap relationOntologyMap, final String
      RELATION_EXTRACTION_ALGORITHM, ImmutableMap<Long,Pair<Entity,Float>>
      relationEntityIdToBestCorefEntity)
        throws Exception{

    checkNotNull(relationContainer);
    checkNotNull(relationOntologyMap);
    checkNotNull(RELATION_EXTRACTION_ALGORITHM);
    checkNotNull(relationEntityIdToBestCorefEntity);
    checkArgument(!RELATION_EXTRACTION_ALGORITHM.isEmpty(),"Relation Algorithm Name cannot be "
        + "empty");

    int numTotalDocRelations = 0;
    int numRelationEntityArgumentsAlignedToCorefEntity = 0;
    int numRelationEntityArgumentsNotAlignedToCorefEntity = 0;
    int numTotalDocRelationsAdded = 0;

    ImmutableList.Builder<DocumentRelation> documentRelationsExtracted = ImmutableList.builder();
    ImmutableMultimap.Builder<Pair<IType,IType>,Pair<Entity,IType>> relationTypesToEntityArgs = ImmutableMultimap.builder();

    String docId = relationContainer.getDocumentId();

    if(relationContainer.getDocumentRelations()!=null) {

      for (DocumentRelation relation : relationContainer.getDocumentRelations()) {
//        MasterContainerUtils.checkRelationPreconditions(relation,relationOntologyMap,
//            KBOntologyMap.getRichEREOntologyMap(),"BEFORE_COREF_REPLACEMENT");
        numTotalDocRelations++;
        boolean allArgumentsValid = true;//If all arguments are not valid, ignore this relation
        ImmutableSet.Builder<DocumentRelationArgument>
            documentRelationArguments = ImmutableSet.builder();
        String relationInfo = "id=" + relation.getIdString() + " value=" +
            relation.getValue() + " type=" + relation.getRelationType().getType() + " docId="
            + docId;
        log.info("Going over relation: {}" , relationInfo);
        if (relation.getArguments() == null || relation.getArguments().size() == 0) {
          log.error("No arguments found for relation: {}" , relationInfo);
          throw new Exception("No arguments found for relation: " + relationInfo);
        }
        log.info("No. of relation arguments={}" , relation.getArguments().size());
        for (DocumentRelationArgument docArgument : relation.getArguments()) {
          Optional<OntType> roleOntType = relationOntologyMap.getKBRoleForType(
              relation.getRelationType(), docArgument.getRole());
          if (!roleOntType.isPresent()) {
            String errorMsg = "docRelationArgumentRole: " + docArgument.getRole().getType()
                + " was not found in ontology";
            log.error(errorMsg);
            throw new Exception(errorMsg);
          }
          log.info("OntType arg-role={}" , roleOntType.get().getType());
          DocumentRelationArgument.Filler newFiller = null;
          Optional<Item> item = docArgument.getFiller().asItem();
          if (item.isPresent()) {
            if (item.get() instanceof Entity) {

              log.info("{} Entity argument",RELATION_EXTRACTION_ALGORITHM );
              Entity entity = (Entity) item.get();
              log.info("{} Entity:\tentity-id={} algorithm-name={} entity-type={} canonical-mention={}" ,
                      RELATION_EXTRACTION_ALGORITHM, entity.getIdString(),
                      entity.getAlgorithmName() + entity.getEntityType()
                      .getType(), entity.getCanonicalMention().getValue());
              if (!relationEntityIdToBestCorefEntity.isEmpty()) {
                if (relationEntityIdToBestCorefEntity.containsKey(entity.getEntityId())) {
                  Entity corefEntity =
                      relationEntityIdToBestCorefEntity.get(entity.getEntityId()).getL();
                  log.info("Found a Coref entity for {} entity",
                          RELATION_EXTRACTION_ALGORITHM);
                  log.info(
                      "Coref Entity:\t entity-id={} algorithm-name={} entity-type={} canonical-mention={}",
                          corefEntity.getIdString(),
                          corefEntity.getAlgorithmName(), corefEntity
                          .getEntityType().getType(), corefEntity.getCanonicalMention()
                          .getValue());
                  relationTypesToEntityArgs.put(new Pair(relation.getRelationType(),docArgument.getRole()),
                      new Pair(corefEntity,entity.getEntityType()));
                  newFiller =
                      DocumentRelationArgument.Filler.fromEntity(corefEntity);
                  numRelationEntityArgumentsAlignedToCorefEntity++;
                } else {
                  log.info("Found no Coref Entity for {} Entity. Skipping this relation.",
                      RELATION_EXTRACTION_ALGORITHM);
                  numRelationEntityArgumentsNotAlignedToCorefEntity++;
                  allArgumentsValid = false;
                  break;
                }
              } else {
                log.info(
                        "{} not found in entityIdToBestCorefEntityByAlgorithm. "
                        + "Skipping this relation.",RELATION_EXTRACTION_ALGORITHM);
                numRelationEntityArgumentsNotAlignedToCorefEntity++;
                allArgumentsValid = false;
                break;
              }
            } else if (item.get() instanceof NumericValue) {
              NumericValue numericValue = (NumericValue) item.get();
              log.info("{}: NumericValue argument: {}",
                      RELATION_EXTRACTION_ALGORITHM , numericValue
                      .asNumber().floatValue());
              newFiller = DocumentRelationArgument.Filler.fromNumericValue(numericValue);
            } else if (item.get() instanceof TemporalValue) {
              TemporalValue temporalValue = (TemporalValue) item.get();
              log.info("{}: TemporalValue argument: {}",
                      RELATION_EXTRACTION_ALGORITHM , temporalValue
                  .asString());
              newFiller = DocumentRelationArgument.Filler.fromTemporalValue(temporalValue);
            } else if (item.get() instanceof GenericThing) {
              GenericThing genericThing = (GenericThing) item.get();
              log.info("{}: GenericThing argument: {} type={}", RELATION_EXTRACTION_ALGORITHM, genericThing
                      .getValue() , genericThing.getType().getType());
              newFiller = DocumentRelationArgument.Filler.fromGenericThing(genericThing);
            }
          } else {
            String errorMsg =
                RELATION_EXTRACTION_ALGORITHM + ": No filler found for an argument in relation"
                    + ": " + relationInfo;
            log.error(errorMsg);
            throw new Exception(errorMsg);
          }
          DocumentRelationArgument.Builder newDocRelationArgumentBuilder
              = DocumentRelationArgument
              .builder(docArgument.getRole(), newFiller, docArgument.getConfidence());
          newDocRelationArgumentBuilder.setAttributes(docArgument.getScoredUnaryAttributes());
          //set algorithmName to all docArgProvenances
          for(RelationMention.Filler docArgProvenance : docArgument.getProvenances()){
            if(docArgProvenance.asChunk().isPresent()){
              docArgProvenance.asChunk().get().setAlgorithmName(RELATION_EXTRACTION_ALGORITHM);
            }
          }
          newDocRelationArgumentBuilder.addProvenances(docArgument.getProvenances());
          DocumentRelationArgument newDocRelationArgument = newDocRelationArgumentBuilder.build();
          newDocRelationArgument.setSourceAlgorithm(relationContainer.getSourceAlgorithm());
          newDocRelationArgument.setAlgorithmName(relationContainer.getAlgorithmName());
          documentRelationArguments.add(newDocRelationArgument);
          log.info("{}: Created a new DocumentRelationArgument", RELATION_EXTRACTION_ALGORITHM );
        }
        if (!allArgumentsValid) {
          log.info("{}: Not all arguments valid for relation: {}",
                  RELATION_EXTRACTION_ALGORITHM,  relationInfo);
          continue;
        }
        DocumentRelation.Builder documentRelationBuilder =
            DocumentRelation.builder(relation.getRelationType());
        documentRelationBuilder.setAttributes(relation.getScoredUnaryAttributes());
        documentRelationBuilder.addArguments(documentRelationArguments.build());
        //set algorithmName to all relation provenances (both relationmentions and their
        // justifications)
        for(RelationMention relationProvenance : relation.getProvenances()){
          relationProvenance.setAlgorithmName(RELATION_EXTRACTION_ALGORITHM);//This provenance is
          // NOT used for upload tho
          if(relationProvenance.getJustification()!=null) {//this provenance is used for upload
            relationProvenance.getJustification().setAlgorithmName(RELATION_EXTRACTION_ALGORITHM);
          }
        }
        documentRelationBuilder.addProvenances(relation.getProvenances());
        documentRelationBuilder.setConfidence(relation.getConfidence());
        DocumentRelation newDocumentRelation = documentRelationBuilder.build();
        newDocumentRelation.setSourceAlgorithm(relationContainer.getSourceAlgorithm());
        String algorithmName = relation.getAlgorithmName();
        newDocumentRelation.setAlgorithmName(relationContainer.getAlgorithmName());
        documentRelationsExtracted.add(newDocumentRelation);
        log.info("Created a new {} DocumentRelation",RELATION_EXTRACTION_ALGORITHM);
        numTotalDocRelationsAdded++;
//        MasterContainerUtils.checkRelationPreconditions(newDocumentRelation,relationOntologyMap,
//            KBOntologyMap.getRichEREOntologyMap(),"AFTER_COREF_REPLACEMENT");
      }
    }
    ImmutableMap.Builder<String,Integer> artifactCounts = ImmutableMap.builder();

    artifactCounts.put("numRelationEntityArgumentsAlignedToCorefEntity",numRelationEntityArgumentsAlignedToCorefEntity);
    artifactCounts.put("numRelationEntityArgumentsNotAlignedToCorefEntity",numRelationEntityArgumentsNotAlignedToCorefEntity);
    artifactCounts.put("numTotalDocRelations",numTotalDocRelations);
    artifactCounts.put("numTotalDocRelationsAdded",numTotalDocRelationsAdded);

    return new MasterDocRelations(documentRelationsExtracted.build(),
        artifactCounts.build(),relationTypesToEntityArgs.build());
  }

  public List<DocumentRelation> getExtractedRelations(){
    return documentRelationsExtracted;
  }

  public Map<String,Integer> getArtifactCounts(){
    return artifactCounts;
  }

  public ImmutableMultimap<Pair<IType,IType>,Pair<Entity,IType>> getRelationTypesToEntityArgsMap(){
    return relationTypesToEntityArgs;
  }
}


