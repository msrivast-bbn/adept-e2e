package adept.e2e.mastercontainer;

import adept.common.*;
import adept.kbapi.KBOntologyMap;
import adept.metadata.SourceAlgorithm;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class MasterDocEvents {

  private static Logger log = LoggerFactory.getLogger(MasterDocEvents.class);

  private final ImmutableList<DocumentEvent> documentEventsExtracted;
  private final ImmutableMap<String,Integer> artifactCounts;

  private MasterDocEvents(ImmutableList<DocumentEvent> documentEventsExtracted,
      ImmutableMap<String,Integer> artifactCounts){
    checkNotNull(documentEventsExtracted);
    checkNotNull(artifactCounts);
    this.documentEventsExtracted = documentEventsExtracted;
    this.artifactCounts = artifactCounts;
  }

  public static MasterDocEvents extractDocEvents(
      HltContentContainer eventContainer, KBOntologyMap eventOntologyMap, final String
      EVENT_EXTRACTION_ALGORITHM, ImmutableMap<Long,Pair<Entity,Float>>
      eventEntityIdToBestCorefEntityMap)
        throws Exception{

    checkNotNull(eventContainer);
    checkNotNull(eventOntologyMap);

    int numTotalDocEvents = 0;
    int numEventEntityArgumentsAlignedToCorefEntity = 0;
    int numEventEntityArgumentsNotAlignedToCorefEntity = 0;
    int numTotalDocEventsAdded = 0;

    ImmutableList.Builder<DocumentEvent> documentEventsExtracted = ImmutableList.builder();

    String docId = eventContainer.getDocumentId();

    if(eventContainer.getDocumentEvents()!=null) {
      for (DocumentEvent event : eventContainer.getDocumentEvents()) {
        KBOntologyMap ontologyMap = eventOntologyMap;
        ImmutableMap<Long,Pair<Entity,Float>> nonCorefEntityIdToBestCorefEntityMap =
            eventEntityIdToBestCorefEntityMap;
          numTotalDocEvents++;
//        MasterContainerUtils.checkEventPreconditions(event,ontologyMap,
//            KBOntologyMap.getRichEREOntologyMap(),"BEFORE_COREF_REPLACEMENT");
        String eventInfo = "id=" + event.getIdString() + " value="
            + event.getValue() + " type=" + event.getEventType().getType() + " docId=" + docId;
        log.info("Going over docEvent: {}" , eventInfo);
        boolean allArgumentsValid = true;//If all arguments are not inserted, event insertion will fail
        if (event.getArguments() == null || event.getArguments().size() == 0) {
          String errorMsg = "No event arguments found for the doc-event " + eventInfo;
          log.error(errorMsg);
          //not throwing the exception here since it's a known issue that sherlock drops
          // arguments in certain cases
          //throw new Exception(errorMsg);
        }
        log.info("No. of event arguments={}" , event.getArguments().size());
        ImmutableSet.Builder<DocumentEventArgument> documentEventArgumentsBuilder
            = ImmutableSet.builder();
        for (DocumentEventArgument docArgument : event.getArguments()) {
          IType eventType = event.getEventType();
          Optional<OntType> roleOntType = ontologyMap.getKBRoleForType(
              eventType, docArgument.getRole());
          if (!roleOntType.isPresent()) {
            String errorMsg = EVENT_EXTRACTION_ALGORITHM + ": "
                + "docEventArgumentRole: " +
                docArgument.getRole().getType() + " for event: "+eventType.getType()+" not found "
                + "in the "
                + "ontology";
            log.error(errorMsg);
            throw new Exception(errorMsg);
            //allArgumentsValid = false;
            //break;
          }
          DocumentEventArgument.Filler newFiller = null;
          Optional<Item> item = docArgument.getFiller().asItem();
          if (item.isPresent()) {
            if (item.get() instanceof Entity) {
              Entity entity = (Entity) item.get();
              log.info("{}: Entity Argument", EVENT_EXTRACTION_ALGORITHM);
              log.info("{} Entity:\tentity-id={}; algorithm-name={}; entity-type={}; canonical-mention={}",
                  EVENT_EXTRACTION_ALGORITHM, entity.getIdString(), entity.getAlgorithmName()
                      , entity.getEntityType().getType(), entity.getCanonicalMention().getValue());
              if (!nonCorefEntityIdToBestCorefEntityMap.isEmpty()) {
                if (nonCorefEntityIdToBestCorefEntityMap.containsKey(entity.getEntityId())) {
                  Entity corefEntity = nonCorefEntityIdToBestCorefEntityMap.get(entity.getEntityId()
                  ).getL();
                  log.info("Found a Coref entity for {} entity" ,
                      EVENT_EXTRACTION_ALGORITHM);
                  log.info("Coref Entity:\tentity-id={}; algorithm-name={}; entity-type={}; "
                          + "canonical-mention={}", corefEntity
                      .getIdString() , corefEntity
                      .getAlgorithmName(), corefEntity.getEntityType().getType()
                          , corefEntity.getCanonicalMention().getValue());
                  newFiller = DocumentEventArgument.Filler.fromEntity(corefEntity);
                  numEventEntityArgumentsAlignedToCorefEntity++;
                } else {
                  log.info("Found no Coref Entity for {} entity. Skipping this event...",
                      EVENT_EXTRACTION_ALGORITHM);
                  numEventEntityArgumentsNotAlignedToCorefEntity++;
                  allArgumentsValid = false;
                  break;
                }
              } else {
                log.info("{} not found in entityIdToBestCorefEntityByAlgorithm. Skipping this "
                    + "event.", EVENT_EXTRACTION_ALGORITHM);
                numEventEntityArgumentsNotAlignedToCorefEntity++;
                allArgumentsValid = false;
                break;
              }
            } else if (item.get() instanceof TemporalValue) {
              TemporalValue temporalValue = (TemporalValue) item.get();
              log.info("{}: TemporalValue Argument: {}", EVENT_EXTRACTION_ALGORITHM, temporalValue.asString());
              newFiller = DocumentEventArgument.Filler.fromTemporalValue(temporalValue);
            } else if (item.get() instanceof GenericThing) {
              GenericThing genericThing = (GenericThing) item.get();
              log.info("{}: GenericThing Argument: {} type={}",
                  EVENT_EXTRACTION_ALGORITHM, genericThing.getValue(), genericThing.getType().getType());
              newFiller = DocumentEventArgument.Filler.fromGenericThing(genericThing);
            }
          } else {
            String errorMsg = String.format("%s: No Filler found for an argument in event %s",
                EVENT_EXTRACTION_ALGORITHM, eventInfo);
            log.error(errorMsg);
            throw new Exception(errorMsg);
          }
          DocumentEventArgument.Builder newDocEventArgumentBuilder
              = DocumentEventArgument
              .builder(docArgument.getEventType(), docArgument.getRole(), newFiller);
          newDocEventArgumentBuilder.setAttributes(docArgument.getScoredUnaryAttributes());
          //set algorithmName to all docArgProvenances
          for(DocumentEventArgument.Provenance docArgProvenance : docArgument.getProvenances()){
            docArgProvenance.getEventMentionArgument().setAlgorithmName
                (EVENT_EXTRACTION_ALGORITHM);//this is NOT used for upload
            docArgProvenance.getEventMentionArgument().getFiller().setAlgorithmName
                (EVENT_EXTRACTION_ALGORITHM); //The filler is used for upload
            if(!docArgProvenance.getEventMentionArgument().getScore().isPresent()) {
              docArgProvenance.getEventMentionArgument().updateScore(1.0f);
            }
          }
          newDocEventArgumentBuilder.addProvenances(docArgument.getProvenances());
          if(docArgument.getScore().isPresent()) {
            newDocEventArgumentBuilder.setScore(docArgument.getScore().get());
          }
          DocumentEventArgument newDocEventArgument = newDocEventArgumentBuilder.build();
          SourceAlgorithm sourceAlgorithm = eventContainer.getSourceAlgorithm();
          newDocEventArgument.setSourceAlgorithm(sourceAlgorithm);
          String algorithmName = eventContainer.getAlgorithmName();
          newDocEventArgument.setAlgorithmName(algorithmName);
          documentEventArgumentsBuilder.add(newDocEventArgument);
          log.info("{}: Created a new DocumentEventArgument", EVENT_EXTRACTION_ALGORITHM);
        }
        if (!allArgumentsValid) {
          log.info("{} Not all arguments valid for event {}", EVENT_EXTRACTION_ALGORITHM, eventInfo);
          continue;
        }
        DocumentEvent.Builder documentEventBuilder = DocumentEvent.builder(event.getEventType());
        if(event.getScore().isPresent()) {
          documentEventBuilder.setScore(event.getScore().get());
        }
        documentEventBuilder.setAttributes(event.getScoredUnaryAttributes());
        documentEventBuilder.addArguments(documentEventArgumentsBuilder.build());
        //set algorithmName to all eventProvenances
        for(DocumentEvent.Provenance eventProvenance : event.getProvenances()){
          if(eventProvenance.getEventMention().isPresent()) {//This is not used for upload
            eventProvenance.getEventMention().get().setAlgorithmName(EVENT_EXTRACTION_ALGORITHM);
          }
          //The following chunks are used for upload
          for(Chunk eventTextProvenance : eventProvenance.getEventText().getProvenanceChunks()) {
            eventTextProvenance.setAlgorithmName(EVENT_EXTRACTION_ALGORITHM);
          }
        }
        documentEventBuilder.addProvenances(event.getProvenances());
        DocumentEvent newDocumentEvent = documentEventBuilder.build();
        SourceAlgorithm sourceAlgorithm = eventContainer.getSourceAlgorithm();
        newDocumentEvent.setSourceAlgorithm(sourceAlgorithm);
        String algorithmName = eventContainer.getAlgorithmName();
        newDocumentEvent.setAlgorithmName(algorithmName);
        documentEventsExtracted.add(newDocumentEvent);
        log.info("Created a new {} DocumentEvent", EVENT_EXTRACTION_ALGORITHM);
        numTotalDocEventsAdded++;
//        MasterContainerUtils.checkEventPreconditions(newDocumentEvent,ontologyMap,
//            KBOntologyMap.getRichEREOntologyMap(),"AFTER_COREF_REPLACEMENT");
      }
    }
    ImmutableMap.Builder<String,Integer> artifactCounts = ImmutableMap.builder();

    artifactCounts.put("numEventEntityArgumentsAlignedToCorefEntity ("+EVENT_EXTRACTION_ALGORITHM+")",
        numEventEntityArgumentsAlignedToCorefEntity);
    artifactCounts.put("numEventEntityArgumentsNotAlignedToCorefEntity ("+EVENT_EXTRACTION_ALGORITHM+")",
        numEventEntityArgumentsNotAlignedToCorefEntity);
    artifactCounts.put("numTotalDocEvents ("+EVENT_EXTRACTION_ALGORITHM+")",numTotalDocEvents);
    artifactCounts.put("numTotalDocEventsAdded ("+EVENT_EXTRACTION_ALGORITHM+")",numTotalDocEventsAdded);

    return new MasterDocEvents(documentEventsExtracted.build(),
        artifactCounts.build());
  }

  public List<DocumentEvent> getExtractedEvents(){
    return documentEventsExtracted;
  }

  public Map<String,Integer> getArtifactCounts(){
    return artifactCounts;
  }
}


