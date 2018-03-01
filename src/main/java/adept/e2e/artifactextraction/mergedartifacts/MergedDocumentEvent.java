package adept.e2e.artifactextraction.mergedartifacts;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.EventMentionArgument;
import adept.common.EventText;
import adept.common.IType;
import adept.common.OntType;
import adept.e2e.artifactextraction.artifactkeys.ArgumentKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 9/28/16.
 */
public final class MergedDocumentEvent implements Serializable {

  final EventKey eventKey;
  ImmutableSet<DocumentEvent.Provenance> provenances;
  ImmutableMap<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>> argumentProvenances;
  ImmutableMap<ArgumentKey, Float> argumentConfidences;
  float confidence;

  private MergedDocumentEvent(EventKey eventKey,
      ImmutableSet<DocumentEvent.Provenance>
          provenances,
      ImmutableMap<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>>
          argumentProvenances, ImmutableMap<ArgumentKey,Float> argumentConfidences, float
      confidence) {
    this.eventKey = eventKey;
    this.provenances = provenances;
    this.argumentProvenances = argumentProvenances;
    this.argumentConfidences = argumentConfidences;
    this.confidence = confidence;

  }

  public static MergedDocumentEvent getMergedDocumentEvent(DocumentEvent documentEvent,
      ImmutableMap<ArgumentKey, DocumentEventArgument>
          argKeyToArgMap, EventKey eventKey, KBOntologyMap eventOntologyMap) throws Exception{
    checkNotNull(eventKey);
    checkNotNull(documentEvent);
    checkNotNull(documentEvent.getProvenances());
    checkNotNull(argKeyToArgMap);
    checkArgument(eventKey.getArgumentKeys().equals(argKeyToArgMap.keySet()));

    OntType eventKBType = checkNotNull(EventKey.getKBTypeForEventType(documentEvent.getEventType()
    , eventOntologyMap).orNull(),"EventType: "+documentEvent.getEventType().getType()+" not found in the ontology.");
    Optional<OntType> realisType = EventKey.getRealisKBType(documentEvent, eventOntologyMap);
    checkArgument(eventKey.getEventKBType().equals(eventKBType));
    checkArgument(eventKey.getRealisKBType().equals(realisType));

    ImmutableMap<ArgumentKey,ImmutableSet<DocumentEventArgument.Provenance>> argumentProvenances =
        getArgumentProvenancesMap(argKeyToArgMap,documentEvent.getEventType(),
            eventKBType,eventOntologyMap);

    ImmutableMap.Builder<ArgumentKey,Float> argumentConfidences = ImmutableMap.builder();
    for(Map.Entry<ArgumentKey,DocumentEventArgument> argKeyAndArg : argKeyToArgMap.entrySet()){
      float confidence = argKeyAndArg.getValue().getScore().or(documentEvent.getScore().or(0.5f));
      argumentConfidences.put(argKeyAndArg.getKey(),confidence);
    }

    ImmutableSet<DocumentEvent.Provenance> eventProvenancesWithKBTypes =
        getEventProvenancesWithKBTypes(documentEvent.getProvenances(),eventKBType);


    float confidence = documentEvent.getScore().or(getMinArgConfidence(documentEvent.getArguments
        ()));

    return new MergedDocumentEvent(eventKey,eventProvenancesWithKBTypes,
        argumentProvenances,argumentConfidences.build(),confidence);
  }

  private static ImmutableMap<ArgumentKey,ImmutableSet<DocumentEventArgument.Provenance>>
    getArgumentProvenancesMap(ImmutableMap<ArgumentKey,DocumentEventArgument> argKeyToArgMap,
      IType eventSourceType, OntType eventKBType, KBOntologyMap eventOntologyMap) throws Exception{

    ImmutableMap.Builder<ArgumentKey,ImmutableSet<DocumentEventArgument.Provenance>> argumentProvenancesMap
        = ImmutableMap.builder();
    for(Map.Entry<ArgumentKey,DocumentEventArgument> entry : argKeyToArgMap.entrySet()){
      ArgumentKey argumentKey = entry.getKey();
      DocumentEventArgument eventArgument = entry.getValue();
      ImmutableSet<DocumentEventArgument.Provenance> updatedProvenances =
          getEventArgumentProvenancesWithKBTypes(eventArgument.getProvenances(),
              eventSourceType,eventKBType,eventOntologyMap);
      argumentProvenancesMap.put(argumentKey,updatedProvenances);
    }
    return argumentProvenancesMap.build();
  }

  private static ImmutableSet<DocumentEventArgument.Provenance> getEventArgumentProvenancesWithKBTypes
      (ImmutableSet<DocumentEventArgument.Provenance> eventArgProvenances, IType eventSourceType,
          OntType eventKBType, KBOntologyMap eventOntologyMap)
      throws
      Exception{
    ImmutableSet.Builder updatedEventArgProvenances = ImmutableSet.builder();
    for(DocumentEventArgument.Provenance eventArgProvenance : eventArgProvenances){
      EventMentionArgument mentionArg = eventArgProvenance.getEventMentionArgument();
      OntType mentionArgKBRole = eventOntologyMap.getKBRoleForType(eventSourceType,mentionArg
          .getRole()).orNull();
      checkNotNull(mentionArgKBRole,"Role: "+mentionArg.getRole().getType()+" for eventType: "
          + ""+eventSourceType.getType()+" not found in ontology");
      EventMentionArgument.Builder newMentionArgBuilder = EventMentionArgument.builder(eventKBType,
          mentionArgKBRole,mentionArg.getFiller());
      if(mentionArg.getScore().isPresent()){
        newMentionArgBuilder.setScore(mentionArg.getScore().get());
      }
      newMentionArgBuilder.setAttributes(mentionArg.getScoredUnaryAttributes());
      EventMentionArgument newMentionArg = newMentionArgBuilder.build();
      DocumentEventArgument.Provenance newEventArgProvenance = DocumentEventArgument.Provenance
          .fromStandaloneEventMentionArgument(newMentionArg);//purposefully disregarding any
      // eventMention that
      // might be present in the provenance, for the sake of simplicity
      updatedEventArgProvenances.add(newEventArgProvenance);
    }
    return updatedEventArgProvenances.build();
  }


  private static ImmutableSet<DocumentEvent.Provenance> getEventProvenancesWithKBTypes
      (ImmutableSet<DocumentEvent.Provenance> provenances, OntType eventKBType)
      throws
      Exception{
    ImmutableSet.Builder updatedProvenances = ImmutableSet.builder();
    for(DocumentEvent.Provenance eventProvenance : provenances){
      EventText eventText = eventProvenance.getEventText();
      EventText.Builder newEventTextBuilder = EventText.builder(eventKBType,eventText
          .getProvenanceChunks());
      if(eventText.getScore().isPresent()){
        newEventTextBuilder.setScore(eventText.getScore().get());
      }
      newEventTextBuilder.setAttributes(eventText.getScoredUnaryAttributes());
      EventText newEventText = newEventTextBuilder.build();

      DocumentEvent.Provenance newEventProvenance = DocumentEvent.Provenance.fromEventTextAlone
          (newEventText);//purposefully disregarding any eventMention that might be present in
      // the provenance, for the sake of simplicity

      updatedProvenances.add(newEventProvenance);
    }
    return updatedProvenances.build();
  }

  public void update(DocumentEvent documentEvent, ImmutableMap<ArgumentKey, DocumentEventArgument>
      argKeyToArgMap, KBOntologyMap eventOntologyMap) throws Exception{
    checkNotNull(documentEvent);
    checkNotNull(documentEvent.getProvenances());
    checkNotNull(argKeyToArgMap);

    OntType eventKBType = checkNotNull(EventKey.getKBTypeForEventType(documentEvent.getEventType
        (),eventOntologyMap).orNull(),"EventType "+documentEvent.getEventType().getType()+" not found in the ontology.");
    checkArgument(eventKey.getEventKBType().equals(eventKBType));
    checkArgument(eventKey.getArgumentKeys().equals(argKeyToArgMap.keySet()));

    int currentEventWeight = this.provenances.size();
    float currentEventConfidence = this.confidence;
    float updateEventConfidence = documentEvent.getScore().or(getMinArgConfidence(documentEvent
        .getArguments()));
    int updateEventWeight = documentEvent.getProvenances().size();
    this.updateProvenances(documentEvent.getProvenances());
    this.confidence =
        (currentEventWeight*currentEventConfidence + updateEventWeight*updateEventConfidence)/this.provenances.size();

    Map<ArgumentKey,Float> updatedArgConfidences = new HashMap<>();
    for(Map.Entry<ArgumentKey,DocumentEventArgument> argKeyAndArg :
        argKeyToArgMap.entrySet()){
      int currentArgWeight = this.argumentProvenances.get(argKeyAndArg.getKey()).size();
      int updateArgWeight = argKeyAndArg.getValue().getProvenances().size();
      if(currentArgWeight==0||updateArgWeight==0){
        currentArgWeight = currentEventWeight;
        updateArgWeight = updateEventWeight;
      }
      float currentArgConfidence = this.argumentConfidences.get(argKeyAndArg.getKey());
      float updateArgConfidence = argKeyAndArg.getValue().getScore().or(documentEvent.getScore()
          .or(0.5f));
      float updatedArgConfidence = (currentArgWeight*currentArgConfidence +
                                        updateArgWeight*updateArgConfidence)/(currentArgWeight+updateArgWeight);
      updatedArgConfidences.put(argKeyAndArg.getKey(),updatedArgConfidence);
    }
    this.argumentConfidences = ImmutableMap.copyOf(updatedArgConfidences);

    this.updateArgumentProvenances(getArgumentProvenancesMap(argKeyToArgMap,documentEvent
        .getEventType(),eventKBType,eventOntologyMap));
  }

  private void updateProvenances(ImmutableSet<DocumentEvent.Provenance> eventProvenances) throws Exception {
    if (eventProvenances == null || eventProvenances.isEmpty()) {
      return;
    }
    Set<DocumentEvent.Provenance> newProvenances = new HashSet<>(this.provenances);
    newProvenances.addAll(getEventProvenancesWithKBTypes(eventProvenances,this
        .eventKBType()));
    this.provenances = ImmutableSet.copyOf(newProvenances);
  }

  private void updateArgumentProvenances(
      ImmutableMap<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>> argumentProvenances) {
    if (argumentProvenances == null || argumentProvenances.isEmpty()) {
      return;
    }
    checkArgument(eventKey.getArgumentKeys().equals(argumentProvenances.keySet()));
    Map<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>> newArgumentProvenances = new HashMap
        (this.argumentProvenances);
    for (Map.Entry<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>> entry : argumentProvenances
        .entrySet()) {
      ArgumentKey argumentKey = entry.getKey();
      Set<DocumentEventArgument.Provenance> provenanceSet = new HashSet<>(entry.getValue());
      if (newArgumentProvenances.containsKey(argumentKey)) {
        provenanceSet.addAll(newArgumentProvenances.get(argumentKey));
      }
      newArgumentProvenances.put(argumentKey, ImmutableSet.<DocumentEventArgument.Provenance>copyOf
          (provenanceSet));
    }
    this.argumentProvenances = ImmutableMap.copyOf(newArgumentProvenances);
  }

  private static float getMinArgConfidence(Iterable<DocumentEventArgument> args){
    float minConfidence = 1.0f;
    boolean foundAtLeastOneArgConfidence=false;
    for(DocumentEventArgument arg : args){
      if(arg.getScore().isPresent()&&arg.getScore().get()<minConfidence){
        minConfidence = arg.getScore().get();
        foundAtLeastOneArgConfidence=true;
      }
    }
    if(!foundAtLeastOneArgConfidence){
      minConfidence = 0.5f;
    }
    return minConfidence;
  }

  public OntType eventKBType() {
    return this.eventKey.getEventKBType();
  }

  public Optional<OntType> realisKBType() {
    return this.eventKey.getRealisKBType();
  }


  public Set<DocumentEvent.Provenance> provenances() {
    return provenances;
  }

  public ImmutableMap<ArgumentKey, ImmutableSet<DocumentEventArgument.Provenance>>
  argumentProvenances() {
    return argumentProvenances;
  }

  public ImmutableMap<ArgumentKey, Float> argumentConfidences() {
    return argumentConfidences;
  }

  public float confidence(){
    return confidence;
  }
}
