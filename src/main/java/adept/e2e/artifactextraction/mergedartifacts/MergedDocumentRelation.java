package adept.e2e.artifactextraction.mergedartifacts;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.IType;
import adept.common.OntType;
import adept.common.RelationMention;
import adept.common.Type;
import adept.e2e.artifactextraction.artifactkeys.ArgumentKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 9/28/16.
 */
public final class MergedDocumentRelation implements Serializable {

  final RelationKey relationKey;
  ImmutableSet<RelationMention> provenances;
  ImmutableMap<ArgumentKey, ImmutableSet<RelationMention.Filler>> argumentProvenances;
  ImmutableMap<ArgumentKey, Float> argumentConfidences;
  float confidence;


  private MergedDocumentRelation(RelationKey
      relationKey, ImmutableSet<RelationMention>
      provenances, ImmutableMap<ArgumentKey, ImmutableSet<RelationMention.Filler>>
      argumentProvenances, ImmutableMap<ArgumentKey,Float> argumentConfidences, float confidence) {
    this.relationKey = relationKey;
    this.provenances = provenances;
    this.argumentProvenances = argumentProvenances;
    this.argumentConfidences = argumentConfidences;
    this.confidence = confidence;
  }

  public static MergedDocumentRelation getMergedDocumentRelation(DocumentRelation
      documentRelation, ImmutableMap<ArgumentKey,DocumentRelationArgument>
      argKeyToArgMap, RelationKey relationKey, KBOntologyMap relationOntologyMap) throws Exception{
    checkNotNull(documentRelation);
    checkNotNull(relationKey);
    checkNotNull(documentRelation.getRelationType());
    checkNotNull(documentRelation.getProvenances());
    checkNotNull(documentRelation.getArguments());
    checkNotNull(argKeyToArgMap);
    checkArgument(relationKey.getArgumentKeys().equals(argKeyToArgMap.keySet()));

    OntType relationKBType = checkNotNull(RelationKey.getKBTypeForRelationType(documentRelation
        .getRelationType(),relationOntologyMap).orNull(),
        "RelationType: "+documentRelation.getRelationType().getType()+" not found in ontology");
    checkArgument(relationKey.getRelationKBType().equals(relationKBType));

    ImmutableMap<ArgumentKey,ImmutableSet<RelationMention.Filler>> argumentProvenances =
        getArgumentProvenancesMap(argKeyToArgMap,documentRelation.getRelationType(),relationOntologyMap);

    ImmutableMap.Builder<ArgumentKey,Float> argumentConfidences = ImmutableMap.builder();
    for(Map.Entry<ArgumentKey,DocumentRelationArgument> argKeyAndArg : argKeyToArgMap.entrySet()){
      float confidence = argKeyAndArg.getValue().getConfidence();
      argumentConfidences.put(argKeyAndArg.getKey(),confidence);
    }

    ImmutableSet<RelationMention> relationProvenancesWithKBTypes =
        getRelationProvenancesWithKBTypes(documentRelation.getProvenances(),relationKBType,
            documentRelation.getRelationType(),relationOntologyMap);

    return new MergedDocumentRelation(relationKey,
        relationProvenancesWithKBTypes, argumentProvenances, argumentConfidences.build(),
        documentRelation.getConfidence());
  }

  private static ImmutableMap<ArgumentKey,ImmutableSet<RelationMention.Filler>>
    getArgumentProvenancesMap(ImmutableMap<ArgumentKey,DocumentRelationArgument> argKeyToArgMap,
      IType relationSourceType, KBOntologyMap relationOntologyMap) throws Exception{

    ImmutableMap.Builder<ArgumentKey,ImmutableSet<RelationMention.Filler>> argumentProvenancesMap
        = ImmutableMap.builder();
    for(Map.Entry<ArgumentKey,DocumentRelationArgument> entry : argKeyToArgMap.entrySet()){
      ArgumentKey argumentKey = entry.getKey();
      DocumentRelationArgument relationArgument = entry.getValue();
      ImmutableSet<RelationMention.Filler> updatedProvenances =
          getRelationMentionFillersWithKBTypes(relationArgument.getProvenances(),
              relationSourceType,relationOntologyMap);
      argumentProvenancesMap.put(argumentKey,updatedProvenances);
    }
    return argumentProvenancesMap.build();
  }

  private static ImmutableSet<RelationMention> getRelationProvenancesWithKBTypes
    (ImmutableSet<RelationMention> provenances, OntType relationKBType, IType relationSourceType,
        KBOntologyMap relationOntologyMap)
      throws
    Exception{
    ImmutableSet.Builder updatedProvenances = ImmutableSet.builder();
    for(RelationMention relMention : provenances){
      RelationMention.Builder relMentionBuilder = RelationMention.builder(relationKBType);
      relMentionBuilder.addJustification(relMention.getJustification());
      relMentionBuilder.setConfidence(relMention.getConfidence());
      relMentionBuilder.addArguments(getRelationMentionFillersWithKBTypes(relMention.getArguments
          (),relationSourceType,relationOntologyMap));
      updatedProvenances.add(relMentionBuilder.build());
    }
    return updatedProvenances.build();
  }

  private static ImmutableSet<RelationMention.Filler> getRelationMentionFillersWithKBTypes
      (ImmutableSet<RelationMention.Filler> relationMentionArgs, IType relationSourceType,
          KBOntologyMap relationOntologyMap)
      throws
      Exception{
    ImmutableSet.Builder updatedMentionArgs = ImmutableSet.builder();
    for(RelationMention.Filler mentionArg : relationMentionArgs){
      OntType mentionArgKBRole = checkNotNull(RelationKey.getKBRoleForType(relationSourceType,new Type(mentionArg
              .getArgumentType()),relationOntologyMap).orNull(),
          "Role: "+mentionArg.getArgumentType()+" not found in ontology");
      RelationMention.Filler newMentionArg = null;
      if(mentionArg.asEntityMention().isPresent()){
        newMentionArg = RelationMention.Filler.fromEntityMention(mentionArg.asEntityMention()
            .get(), mentionArgKBRole,mentionArg.getConfidence());
      }else if(mentionArg.asNumberPhrase().isPresent()){
        newMentionArg = RelationMention.Filler.fromNumberPhrase(mentionArg.asNumberPhrase()
            .get(), mentionArgKBRole,mentionArg.getConfidence());
      }else if(mentionArg.asTimePhrase().isPresent()){
        newMentionArg = RelationMention.Filler.fromTimePhrase(mentionArg.asTimePhrase()
            .get(), mentionArgKBRole,mentionArg.getConfidence());
      }else if(mentionArg.asGenericChunk().isPresent()){
        newMentionArg = RelationMention.Filler.fromGenericChunk(mentionArg.asGenericChunk()
            .get(), mentionArgKBRole,mentionArg.getConfidence());
      }
      updatedMentionArgs.add(newMentionArg);
    }
    return updatedMentionArgs.build();
  }

  public void update(DocumentRelation documentRelation, ImmutableMap<ArgumentKey,
      DocumentRelationArgument> argKeyToArgMap, KBOntologyMap relationOntologyMap) throws Exception{
    checkNotNull(documentRelation);
    checkNotNull(documentRelation.getProvenances());
    checkNotNull(argKeyToArgMap);

    OntType relationKBType = checkNotNull(RelationKey.getKBTypeForRelationType(documentRelation
            .getRelationType(),relationOntologyMap).orNull(),
        "RelationType: "+documentRelation.getRelationType().getType()+" not found in ontology");
    checkArgument(relationKBType.equals(this.relationKBType()));
    checkArgument(relationKey.getArgumentKeys().equals(argKeyToArgMap.keySet()));

    int currentRelWeight = this.provenances.size();
    float currentRelConfidence = this.confidence;
    float updateRelConfidence = documentRelation.getConfidence();
    int updateRelWeight = documentRelation.getProvenances().size();
    this.updateProvenances(documentRelation.getProvenances(),documentRelation.getRelationType(),
        relationOntologyMap);
    this.confidence =
        (currentRelWeight*currentRelConfidence + updateRelWeight*updateRelConfidence)/this.provenances.size();

    Map<ArgumentKey,Float> updatedArgConfidences = new HashMap<>();
    for(Map.Entry<ArgumentKey,DocumentRelationArgument> argKeyAndArg :
        argKeyToArgMap.entrySet()){
      int currentArgWeight = this.argumentProvenances.get(argKeyAndArg.getKey()).size();
      int updateArgWeight = argKeyAndArg.getValue().getProvenances().size();
      if(currentArgWeight==0||updateArgWeight==0){
        currentArgWeight = currentRelWeight;
        updateArgWeight = updateRelWeight;
      }
      float currentArgConfidence = this.argumentConfidences.get(argKeyAndArg.getKey());
      float updateArgConfidence = argKeyAndArg.getValue().getConfidence();
      float updatedArgConfidence = (currentArgWeight*currentArgConfidence +
                                        updateArgWeight*updateArgConfidence)/(currentArgWeight+updateArgWeight);
      updatedArgConfidences.put(argKeyAndArg.getKey(),updatedArgConfidence);
    }
    this.argumentConfidences = ImmutableMap.copyOf(updatedArgConfidences);

    this.updateArgumentProvenances(getArgumentProvenancesMap(argKeyToArgMap,documentRelation
        .getRelationType(),relationOntologyMap));
  }

  private void updateProvenances(ImmutableSet<RelationMention> relationProvenances, IType
      relationSourceType, KBOntologyMap relationOntologyMap) throws Exception {
    if (relationProvenances == null || relationProvenances.isEmpty()) {
      return;
    }
    Set<RelationMention> newProvenances = new HashSet<>(this.provenances);
    newProvenances.addAll(getRelationProvenancesWithKBTypes(relationProvenances,this
            .relationKBType(), relationSourceType,relationOntologyMap));
    this.provenances = ImmutableSet.copyOf(newProvenances);
  }

  private void updateArgumentProvenances(
      ImmutableMap<ArgumentKey, ImmutableSet<RelationMention.Filler>> argumentProvenances) {
    if (argumentProvenances == null || argumentProvenances.isEmpty()) {
      return;
    }
    checkArgument(relationKey.getArgumentKeys().equals(argumentProvenances.keySet()));
    Map<ArgumentKey, ImmutableSet<RelationMention.Filler>> newArgumentProvenances = new HashMap<>
        (this.argumentProvenances);
    for (Map.Entry<ArgumentKey, ImmutableSet<RelationMention.Filler>> entry : argumentProvenances
        .entrySet()) {
      ArgumentKey argumentKey = entry.getKey();
      Set<RelationMention.Filler> provenanceSet = new HashSet<>(entry.getValue());
      if (newArgumentProvenances.containsKey(argumentKey)) {
        provenanceSet.addAll(newArgumentProvenances.get(argumentKey));
      }
      newArgumentProvenances.put(argumentKey, ImmutableSet.<RelationMention.Filler>copyOf
          (provenanceSet));
    }
    this.argumentProvenances = ImmutableMap.copyOf(newArgumentProvenances);
  }

  public OntType relationKBType() {
    return this.relationKey.getRelationKBType();
  }

  public Set<RelationMention> provenances() {
    return provenances;
  }

  public ImmutableMap<ArgumentKey, ImmutableSet<RelationMention.Filler>> argumentProvenances() {
    return argumentProvenances;
  }

  public ImmutableMap<ArgumentKey,Float> argumentConfidences(){
    return argumentConfidences;
  }

  public float confidence(){
    return confidence;
  }
}
