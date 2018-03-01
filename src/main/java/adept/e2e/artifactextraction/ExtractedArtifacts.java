package adept.e2e.artifactextraction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.Relation;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.artifactextraction.mergedartifacts.ItemWithProvenances;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentEvent;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentRelation;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 10/12/16.
 */
public final class ExtractedArtifacts implements Serializable {

  private static final long serialVersionUID = -5432944137118823701L;
  final ImmutableMap<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergedEntities;
  final ExtractedArguments extractedArguments;
  final ImmutableMap<RelationKey, MergedDocumentRelation> mergedRelations;
  final ImmutableMap<EventKey, MergedDocumentEvent> mergedEvents;
  final ImmutableList<Relation> openIERelations;

  private ExtractedArtifacts(ImmutableMap<EntityKey, ItemWithProvenances<Entity,EntityMention>>
      mergedEntities,
      ExtractedArguments extractedArguments,
      ImmutableMap<RelationKey, MergedDocumentRelation> mergedRelations,
      ImmutableMap<EventKey, MergedDocumentEvent> mergedEvents, ImmutableList<Relation>
      openIERelations) {
    this.mergedEntities = mergedEntities;
    this.extractedArguments = extractedArguments;
    this.mergedRelations = mergedRelations;
    this.mergedEvents = mergedEvents;
    this.openIERelations = openIERelations;
  }

  public static ExtractedArtifacts create(
      ImmutableMap<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergedEntities,
      final ExtractedArguments extractedArguments,
      ImmutableMap<RelationKey, MergedDocumentRelation> mergedRelations,
      ImmutableMap<EventKey, MergedDocumentEvent> mergedEvents, ImmutableList<Relation>
      openIERelations) {
    checkNotNull(mergedEntities);
    checkNotNull(extractedArguments);
    checkNotNull(mergedRelations);
    checkNotNull(mergedEvents);
    checkNotNull(openIERelations);
    return new ExtractedArtifacts(mergedEntities, extractedArguments, mergedRelations, mergedEvents, openIERelations);
  }

  public ImmutableMap<EntityKey, ItemWithProvenances<Entity,EntityMention>> mergedEntities() {
    return mergedEntities;
  }

  public final ExtractedArguments extractedArguments() {
    return extractedArguments;
  }

  public ImmutableMap<RelationKey, MergedDocumentRelation> mergedRelations() {
    return mergedRelations;
  }

  public ImmutableMap<EventKey, MergedDocumentEvent> mergedEvents() {
    return mergedEvents;
  }

  public ImmutableList<Relation> openIERelations(){
    return openIERelations;
  }

}
