package adept.e2e.chunkalignment;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import adept.common.Argument;
import adept.common.Chunk;
import adept.common.Coreference;
import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.Event;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Relation;
import adept.common.RelationMention;
import adept.common.Type;
import adept.e2e.driver.E2eConstants;
import adept.metadata.SourceAlgorithm;

/**
 * Default {@link Chunk} alignment implementation. To use this class, create an instance and then
 * call the {@link #align(HltContentContainer, DocumentAlignmentOptions)} method as many number of
 * times as the number of containers you want to align chunks from.<br>
 *
 * The {@link DocumentAlignmentOptions} object can be same or different for each container to be
 * aligned. You can either create a default option object as in the example below, or use various
 * public methods of DocumentAlignmentOptions to configure the option object per your requirement.
 * However, note that any pivot containers (specified by {@link DocumentAlignmentOptions#isPivot} in
 * their option), must be aligned before all non-pivot containers.<br>
 *
 * After you are done with all the containers, you can get the resultant alignment as a {@link
 * ChunkQuotientSet} object by calling {@link #buildAlignment()} method on the DefaultChunkAligner
 * object.<br>
 *
 * <code> DefaultChunkAligner chunkAligner = new DefaultChunkAligner();
 * DefaultChunkAligner.DocumentAlignmentOptions alignmentOptions = new
 * DefaultChunkAligner.DocumentAlignmentOptions(); for(HltContentContainer containerForAlignment:
 * listOfContainersToBeAligned){ chunkAligner.align(containerForAlignment,alignmentOptions); }
 * ChunkQuotientSet finalChunkAlignment = chunkAligner.buildAlignment(); </code> Only the following
 * {@link Chunk} objects can be aligned (which ones are actually aligned can be configured in the
 * DocumentAlignmentOptions object):<br> - {@link EntityMention} and {@link Chunk}s corresponding to
 * the {@link Argument}s of {@link Relation}s and {@link Event}s.
 *
 * @author mroy
 * @see DocumentAlignmentOptions
 * @see Chunk
 */
public class DefaultChunkAligner {
  // if a pivot document will be used, it must be added before the first call to
  // align or an IllegalStateException will be thrown
  // I don't know if doc alignment options apply to pivots?

  Logger log = LoggerFactory.getLogger(DefaultChunkAligner.class);
  E2eConstants.LANGUAGE language;
  ChunkQuotientSet mQuotientSet;
  boolean nonPivotAligned = false;
  private static final String UNKNOWN = "UNKNOWN";

  public DefaultChunkAligner() {
    this.language = E2eConstants.LANGUAGE.EN;//default to english language
    mQuotientSet = new ChunkQuotientSet(language);
  }

  public DefaultChunkAligner(E2eConstants.LANGUAGE language) {
    this.language = language;
    mQuotientSet = new ChunkQuotientSet(language);
  }

  /**
   * Given an {@link HltContentContainer} object and alignment options in a {@link
   * DocumentAlignmentOptions} object, align the chunks from input container with previous
   * alignments.<br> You should call this method as many number of times as the number of containers
   * you want to align chunks from.<br> The final alignment is a {@link ChunkQuotientSet} object,
   * which can be retrieved by making a call to {@link #buildAlignment()} once chunks from all the
   * containers have been aligned.<br>
   *
   * @param hltcc   Input HltContentContainer whose chunks are to be aligned with the rest of the
   *                alignment
   * @param options DocumentAlignmentOptions object encapsulating the alignment options for the
   *                input container
   * @throws UnsupportedOperationException When the alignment cannot be done for some reason.
   * @see #buildAlignment(), {@link DocumentAlignmentOptions}
   */
  public void align(HltContentContainer hltcc, DocumentAlignmentOptions options)
      throws UnsupportedOperationException {
    Collection<ChunkEquivalenceClass> equivalenceClasses = mQuotientSet.equivalenceClasses();
    //We need to keep track of when a non-pivot is aligned, because we don't
    // allow a pivot to be
    //aligned after a non-pivot
    if (!options.isPivot) {
      nonPivotAligned = true;
    }
    validateOptions(options);

    int numChunksNotAligned = 0;
    //extract all target chunks
    List<Chunk> chunks = extractChunks(hltcc, options);

    //for each chunk we check if there is a pivot chunk that this chunk matches
    for (Chunk chunk : chunks) {
      //If there are no existing alignments (equivalenceClass) and this container is a pivot
      Collection<ChunkEquivalenceClass> postAlignmentEquivalenceClasses = new ArrayList<>();
      if (equivalenceClasses.isEmpty() && options.isPivot) {
        Collection<Chunk> newPivot = new ArrayList<Chunk>();
        Collection<Chunk> newChunks = new ArrayList<Chunk>();
        newPivot.add(chunk);
        newChunks.add(chunk);
        postAlignmentEquivalenceClasses.add(ChunkEquivalenceClass.from(newChunks, newPivot));
      } else {
        Collection<ChunkEquivalenceClass> relaxedMatchEquivalenceClasses = new ArrayList<>();
        boolean chunkMatchesWithAPivot = matchChunkAgainstPivots(chunk,equivalenceClasses,
            postAlignmentEquivalenceClasses,relaxedMatchEquivalenceClasses,options);
        if(!chunkMatchesWithAPivot){
          if(options.useRelaxedAlignmentRule &&!relaxedMatchEquivalenceClasses.isEmpty()) {
            chunkMatchesWithAPivot=true;
            Collection<ChunkEquivalenceClass> finalAlignmentEquivalenceClasses = new ArrayList<>();
            for(ChunkEquivalenceClass ceq : postAlignmentEquivalenceClasses){
              if(relaxedMatchEquivalenceClasses.contains(ceq)){
                List<Chunk> newPivots = new ArrayList(ceq.pivots());
                List<Chunk> newChunks = new ArrayList<>(ceq.chunks());
                newChunks.add(chunk);
                ceq = ChunkEquivalenceClass.from(newChunks,newPivots);
              }
              finalAlignmentEquivalenceClasses.add(ceq);
            }
            postAlignmentEquivalenceClasses = finalAlignmentEquivalenceClasses;
          }
        }
        if(!chunkMatchesWithAPivot){
             numChunksNotAligned++;   //else ignore the non-pivot chunk that was not aligned to a pivot
        }
      }
      //We want to consider the newly added classes as possible pivots for the next chunk
      equivalenceClasses = postAlignmentEquivalenceClasses;
    }
    mQuotientSet = new ChunkQuotientSet(language, equivalenceClasses);
    log.info("Total no. of chunks NOT aligned={}", numChunksNotAligned);
  }

  private boolean matchChunkAgainstPivots(Chunk chunk, Collection<ChunkEquivalenceClass> equivalenceClasses,
      Collection<ChunkEquivalenceClass> postAlignmentEquivalenceClasses, Collection<ChunkEquivalenceClass> relaxedMatchEquivalenceClasses,
      DocumentAlignmentOptions options){
    boolean chunkAlignedWithAPivotChunk = false;
    for (ChunkEquivalenceClass eqClass : equivalenceClasses) {
      Collection<Chunk> eqPivots = eqClass.pivots();
      Collection<Chunk> eqChunks = eqClass.chunks();
      Collection<Chunk> newPivots = new ArrayList<Chunk>(eqClass.pivots());
      Collection<Chunk> newChunks = new ArrayList<Chunk>(eqClass.chunks());
      boolean regularMatch = false;
      boolean relaxedMatch = false;
      for (Chunk pivotChunk : eqPivots) {//for all pivot chunks in this equivalence class
        //see if the chunk matches the pivotChunk
        if(mQuotientSet.areEquivalent(pivotChunk, chunk)) {
          regularMatch = true;
        }else if(mQuotientSet.areEquivalentByContainmentInPivot(pivotChunk,chunk)) {
          relaxedMatch = true;
        }
        if( (regularMatch||relaxedMatch) && (pivotChunk instanceof EntityMention && chunk instanceof EntityMention) ){
          String pivotType = ((EntityMention) pivotChunk).getMentionType().getType();
          String chunkType = ((EntityMention) chunk).getMentionType().getType();
          if (!options.allowCrossTypeAlignment||relaxedMatch) {//don't allow cross mention-type alignment for relaxed matches
              if (!pivotType.equals(UNKNOWN)&&!chunkType.equals(UNKNOWN)&&!pivotType.equals(chunkType)) {
                regularMatch = false;
                relaxedMatch = false;
              }
          }
//          else if(relaxedMatch){
//            //even if cross-type-alignment is allowed, don't allow the following alignments
//            if(pivotType.equals("NAME")||chunkType.equals("NAME")&&!pivotType.equals(chunkType)){//don't allow NAME to non-NAME mention match
//              relaxedMatch=false;
//            }
//          }
        }
        if (regularMatch) {
          relaxedMatch = false;
          break;
        }
        if(relaxedMatch && (pivotChunk instanceof EntityMention && chunk instanceof EntityMention)){//dont' allow cross entity type alignments for relaxed matches either
          String pivotEntityType = ((EntityMention) pivotChunk).getEntityType().getType();
          String chunkEntityType = ((EntityMention) chunk).getEntityType().getType();
          if (!pivotEntityType.equals(UNKNOWN)&&!chunkEntityType.equals(UNKNOWN)
              &&!chunkEntityType.equals("OTH")&&!chunkEntityType.equals("UNK")
              &&!chunkEntityType.equals("OTHER")
              &&!pivotEntityType.equals(chunkEntityType)) {
            relaxedMatch = false;
          }
        }
      }
      if (regularMatch) {//One of the pivots matched with
        //this chunk, add this chunk to this equivalence class
        chunkAlignedWithAPivotChunk = true;
        if (options.isPivot) {
          newPivots.add(chunk);
        }
        newChunks.add(chunk);
        postAlignmentEquivalenceClasses.add(ChunkEquivalenceClass.from(newChunks, newPivots));
      }else {
        //else retain the current equivalence class before moving to the next one
        postAlignmentEquivalenceClasses.add(ChunkEquivalenceClass.from(eqChunks, eqPivots));
      }
      if(relaxedMatch){
        relaxedMatchEquivalenceClasses.add(eqClass);
      }
    }
    if(!chunkAlignedWithAPivotChunk && (!options.useRelaxedAlignmentRule || relaxedMatchEquivalenceClasses.isEmpty())){
      if (options.isPivot) {//if this is a pivot chunk, create a new equivalence class for it
        Collection<Chunk> newPivot = new ArrayList<Chunk>();
        newPivot.add(chunk);
        postAlignmentEquivalenceClasses.add(ChunkEquivalenceClass.from(newPivot, newPivot));
      }
    }
    return chunkAlignedWithAPivotChunk;
  }

  public ChunkQuotientSet buildAlignment() {
    return mQuotientSet;
  }


  List<Chunk> extractChunks(HltContentContainer container, DocumentAlignmentOptions options) {
    //When aligning documents, each type of chunk that is to be aligned is specified by the options object.
    //This method will extract all the chunks that belong to those classes.
    List<Chunk> chunks = new ArrayList<Chunk>();
    chunks.addAll(extractEntityChunks(container, options));
    int currentChunksSize = chunks.size();
    log.info("No. of entity-mention chunks={}",  currentChunksSize);
    chunks.addAll(extractRelationArgumentChunks(container, options));
    log.info("No. of relation-arg chunks={}", (chunks.size() - currentChunksSize));
    currentChunksSize = chunks.size();
    chunks.addAll(extractEventArgumentChunks(container, options));
    log.info("No. of event-arg chunks={}", (chunks.size() - currentChunksSize));
//        chunks = filterChunkClasses(chunks, options);
    //if any chunk has null mention-type, set it to UNKNOWN
    return FluentIterable.from(chunks).transform(new Function<Chunk, Chunk>() {
      public Chunk apply(Chunk c){
        if(c instanceof EntityMention && ((EntityMention) c).getMentionType()==null){
          ((EntityMention)c).setMentionType(new Type(UNKNOWN));
        }
        return c;
      }
    }).toList();
  }

  /*Certain combinations of options are not allowed, throw exceptions if an illegal combination is found.*/
  private void validateOptions(DocumentAlignmentOptions options) {

    //Do not allow a pivot to be added after a non-pivot
    if (options.isPivot && nonPivotAligned) {
      throw new UnsupportedOperationException(
          "Cannot add pivot after non-pivots have been aligned.");
    }

    //Options must specify which chunks we should be aligning.
    if (!options.getAllEntityMentions && !options.getAllRelationArguments
        && !options.getAllEventArguments) {
      throw new UnsupportedOperationException(
          "Cannot call align without specifying chunk classes to align.");
    }

    //Options must specify which chunk classes to allow.
    if (options.allowedChunkClasses == null || options.allowedChunkClasses.isEmpty()) {
      throw new UnsupportedOperationException(
          "Cannot call align without specifying allowable chunk classes.");
    }
    if (!options.isPivot && mQuotientSet.equivalenceClasses().size() == 0) {
      throw new UnsupportedOperationException("Cannot align non-pivot before aligning pivots.");
    }

    //If we aren't aligning relations, there is no need to set options about relations, user probably made a mistake
    if (!options.getAllRelationArguments) {
      if (options.allowedRelationTypes != null || options.allowedRelationArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Should not set allowableRelationTypes or allowableRelationArgumentTypes if not aligning relation arguments.");
      }
      if (options.unallowedRelationTypes != null
          || options.unallowedRelationArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Should not set disallowableRelationTypes or disallowableRelationArgumentTypes if not aligning relation arguments.");
      }
    }

    //If we aren't aligning events, there is no need to set options about events, user probably made a mistake
    if (!options.getAllEventArguments) {
      if (options.allowedEventTypes != null || options.allowedEventArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Should not set allowableEventTypes or allowableEventArgumentTypes if not aligning event arguments.");
      }
      if (options.unallowedEventTypes != null || options.unallowedEventArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Should not set disallowableEventTypes or disallowableEventArgumentTypes if not aligning event arguments.");
      }
    }

    //If we are aligning EntityMentions but are not allowing them as chunkClasses this is an error
    if (options.getAllEntityMentions) {
      if (!options.allowedChunkClasses.contains(EntityMention.class)) {
        throw new UnsupportedOperationException(
            "Must specify EntityMention as an allowable class if aligning EntityMentions.");
      }
    }
  }

  /*Pull out entityMentions from the container if requested by the options.*/
  private List<Chunk> extractEntityChunks(HltContentContainer container,
      DocumentAlignmentOptions options) {
    List<Chunk> newChunks = new ArrayList<Chunk>();

    if (options.allowedChunkClasses == null || options.allowedChunkClasses.isEmpty()) {
      return newChunks;
    }

    if (options.getAllEntityMentions) {
      List<EntityMention> mentionsFromContainer = new ArrayList<>();
      //Find mentions from Coreferences
      if (container.getCoreferences() != null) {
        for (Coreference coreference : container.getCoreferences()) {
          if (coreference.getResolvedMentions() == null) {
            continue;
          }
          //Include resolvedMentions from this coreference
          for (EntityMention entityMention : coreference.getResolvedMentions()) {
            if (options.algorithmName != null) {
              entityMention.setAlgorithmName(options.algorithmName);
            }
            if (options.sourceAlgorithm != null) {
              entityMention
                  .setSourceAlgorithm(options.sourceAlgorithm);
            }
            mentionsFromContainer.add(entityMention);
          }
          //Include all canonicalMentions from this coreference
          for (Entity entity : coreference.getEntities()){
              EntityMention canonicalMention = entity.getCanonicalMention();
              if (canonicalMention != null) {
                if (options.algorithmName != null) {
                  canonicalMention.setAlgorithmName(options.algorithmName);
                }
                if (options.sourceAlgorithm != null) {
                  canonicalMention
                          .setSourceAlgorithm(options.sourceAlgorithm);
                }
                mentionsFromContainer.add(canonicalMention);
              }
          }
        }
      }
      if ((mentionsFromContainer.isEmpty() || !options.isPivot) && container.getEntityMentions() != null) {//for pivots, the entitymentions should come from Coreference.resolvedMentions
        for (EntityMention entityMention : container.getEntityMentions()) {
          if (options.algorithmName != null) {
            entityMention.setAlgorithmName(options.algorithmName);
          }
          if (options.sourceAlgorithm != null) {
            entityMention
                .setSourceAlgorithm(options.sourceAlgorithm);
          }
          mentionsFromContainer.add(entityMention);
          ;
        }
      }
      List<? extends Chunk> retrievedChunks = new ArrayList<>(mentionsFromContainer);
      if (!retrievedChunks.isEmpty()) {
        for (Chunk c : retrievedChunks) {
          String entityType = ((EntityMention) c).getEntityType().getType();
          String mentionType = ((EntityMention) c).getMentionType() == null ? UNKNOWN :
                               ((EntityMention)c).getMentionType().getType();

          boolean classAllowed = options.allowedChunkClasses.contains(c.getClass());
          boolean entTypeAllowed = true;
          boolean menTypeAllowed = true;

          //We want to check the base type of entities and mentions, ie PER.Individual matches PER

          String entityBaseType =
              entityType.substring(0, entityType.contains(".") ? entityType.lastIndexOf(".") :
                                      entityType.length());
          //Filter out entity types
          if (options.allowedEntityTypes != null) {
            if (options.allowedEntityTypes.contains(entityType)) {
              entTypeAllowed = true;
            } else if (options.allowedEntityTypes.contains(entityBaseType)) {
              entTypeAllowed = true;
            } else {
              entTypeAllowed = false;
            }
          } else if (options.unallowedEntityTypes != null) {
            if (options.unallowedEntityTypes.contains(entityType)) {
              entTypeAllowed = false;
            } else if (options.unallowedEntityTypes.contains(entityBaseType)) {
              entTypeAllowed = false;
            }
          }

          //Filter out mention types
          String mentionBaseType =
              mentionType.substring(0, mentionType.contains(".") ? mentionType.lastIndexOf(".") :
                                       mentionType.length());
          if (options.allowedMentionTypes != null) {
            if (options.allowedMentionTypes.contains(mentionType)) {
              menTypeAllowed = true;
            } else if (options.allowedMentionTypes.contains(mentionBaseType)) {
              menTypeAllowed = true;
            } else {
              menTypeAllowed = false;
            }
          } else if (options.unallowedMentionTypes != null) {
            if (options.unallowedMentionTypes.contains(mentionType)) {
              menTypeAllowed = false;
            } else if (options.unallowedMentionTypes.contains(mentionBaseType)) {
              menTypeAllowed = false;
            }
          }

          if (classAllowed && entTypeAllowed && menTypeAllowed) {
            newChunks.add(c);
          }
        }
      }
    }
    return newChunks;
  }

  /*Pull out all the Chunks that are arguments of Relations if requested by options.*/
  private List<Chunk> extractRelationArgumentChunks(HltContentContainer container,
      DocumentAlignmentOptions options) {

    List<Chunk> newChunks = new ArrayList<Chunk>();

    if (options.allowedChunkClasses == null || options.allowedChunkClasses.isEmpty()) {
      return newChunks;
    }

    if (options.getAllRelationArguments) {
      List<Chunk> relArgChunks = new ArrayList<>();
      //First retrieve rel-arg chunks from relations
      if (container.getRelations() != null) {
        for (Relation relation : container.getRelations()) {
          String relType = relation.getRelationType();
          boolean relTypeAllowed = true;
          boolean arg1Allowed;
          //Filter relation types
          if (options.allowedRelationTypes != null && !options.allowedRelationTypes
              .contains(relType)) {
            relTypeAllowed = false;
          } else if (options.unallowedRelationTypes != null && options.unallowedRelationTypes
              .contains(relType)) {
            relTypeAllowed = false;
          }
          if (!relTypeAllowed) {
            continue;
          }
          //Filter argument types
          for (Argument argument : relation.getArguments()) {
            arg1Allowed = true;
            String argType = argument.getArgumentType();
//                    Chunk arg = argument.getArgumentDistribution().get(0).getL();
            Chunk arg = argument.getBestArgument();
            if (!options.allowedChunkClasses.contains(arg.getClass())) {
              arg1Allowed = false;
            }
            //Filter on Argument types
            if (options.allowedRelationArgumentTypes != null) {
              if (!options.allowedRelationArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (options.unallowedRelationArgumentTypes != null) {
              if (options.unallowedRelationArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (relTypeAllowed) {
              if (arg1Allowed) {
                if (options.algorithmName != null) {
                  arg.setAlgorithmName(options.algorithmName);
                }
                if (options.sourceAlgorithm != null) {
                  arg.setSourceAlgorithm(options.sourceAlgorithm);
                }
                relArgChunks.add(arg);
              }
            }
          }
        }
      }
      //now get rel-arg chunks from DocumentRelations
      if (container.getDocumentRelations() != null) {
        for (DocumentRelation relation : container.getDocumentRelations()) {
          String relType = relation.getRelationType().getType();
          boolean relTypeAllowed = true;
          boolean arg1Allowed;
          //Filter relation types
          if (options.allowedRelationTypes != null && !options.allowedRelationTypes
              .contains(relType)) {
            relTypeAllowed = false;
          } else if (options.unallowedRelationTypes != null && options.unallowedRelationTypes
              .contains(relType)) {
            relTypeAllowed = false;
          }
          if (!relTypeAllowed) {
            continue;
          }
          //Filter argument types
          for (DocumentRelationArgument argument : relation.getArguments()) {
            String argType = argument.getRole().getType();
            arg1Allowed = true;
            //Filter on Argument types
            if (options.allowedRelationArgumentTypes != null) {
              if (!options.allowedRelationArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (options.unallowedRelationArgumentTypes != null) {
              if (options.unallowedRelationArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (!arg1Allowed) {
              continue;
            }
            //first try to get chunks from argument's filler
            DocumentRelationArgument.Filler argFiller = argument.getFiller();
            Chunk arg = null;
            Optional<Entity> entity = argFiller.asEntity();
            if (entity.isPresent()) {
              arg = entity.get().getCanonicalMention();
            }

            if (arg == null || !options.allowedChunkClasses.contains(arg.getClass())) {
              arg1Allowed = false;
            }
            if (relTypeAllowed) {
              if (arg1Allowed) {
                if (options.algorithmName != null) {
                  arg.setAlgorithmName(options.algorithmName);
                }
                if (options.sourceAlgorithm != null) {
                  arg.setSourceAlgorithm(options.sourceAlgorithm);
                }
                relArgChunks.add(arg);
              }
            }
            //then try to get chunks from argument's provenances
            for (RelationMention.Filler filler : argument.getProvenances()) {
              arg1Allowed = true;
              Optional<Chunk> optionalArg = filler.asChunk();
              if (!optionalArg.isPresent()) {
                continue;
              }
              arg = optionalArg.get();
              if (!options.allowedChunkClasses.contains(arg.getClass())) {
                arg1Allowed = false;
              }
              if (relTypeAllowed) {
                if (arg1Allowed) {
                  if (options.algorithmName != null) {
                    arg.setAlgorithmName(options.algorithmName);
                  }
                  if (options.sourceAlgorithm != null) {
                    arg.setSourceAlgorithm(options.sourceAlgorithm);
                  }
                  relArgChunks.add(arg);
                }
              }
            }
          }
        }
      }
      newChunks = relArgChunks;
    }
    return newChunks;
  }

  /*Pull out all the Chunks that are arguments of Events if requested by options.*/
  private List<Chunk> extractEventArgumentChunks(HltContentContainer container,
      DocumentAlignmentOptions options) {

    List<Chunk> newChunks = new ArrayList<Chunk>();

    if (options.allowedChunkClasses == null || options.allowedChunkClasses.isEmpty()) {
      return newChunks;
    }
    //first, getting event-arg-chunks from events

    if (options.getAllEventArguments) {
      List<Chunk> eventArgChunks = new ArrayList<>();
      if (container.getEvents() != null) {
        for (Event event : container.getEvents()) {
          String eventType = event.getType();
          boolean eventTypeAllowed = true;
          boolean arg1Allowed;
          //Filter event types
          if (options.allowedEventTypes != null && !options.allowedEventTypes
              .contains(eventType)) {
            eventTypeAllowed = false;
          } else if (options.unallowedEventTypes != null && options.unallowedEventTypes
              .contains(eventType)) {
            eventTypeAllowed = false;
          }
          if (!eventTypeAllowed) {
            continue;
          }
          //Filter argument types
          for (Argument argument : event.getArguments()) {
            arg1Allowed = true;
            String argType = argument.getArgumentType();
            Chunk arg = argument.getBestArgument();
            if (!options.allowedChunkClasses.contains(arg.getClass())) {
              arg1Allowed = false;
            }
            //Filter on Argument types
            if (options.allowedEventArgumentTypes != null) {
              if (!options.allowedEventArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (options.unallowedEventArgumentTypes != null) {
              if (options.unallowedEventArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (eventTypeAllowed) {
              if (arg1Allowed) {
                if (options.algorithmName != null) {
                  arg.setAlgorithmName(options.algorithmName);
                }
                if (options.sourceAlgorithm != null) {
                  arg.setSourceAlgorithm(options.sourceAlgorithm);
                }
                eventArgChunks.add(arg);
              }
            }
          }
        }
      }
      //now extracting arg chunks from DocumentEvents
      if(container.getDocumentEvents()!=null) {
        for (DocumentEvent event : container.getDocumentEvents()) {
          String eventType = event.getEventType().getType();
          boolean eventTypeAllowed = true;
          boolean arg1Allowed;
          //Filter event types
          if (options.allowedEventTypes != null && !options.allowedEventTypes
              .contains(eventType)) {
            eventTypeAllowed = false;
          } else if (options.unallowedEventTypes != null && options.unallowedEventTypes
              .contains(eventType)) {
            eventTypeAllowed = false;
          }
          if (!eventTypeAllowed) {
            continue;
          }
          //Filter argument types
          for (DocumentEventArgument argument : event.getArguments()) {
            String argType = argument.getRole().getType();
            arg1Allowed = true;
            //Filter on Argument types
            if (options.allowedEventArgumentTypes != null) {
              if (!options.allowedEventArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (options.unallowedEventArgumentTypes != null) {
              if (options.unallowedEventArgumentTypes.contains(argType)) {
                arg1Allowed = false;
              }
            }
            if (!arg1Allowed) {
              continue;
            }
            //first get chunk from EventArgument filler
            DocumentEventArgument.Filler argFiller = argument.getFiller();
            Chunk arg = null;
            Optional<Entity> entity = argFiller.asEntity();
            if (entity.isPresent()) {
              arg = entity.get().getCanonicalMention();
            }
            if (arg == null || !options.allowedChunkClasses.contains(arg.getClass())) {
              arg1Allowed = false;
            }
            if (eventTypeAllowed) {
              if (arg1Allowed) {
                if (options.algorithmName != null) {
                  arg.setAlgorithmName(options.algorithmName);
                }
                if (options.sourceAlgorithm != null) {
                  arg.setSourceAlgorithm(options.sourceAlgorithm);
                }
                eventArgChunks.add(arg);
              }
            }
            //then get chunks from EventArgument provenances
            for (DocumentEventArgument.Provenance provenance : argument.getProvenances()) {
              arg1Allowed = true;
              arg = provenance.getEventMentionArgument().getFiller();
              if (!options.allowedChunkClasses.contains(arg.getClass())) {
                arg1Allowed = false;
              }
              if (eventTypeAllowed) {
                if (arg1Allowed) {
                  if (options.algorithmName != null) {
                    arg.setAlgorithmName(options.algorithmName);
                  }
                  if (options.sourceAlgorithm != null) {
                    arg.setSourceAlgorithm(options.sourceAlgorithm);
                  }
                  eventArgChunks.add(arg);
                }
              }
            }
          }
        }
      }

      newChunks = eventArgChunks;
    }
    return newChunks;
  }

  /**
   * Class encapsulating various alignment options. Use various public methods on an instance of
   * this class to configure the alignment options.
   *
   * @author mroy
   */
  public final class DocumentAlignmentOptions {

    boolean isPivot = false;
    boolean allowCrossTypeAlignment = false;
    //
    boolean getAllEntityMentions = false;
    boolean getAllRelationArguments = false;
    boolean getAllEventArguments = false;

    boolean useRelaxedAlignmentRule = false;

    //If these are set in the options, they're set to the container artifacts being aligned
    String algorithmName;
    SourceAlgorithm sourceAlgorithm;

    //
    List<Class> allowedChunkClasses;
    //
    List<String> allowedEntityTypes;
    List<String> unallowedEntityTypes;
    //
    List<String> allowedMentionTypes;
    List<String> unallowedMentionTypes;
    //
    List<String> allowedRelationTypes;
    List<String> unallowedRelationTypes;
    //
    List<String> allowedRelationArgumentTypes;
    List<String> unallowedRelationArgumentTypes;
    //
    List<String> allowedEventTypes;
    List<String> unallowedEventTypes;
    //
    List<String> allowedEventArgumentTypes;
    List<String> unallowedEventArgumentTypes;


    /**
     * Align all entity mentions from the document
     *
     * @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions alignAllEntityMentions() {
      this.getAllEntityMentions = true;
      return this;
    }

    /**
     * Align the arguments to relations
     *
     * @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions alignAllRelationArguments() {
      this.getAllRelationArguments = true;
      return this;
    }

    public DocumentAlignmentOptions algorithmName(String algorithmName) {
      this.algorithmName = algorithmName;
      return this;
    }

    public DocumentAlignmentOptions sourceAlgorithm(SourceAlgorithm sourceAlgorithm) {
      this.sourceAlgorithm = sourceAlgorithm;
      return this;
    }


    /**
     * Align the arguments to events
     *
     * @return @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions alignAllEventArguments() {
      this.getAllEventArguments = true;
      return this;
    }

    /**
     * Configure allowable Chunk-type classes for alignment. Currently only {@link EntityMention}
     * and {@link Chunk} are allowed.
     *
     * @param targets List of Chunk-type Classes to be allowed
     * @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions allowableChunkClasses(List<Class> targets) {

      if (targets == null) {
        return this;
      }

      //this function or the unallowable version is only allowed to be called once
      if (allowedChunkClasses != null) {
        throw new UnsupportedOperationException("Cannot call allowableChunkClasses twice.");
      }

      this.allowedChunkClasses = new ArrayList<Class>();
      for (Class c : targets) {
        if (c.equals(EntityMention.class) || c.equals(Chunk.class)) {
          this.allowedChunkClasses.add(c);
        } else {
          throw new IllegalArgumentException("Chunk subclass not supported as allowable class.");
        }
      }

      return this;
    }

    /**
     * Configure allowable Entity types for alignment. Only EntityMentions whose Entities coform to
     * the allowed types will be considered for alignment.
     *
     * @param types List of {@link IType} object as entity types to be allowed
     * @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions allowableEntityTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedEntityTypes != null) {
        throw new UnsupportedOperationException("Cannot call allowableEntityTypes twice.");
      } else if (unallowedEntityTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableEntityTypes and allowableEntityTypes.");
      }
      allowedEntityTypes = new ArrayList<String>();
      for (IType type : types) {
        allowedEntityTypes.add(type.getType());
      }
      return this;
    }

    /**
     * Configure Entity types not allowed for alignment. EntityMentions whose Entities have a
     * disallowed type will not be considered for alignment.
     *
     * @param types List of {@link IType} object as entity types to be disallowed
     * @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions disallowableEntityTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (unallowedEntityTypes != null) {
        throw new UnsupportedOperationException("Cannot call disallowableEntityTypes twice.");
      } else if (allowedEntityTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableEntityTypes and disallowableEntityTypes.");
      }
      unallowedEntityTypes = new ArrayList<String>();
      for (IType type : types) {
        unallowedEntityTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions allowableMentionTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedMentionTypes != null) {
        throw new UnsupportedOperationException("Cannot call allowableMentionTypes twice.");
      } else if (unallowedMentionTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableMentionTypes and allowableMentionTypes.");
      }

      allowedMentionTypes = new ArrayList<String>();
      for (IType type : types) {
        allowedMentionTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions disallowableMentionTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (unallowedMentionTypes != null) {
        throw new UnsupportedOperationException("Cannot call disallowableMentionTypes twice.");
      } else if (allowedMentionTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableMentionTypes and disallowableMentionTypes.");
      }

      unallowedMentionTypes = new ArrayList<String>();
      for (IType type : types) {
        unallowedMentionTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions allowableRelationTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedRelationTypes != null) {
        throw new UnsupportedOperationException("Cannot call allowableRelationTypes twice.");
      } else if (unallowedRelationTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableRelationTypes and allowableRelationTypes.");
      }

      allowedRelationTypes = new ArrayList<String>();
      for (IType type : types) {
        allowedRelationTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions disallowableRelationTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedRelationTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableRelationTypes and disallowableRelationTypes.");
      } else if (unallowedRelationTypes != null) {
        throw new UnsupportedOperationException("Cannot call disallowableRelationTypes twice.");
      }

      unallowedRelationTypes = new ArrayList<String>();
      for (IType type : types) {
        unallowedRelationTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions allowableRelationArgumentTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedRelationArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableRelationArgumentTypes twice.");
      } else if (unallowedRelationArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableRelationArgumentTypes and allowableRelationArgumentTypes.");
      }

      allowedRelationArgumentTypes = new ArrayList<String>();
      for (IType type : types) {
        allowedRelationArgumentTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions disallowableRelationArgumentTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (unallowedRelationArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableRelationArgumentTypes twice.");
      } else if (allowedRelationArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableRelationArgumentTypes and disallowableRelationArgumentTypes.");
      }

      unallowedRelationArgumentTypes = new ArrayList<String>();
      for (IType type : types) {
        unallowedRelationArgumentTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions allowableEventTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedEventTypes != null) {
        throw new UnsupportedOperationException("Cannot call allowableEventTypes twice.");
      } else if (unallowedEventTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableEventTypes and allowableEventTypes.");
      }

      allowedEventTypes = new ArrayList<String>();
      for (IType type : types) {
        allowedEventTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions disallowableEventTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedEventTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableEventTypes and disallowableEventTypes.");
      } else if (unallowedEventTypes != null) {
        throw new UnsupportedOperationException("Cannot call disallowableEventTypes twice.");
      }

      unallowedEventTypes = new ArrayList<String>();
      for (IType type : types) {
        unallowedEventTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions allowableEventArgumentTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (allowedEventArgumentTypes != null) {
        throw new UnsupportedOperationException("Cannot call allowableEventArgumentTypes twice.");
      } else if (unallowedEventArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableEventArgumentTypes and allowableEventArgumentTypes.");
      }

      allowedEventArgumentTypes = new ArrayList<String>();
      for (IType type : types) {
        allowedEventArgumentTypes.add(type.getType());
      }
      return this;
    }

    public DocumentAlignmentOptions disallowableEventArgumentTypes(List<IType> types) {
      if (types == null) {
        return this;
      }

      if (unallowedEventArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call disallowableEventArgumentTypes twice.");
      } else if (allowedEventArgumentTypes != null) {
        throw new UnsupportedOperationException(
            "Cannot call allowableEventArgumentTypes and disallowableEventArgumentTypes.");
      }

      unallowedEventArgumentTypes = new ArrayList<String>();
      for (IType type : types) {
        unallowedEventArgumentTypes.add(type.getType());
      }
      return this;
    }

    /**
     * Call this method to configure that the container used with this DocumentAlignmentOptions
     * object is considered as a pivot.
     *
     * @return this DocumentAligmentOptions object
     */
    public DocumentAlignmentOptions useAsPivot() {
      if(this.useRelaxedAlignmentRule){
        throw new UnsupportedOperationException("Cannot use Relaxed Alignment Rule for pivots");
      }
      this.isPivot = true;
      return this;
    }

    /**
     * Allow EntityMention chunks of different types to be aligned to each other (e.g. a PER
     * EntityMention aligning to an ORG EntityMention), if their spans match.
     *
     * @return this DocumentAlignmentOptions object
     */
    public DocumentAlignmentOptions allowCrossType() {
      this.allowCrossTypeAlignment = true;
      return this;
    }

    public DocumentAlignmentOptions useRelaxedAlignmentRule(){
      if(this.isPivot){
        throw new UnsupportedOperationException("Relaxed Alignment Rule is for non-pivot alignment");
      }
      this.useRelaxedAlignmentRule = true;
      return this;
    }

  }
}
