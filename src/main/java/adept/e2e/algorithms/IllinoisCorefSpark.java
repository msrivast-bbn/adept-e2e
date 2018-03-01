package adept.e2e.algorithms;

import com.google.common.collect.HashBiMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import adept.common.Chunk;
import adept.common.ContentType;
import adept.common.Coreference;
import adept.common.Document;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Sentence;
import adept.common.TokenStream;
import adept.common.TokenizerType;
import adept.e2e.chunkalignment.ChunkEquivalenceClass;
import adept.e2e.chunkalignment.ChunkQuotientSet;
import adept.e2e.chunkalignment.DefaultChunkAligner;
import adept.e2e.driver.E2eUtil;
import adept.e2e.exceptions.E2eException;
import adept.e2e.exceptions.ModuleActivationException;
import adept.metadata.SourceAlgorithm;
import adept.module.AdeptModuleException;
import edu.uiuc.chunk.IllinoisChunker;
import edu.uiuc.common.IllinoisConstants;
import edu.uiuc.common.UiucModuleConfig;
import edu.uiuc.ner.IllinoisEntityRecognizer;
import edu.uiuc.nlp.IllinoisSentenceSegmenter;
import edu.uiuc.pos.IllinoisPosTagger;

/**
 * Created by cstokes on 12/8/2015.
 */
public class IllinoisCorefSpark extends SparkAlgorithmComponent{

  private static IllinoisSentenceSegmenter iss;
  private static IllinoisPosTagger posTagger;
  private static IllinoisChunker chunker;
  private static IllinoisEntityRecognizer ner;
  private static UiucModuleConfig config;
  private static TokenizerType tokenizerType;
  private static AlgorithmActivationStatus adjunctAlgorithmsActivationStatus =
      new AlgorithmActivationStatus();


  public IllinoisCorefSpark(AlgorithmSpecifications algorithmSpecifications, int timeOut) {
    super(algorithmSpecifications, timeOut);
  }

  @Override
  public void activateModule() throws ModuleActivationException {
    super.activateModule();
    synchronized (adjunctAlgorithmsActivationStatus) {
      if (!adjunctAlgorithmsActivationStatus.isAlgorithmActivated()) {
        adjunctAlgorithmsActivationStatus.setAlgorithmAsActivated();
        iss = new IllinoisSentenceSegmenter();
        try {
          posTagger = new IllinoisPosTagger();
          chunker = new IllinoisChunker();
          ner = new IllinoisEntityRecognizer();
          config = new UiucModuleConfig(
              this.algorithmSpecifications.configFilePath());
          chunker.activate(this.algorithmSpecifications.configFilePath());
          posTagger.activate(this.algorithmSpecifications.configFilePath());
          iss.activate(this.algorithmSpecifications.configFilePath());
          ner.activate(new UiucModuleConfig("edu/uiuc/ner/conllConfig.xml"));
        }catch (IOException e){
          throw new ModuleActivationException(e);
        }
        tokenizerType =
            TokenizerType.valueOf(config.getString(IllinoisConstants.TOKENIZER_TYPE));
      }
    }
  }

  @Override
  public HltContentContainer preProcessHltCC(HltContentContainer hltContentContainer)
      throws E2eException {
    Document document = hltContentContainer.getDocument();
    hltContentContainer = iss.tokenizeDocument(document);

    if (hltContentContainer == null || !checkRequiredViews(document, hltContentContainer)) {
      hltContentContainer = new HltContentContainer();

      TokenStream tokenStream =
          new TokenStream(tokenizerType, null, "English", null, ContentType.TEXT, document);
//				tokenStream.setDocument( document );
      document.addTokenStream(tokenStream);
      List<Sentence> sentences =
          iss.getSentences(document.getValue(), document.getTokenStream(tokenizerType));
      hltContentContainer.setSentences(sentences);
      try {
        posTagger.process(document, hltContentContainer);
        chunker.process(document, hltContentContainer);
      }catch (AdeptModuleException e){
        throw new E2eException(e);
      }
    }
    return hltContentContainer;
  }

  @Override
  public HltContentContainer postProcessHltCC(HltContentContainer hltContentContainer)
      throws E2eException {
    List<EntityMention> hltccEntityMentions = new ArrayList<EntityMention>();
    HashBiMap<EntityMention, Entity> resolvedMentionToEntityMap =
        HashBiMap.create();
    //IllinoisCoref does not set set entityMentions to container, so the following is required
    for (Coreference coreference : hltContentContainer.getCoreferences()) {
      hltccEntityMentions.addAll(coreference.getResolvedMentions());
      for (Entity entity : coreference.getEntities()) {
        //The following check is no more needed since IllinoisCoref sets entityIDDistributions to
        // canonicalMentions
//        if (entity.getCanonicalMention().getEntityIdDistribution() == null
//            || entity.getCanonicalMention().getEntityIdDistribution().size() == 0) {
//          Map<Long, Float> entityIdDistribution = new HashMap<Long, Float>();
//          entityIdDistribution
//              .put(entity.getEntityId(), (float) entity.getCanonicalMentionConfidence());
//          entity.getCanonicalMention().setEntityIdDistribution(entityIdDistribution);
//        }
        for (EntityMention em : coreference.getResolvedMentions()) {
          if (entity.getEntityId() == em.getBestEntityId()) {
            if (resolvedMentionToEntityMap.containsValue(entity)) {
              EntityMention otherEM = resolvedMentionToEntityMap.inverse().get(entity);
              float otherConf = otherEM.getConfidence(entity.getEntityId());
              float thisConf = em.getConfidence(entity.getEntityId());
              if (thisConf > otherConf) {
                resolvedMentionToEntityMap.forcePut(em, entity);
              }
            } else {
              resolvedMentionToEntityMap.put(em, entity);
            }
          }
        }
      }
    }
    hltContentContainer.setEntityMentions(hltccEntityMentions);
    //If hltContentContainer does not have any mentions, return it from here. This can happen
    // when the document is too small for IllinoisCoref to find any mentions at all
    if (hltContentContainer.getEntityMentions().isEmpty()) {
      return hltContentContainer;
    }
//    String ems="";
//    for(EntityMention em : hltccEntityMentions){
//      ems+=em+",";
//    }
//    log.info("All ems: "+ems);

    DefaultChunkAligner chunkAligner = new DefaultChunkAligner();
    DefaultChunkAligner.DocumentAlignmentOptions alignmentOptions = chunkAligner.
        new DocumentAlignmentOptions();
    List<Class> allowableChunkClasses = new ArrayList<>();
    allowableChunkClasses.add(EntityMention.class);
    alignmentOptions.useAsPivot().alignAllEntityMentions()
        .allowableChunkClasses(allowableChunkClasses).allowCrossType().algorithmName
        ("IllinoisCoref").sourceAlgorithm(new SourceAlgorithm("IllinoisCoref", "Illinois"));
    log.info("Postprocessing: Aligning Coref entity-mentions...");
    chunkAligner.align(hltContentContainer, alignmentOptions);

    log.info("Postprocessing: Running NER...");
    try {
      ner.process(hltContentContainer.getDocument(),
          hltContentContainer);
    }catch (AdeptModuleException e){
      throw new E2eException(e);
    }
    HltContentContainer hltContentContainerNer = new HltContentContainer();
    hltContentContainerNer.setEntityMentions(hltContentContainer.getNamedEntities());

    alignmentOptions = chunkAligner.new DocumentAlignmentOptions();
    alignmentOptions.alignAllEntityMentions().allowableChunkClasses(allowableChunkClasses).
        allowCrossType().algorithmName("IllinoisNER").sourceAlgorithm(new SourceAlgorithm(
        "IllinoisNER", "Illinois"));
    log.info("Postprocessing: Aligning NER entity-mentions...");
    chunkAligner.align(hltContentContainerNer, alignmentOptions);

    ChunkQuotientSet alignment = chunkAligner.buildAlignment();
    log.info("Postprocessing: Resetting entity-types...");
    for (ChunkEquivalenceClass ceq : alignment.equivalenceClasses()) {
      Map<IType, Integer> entityTypeCount = new HashMap<>();
      int maxCount = 0;
      IType maxType = null;
      for (Chunk entityMention : ceq.chunks()) {
        if (entityMention.getAlgorithmName().equals("IllinoisNER") &&
            !((EntityMention) entityMention).getEntityType().getType().equalsIgnoreCase("OTHER") &&
            !((EntityMention) entityMention).getEntityType().getType()
                .equalsIgnoreCase("UNKNOWN")) {
          IType entityType = ((EntityMention) entityMention).getEntityType();
          Integer count = entityTypeCount.get(entityType);
          if (count == null) {
            count = 0;
          }
          count++;
          entityTypeCount.put(entityType, count);
          if (count > maxCount) {
            maxCount = count;
            maxType = entityType;
          }
        }
      }
      if (maxType == null) {
        log.info("Postprocessing: No valid-type found from NER mentions...");
        continue;
      }
      log.info("Postprocessing: Setting type {} to Coref mentions...", maxType.getType());
      for (Chunk corefEntityMention : ceq.pivots()) {
        Entity entity = resolvedMentionToEntityMap.get((EntityMention) corefEntityMention);
        if (entity == null) {
          continue;
        }
        if(!maxType.equals(entity.getEntityType())) {
          E2eUtil.reAssignEntityType(entity, maxType, hltContentContainer);
          ((EntityMention) corefEntityMention).setEntityType(maxType);
        }
      }
    }
    return hltContentContainer;
  }

  /**
   * determine whether or not prerequisites are satisfied.  If they are, make sure tokenization type
   * is set to whatever is provided, rather than what is specified in the config file.
   */

  private boolean checkRequiredViews(Document doc, HltContentContainer hcc) {
    boolean isSentences = false;
    boolean isTokenizerTypePresent = false;
    boolean isPos = false;
    boolean isChunk = false;

    if (null != hcc.getSentences() && !hcc.getSentences().isEmpty()) {
      isSentences = true;
    }

    if (null != hcc.getPartOfSpeechs() && !hcc.getPartOfSpeechs().isEmpty()) {
      isPos = true;
    }

    if (null != hcc.getSyntacticChunks() && !hcc.getSyntacticChunks().isEmpty()) {
      isChunk = true;
    }

    boolean isHigherLevelAnnotationPresent = isSentences && isPos && isChunk;

    if (null != doc.getTokenStream(tokenizerType)) {
      isTokenizerTypePresent = true;
    } else if (isHigherLevelAnnotationPresent && !doc.getTokenStreamList().isEmpty()) {
      isTokenizerTypePresent = true;

      TokenizerType newTokenizerType = doc.getTokenStreamList().get(0).getTokenizerType();

      log.info(".checkRequiredViews(): tokenizer type specified in config file "
          + "({}) not present, but other annotations are. Setting tokenizer type to '{}'."
          , tokenizerType.name() ,newTokenizerType.name());

      tokenizerType = newTokenizerType;
    }
    return (isTokenizerTypePresent && isHigherLevelAnnotationPresent);
  }

}
