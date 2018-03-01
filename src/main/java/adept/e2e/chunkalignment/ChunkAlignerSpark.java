package adept.e2e.chunkalignment;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import adept.common.Chunk;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Type;
import adept.e2e.driver.E2eConstants;
import adept.e2e.driver.E2eUtil;
import adept.e2e.stageresult.DocumentResultObject;


public class ChunkAlignerSpark {

  private static final Logger log = LoggerFactory.getLogger(ChunkAlignerSpark.class);

  public static JavaPairRDD<String,
      DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
          HltContentContainer>>,
          ChunkQuotientSet>>
  processHltCCs(
      JavaPairRDD<String, DocumentResultObject<ImmutableList<DocumentResultObject
          <HltContentContainer, HltContentContainer>>,
          ChunkQuotientSet>> inputFiles, boolean useMissingEventTypesHack, E2eConstants.LANGUAGE language, List<String> algorithmTypesForRelaxedChunkAlignment,
      boolean throwExceptions) {
    JavaPairRDD<String, DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer,
            HltContentContainer>>,
        ChunkQuotientSet>> alignedFiles =
        inputFiles.mapValues(new Process(useMissingEventTypesHack,language, algorithmTypesForRelaxedChunkAlignment, throwExceptions));
    return alignedFiles;
  }

  static class Process
      implements Function<DocumentResultObject<ImmutableList<DocumentResultObject
      <HltContentContainer,
          HltContentContainer>>, ChunkQuotientSet>,
      DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
          HltContentContainer
          >>, ChunkQuotientSet>> {

    private final boolean useMissingEventTypesHack;
    private final E2eConstants.LANGUAGE language;

    private final List<String> algorithmsForRelaxedChunkAlignment;
    private final boolean throwExceptions;


    public Process(boolean useMissingEventTypesHack, E2eConstants.LANGUAGE language, List<String> algorithmTypesForRelaxedChunkAlignment,
                   boolean throwExceptions){
      this.useMissingEventTypesHack = useMissingEventTypesHack;
      this.language = language;
      this.algorithmsForRelaxedChunkAlignment = algorithmTypesForRelaxedChunkAlignment;
      this.throwExceptions = throwExceptions;

    }

    @Override
    public DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
        HltContentContainer>>, ChunkQuotientSet>
    call(
        DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
            HltContentContainer>>, ChunkQuotientSet> documentResultObject)
        throws Exception {
      String docId = null;
      long timeTaken = 0L;
      ImmutableMap.Builder propertiesMap = ImmutableMap.builder();
      try {

        if (documentResultObject.isSuccessful()) {
          //if the input is already successful, no processing is required
          return documentResultObject;
        }

        // Check if any algorithms failed to produce output, log it, and return null as we do not
        // want to run chunk alignment on unfinished results
        for (DocumentResultObject algoResult : documentResultObject.getInputArtifact()) {
          if (!algoResult.isSuccessful()) {
            log.info("Skipping alignment for document: {} due to {} processing failing.", docId , algoResult
                .getProperty(E2eConstants.ALGORITHM_NAME));
            //return as is, since default value of successfulResult is false
            return documentResultObject;
          }
        }

        DefaultChunkAligner chunkAligner = new DefaultChunkAligner(language);
        Map<String, DefaultChunkAligner.DocumentAlignmentOptions> alignmentOptionsMap
            = new HashMap<String, DefaultChunkAligner.DocumentAlignmentOptions>();
        List<Class> allowableChunkClasses = new ArrayList<Class>();
        allowableChunkClasses.add(EntityMention.class);

        DefaultChunkAligner.DocumentAlignmentOptions corefAlignmentOptions =
            chunkAligner.new DocumentAlignmentOptions();
        corefAlignmentOptions = corefAlignmentOptions.alignAllEntityMentions().
            allowableChunkClasses(allowableChunkClasses).useAsPivot();
        alignmentOptionsMap.put(E2eConstants.ALGORITHM_TYPE_COREF, corefAlignmentOptions);

        DefaultChunkAligner.DocumentAlignmentOptions wikificationAlignmentOptions =
                chunkAligner.new DocumentAlignmentOptions();
        wikificationAlignmentOptions = wikificationAlignmentOptions.alignAllEntityMentions().
                allowableChunkClasses(allowableChunkClasses);
        alignmentOptionsMap
                .put(E2eConstants.ALGORITHM_TYPE_NIL_CLUSTERING, wikificationAlignmentOptions);

        DefaultChunkAligner.DocumentAlignmentOptions entityLinkingAlignmentOptions =
            chunkAligner.new DocumentAlignmentOptions();
        entityLinkingAlignmentOptions = entityLinkingAlignmentOptions.alignAllEntityMentions().
            allowableChunkClasses(allowableChunkClasses).allowCrossType();
        alignmentOptionsMap
            .put(E2eConstants.ALGORITHM_TYPE_ENTITY_LINKING, entityLinkingAlignmentOptions);

        allowableChunkClasses.add(Chunk.class);
        DefaultChunkAligner.DocumentAlignmentOptions relationAlignmentOptions =
            chunkAligner.new DocumentAlignmentOptions();
        relationAlignmentOptions = relationAlignmentOptions.alignAllEntityMentions()
            .alignAllRelationArguments().allowableChunkClasses(allowableChunkClasses).allowCrossType();
        if(useMissingEventTypesHack){
          relationAlignmentOptions.alignAllEventArguments().allowableEventTypes(FluentIterable.from(
              E2eUtil.missingEventTypes).
              transform(new com.google.common.base.Function<String, IType>() {
                @Nullable
                @Override
                public Type apply(@Nullable final String input) {
                  return new Type(input);
                }
              }).toList());
        }

        alignmentOptionsMap
            .put(E2eConstants.ALGORITHM_TYPE_RELATION_EXTRACTION, relationAlignmentOptions);

        DefaultChunkAligner.DocumentAlignmentOptions eventAlignmentOptions =
            chunkAligner.new DocumentAlignmentOptions();
        eventAlignmentOptions = eventAlignmentOptions.alignAllEntityMentions()
            .alignAllEventArguments().allowableChunkClasses(allowableChunkClasses).allowCrossType();
        alignmentOptionsMap
            .put(E2eConstants.ALGORITHM_TYPE_EVENT_EXTRACTION, eventAlignmentOptions);

        //Getting the first HltContentContainer in order to get the docid for logging purposes
        docId = ((HltContentContainer) documentResultObject.getInputArtifact().get(0).
            getOutputArtifact().get()).getDocumentId();

        log.info("Started chunk-alignment for document: {}", docId);

        long start = System.currentTimeMillis();

        //If coref_algorithm is not the first algorithm in inputArtifact list, make it so
        List<DocumentResultObject<HltContentContainer, HltContentContainer>> algorithmResults =
            new ArrayList<>();
        for (DocumentResultObject algoResult : documentResultObject.getInputArtifact()) {
          if (algoResult.getProperty(E2eConstants.ALGORITHM_TYPE).equals(E2eConstants
              .ALGORITHM_TYPE_COREF)) {
            algorithmResults.add(0, algoResult);
          } else {
            algorithmResults.add(algoResult);
          }
        }

        for (DocumentResultObject algoResult : algorithmResults) {
          log.info("Aligning chunks for {} for document: {}", algoResult.getProperty(E2eConstants.ALGORITHM_NAME),
                  docId);
          HltContentContainer containerForAlignment = (HltContentContainer) algoResult.
              getOutputArtifact().get();
          String algorithmType = (String)algoResult.getProperty(E2eConstants.ALGORITHM_TYPE);
          DefaultChunkAligner.DocumentAlignmentOptions alignmentOptions = alignmentOptionsMap.get(
              algorithmType);
          if(algorithmsForRelaxedChunkAlignment.contains(algorithmType)){
            alignmentOptions.useRelaxedAlignmentRule();
          }
          chunkAligner.align(containerForAlignment,
                  alignmentOptions.algorithmName(
                      containerForAlignment.getAlgorithmName()).sourceAlgorithm
                  (containerForAlignment.getSourceAlgorithm()));
          log.info("Chunk alignment finished for {}", algoResult.getProperty(
              E2eConstants.ALGORITHM_NAME));
        }
        ChunkQuotientSet finalChunkAlignment = chunkAligner.buildAlignment();
        if (finalChunkAlignment.equivalenceClasses() == null ||
            finalChunkAlignment.equivalenceClasses().size() == 0) {
          throw new Exception("No equivalenceClasses found after chunk-alignment for document: "
              + docId);
        }
        long end = System.currentTimeMillis();
        timeTaken = end - start;
        log.info("Finished chunk-alignment for document: {}  in " +
            " %%%% {}s %%%%", docId, (timeTaken / 1000));
        documentResultObject.setOutputArtifact(finalChunkAlignment);
        documentResultObject.markSuccessful();
      } catch (Exception e) {
        log.error("Could not align chunks for document: {}", docId, e);
        if(throwExceptions){
          throw e;
        }
        documentResultObject.setOutputArtifact(null);
        documentResultObject.markFailed();
        propertiesMap.put(E2eConstants.PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
        propertiesMap.put(E2eConstants.PROPERTY_EXCEPTION_MESSAGE, e.getMessage()!=null?e
            .getMessage():"");
        propertiesMap.put(E2eConstants.PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
      propertiesMap.put(E2eConstants.PROPERTY_DOCID, docId);
      propertiesMap.put(E2eConstants.PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      propertiesMap.put(E2eConstants.PROPERTY_TIME_TAKEN, timeTaken);
      documentResultObject.setPropertiesMap(propertiesMap.build());
      return documentResultObject;
    }

  }
}

