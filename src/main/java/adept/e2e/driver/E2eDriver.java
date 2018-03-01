package adept.e2e.driver;

import adept.e2e.chunkalignment.AlignedChunksCSVFormatter;
import adept.e2e.chunkalignment.ChunkAlignerSpark;
import adept.e2e.documentdeduplication.DocumentDeduplicatorSpark;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import com.google.common.base.Joiner;
import com.google.common.collect.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import adept.common.Corpus;
import adept.common.HltContentContainer;
import adept.common.KBID;
import adept.common.OntType;
import adept.common.Pair;
import adept.common.TokenizerType;
import adept.e2e.algorithms.AlgorithmSpecifications;
import adept.e2e.algorithms.SparkAlgorithmComponent;
import adept.e2e.artifactextraction.ArtifactExtractor;
import adept.e2e.artifactextraction.ExtractedArguments;
import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.artifactextraction.artifactfilters.additionalargtypefilter.AdditionalArgTypeArtifactFilter;
import adept.e2e.artifactextraction.artifactfilters.additionalargtypefilter.EntityKeyToAdditionalTypesMapper;
import adept.e2e.artifactextraction.artifactfilters.additionalargtypefilter.EntityKeyToAdditionalTypesReducer;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.chunkalignment.ChunkQuotientSet;
import adept.e2e.kbupload.artifactdeduplicator.EntityDeduplicator;
import adept.e2e.kbupload.artifactdeduplicator.EventDeduplicator;
import adept.e2e.kbupload.artifactdeduplicator.RelationDeduplicator;
import adept.e2e.kbupload.artifactuploader.EntityUploader;
import adept.e2e.kbupload.artifactuploader.EventUploader;
import adept.e2e.kbupload.artifactuploader.NonEntityArgUploader;
import adept.e2e.kbupload.artifactuploader.RelationUploader;
import adept.e2e.kbupload.uploadedartifacts.UploadedEvents;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.kbupload.uploadedartifacts.UploadedRelations;
import adept.e2e.mastercontainer.MasterAlgorithmContainer;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.DocumentResultObject;
import adept.e2e.utilities.PprintHltcc;
import adept.kbapi.KB;
import adept.kbapi.KBParameters;
import scala.Tuple2;

import static adept.e2e.driver.E2eConstants.*;
import static adept.e2e.driver.E2eUtil.checkpointHasFailedDocs;
import static adept.e2e.driver.E2eUtil.generateStats;
import static adept.e2e.driver.E2eUtil.generateStatsLite;
import static adept.e2e.driver.E2eUtil.getFilePathsFromRDD;
import static adept.e2e.driver.E2eUtil.getSuccessfulDocIdsFromBatchResult;
import static adept.e2e.driver.E2eUtil.hasNewDocsToProcess;
import static adept.e2e.driver.E2eUtil.includeNewDocsInRDD;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class implements the language-specific E2E (short for end to end) pipeline. It is responsible for:
 *
 * 1. Reading the raw text files
 * 2. De-duplicating the text files (depending on the run config) using @{@link DocumentDeduplicatorSpark} class
 * 3. Converting the text files to initial @{@link HltContentContainer} objects using @{@link FileToHltCCConverter}
 * 4. Running the language-specific Information Extraction (IE) algorithms on the initial @{@link HltContentContainer}s
 * 5. Aligning textual chunks extracted by the IE algorithms using @{@link ChunkAlignerSpark} class
 * 6. Aggregating the output of various IE algorithms using @{@link MasterAlgorithmContainer} class
 * 7. Merging the per-document output at batch or partition level using @{@link ArtifactExtractor} class
 * 8. Uploading the extracted artifacts to KB
 *
 * The public method {@link #extractArtifacts(JavaSparkContext)} takes care of running steps 1 to 7 of the pipeline. Thereafter,
 * the @{@link MainE2eDriver} calls the public methods {@link #uploadEntities(JavaSparkContext)},
 * {@link #uploadBatchLevelRelations(JavaRDD, UploadedEntities, UploadedNonEntityArguments, KBParameters, JavaSparkContext)}
 * {@link #uploadBatchLevelEvents(JavaRDD, UploadedEntities, UploadedNonEntityArguments, KBParameters, JavaSparkContext)} to upload
 * the batch-level entities, relations and events respectively. In addition, @{@link MainE2eDriver} also calls the method
 * {@link #uploadNonEntityArgs(JavaSparkContext)} to upload non-Entity type relation and event arguments (e.g. Title, Date, etc.).
 *
 * @author msrivast
 */

public class E2eDriver implements Serializable{

  private static final Logger log = LoggerFactory.getLogger(E2eDriver.class);
  private static final long serialVersionUID = 249354266362827897L;
  private Map<String,Pair<String,String>> algorithmTypeToOntologyFilesMap;
  private final String outputDirectory;
  private final String inputDirectory;
  private final int numPartitions;
  private final E2eConfig e2eConfig;
  private final LANGUAGE language;
  private final Corpus corpus;

  private JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts;
  private JavaPairRDD<String, HltContentContainer> hltContentContainers;
  JavaRDD<BatchResultObject<UploadedEntities>> uploadedEntities;
  JavaRDD<BatchResultObject<UploadedEntities>> deduplicatedEntities;
  JavaRDD<BatchResultObject<UploadedNonEntityArguments>> deduplicatedNonEntityArgs;

  public E2eDriver(String inputDirectory, String outputDirectory,
      int numPartitions, E2eConfig e2eConfig, LANGUAGE language) {
    checkNotNull(inputDirectory);
    checkNotNull(outputDirectory);
    checkNotNull(e2eConfig);
    this.inputDirectory = inputDirectory;
    this.outputDirectory = outputDirectory;
    this.numPartitions = numPartitions;
    this.e2eConfig = e2eConfig;
    this.language = language;
    corpus = new Corpus(e2eConfig.corpusId(), null, e2eConfig.corpusId(), null);
  }

  public String getOutputDirectory(){
    return outputDirectory;
  }

  public JavaRDD<BatchResultObject<ExtractedArtifacts>>
      extractArtifacts(JavaSparkContext sc) throws Exception {
    if(extractedArtifacts!=null){
      return extractedArtifacts;
    }
    log.info("Input dir: {}" , inputDirectory);
    log.info("Output dir: {}" , outputDirectory);

    // delete any temp directories from previous runs
    E2eUtil.clearTempHdfsDirectories(outputDirectory, sc);

    log.info("Reading input files");
    JavaPairRDD<String, String> files = readInputFiles(sc);

    JavaPairRDD<String, HltContentContainer> possiblyDuplicateHltContentContainers =
        createHltContentContainers
        (files, e2eConfig.stagesToSkip(language).contains(STAGE_NAME_HLTCC_CREATION), sc).filter(new
            E2eUtil.SuccessfulResultObjectsFilter<>()).mapValues(
            (DocumentResultObject<String,HltContentContainer> dro) -> dro.getOutputArtifact()
                .get());

    //be mindful of unpersisting RDDs as soon as you're done with them in order to free up memory
    log.info("Unpersisting inputFiles RDD...");
    files.unpersist();

    hltContentContainers = possiblyDuplicateHltContentContainers;

    if(e2eConfig.runDocumentDeduplication()) {
      log.info("Running deduplication on input files");
      hltContentContainers = runDeduplication
          (possiblyDuplicateHltContentContainers,
              e2eConfig.dedupParams(), e2eConfig.kbParameters(), e2eConfig.stagesToSkip(language).contains
                  (STAGE_NAME_DOCUMENT_DEDUPLICATION), sc);
      log.info("Unpersisting possiblyDuplicateHltContentContainers RDD...");
      possiblyDuplicateHltContentContainers.unpersist();
    }

    if (numPartitions != -1) {
      //If numPartiotions is -1 instead, leave it to Spark to decide the number of partitions.
      //However, using numPartitions=-1 is suggested only during experiments, and not for the
      //deployed/released system, where the person running E2E should have fair intuition about
      //the right value for numPartitions to use given the size of corpus, computing power, etc.
//		log.info("Repartitioning deduplicated containers...");
      hltContentContainers = hltContentContainers.repartition(numPartitions);
    }

    JavaPairRDD<String, ImmutableList<DocumentResultObject<HltContentContainer,
        HltContentContainer>>>
        groupedAlgorithmOutput =
        runAlgorithms(hltContentContainers, e2eConfig.algorithmSpecifications(language), e2eConfig.stagesToSkip
            (language), sc);
    groupedAlgorithmOutput.persist(StorageLevel.MEMORY_AND_DISK());

    if (e2eConfig.getAlgorithmByType(language, E2eConstants.ALGORITHM_TYPE_NIL_CLUSTERING).isPresent()) {
      log.info("Doing Nil-Clustering...");
      try {
        groupedAlgorithmOutput =
            doNilClustering(groupedAlgorithmOutput, sc, e2eConfig.isDebugMode());
      }catch (Exception e){
        if(e2eConfig.isDebugMode()){
          throw e;
        }
      }
      groupedAlgorithmOutput.persist(StorageLevel.MEMORY_AND_DISK());
    }

    //Unpersist unwanted RDDs now. All stages make a call to saveCheckpoint, thereby ensuring
    // that the executor has run the required operation. Therefore, the input RDDs can be
    // unpersisted after returning from the method.
    //hold off unpersisting hltContentContainers since they're used to upload document-text

    log.info("Aligning Chunks");
    JavaPairRDD<String, DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>>
        alignedChunks
        =
        alignChunks(groupedAlgorithmOutput, e2eConfig.stagesToSkip(language).contains
            (STAGE_NAME_CHUNK_ALIGNMENT), e2eConfig.useMissingEventTypesHack(language), sc);
    //unpersist groupedAlgorithm output now
    log.info("Unpersisting groupedAlgorithmOutput RDD...");
    groupedAlgorithmOutput.unpersist();

    log.info("Reassigning entity-types...");
    alignedChunks = reassignEntityTypes(alignedChunks, e2eConfig.stagesToSkip(language).contains
            (STAGE_NAME_TYPE_REASSIGNMENT), sc);

    log.info("Creating Master Containers...");
    JavaPairRDD<String, DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>, HltContentContainer>>
        masterAlgorithmContainers =
        combineAlgorithmContainers
            (alignedChunks, e2eConfig.stagesToSkip(language).contains(STAGE_NAME_CONTAINER_MERGING), e2eConfig.useMissingEventTypesHack(language), sc);

    //unpersis alignedChunks now
    log.info("Unpersisting alignedChunks RDD...");
    alignedChunks.unpersist();
    //Get only master HltCC (from successful results) for the next step, since other artifacts are
    // not needed
    log.info("Getting Master Containers...");
    JavaPairRDD<String, HltContentContainer> masterHltCCs = extractMasterContainers
        (masterAlgorithmContainers);
    if (masterHltCCs.isEmpty()) {
      log.error("All MasterHltContentContainers are empty");
      throw new Exception("All MasterHltContentContainers are empty");
    }

//    So far, we have not felt any need to change the number of partitions in the following
//    repartitioning. Therefore, commenting the below statements.
//    log.info("Repartitioning MasterAlgorithmContainers...");
//    masterHltCCs = masterHltCCs.repartition(numDedupPartitions);
    //TODO: The above repartitioning will have to be looked into since these partitions then take
    // part in partition-level-deduplication.

    //do not unpersist masterAlgorithmContainers yet, since masterHltCCs has not yet been
    // computed ("lazy execution").
    //From masterHltCCs, get a complete list of docIds (not file-paths!!)
    log.info("Getting all the docIds from master containers...");
    Set<String> masterHltCCDocIds = getDocIdsFromMasterContainers(masterHltCCs);
    //Since we've called collect, masterHltCCs have been created. Unpersist
    // masterAlgorithmContainers now
    log.info("Unpersisting masterAlgorithmContainers RDD...");
    masterAlgorithmContainers.unpersist();

    log.info("Extracting partition-level KB artifacts from master HltCCs...");
    extractedArtifacts =
        extractPartitionLevelArtifacts(masterHltCCs, masterHltCCDocIds, e2eConfig.stagesToSkip(language)
            .contains(STAGE_NAME_ARTIFACT_EXTRACTION), sc);

    if (extractedArtifacts.isEmpty()) {
      throw new Exception("Extracted artifacts are empty.");
    }
    //unpersist masterHltCCs since it's been used
    log.info("Unpersisting masterHltCCs RDD...");
    masterHltCCs.unpersist();

    //drop merged relations and events whose Entity-args have minoity additional type,
    //where additional type is City, Country, StateProvince
    log.info("Dropping merged relations and events with minority additional types...");
    extractedArtifacts =
        dropArtifactsWithMinorityAddnlType(extractedArtifacts, sc);
    //document text upload should happen has part of artifact-extraction
    uploadDocumentText(sc);
    return extractedArtifacts;
  }

  public JavaRDD<BatchResultObject<ExtractedArtifacts>> getExtractedArtifacts(JavaSparkContext sc) throws Exception{
    return this.extractArtifacts(sc);
  }

  public JavaRDD<BatchResultObject<UploadedEntities>> deduplicateEntities(JavaSparkContext sc) throws Exception{
    if(deduplicatedEntities!=null){
       return deduplicatedEntities;
    }
    if(uploadedEntities==null) {
      uploadedEntities = this.uploadEntities(sc);
    }
    log.info("De-duplicating entities...");
    deduplicatedEntities =
        deduplicateEntities(uploadedEntities, e2eConfig.kbParameters(), sc);
    //unpersist entityKeyToInsertedKBIDMap
    log.info("Unpersisting entityKeyToInsertedKBIDMap RDD...");
    uploadedEntities.unpersist();
    return deduplicatedEntities;
  }

  public JavaRDD<BatchResultObject<UploadedEntities>> uploadEntities(JavaSparkContext sc) throws Exception{
    if(uploadedEntities!=null){
      return uploadedEntities;
    }
    //upload and deduplicate batch-level entities
    uploadedEntities =
            uploadBatchLevelEntities(extractedArtifacts, e2eConfig.kbParameters(), sc);
    return uploadedEntities;
  }

  public JavaRDD<BatchResultObject<UploadedEntities>> getUploadedEntities(JavaSparkContext sc) throws Exception{
    return this.uploadEntities(sc);
  }

  public JavaRDD<BatchResultObject<UploadedEntities>> getDeduplicatedEntities(JavaSparkContext sc) throws Exception{
    return this.deduplicateEntities(sc);
  }

  public JavaRDD<BatchResultObject<UploadedNonEntityArguments>> uploadNonEntityArgs(JavaSparkContext sc) throws Exception{
    if(deduplicatedNonEntityArgs!=null){
      return deduplicatedNonEntityArgs;
    }
    //extract ExtractedArguments from ExtractedArtifacts; coalesce them in 1 partition; and upload them
    //to ensure single upload of all non-Entity arguments
    deduplicatedNonEntityArgs =
        uploadNonEntityArguments(extractedArtifacts, e2eConfig.kbParameters(), sc);
    return deduplicatedNonEntityArgs;
  }

  public JavaRDD<BatchResultObject<UploadedNonEntityArguments>> getDeduplicatedNonEntityArgs(JavaSparkContext sc) throws Exception{
    return this.uploadNonEntityArgs(sc);
  }

  private void uploadDocumentText(JavaSparkContext sc) throws Exception{
    //Uploading document text so that mentions can have provenances
    log.info("Uploading document text...");
    uploadDocumentText(hltContentContainers, e2eConfig.kbParameters(), sc);
    log.info("Finished uploading document text.");
    log.info("Unpersisting hltContentContainers RDD...");
    hltContentContainers.unpersist();
  }

  private JavaPairRDD<String, String> readInputFiles(JavaSparkContext sc) {
    JavaPairRDD<String, String> files =
        sc.wholeTextFiles(inputDirectory).coalesce(1);//Using coalesce
    //ensures that "files" is not split into more than one partitions. This makes deduplication
    //deterministic and race-condition free, since sometimes Spark assigns the same document to more
    //than one partitions (we don't yet know why), meaning that deduplication is attempted on
    // that document multiple times.
    files.persist(StorageLevel.MEMORY_AND_DISK());
//    if (!E2eUtil.doesHdfsDirectoryExist(outputDirectory, sc)) {
//      log.info("Saving original input files to base output directory");
//      files.saveAsTextFile(outputDirectory);
//    }
    return files;
  }

  private JavaPairRDD<String, HltContentContainer> runDeduplication
      (JavaPairRDD<String, HltContentContainer>
      hltCCs, Map<String, String> dedupParams, KBParameters kbParameters, boolean skipThisStage,
      JavaSparkContext sc) throws Exception{
    String dedupCheckpointOutputDirectory =
        outputDirectory + "/" + CHECKPOINT_DIR_DOCUMENT_DEDUP;
    JavaPairRDD<String, HltContentContainer> deduplicatedHltCCs = null;
    boolean isRerunRequired = true;
    if (E2eUtil.doesHdfsDirectoryExist(dedupCheckpointOutputDirectory, sc)) {
      log.info("E2eDriver: Deduplication checkpoint found, using existing files");
      deduplicatedHltCCs = JavaPairRDD.fromJavaRDD(sc.objectFile(dedupCheckpointOutputDirectory));
      if (skipThisStage) {
        log.info("Skipping this stage based on stages_to_skip e2e property");
        deduplicatedHltCCs.persist(StorageLevel.MEMORY_AND_DISK());
        return deduplicatedHltCCs;
      }
      if (hasNewDocsToProcess(deduplicatedHltCCs,
          getFilePathsFromRDD
              (hltCCs))) {
        isRerunRequired = true;
        log.info("Found some new documents to process");
      } else {
        isRerunRequired = false;
      }
    }
    if (isRerunRequired) {
       log.info("Running deduplication on input files...");
       deduplicatedHltCCs = DocumentDeduplicatorSpark.processHltCCs(hltCCs,
           dedupParams);
    }
    // cache the results so it doesn't attempt to re-run when calling saveAsObjectFile
    deduplicatedHltCCs.persist(StorageLevel.MEMORY_AND_DISK());
    if (isRerunRequired) {
      E2eUtil.saveCheckpoint(deduplicatedHltCCs, sc, dedupCheckpointOutputDirectory);
    }
    return deduplicatedHltCCs;
  }

  private JavaPairRDD<String, DocumentResultObject<String,HltContentContainer>>
    createHltContentContainers
      (JavaPairRDD<String, String>
      inputFiles, boolean skipThisStage, JavaSparkContext sc) throws Exception{

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_HLTCC;
    JavaPairRDD<String, DocumentResultObject<String,HltContentContainer>>
        hltContentContainers = null;
    if (!E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      hltContentContainers = inputFiles.mapValues( (String fileContent) -> DocumentResultObject
          .create(fileContent));
    } else {
      log.info("{} checkpoint found, using existing files", STAGE_NAME_HLTCC_CREATION);
      hltContentContainers = JavaPairRDD.fromJavaRDD(sc.objectFile(checkpointDirectory));
      if (skipThisStage) {
        log.info("Skipping this stage based on stages_to_skip e2e property");
        hltContentContainers.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(hltContentContainers,e2eConfig,language);
        return hltContentContainers;
      }
      boolean checkpointHasFailedDocs = checkpointHasFailedDocs(hltContentContainers);
      boolean hasNewDocsToProcess = hasNewDocsToProcess
          (hltContentContainers, getFilePathsFromRDD(inputFiles));
      boolean isRerunRequired = checkpointHasFailedDocs || hasNewDocsToProcess;
      if (!isRerunRequired) {
        log.info("No unsuccessful or uprocessed documents found for stage {}",
            STAGE_NAME_HLTCC_CREATION);
        hltContentContainers.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(hltContentContainers,e2eConfig,language);
        return hltContentContainers;
      } else if (hasNewDocsToProcess) {
        hltContentContainers = includeNewDocsInRDD(hltContentContainers, inputFiles, sc);
      }
    }
    log.info("Creating HltContentContainers from input files");
    hltContentContainers = hltContentContainers.mapToPair(new FileToHltCCConverter(corpus,e2eConfig
        .xmlReadMode()));
    // cache the results so it doesn't attempt to re-run when calling saveAsObjectFile
    hltContentContainers.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(hltContentContainers, sc, checkpointDirectory);
    E2eUtil.generateStats(hltContentContainers,e2eConfig,language);
    return hltContentContainers;
  }

  private JavaPairRDD<String, ImmutableList<DocumentResultObject<HltContentContainer,
      HltContentContainer>>> runAlgorithms
      (JavaPairRDD<String, HltContentContainer> hltContentContainers, List<AlgorithmSpecifications>
          algorithmSpecifiationsList, List<String> algorithmsToSkip, JavaSparkContext sc)
      throws Exception {
    ImmutableList.Builder<JavaPairRDD<String, DocumentResultObject<HltContentContainer, HltContentContainer>>>
        preppedAlgorithmOutputList
        = ImmutableList.builder();
    if(algorithmTypeToOntologyFilesMap==null){
      algorithmTypeToOntologyFilesMap = new HashMap();
    }
    for (AlgorithmSpecifications algorithmSpecifications : algorithmSpecifiationsList) {
      String algorithmName = algorithmSpecifications.algorithmName();
      String ontologyMappingFilePath = algorithmSpecifications.ontologyMappingFilePath();
      String reverseOntologyMappingFilePath = algorithmSpecifications.reverseOntologyMappingFilePath();
      if(ontologyMappingFilePath!=null){
        algorithmTypeToOntologyFilesMap.put(algorithmSpecifications.algorithmType(),new Pair(ontologyMappingFilePath,
                reverseOntologyMappingFilePath));
      }

      String checkpointDirectory = outputDirectory + "/" + algorithmName + "_"+algorithmSpecifications.algorithmType()+"_checkpoint";
      log.info("Running algorithm: {} with config: {}",algorithmSpecifications.algorithmModule().getName(),
        algorithmSpecifications.configFilePath());

      Class<? extends SparkAlgorithmComponent> sparkWrapperClass = algorithmSpecifications.sparkWrapperClass();
      SparkAlgorithmComponent algorithmComponent =
          (SparkAlgorithmComponent) sparkWrapperClass.getConstructor(AlgorithmSpecifications.class,int.class).newInstance
              (algorithmSpecifications,e2eConfig.algorithmTimeOut());

      JavaPairRDD<String, DocumentResultObject<HltContentContainer, HltContentContainer>>
          preppedAlgorithmOutput = null;
      if (!E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
        preppedAlgorithmOutput = hltContentContainers.mapValues((HltContentContainer hltcc) ->
            DocumentResultObject.create(hltcc));
      } else {
        log.info("{} checkpoint found, using existing files",algorithmName);
        preppedAlgorithmOutput = JavaPairRDD.fromJavaRDD(sc.objectFile(checkpointDirectory));
        if (algorithmsToSkip.contains(algorithmSpecifications.algorithmType())) {
          log.info("Skipping this algorithm based on stages_to_skip e2e property");
          preppedAlgorithmOutput.persist(StorageLevel.MEMORY_AND_DISK());
          preppedAlgorithmOutputList.add(preppedAlgorithmOutput);
          generateStats(preppedAlgorithmOutput,e2eConfig,language);
          continue;
        }
        //if algorithm is not supposed to be skipped altogether, see if it should be re-run based
        // on number of failed documents from the last run or presence of any new documents in
        // input set
        boolean checkpointHasFailedDocs = checkpointHasFailedDocs(preppedAlgorithmOutput);
        boolean hasNewDocsToProcess = hasNewDocsToProcess
            (preppedAlgorithmOutput, getFilePathsFromRDD(hltContentContainers));
//        boolean isRerunRequired = isRerunRequired(preppedAlgorithmOutput,getFilePathsFromRDD
//            (hltContentContainers));
        boolean isRerunRequired = checkpointHasFailedDocs || hasNewDocsToProcess;
        if (!isRerunRequired) {
          log.info("No unsuccessful or unprocessed documents found for algorithm {}", algorithmName);
          preppedAlgorithmOutput.persist(StorageLevel.MEMORY_AND_DISK());
          preppedAlgorithmOutputList.add(preppedAlgorithmOutput);
          generateStats(preppedAlgorithmOutput,e2eConfig,language);
          continue;
        } else if (hasNewDocsToProcess) {
          preppedAlgorithmOutput = includeNewDocsInRDD(preppedAlgorithmOutput,
              hltContentContainers, sc);
        }
      }
      //doing a re-run
      log.info("Processing files with algorithm {}" , algorithmName);
      preppedAlgorithmOutput
          = preppedAlgorithmOutput.mapValues(algorithmComponent);
      // Cache algorithm output before saving as object file to avoid re-processing
      preppedAlgorithmOutput.persist(StorageLevel.MEMORY_AND_DISK());
      //save checkpoint
      E2eUtil.saveCheckpoint(preppedAlgorithmOutput, sc, checkpointDirectory);
      preppedAlgorithmOutputList.add(preppedAlgorithmOutput);
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + algorithmName+"_pprint_output", sc);
      if(e2eConfig.serializeRDDs()) {
        JavaPairRDD<String, String> pprintHltCCs = preppedAlgorithmOutput.mapValues(
                (DocumentResultObject<HltContentContainer, HltContentContainer> resultObject) ->
                        PprintHltcc.getPprintOutput(resultObject.getOutputArtifact()));
        pprintHltCCs.saveAsTextFile(outputDirectory + "/" + algorithmName + "_pprint_output");
      }
      //calling saveAsTextFile on unserialized HltContentContainers would make a call to .toString()
      //to get their string versions, which are not helpful
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + algorithmName, sc);
      if(e2eConfig.serializeRDDs()) {
        JavaPairRDD<String, String> serializedAlgorithmOutput =
                preppedAlgorithmOutput.mapValues(
                        (DocumentResultObject<HltContentContainer, HltContentContainer> resultObject) ->
                                SerializerUtil.serialize(resultObject.getOutputArtifact().orNull()));
        serializedAlgorithmOutput.saveAsTextFile(outputDirectory + "/" + algorithmName);
      }

      generateStats(preppedAlgorithmOutput,e2eConfig,language);
      if(preppedAlgorithmOutput.filter(new E2eUtil.SuccessfulResultObjectsFilter()).isEmpty()){
        log.error("Algorithm {} failed on all input documents. Check stats file for specific "
            + "exceptions thrown.",algorithmName);
        throw new Exception("Algorithm "+algorithmName+" failed on all input documents.");
      }
    }

    //Let's group various algorithm output by filenames as the key
    JavaPairRDD<String, ImmutableList<DocumentResultObject<HltContentContainer,
        HltContentContainer>>> groupedAlgorithmOutput =
        E2eUtil.groupAlgorithmOutput(preppedAlgorithmOutputList.build());
    return groupedAlgorithmOutput;
  }

  private JavaPairRDD<String, ImmutableList<DocumentResultObject<HltContentContainer,
      HltContentContainer>>> doNilClustering(JavaPairRDD<String, ImmutableList<DocumentResultObject<HltContentContainer,
      HltContentContainer>>> groupedAlgorithmOutput, JavaSparkContext sc, boolean throwExceptions) throws Exception{
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_NIL_CLUSTERING;
    JavaRDD<HltContentContainer> nilClusteredRDD = sc.emptyRDD();
    List<HltContentContainer> nilClusteredHltCCs = new ArrayList<>();
    boolean checkpointExists = E2eUtil.doesHdfsDirectoryExist(checkpointDirectory,sc);
    if(checkpointExists) {
        log.info("Nil-Clustering checkpoint found. Loading existing list of HltCCs...");
      nilClusteredRDD = sc.objectFile(checkpointDirectory);
      nilClusteredHltCCs = nilClusteredRDD.collect();
    }else{
      // get a List<HltContentContainer> of only the wikifier output
      log.info("Collecting Wikified HltCCs...");
      List<HltContentContainer> hltContentContainersFromWikifier = groupedAlgorithmOutput.map(new Function<Tuple2<String, ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>>, HltContentContainer>() {
        @Override
        public HltContentContainer call(Tuple2<String, ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>> fileToDocumentResultObjectList) throws Exception {
          for (DocumentResultObject<HltContentContainer, HltContentContainer> documentResultObject : fileToDocumentResultObjectList._2()) {
            if (documentResultObject.getProperty(ALGORITHM_TYPE).equals
                (ALGORITHM_TYPE_NIL_CLUSTERING)) {
              // isSuccessful() implies that getOutputArtifact().isPresent()
              HltContentContainer hltCC = documentResultObject.isSuccessful() ? documentResultObject.getOutputArtifact().get() : null;
              if(hltCC!=null){
                // the nil clustering code attempts to use the corpus of a document as its id; temporarily unset the documents'
                // corpora to prevent it from doing so
                hltCC.getDocument().setCorpus(null);
              }
              return hltCC;
            }
          }
          throw new NoSuchElementException("DocumentResultObject for " + fileToDocumentResultObjectList._1() + " does not have wikifier output.");
        }
      }).filter(new E2eUtil.NonNullRDDFilter<>()).collect();

      // if we get here but the wikifier has not run due to the existence of successful wikifier checkpoint,
      // then we must still ensure that the UIUC ConfigParameters are populated
      edu.illinois.cs.cogcomp.xlwikifier.ConfigParameters.setPropValues("xlwikifier-demo.config");
      // do nil clustering
      long nilClusteringStartTime = System.currentTimeMillis();
      long nilClusteringEndTime = 0L;
      try {
        edu.uiuc.xlwikifier.NilClustering nilClusterer =
            new edu.uiuc.xlwikifier.NilClustering(new edu.uiuc.corenextgen.common.UiucModuleConfig("edu/uiuc/xlwikifier/en/xlwikifier-en.xml"));
        log.info("Doing nil-clustering...");
        nilClusterer.process(hltContentContainersFromWikifier);
        nilClusteringEndTime = System.currentTimeMillis();
        nilClusteredHltCCs = hltContentContainersFromWikifier;
        log.info("Done nil-clustering...");
      }catch(Exception e){
        nilClusteringEndTime = System.currentTimeMillis();
        log.error("Nil Clustering caught an exception",e);
        if(throwExceptions){
          throw e;
        }
      }
      log.info("Time taken by NilClustering: {}s",(nilClusteringEndTime-nilClusteringStartTime)/1000);
      log.info("Creating RDD from nil-clustering output...");
      nilClusteredRDD = sc.parallelize(nilClusteredHltCCs,numPartitions);
      log.info("Restoring original corpora for documents...");
      Broadcast<Corpus> corpusBroadcast = sc.broadcast(corpus);
      nilClusteredRDD = nilClusteredRDD.map(
          new Function<HltContentContainer, HltContentContainer>() {
            @Override
            public HltContentContainer call(final HltContentContainer v1) throws Exception {
              v1.getDocument().setCorpus(corpusBroadcast.getValue());
              return v1;
            }
          });
      log.info("Saving nil-clustering checkpoint...");
      E2eUtil.saveCheckpoint(nilClusteredRDD,sc,checkpointDirectory);
//      generateStats(nilClusteredRDD);
      if(e2eConfig.serializeRDDs()) {
        log.info("Serializing nil-clustering output...");
        E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_NIL_CLUSTERING, sc);
        JavaPairRDD<String, String> serializedHltCCs = nilClusteredRDD.mapToPair(
                new PairFunction<HltContentContainer, String, String>() {
                  @Override
                  public Tuple2<String, String> call(
                          final HltContentContainer hltContentContainer)
                          throws Exception {
                    return new Tuple2<>(hltContentContainer.getDocument().getUri(),
                            SerializerUtil.serialize(hltContentContainer));
                  }
                });
        serializedHltCCs.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_NIL_CLUSTERING);
        log.info("Pretty-printing nil-clustering output...");
        E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_NIL_CLUSTERING + "_pprint_output", sc);
        JavaPairRDD<String, String> pprintHltCCs = nilClusteredRDD.mapToPair(
                new PairFunction<HltContentContainer, String, String>() {
                  @Override
                  public Tuple2<String, String> call(
                          final HltContentContainer hltContentContainer)
                          throws Exception {
                    return new Tuple2<>(hltContentContainer.getDocument().getUri(), PprintHltcc.getPprintOutput(hltContentContainer));
                  }
                });
        pprintHltCCs.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_NIL_CLUSTERING + "_pprint_output");
      }
    }
    if(nilClusteredHltCCs.isEmpty()){
      return groupedAlgorithmOutput;
    }
    checkpointDirectory = outputDirectory+"/"+CHECKPOINT_DIR_POST_NIL_CLUSTERING_ALGORITHMS;
    if(E2eUtil.doesHdfsDirectoryExist(checkpointDirectory,sc)){
      log.info("Post nil-clustering algorithms checkpoint found. Loading final algorithm output...");
      groupedAlgorithmOutput = JavaPairRDD.fromJavaRDD(sc.objectFile(checkpointDirectory));
    }else {
      log.info("Pair NilClustered HltCCs with doc-URIs...");
      JavaPairRDD<String, HltContentContainer> nilClusteredRDDWithDocId = nilClusteredRDD.mapToPair(
          new PairFunction<HltContentContainer, String, HltContentContainer>() {
            @Override
            public Tuple2<String, HltContentContainer> call(
                final HltContentContainer hltContentContainer)
                throws Exception {
              return new Tuple2<String, HltContentContainer>(
                  hltContentContainer.getDocument().getUri(),
                  hltContentContainer);
            }
          });
      log.info("Creating composite algo output using NilClustered HltCCs...");
      JavaPairRDD<String, Tuple2<ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>,
          org.apache.spark.api.java.Optional<HltContentContainer>>>
          compositeAlgorithmOutput = groupedAlgorithmOutput.leftOuterJoin(nilClusteredRDDWithDocId);
      // overwrite the original wikifier containers with the nil-clustered ones
      log.info("Overwriting wikification HltCCs with post nil-clustering HltCCs...");
      groupedAlgorithmOutput = compositeAlgorithmOutput.mapValues(
          new Function<Tuple2<ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>, org.apache.spark.api.java.Optional<HltContentContainer>>, ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>>() {
            @Override
            public ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>> call(
                final Tuple2<ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>, org.apache.spark.api.java.Optional<HltContentContainer>> v1)
                throws Exception {
              org.apache.spark.api.java.Optional<HltContentContainer> nilClusteredHltCC = v1._2();
              if (!nilClusteredHltCC.isPresent()) {
                return v1._1();
              }
              List<DocumentResultObject<HltContentContainer, HltContentContainer>> retList =
                  new ArrayList<>(v1._1());
              for (DocumentResultObject<HltContentContainer, HltContentContainer> dro : retList) {
                if (dro.getProperty(ALGORITHM_TYPE).equals(
                    ALGORITHM_TYPE_NIL_CLUSTERING)) {
                  dro.setOutputArtifact(nilClusteredHltCC.get());
                }
              }
              return ImmutableList.copyOf(retList);
            }
          });
      E2eUtil.saveCheckpoint(groupedAlgorithmOutput,sc,checkpointDirectory);
    }
    log.info("Returning groupedAlgorithmOutput...");
    return groupedAlgorithmOutput;
  }

  private JavaPairRDD<String, DocumentResultObject<ImmutableList<DocumentResultObject
      <HltContentContainer, HltContentContainer>>,
      ChunkQuotientSet>>
  alignChunks(
      JavaPairRDD<String, ImmutableList<DocumentResultObject<HltContentContainer,
          HltContentContainer>>>
          groupedAlgorithmOutput, boolean skipThisStage, boolean useMissingEvenTypesHack, JavaSparkContext sc) throws Exception {

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_CHUNK_ALIGNMENT;
    JavaPairRDD<String, DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>>
        alignedChunks = null;
    if (!E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      alignedChunks = groupedAlgorithmOutput
          .mapValues(
              new Function<ImmutableList<DocumentResultObject<HltContentContainer,
                  HltContentContainer>>, DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
                  HltContentContainer>>, ChunkQuotientSet>>() {
                @Override
                public DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>, ChunkQuotientSet> call(
                    final ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>> v1)
                    throws Exception {
                  return DocumentResultObject.create(v1);
                }
              });//not using lambda expression, since on spark this statement with lambda
      // expression was throwing Invalid lambda deserialization exception
    } else {
      log.info("{} checkpoint found, using existing files", STAGE_NAME_CHUNK_ALIGNMENT);
      alignedChunks = JavaPairRDD.fromJavaRDD(sc.objectFile(checkpointDirectory));
      if (skipThisStage) {
        log.info("Skipping this stage based on stages_to_skip e2e property");
        alignedChunks.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(alignedChunks,e2eConfig,language);
        return alignedChunks;
      }
      boolean checkpointHasFailedDocs = checkpointHasFailedDocs(alignedChunks);
      boolean hasNewDocsToProcess = hasNewDocsToProcess
          (alignedChunks, getFilePathsFromRDD(groupedAlgorithmOutput));
//      boolean isRerunRequired = isRerunRequired(alignedChunks,getFilePathsFromRDD
//          (groupedAlgorithmOutput));
      boolean isRerunRequired = checkpointHasFailedDocs || hasNewDocsToProcess;
      if (!isRerunRequired) {
        log.info("No unsuccessful or uprocessed documents found for stage {}",
            STAGE_NAME_CHUNK_ALIGNMENT);
        alignedChunks.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(alignedChunks,e2eConfig,language);
        return alignedChunks;
      } else if (hasNewDocsToProcess) {
        alignedChunks = includeNewDocsInRDD(alignedChunks, groupedAlgorithmOutput, sc);
      }
    }
    //TODO: Eventually, the hack for relaxed chunk alignment should be removed from ChunkAlignerSpark
    //TODO: Eventually, there should be a better way to handle language-specific ChunkAlignment than pass the language as argument
    alignedChunks = ChunkAlignerSpark
        .processHltCCs(alignedChunks, useMissingEvenTypesHack, language, ImmutableList.of(ALGORITHM_TYPE_RELATION_EXTRACTION,ALGORITHM_TYPE_EVENT_EXTRACTION),
                e2eConfig.isDebugMode());
    alignedChunks.persist(StorageLevel
        .MEMORY_AND_DISK());//cache the alignedChunks RDD since it will be used down the line in the pipeline
    E2eUtil.saveCheckpoint(alignedChunks, sc, checkpointDirectory);

    if(e2eConfig.serializeRDDs()) {
      JavaPairRDD<String, String> alignedChunksCSV = alignedChunks.mapValues(
              new AlignedChunksCSVFormatter());
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_CHUNK_ALIGNMENT, sc);
      alignedChunksCSV.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_CHUNK_ALIGNMENT);
    }
    generateStats(alignedChunks, e2eConfig, language);
    return alignedChunks;
  }

  private JavaPairRDD<String, DocumentResultObject<ImmutableList
      <DocumentResultObject
      <HltContentContainer, HltContentContainer>>,
      ChunkQuotientSet>>
  reassignEntityTypes(
      JavaPairRDD<String, DocumentResultObject<ImmutableList<DocumentResultObject
          <HltContentContainer,
          HltContentContainer>>,ChunkQuotientSet>>
          alignedChunks, boolean skipThisStage, JavaSparkContext sc) throws Exception {

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_ENTITY_TYPE_REASSIGNMENT;
    JavaPairRDD<String, DocumentResultObject<DocumentResultObject<ImmutableList
        <DocumentResultObject
        <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,ImmutableList<String>>>
        typeAssignmentOutput = null;
    if (!E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      typeAssignmentOutput = alignedChunks.mapValues(
          (DocumentResultObject<ImmutableList<DocumentResultObject
               <HltContentContainer, HltContentContainer>>,ChunkQuotientSet>
              chunkAlignmentOutput) -> DocumentResultObject.create(chunkAlignmentOutput));
    } else {
      log.info("{} checkpoint found, using existing files", STAGE_NAME_TYPE_REASSIGNMENT);
      typeAssignmentOutput = JavaPairRDD.fromJavaRDD(sc.objectFile(checkpointDirectory));
      if (skipThisStage) {
        log.info("Skipping this stage based on stages_to_skip e2e property");
        typeAssignmentOutput.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(typeAssignmentOutput,e2eConfig,language);
        return typeAssignmentOutput.mapValues(
            (DocumentResultObject<DocumentResultObject<ImmutableList
            <DocumentResultObject
            <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,ImmutableList<String>>
            containersWithTypesReassigned) -> containersWithTypesReassigned.getInputArtifact());
      }
      boolean checkpointHasFailedDocs = checkpointHasFailedDocs(typeAssignmentOutput);
      boolean hasNewDocsToProcess = hasNewDocsToProcess
          (typeAssignmentOutput, getFilePathsFromRDD(alignedChunks));
//      boolean isRerunRequired = isRerunRequired(typeAssignmentOutput,getFilePathsFromRDD
//          (typeAssignmentOutput));
      boolean isRerunRequired = checkpointHasFailedDocs || hasNewDocsToProcess;
      if (!isRerunRequired) {
        log.info("No unsuccessful or uprocessed documents found for stage {}",
            STAGE_NAME_TYPE_REASSIGNMENT);
        typeAssignmentOutput.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(typeAssignmentOutput,e2eConfig,language);
        return typeAssignmentOutput.mapValues(
            (DocumentResultObject<DocumentResultObject<ImmutableList
                <DocumentResultObject
                    <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,ImmutableList<String>>
                containersWithTypesReassigned) -> containersWithTypesReassigned.getInputArtifact());
      } else if (hasNewDocsToProcess) {
        typeAssignmentOutput = includeNewDocsInRDD(typeAssignmentOutput, alignedChunks, sc);
      }
    }
    typeAssignmentOutput = typeAssignmentOutput.mapValues(new EntityTypeReassigner(e2eConfig.algorithmsForEntityTypeReassignment(language),
            e2eConfig.isDebugMode()));
    typeAssignmentOutput.persist(StorageLevel
        .MEMORY_AND_DISK());//cache the typeAssignmentOutput RDD since it will be used down the line in the pipeline
    E2eUtil.saveCheckpoint(typeAssignmentOutput, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      JavaPairRDD<String, String> typeAassignmentDetails = typeAssignmentOutput
              .filter(new
                      E2eUtil.SuccessfulResultObjectsFilter<String,
                              DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
                                      HltContentContainer>>, ChunkQuotientSet>, ImmutableList<String>>())
              .mapValues(
                      new Function<DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>, ChunkQuotientSet>, ImmutableList<String>>, String>() {
                        @Override
                        public String call(
                                final DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>, ChunkQuotientSet>, ImmutableList<String>> v1)
                                throws Exception {
                          return Joiner.on("\n").join(v1.getOutputArtifact().get());
                        }
                      });
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_TYPE_REASSIGNMENT, sc);
      typeAassignmentDetails.saveAsTextFile(outputDirectory + "/" +
              OUTPUT_DIR_TYPE_REASSIGNMENT);
    }
    generateStats(typeAssignmentOutput, e2eConfig, language);
    return typeAssignmentOutput.mapValues(
        (DocumentResultObject<DocumentResultObject<ImmutableList
            <DocumentResultObject
                <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,ImmutableList<String>>
            containersWithTypesReassigned) -> containersWithTypesReassigned.getInputArtifact());
  }

  private JavaPairRDD<String, DocumentResultObject<DocumentResultObject<ImmutableList
      <DocumentResultObject<HltContentContainer, HltContentContainer>>,
      ChunkQuotientSet>, HltContentContainer>> combineAlgorithmContainers(
      JavaPairRDD<String, DocumentResultObject<
          ImmutableList<DocumentResultObject<HltContentContainer, HltContentContainer>>,
          ChunkQuotientSet>> alignedChunks,
      boolean skipThisStage, boolean useMissingEventTypesHack, JavaSparkContext sc) throws Exception {
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_MASTER_CONTAINERS;
    JavaPairRDD<String, DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
        <HltContentContainer, HltContentContainer>>,
        ChunkQuotientSet>, HltContentContainer>> masterAlgorithmContainers = null;
    if (!E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      masterAlgorithmContainers = alignedChunks.mapValues(
          (DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
              HltContentContainer>>, ChunkQuotientSet>
              chunkAlignmentResult) ->
              DocumentResultObject.create(chunkAlignmentResult));
    } else {
      log.info("{} checkpoint found, using existing files", STAGE_NAME_CONTAINER_MERGING);
      masterAlgorithmContainers = JavaPairRDD.fromJavaRDD(sc.objectFile(checkpointDirectory));
      if (skipThisStage) {
        log.info("Skipping this stage based on stages_to_skip e2e property");
        masterAlgorithmContainers.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(masterAlgorithmContainers,e2eConfig,language);
        return masterAlgorithmContainers;
      }
      boolean checkpointHasFailedDocs = checkpointHasFailedDocs(masterAlgorithmContainers);
      boolean hasNewDocsToProcess = hasNewDocsToProcess
          (masterAlgorithmContainers, getFilePathsFromRDD(alignedChunks));
//      boolean isRerunRequired = isRerunRequired(masterAlgorithmContainers,getFilePathsFromRDD
//          (alignedChunks));
      boolean isRerunRequired = checkpointHasFailedDocs || hasNewDocsToProcess;
      if (!isRerunRequired) {
        log.info("No unsuccessful documents found for stage {}", STAGE_NAME_CONTAINER_MERGING);
        masterAlgorithmContainers.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(masterAlgorithmContainers,e2eConfig,language);
        return masterAlgorithmContainers;
      } else if (hasNewDocsToProcess) {
        masterAlgorithmContainers =
            includeNewDocsInRDD(masterAlgorithmContainers, alignedChunks, sc);
      }
    }
    masterAlgorithmContainers = masterAlgorithmContainers.mapToPair(
          new MasterAlgorithmContainer(e2eConfig.isDebugMode(),
              useMissingEventTypesHack,false));
    masterAlgorithmContainers.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(masterAlgorithmContainers, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      log.info("Saving master HltCCs to disk...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_MASTER_CONTAINERS + "_pprint_output", sc);
      JavaPairRDD<String, String> pprintHltCCs = masterAlgorithmContainers.mapValues((DocumentResultObject<
              DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
                      HltContentContainer>>, ChunkQuotientSet>,
              HltContentContainer>
                                                                                              resultObject) ->
              PprintHltcc.getPprintOutput(resultObject.getOutputArtifact()));
      pprintHltCCs.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_MASTER_CONTAINERS + "_pprint_output");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_MASTER_CONTAINERS, sc);
      JavaPairRDD<String, String> serializedMasterContainers =
              masterAlgorithmContainers.mapValues((DocumentResultObject<
                      DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
                              HltContentContainer>>, ChunkQuotientSet>,
                      HltContentContainer>
                                                           resultObject) ->
                      SerializerUtil.serialize(resultObject.getOutputArtifact()));

      serializedMasterContainers.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_MASTER_CONTAINERS);
    }
    generateStats(masterAlgorithmContainers, e2eConfig, language);
    return masterAlgorithmContainers;
  }

  private JavaPairRDD<String, HltContentContainer> extractMasterContainers(JavaPairRDD<String,
      DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
          <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>, HltContentContainer>>
      masterAlgorithmContainers) {
    JavaPairRDD<String, HltContentContainer> masterHltCCs =
        masterAlgorithmContainers.filter(new E2eUtil.SuccessfulResultObjectsFilter
            <String,DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
                HltContentContainer>>, ChunkQuotientSet>,HltContentContainer>()).mapValues(
            (DocumentResultObject<DocumentResultObject<ImmutableList<DocumentResultObject
                <HltContentContainer, HltContentContainer>>, ChunkQuotientSet>,
                HltContentContainer>
                result) ->
                (result.getOutputArtifact().get()));
    masterHltCCs.persist(StorageLevel.MEMORY_AND_DISK());
    return masterHltCCs;
  }

  private Set<String> getDocIdsFromMasterContainers(JavaPairRDD<String, HltContentContainer>
      masterHltCCs) {
    Set<String> docIds = new HashSet<>();
    docIds.addAll(
        masterHltCCs.map(new Function<Tuple2<String, HltContentContainer>,
            String>() {
          @Override
          public String call(Tuple2<String, HltContentContainer> masterHltCC) {
            return masterHltCC._2().getDocumentId();
          }
        }).collect());
    return docIds;
  }

  private JavaRDD<BatchResultObject<ExtractedArtifacts>> extractPartitionLevelArtifacts
      (JavaPairRDD<String, HltContentContainer> masterHltCCs, Set<String>
          masterHltCCDocIds, boolean skipThisStage, JavaSparkContext sc) throws Exception {
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_ARTIFACT_EXTRACTION;
    JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts = null;
    boolean isRerunRequired = true;
    boolean ifCheckpointDirExists = false;
    if (!E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      extractedArtifacts = masterHltCCs.mapPartitions(
          new ArtifactExtractor(e2eConfig.isDebugMode(),algorithmTypeToOntologyFilesMap
              ,e2eConfig.useMissingEventTypesHack(language)));
    } else {
      ifCheckpointDirExists = true;
      log.info("Artifact-extraction checkpoint found, loading existing files");
      extractedArtifacts = sc.objectFile(checkpointDirectory);
      if (skipThisStage) {
        log.info("Skipping this stage based on stages_to_skip e2e property");
        extractedArtifacts.persist(StorageLevel.MEMORY_AND_DISK());
        generateStats(extractedArtifacts,e2eConfig,language);
        return extractedArtifacts;
      }
      //from extracted artifacts, get all the "successful" docIds from last time
      List<String> alreadyProcessedDocIds = getSuccessfulDocIdsFromBatchResult(extractedArtifacts);
      //from masterHltCCDocIds, remove alreadyProcessedDocIds read above
      Set<String> unprocessedDocIds = new HashSet(masterHltCCDocIds);
      unprocessedDocIds.removeAll(alreadyProcessedDocIds);
      //get the masterHltCCs with the to-be-processed docIds
      if (!unprocessedDocIds.isEmpty()) {
        Broadcast<Set<String>> docIdsToProcess = sc.broadcast(unprocessedDocIds);
        masterHltCCs = masterHltCCs.filter(
            new Function<Tuple2<String, HltContentContainer>, Boolean>() {
              @Override
              public Boolean call(final Tuple2<String, HltContentContainer> v1) throws Exception {
                return docIdsToProcess.value().contains(v1._2().getDocumentId());
              }
            });
        //merge new extracted-artifacts with the chekcpointed ones
        extractedArtifacts = extractedArtifacts.union(masterHltCCs.mapPartitions(
            new ArtifactExtractor(e2eConfig.isDebugMode(),algorithmTypeToOntologyFilesMap,
                e2eConfig.useMissingEventTypesHack(language))));
      } else {
        isRerunRequired = false;
      }
    }
    extractedArtifacts.persist(StorageLevel.MEMORY_AND_DISK());
    if (!isRerunRequired) {
      generateStats(extractedArtifacts,e2eConfig,language);
      return extractedArtifacts;
    }

    if(isRerunRequired&&ifCheckpointDirExists){
      //If artifact-extraction results in new documents, a KB re-upload might be required for this language
      //which is simply not allowed in multlingual E2E
//      throw new CannotProceedWithoutClearingKBException(language,STAGE_NAME_ARTIFACT_EXTRACTION);
      //TODO: Ideally, the WARN message should be to ask user to delete this checkpoint and all checkpoints that follow in the
      //pipeline. But, since we're not doing that currently, it's fine to skip this exception.
    }
    //save extractedArtifacts checkpoint
    E2eUtil.saveCheckpoint(extractedArtifacts, sc, checkpointDirectory);
    //save serialized extractedArtifacts
    if(e2eConfig.serializeRDDs()) {
      log.info("Saving extracted artifacts...");
      JavaRDD<String> serializedArtifacts = extractedArtifacts.map(
              new Function<BatchResultObject<ExtractedArtifacts>, String>() {
                @Override
                public String call(final BatchResultObject<ExtractedArtifacts> v1) throws Exception {
                  return SerializerUtil.serialize(v1);
                }
              });
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_ARTIFACT_EXTRACTION, sc);
      serializedArtifacts.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_ARTIFACT_EXTRACTION);
    }
    generateStats(extractedArtifacts, e2eConfig, language);
    return extractedArtifacts;
  }

  private JavaRDD<BatchResultObject<ExtractedArtifacts>> dropArtifactsWithMinorityAddnlType(final
    JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts, JavaSparkContext sc)
      throws Exception {

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_ARTIFACT_FILTERING;
    JavaRDD<BatchResultObject<ExtractedArtifacts>>
        filteredExtractedArtifacts = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Artifact filtering checkpoint found, using existing output...");
      filteredExtractedArtifacts = sc.objectFile(checkpointDirectory);
      filteredExtractedArtifacts.persist(StorageLevel.MEMORY_AND_DISK());
      generateStats(filteredExtractedArtifacts,e2eConfig,language);
      return filteredExtractedArtifacts;
    }
    log.info("Collecting additional types for entities from extractedArtifacts...");
    JavaRDD<BatchResultObject<ImmutableMultimap<EntityKey,OntType>>>
        entityKeyToAdditionalTypesMap = extractedArtifacts.mapPartitions(new
        EntityKeyToAdditionalTypesMapper(e2eConfig.isDebugMode()));
    log.info("Reducing EntityKey to additional-types maps...");
    BatchResultObject<ImmutableMultimap<EntityKey,OntType>>
        reducedEntityKeyToAdditionalTypesMap = entityKeyToAdditionalTypesMap.reduce(new
        EntityKeyToAdditionalTypesReducer(e2eConfig.isDebugMode()));
    if(!reducedEntityKeyToAdditionalTypesMap.isSuccessful()){
      log.error("Something went wrong while reducing EntityKey to additionalTypes maps");
      generateStats(entityKeyToAdditionalTypesMap,e2eConfig,language);
      return extractedArtifacts;
    }
    generateStats(entityKeyToAdditionalTypesMap,e2eConfig,language);//to print out counts of additional types from
    // batchResultObjects' properties
    ImmutableMap.Builder entityKeyToMajorityAdditionalType = ImmutableMap.builder();
    ImmutableMultimap<EntityKey,OntType> finalEntityKeyToAdditionalTypesMap =
        reducedEntityKeyToAdditionalTypesMap
        .getOutputArtifact().get();
    for(EntityKey entityKey : finalEntityKeyToAdditionalTypesMap.keySet()){
      Multiset<OntType> typeMultiSet  = HashMultiset.create(finalEntityKeyToAdditionalTypesMap.get
          (entityKey));
      typeMultiSet = Multisets.copyHighestCountFirst(typeMultiSet);
      entityKeyToMajorityAdditionalType.put(entityKey,typeMultiSet.iterator().next());
    }
    log.info("Filtering extractedArtifacts to drop relations/events with Entity args "
        + "having minority additional types...");
    filteredExtractedArtifacts = extractedArtifacts.map(new AdditionalArgTypeArtifactFilter
        (e2eConfig.isDebugMode(),entityKeyToMajorityAdditionalType.build()));
    filteredExtractedArtifacts.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(filteredExtractedArtifacts, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      log.info("Saving filtered extracted artifacts...");
      JavaRDD<String> serializedArtifacts = filteredExtractedArtifacts.map(
              new Function<BatchResultObject<ExtractedArtifacts>, String>() {
                @Override
                public String call(final BatchResultObject<ExtractedArtifacts> v1) throws Exception {
                  return SerializerUtil.serialize(v1);
                }
              });

      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_ARTIFACT_FILTERING, sc);
      serializedArtifacts.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_ARTIFACT_FILTERING);
    }
    generateStats(filteredExtractedArtifacts,e2eConfig,language);
    return filteredExtractedArtifacts;
  }

  private JavaRDD<BatchResultObject<UploadedEntities>> uploadBatchLevelEntities(final
  JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts, final KBParameters
      kbParameters, JavaSparkContext sc) throws Exception {

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_ENTITY_UPLOAD;
    JavaRDD<BatchResultObject<UploadedEntities>> uploadedEntities = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Batch-entity-upload checkpoint found...");
      uploadedEntities = sc.objectFile(checkpointDirectory);
      uploadedEntities.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(uploadedEntities, e2eConfig, language);
      return uploadedEntities;
    }
    log.info("Uploading batch-level entities..");
    uploadedEntities = extractedArtifacts
        .mapPartitions(new EntityUploader(kbParameters, e2eConfig.isDebugMode()));
    uploadedEntities.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(uploadedEntities, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      log.info("Serializing uploaded entities map...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_ENTITY_UPLOAD, sc);
      uploadedEntities.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_ENTITY_UPLOAD);
    }
    generateStatsLite(uploadedEntities, e2eConfig, language);
    return uploadedEntities;
  }

  private JavaRDD<BatchResultObject<UploadedNonEntityArguments>> uploadNonEntityArguments(final
  JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts, final KBParameters
      kbParameters, JavaSparkContext sc) throws Exception {
    //extract ExtractedArguments from ExtractedArtifacts; coalesce them in 1 partition; and upload them
    //to ensure single upload of all non-Entity arguments

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_NON_ENTITY_ARG_UPLOAD;
    JavaRDD<BatchResultObject<UploadedNonEntityArguments>> uploadedNonEntityArguments = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Non-entity-argument-upload checkpoint found...");
      uploadedNonEntityArguments = sc.objectFile(checkpointDirectory);
      uploadedNonEntityArguments.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(uploadedNonEntityArguments, e2eConfig, language);
      return uploadedNonEntityArguments;
    }
    log.info("Uploading batch-level non-entity arguments..");
    JavaRDD<BatchResultObject<ExtractedArguments>> extractedArguments =
        extractedArtifacts.map(
            new Function<BatchResultObject<ExtractedArtifacts>, BatchResultObject<ExtractedArguments>>() {
              @Override
              public BatchResultObject<ExtractedArguments> call(
                  final BatchResultObject<ExtractedArtifacts> v1)
                  throws Exception {
                if(!v1.getOutputArtifact().isPresent()){
                  return null;
                }
                BatchResultObject<ExtractedArguments> retVal = BatchResultObject.createEmpty();
                retVal.setOutputArtifact(v1.getOutputArtifact().get().extractedArguments());
                retVal.setArtifactIdsInvolved(v1.getArtifactIds().get());
                retVal.markSuccessful();
                return retVal;
              }
            }).filter(new E2eUtil.NonNullRDDFilter<>());
    extractedArguments = extractedArguments.coalesce(1);
    uploadedNonEntityArguments = extractedArguments
        .mapPartitions(new NonEntityArgUploader(kbParameters, e2eConfig.isDebugMode()));
    uploadedNonEntityArguments.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(uploadedNonEntityArguments, sc, checkpointDirectory);

    if(e2eConfig.serializeRDDs()) {
      log.info("Serializing uploaded non entity args map...");
      UploadedNonEntityArguments dedupArgsKBIDMap =
          E2eUtil.getFinalNonEntityArgKBIDs(uploadedNonEntityArguments);
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_NON_ENTITY_ARG_UPLOAD, sc);
      JavaRDD<String> serializedKBIDMaps =
          sc.parallelize(ImmutableList.of(SerializerUtil.serialize(dedupArgsKBIDMap))).coalesce(1);
      serializedKBIDMaps.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_NON_ENTITY_ARG_UPLOAD);
    }
    generateStatsLite(uploadedNonEntityArguments,e2eConfig,language);
    return uploadedNonEntityArguments;
  }

  private JavaRDD<BatchResultObject<UploadedEntities>> deduplicateEntities(final
  JavaRDD<BatchResultObject<UploadedEntities>> externalKBIDToInsertedEntityMap, final
  KBParameters
      kbParameters, JavaSparkContext sc) throws Exception {

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_ENTITY_DEDUP;
    JavaRDD<BatchResultObject<UploadedEntities>> deduplicatedEntities = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Entity-deduplication checkpoint found, loading kbid map...");
      deduplicatedEntities = sc.objectFile(checkpointDirectory);
      deduplicatedEntities.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(deduplicatedEntities, e2eConfig, language);
      return deduplicatedEntities;
    }
    JavaRDD<BatchResultObject<UploadedEntities>> coalescedKBIDMap =
        externalKBIDToInsertedEntityMap
            .coalesce(1);
    log.info("Deduplicating uploaded entities...");
    deduplicatedEntities = coalescedKBIDMap
        .mapPartitions(new EntityDeduplicator(kbParameters, e2eConfig.isDebugMode()));
    deduplicatedEntities.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(deduplicatedEntities, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      log.info("Serializing details of deduplicated entities...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_ENTITY_DEDUP, sc);
      deduplicatedEntities.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_ENTITY_DEDUP);
    }
    generateStatsLite(deduplicatedEntities,e2eConfig,language);
    return deduplicatedEntities;
  }

  /*private JavaRDD<BatchResultObject<UploadedNonEntityArguments>> deduplicateNonEntityArguments(final
  JavaRDD<BatchResultObject<UploadedNonEntityArguments>> UploadedNonEntityArguments, final
  KBParameters
      kbParameters, JavaSparkContext sc) throws Exception {

    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_NON_ENTITY_ARG_DEDUP;
    JavaRDD<BatchResultObject<UploadedNonEntityArguments>> deduplicatedNonEntityArgs = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Non-entity-arguments-deduplication checkpoint found, loading kbid map...");
      deduplicatedNonEntityArgs = sc.objectFile(checkpointDirectory);
      deduplicatedNonEntityArgs.persist(StorageLevel.MEMORY_AND_DISK());
      generateStats(deduplicatedNonEntityArgs,e2eConfig,language);
      return deduplicatedNonEntityArgs;
    }
    JavaRDD<BatchResultObject<UploadedNonEntityArguments>> coalescedKBIDMap =
        UploadedNonEntityArguments
            .coalesce(1);
    log.info("Deduplicating uploaded non-entity arguments...");
    deduplicatedNonEntityArgs = coalescedKBIDMap
        .mapPartitions(new NonEntityArgDeduplicator(kbParameters, e2eConfig.isDebugMode()));
    deduplicatedNonEntityArgs.persist(StorageLevel.MEMORY_AND_DISK());
    log.info("Serializing details of deduplicated non-entity arguments...");
    E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_NON_ENTITY_ARG_DEDUP, sc);
    deduplicatedNonEntityArgs.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_NON_ENTITY_ARG_DEDUP);
    E2eUtil.saveCheckpoint(deduplicatedNonEntityArgs, sc, checkpointDirectory);
    generateStats(deduplicatedNonEntityArgs,e2eConfig,language);
    return deduplicatedNonEntityArgs;
  }*/

  public JavaRDD<BatchResultObject<UploadedRelations>> uploadBatchLevelRelations
      (JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts, UploadedEntities deduplicatedEntities,
          UploadedNonEntityArguments deduplicatedNonEntityArgs,
          KBParameters kbParameters, JavaSparkContext sc) throws Exception {
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_RELATION_UPLOAD;
    JavaRDD<BatchResultObject<UploadedRelations>> uploadedRelations = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Relation-upload checkpoint found, loading uploaded relations...");
      uploadedRelations = sc.objectFile(checkpointDirectory);
      uploadedRelations.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(uploadedRelations, e2eConfig, language);
      return uploadedRelations;
    }
    log.info("Uploading batch-level relations..");
    uploadedRelations =
        extractedArtifacts.mapPartitions(new RelationUploader(kbParameters, deduplicatedEntities, deduplicatedNonEntityArgs,
            e2eConfig.isDebugMode()));
    uploadedRelations.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(uploadedRelations, sc, checkpointDirectory);
    //serialize globally unique arguments
    if(e2eConfig.serializeRDDs()) {
      log.info("Saving uploaded relations to disk...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_RELATION_UPLOAD, sc);
      uploadedRelations.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_RELATION_UPLOAD);
    }
    generateStatsLite(uploadedRelations, e2eConfig, language);
    return uploadedRelations;
  }

  public JavaRDD<BatchResultObject<ImmutableMap<RelationKey, KBID>>> deduplicateRelations(final
  JavaRDD<BatchResultObject<UploadedRelations>>
      uploadedRelations, final KBParameters kbParameters,
      JavaSparkContext sc) throws Exception {
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_RELATION_DEDUP;
    JavaRDD<BatchResultObject<ImmutableMap<RelationKey, KBID>>> deduplicatedRelations = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Relation-deduplication checkpoint found, loading kbid map...");
      deduplicatedRelations = sc.objectFile(checkpointDirectory);
      deduplicatedRelations.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(deduplicatedRelations,e2eConfig,language);
      return deduplicatedRelations;
    }
    log.info("Coalescing batch-level uploaded relations...");
    JavaRDD<BatchResultObject<UploadedRelations>> coalescedKBIDMap = uploadedRelations.coalesce(1);
    log.info("Deduplicating uploaded relations...");
    deduplicatedRelations = coalescedKBIDMap
        .mapPartitions(new RelationDeduplicator(kbParameters, e2eConfig.isDebugMode()));
    deduplicatedRelations.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(deduplicatedRelations, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      log.info("Serializing details of deduplicated relations...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_RELATION_DEDUP, sc);
      deduplicatedRelations.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_RELATION_DEDUP);
    }
    generateStatsLite(deduplicatedRelations, e2eConfig, language);
    return deduplicatedRelations;
  }

  public JavaRDD<BatchResultObject<UploadedEvents>> uploadBatchLevelEvents
      (JavaRDD<BatchResultObject<ExtractedArtifacts>> extractedArtifacts,
          UploadedEntities deduplicatedEntities, UploadedNonEntityArguments UploadedNonEntityArguments,
          KBParameters kbParameters, JavaSparkContext sc) throws Exception {
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_EVENT_UPLOAD;
    JavaRDD<BatchResultObject<UploadedEvents>> uploadedEvents = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Event-upload checkpoint found, loading uploaded events...");
      uploadedEvents = sc.objectFile(checkpointDirectory);
      uploadedEvents.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(uploadedEvents, e2eConfig, language);
      return uploadedEvents;
    }
    log.info("Uploading batch-level events..");
    uploadedEvents =
        extractedArtifacts.mapPartitions(new EventUploader(kbParameters, deduplicatedEntities,
            UploadedNonEntityArguments, e2eConfig.isDebugMode()));
    uploadedEvents.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(uploadedEvents, sc, checkpointDirectory);
    //serialize globally unique arguments
    if(e2eConfig.serializeRDDs()) {
      log.info("Saving uploaded events to disk...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_EVENT_UPLOAD, sc);
      uploadedEvents.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_EVENT_UPLOAD);
    }
    generateStatsLite(uploadedEvents,e2eConfig,language);
    return uploadedEvents;
  }

  public JavaRDD<BatchResultObject<ImmutableMap<EventKey, KBID>>> deduplicateEvents(final
  JavaRDD<BatchResultObject<UploadedEvents>>
      uploadedEvents, final KBParameters kbParameters, JavaSparkContext sc) throws Exception {
    String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_EVENT_DEDUP;
    JavaRDD<BatchResultObject<ImmutableMap<EventKey, KBID>>> deduplicatedEvents = null;
    if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
      log.info("Event-deduplication checkpoint found, loading kbid map...");
      deduplicatedEvents = sc.objectFile(checkpointDirectory);
      deduplicatedEvents.persist(StorageLevel.MEMORY_AND_DISK());
      generateStatsLite(deduplicatedEvents, e2eConfig, language);
      return deduplicatedEvents;
    }
    log.info("Coalescing batch-level uploaded events...");
    JavaRDD<BatchResultObject<UploadedEvents>> coalescedKBIDMap = uploadedEvents.coalesce(1);
    log.info("Deduplicating uploaded events...");
    deduplicatedEvents = coalescedKBIDMap
        .mapPartitions(new EventDeduplicator(kbParameters, e2eConfig.isDebugMode()));
    deduplicatedEvents.persist(StorageLevel.MEMORY_AND_DISK());
    E2eUtil.saveCheckpoint(deduplicatedEvents, sc, checkpointDirectory);
    if(e2eConfig.serializeRDDs()) {
      log.info("Serializing details of deduplicated events...");
      E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_EVENT_DEDUP, sc);
      deduplicatedEvents.saveAsTextFile(outputDirectory + "/" + OUTPUT_DIR_EVENT_DEDUP);
    }
    generateStatsLite(deduplicatedEvents, e2eConfig, language);
    return deduplicatedEvents;
  }

  public void uploadDocumentText(JavaPairRDD<String, HltContentContainer> hltContentContainers,
      KBParameters kbParameters, JavaSparkContext sc) throws Exception {
    if(E2eUtil.doesHdfsDirectoryExist(outputDirectory+"/"+CHECKPOINT_DIR_TEXT_UPLOAD,sc)){
      log.info("Text-upload checkpoint found, skipping re-upload...");
      return;
    }
    Broadcast<Boolean> isDebugMode = sc.broadcast(e2eConfig.isDebugMode());
    JavaPairRDD<String, String> documentText = hltContentContainers.mapToPair(
        new PairFunction<Tuple2<String, HltContentContainer>, String, String>() {
          @Override
          public Tuple2<String, String> call(
              final Tuple2<String, HltContentContainer> inPair) throws Exception {
            Tuple2<String, String> retVal = null;
            try {
              retVal = new Tuple2<String, String>(inPair._2().getDocumentId(),
                  inPair._2().getDocument().getTokenStream(
                      TokenizerType.STANFORD_CORENLP).getTextValue());
            } catch (Exception e) {
              if (isDebugMode.getValue()) {
                throw e;
              }
              log.error("Caught exception trying get text from hltCC {}", inPair._1(), e);
            }
            return retVal;
          }
        }).filter(new E2eUtil.NonNullRDDFilter<Tuple2<String, String>>());
    documentText.persist(StorageLevel.MEMORY_AND_DISK());
    Broadcast<KBParameters> kbParametersBroadcast = sc.broadcast(kbParameters);
    Broadcast<String> corpusId = sc.broadcast(e2eConfig.corpusId());
    JavaRDD<String> uploadedText = documentText.map(new Function<Tuple2<String, String>, String>
        () {
      @Override
      public String call(Tuple2<String, String> inPair) throws Exception {
        try {
          KB kb = KBSingleton.getInstance(kbParametersBroadcast.getValue());
          kb.saveDocumentText(inPair._1(), corpusId.value(), inPair._2());
        } catch (Exception e) {
          if (isDebugMode.getValue()) {
            throw e;
          }
          log.error("Caught exception trying to upload text for {}", inPair._1(), e);
          return null;
        }
        return inPair._1();
      }
    }).filter(new E2eUtil.NonNullRDDFilter<String>());
    E2eUtil.saveCheckpoint(uploadedText, sc, outputDirectory + "/" + CHECKPOINT_DIR_TEXT_UPLOAD);
  }


}
