package adept.e2e.driver;

import adept.common.KBID;
import adept.e2e.kbupload.uploadedartifacts.UploadedEntities;
import adept.kbapi.KBParameters;
import adept.utilities.ClearKB;
import com.google.common.collect.ImmutableMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import adept.e2e.analysis.KBReportUtil;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.kbresolver.KBResolverAbstractModule;
import adept.e2e.kbupload.artifactdeduplicator.EntityDeduplicator;
import adept.e2e.kbupload.artifactdeduplicator.EventDeduplicator;
import adept.e2e.kbupload.artifactdeduplicator.RelationDeduplicator;
import adept.e2e.kbupload.uploadedartifacts.UploadedEvents;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.kbupload.uploadedartifacts.UploadedRelations;
import adept.e2e.stageresult.BatchResultObject;


import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_ENTITY_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_ENTITY_UPLOAD;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_EVENT_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_EVENT_UPLOAD;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_MULTILINGUAL_ENTITY_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_MULTILINGUAL_EVENT_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_MULTILINGUAL_NON_ENTITY_ARG_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_MULTILINGUAL_RELATION_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_NON_ENTITY_ARG_UPLOAD;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_RELATION_DEDUP;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_RELATION_UPLOAD;
import static adept.e2e.driver.E2eConstants.CHECKPOINT_DIR_TEXT_UPLOAD;
import static adept.e2e.driver.E2eConstants.LANGUAGE;
import static adept.e2e.driver.E2eConstants.OUTPUT_DIR_MULTILINGUAL_ENTITY_DEDUP;
import static adept.e2e.driver.E2eConstants.OUTPUT_DIR_MULTILINGUAL_EVENT_DEDUP;
import static adept.e2e.driver.E2eConstants.OUTPUT_DIR_MULTILINGUAL_RELATION_DEDUP;
import static adept.e2e.driver.E2eUtil.generateGlobalStatsLite;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is the main A2KD (short for Adept Automatic Knowledge Discovery) (also referred to as e2e for End2End) driver class.
 * This class is responsible to instantiate language-specific @{@link E2eDriver}s and run their artifact-extraction pipeline.
 * Subsequent to running the artifact-extraction pipeline for all languages, the main A2KD driver does global (or corpus-level)
 * de-duplication of all the artifacts. This is followed by running the KB-resolver and generating the KB-reports (depending on the
 * run configuration).
 *
 * @author msrivast
 */
public class MainE2eDriver implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(MainE2eDriver.class);
    private static enum ARTIFACT_TYPE {ENTITY, NON_ENTITY_ARGS, RELATION, EVENT}
    private final E2eConfig e2eConfig;
    private final String outputDirectory;

    private MainE2eDriver(E2eConfig e2eConfig, String
            outputDirectory) {
        this.e2eConfig = e2eConfig;
        this.outputDirectory = outputDirectory;
    }

    public static void main(String[] args) throws Exception {

        String outputDirectory = args[0];
        int numPartitions = Integer.parseInt(args[1]);
        String configFilePath = args[2];
        log.info("outputDirectory={}", outputDirectory);
        log.info("Creating E2eConfig from input config file...");
        E2eConfig e2eConfig = E2eConfig.getInstance(configFilePath);
        MainE2eDriver topLevelDriver = new MainE2eDriver(e2eConfig,
                outputDirectory);
        SparkConf sparkConf = new SparkConf().setAppName("adept-e2e").set("spark.driver.maxResultSize", "0");
        if (args.length == 4 && "local".equals(args[3])) {
            sparkConf.setMaster("local");
        }
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.hadoopConfiguration().set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE, "true");
        try {
            E2eUtil.clearTempHdfsDirectories(outputDirectory, sc);
            log.info("Starting A2KD pipeline....");
            if (e2eConfig.ifClearKB()) {
                log.info("Clearing the KB....");
                topLevelDriver.clearKB(e2eConfig.kbParameters());
                log.info("Clearing KB checkpoints for all languages...");
                topLevelDriver.clearKBCheckpoints(e2eConfig.languages(), sc);
            }
            log.info("Creating spark-context...");
            Map<LANGUAGE, E2eDriver> languageSpecificDrivers = new HashMap<>();
            Map<ARTIFACT_TYPE, List<String>> artifactUploadCheckpointDirs = new HashMap<>();
            //extract artifacts for every language
            for (LANGUAGE language : e2eConfig.languages()) {
                String inputDirectory = e2eConfig.inputDirectories().get(language);
                String languageOutputDirectory = outputDirectory + "/" + language;
                E2eUtil.createHdfsDirectory(languageOutputDirectory, sc);
                E2eDriver languageSpecificDriver = new E2eDriver(inputDirectory, languageOutputDirectory,
                        numPartitions, e2eConfig, language);
                languageSpecificDrivers.put(language, languageSpecificDriver);
                log.info("Running E2eDriver for language: {}", language);
                languageSpecificDriver.extractArtifacts(sc);
            }

            //TODO: Since there are much fewer Chinese entities than English entities extracted from TAC eval corpus, we lose quite a bit of time by doing a language-specific entity deduplication before a global deduplication. Currently, I am changing that to do only one entity (or any artifact) deduplication at the global level. This may not be efficient in all scenarios (especially the ones where we expect to reduce the entity-set a big deal by doing language-specific deduplication, e.g. dense entity corpora), and ideally should be configurable, but given the timeline of eval, the current scheme (with a single deduplication stage) is the best one to have.
            //upload entities for every language
            for (LANGUAGE language : e2eConfig.languages()) {
                E2eDriver driver = languageSpecificDrivers.get(language);
                log.info("Uploading entities for language: {}", language);
                driver.uploadEntities(sc);
                includeArtifactUploadCheckpointDirs(artifactUploadCheckpointDirs, driver.getOutputDirectory(),
                        ARTIFACT_TYPE.ENTITY);
            }
            log.info("Doing global deduplication of entities");
            JavaRDD<BatchResultObject<UploadedEntities>> globallyDeduplicatedEntities =
                    topLevelDriver
                            .deduplicateEntities(artifactUploadCheckpointDirs.get(ARTIFACT_TYPE.ENTITY), sc);
            //upload non-entity args for every language
            for (LANGUAGE language : e2eConfig.languages()) {
                E2eDriver driver = languageSpecificDrivers.get(language);
                log.info("Uploading non-entity arguments for language: {}", language);
                driver.uploadNonEntityArgs(sc);
                includeArtifactUploadCheckpointDirs(artifactUploadCheckpointDirs, driver.getOutputDirectory(),
                        ARTIFACT_TYPE.NON_ENTITY_ARGS);
            }
            log.info("Doing global deduplication of non-entity arguments");
            JavaRDD<BatchResultObject<UploadedNonEntityArguments>> globallyDeduplicatedNonEntityArgs =
                    topLevelDriver
                            .combineAllNonEntityArguments(artifactUploadCheckpointDirs.get(ARTIFACT_TYPE.NON_ENTITY_ARGS), sc);

            log.info("Collecting globally deduplicated entities map...");
            UploadedEntities deduplicatedEntitiesMap = globallyDeduplicatedEntities.collect().get(0).getOutputArtifact().orNull();
            checkNotNull(deduplicatedEntitiesMap, "Deduplicated Entities Map cannot be null");
            UploadedNonEntityArguments deduplicatedNonEntityArgsMap = E2eUtil.getFinalNonEntityArgKBIDs(globallyDeduplicatedNonEntityArgs);
            //upload relations and events
            for (LANGUAGE language : e2eConfig.languages()) {
                log.info("Uploading relations for language: {}", language);
                E2eDriver driver = languageSpecificDrivers.get(language);
                //upload batch-level relations
                JavaRDD<BatchResultObject<UploadedRelations>> uploadedRelations =
                        driver.uploadBatchLevelRelations(
                                driver.getExtractedArtifacts(sc), deduplicatedEntitiesMap, deduplicatedNonEntityArgsMap, e2eConfig.kbParameters(),
                                sc);
                includeArtifactUploadCheckpointDirs(artifactUploadCheckpointDirs, driver.getOutputDirectory(), ARTIFACT_TYPE.RELATION);
                //upload batch-level events (deduplication of arguments can happen when all of them
                // have been uploaded to the KB)
                JavaRDD<BatchResultObject<UploadedEvents>> uploadedEvents = sc.emptyRDD();
//       if(!e2eConfig.serializeRDDs()) {
                log.info("Uploading events for language: {}", language);
                uploadedEvents = driver.uploadBatchLevelEvents(
                        driver.getExtractedArtifacts(sc), deduplicatedEntitiesMap, deduplicatedNonEntityArgsMap, e2eConfig.kbParameters(),sc);
                includeArtifactUploadCheckpointDirs(artifactUploadCheckpointDirs, driver.getOutputDirectory(), ARTIFACT_TYPE.EVENT);
                //unpersist unwanted RDDs
                log.info("Unpersisting extractedArtifacts RDD for language: {}", language);
                driver.getExtractedArtifacts(sc).unpersist();
                log.info("Unpersisting uploadedRelations RDD for language: {}", language);
                uploadedRelations.unpersist();
                log.info("Unpersisting uploadedEvents RDD for language: {}", language);
                uploadedEvents.unpersist();
            }
            //do
            //deduplicate relations and events
            log.info("Doing global deduplication of relations");
            topLevelDriver
                     .deduplicateRelations(artifactUploadCheckpointDirs.get(ARTIFACT_TYPE.RELATION), sc);
            log.info("Doing global deduplication of events");
                topLevelDriver.deduplicateEvents(artifactUploadCheckpointDirs.get(ARTIFACT_TYPE.EVENT), sc);

            boolean kbResolverRanSuccessfully = true;
            if (!e2eConfig.kbResolverProperties().isEmpty()) {
                try {
                    runKbResolver(sc, e2eConfig.kbParameters(), e2eConfig.kbResolverProperties());
                } catch (Exception e) {
                    log.error("KB Resolver threw the following exception: ", e);
                    kbResolverRanSuccessfully = false;
                }
            }
            if (kbResolverRanSuccessfully && e2eConfig.generateKBReports()) {
                log.info("Generating KB-reports after E2E run...");
                KBReportUtil.KBSummaryMap
                        kbSummaryPostRun = E2eUtil.generateKBReports(e2eConfig.kbParameters(), e2eConfig
                        .corpusId(), e2eConfig.kbReportsOutputDir().get());
                log.info("Writing KB-summary...");
                E2eUtil.generateKBSummary(e2eConfig.kbParameters(), e2eConfig.kbReportsOutputDir()
                        .get(), kbSummaryPostRun);
            }
            if (!kbResolverRanSuccessfully) {
                log.info("KBResolver ran with errors. This means that the KB may be in a corrupted state. "
                        + "Please set clear_kb config to true and rerun A2KD.");
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        log.info("Finished running End2End pipeline.");
        sc.stop();
    }

    private static void includeArtifactUploadCheckpointDirs(Map<ARTIFACT_TYPE, List<String>>
                                                                    artifactUploadCheckpointDirs, String outputDirectory, ARTIFACT_TYPE artifactType) {
        String uploadCheckpointDir = outputDirectory + "/";
        if (artifactType.equals(ARTIFACT_TYPE.ENTITY)) {
            uploadCheckpointDir += CHECKPOINT_DIR_ENTITY_UPLOAD;
        } else if (artifactType.equals(ARTIFACT_TYPE.NON_ENTITY_ARGS)) {
            uploadCheckpointDir += CHECKPOINT_DIR_NON_ENTITY_ARG_UPLOAD;
        } else if (artifactType.equals(ARTIFACT_TYPE.RELATION)) {
            uploadCheckpointDir += CHECKPOINT_DIR_RELATION_UPLOAD;
        } else if (artifactType.equals(ARTIFACT_TYPE.EVENT)) {
            uploadCheckpointDir += CHECKPOINT_DIR_EVENT_UPLOAD;
        }
        List<String> allUploadDirs = artifactUploadCheckpointDirs.get(artifactType);
        if (allUploadDirs == null) {
            allUploadDirs = new ArrayList<>();
        }
        allUploadDirs.add(uploadCheckpointDir);
        artifactUploadCheckpointDirs.put(artifactType, allUploadDirs);
    }

    private static Iterable<KBResolverAbstractModule> runKbResolver(JavaSparkContext sc, KBParameters kbParameters, Properties kbResolverProperties) throws Exception {
        log.info("Running KB Resolver");
        Iterable<KBResolverAbstractModule> runModules = KBResolverSpark.run(sc, kbParameters, kbResolverProperties);
        log.info("Finished running KB Resolver");
        return runModules;
    }

    private void clearKB(KBParameters kbParameters) throws Exception {
        log.info("Clearing the KB...");
        ClearKB.clearKB(kbParameters);
        //When you clear the KB, create a KB instance from adept.e2e.driver.driver to ensure
        // one-time-loading of the
        // ontology
        log.info("Creating a KB instance from adept.e2e.driver.driver to force loading of ontology...");
        KBSingleton.getInstance(kbParameters);
    }

    private void clearKBCheckpoints(List<E2eConstants.LANGUAGE> languages, JavaSparkContext sc) throws Exception {
        for (LANGUAGE language : languages) {
            String languageOutputDirectory = outputDirectory + "/" + language;
            if (!(E2eUtil
                    .deleteHdfsDirectory(languageOutputDirectory + "/" + CHECKPOINT_DIR_ENTITY_UPLOAD,
                            sc) &&
                    E2eUtil.deleteHdfsDirectory(
                            languageOutputDirectory + "/" + CHECKPOINT_DIR_ENTITY_DEDUP, sc)
                    &&
                    E2eUtil.deleteHdfsDirectory(
                            languageOutputDirectory + "/" + CHECKPOINT_DIR_NON_ENTITY_ARG_UPLOAD, sc)
                    &&
                    E2eUtil
                            .deleteHdfsDirectory(
                                    languageOutputDirectory + "/" + CHECKPOINT_DIR_RELATION_UPLOAD, sc)
                    && E2eUtil
                    .deleteHdfsDirectory(languageOutputDirectory + "/" + CHECKPOINT_DIR_RELATION_DEDUP,
                            sc) && E2eUtil
                    .deleteHdfsDirectory(languageOutputDirectory + "/" + CHECKPOINT_DIR_EVENT_UPLOAD, sc)
                    &&
                    E2eUtil
                            .deleteHdfsDirectory(languageOutputDirectory + "/" + CHECKPOINT_DIR_EVENT_DEDUP,
                                    sc) &&
                    E2eUtil
                            .deleteHdfsDirectory(languageOutputDirectory + "/" + CHECKPOINT_DIR_TEXT_UPLOAD,
                                    sc))) {
                throw new Exception("Could not delete all kb-checkpoints for language: " + language);
            }
        }
        String entityDir = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_ENTITY_DEDUP;
        String nonEntityArgDir = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_NON_ENTITY_ARG_DEDUP;
        String relationDir = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_RELATION_DEDUP;
        String eventDir = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_EVENT_DEDUP;
        if (!(E2eUtil.deleteHdfsDirectory(entityDir, sc) && E2eUtil.deleteHdfsDirectory(relationDir, sc) &&
                E2eUtil.deleteHdfsDirectory(eventDir, sc) && E2eUtil.deleteHdfsDirectory(nonEntityArgDir, sc))) {
            throw new Exception("Could not delete all global dedup checkpoints");
        }
    }

    private JavaRDD<BatchResultObject<UploadedEntities>> deduplicateEntities(List<String>
                                                                                                  allUploadCheckpointDirs, JavaSparkContext sc) throws Exception {
        String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_ENTITY_DEDUP;
        JavaRDD<BatchResultObject<UploadedEntities>> deduplicatedEntities = sc.emptyRDD();
        if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
            log.info("Global entity-deduplication checkpoint found, loading kbid map...");
            deduplicatedEntities = sc.objectFile(checkpointDirectory);
            deduplicatedEntities.persist(StorageLevel.MEMORY_AND_DISK());
            generateGlobalStatsLite(deduplicatedEntities, e2eConfig);
            return deduplicatedEntities;
        }
        JavaRDD<BatchResultObject<UploadedEntities>> combinedRDD = sc.emptyRDD();
        log.info("Loading dedup checkpoints for entities from multiple languages...");
        for (String checkpointDir : allUploadCheckpointDirs) {
            JavaRDD<BatchResultObject<UploadedEntities>> checkpointedRDD = sc.objectFile
                    (checkpointDir);
            combinedRDD = combinedRDD.union(checkpointedRDD);
        }
        combinedRDD = combinedRDD.coalesce(1);
        log.info("Deduplicating uploaded entities from multiple languages...");
        deduplicatedEntities = combinedRDD.mapPartitions(new EntityDeduplicator(e2eConfig
                .kbParameters(), e2eConfig.isDebugMode()));
        log.info("Saving deduplicated entities checkpoint...");
        deduplicatedEntities.persist(StorageLevel.MEMORY_AND_DISK());
        E2eUtil.saveCheckpoint(deduplicatedEntities, sc, checkpointDirectory);
        if (e2eConfig.serializeRDDs()) {
            log.info("Serializing details of globally deduplicated entities...");
            E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_MULTILINGUAL_ENTITY_DEDUP, sc);
            deduplicatedEntities.saveAsTextFile(outputDirectory + "/" +
                    OUTPUT_DIR_MULTILINGUAL_ENTITY_DEDUP);
        }
        generateGlobalStatsLite(deduplicatedEntities,e2eConfig);
        return deduplicatedEntities;
    }

    private JavaRDD<BatchResultObject<UploadedNonEntityArguments>> combineAllNonEntityArguments(List<String>
                                                                                                        allDedupCheckpointDirs, JavaSparkContext sc) throws Exception {
        String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_NON_ENTITY_ARG_DEDUP;
        JavaRDD<BatchResultObject<UploadedNonEntityArguments>> combinedRDD = sc.emptyRDD();
        if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
            log.info("Global non-entity-argument-union checkpoint found, loading kbid map...");
            combinedRDD = sc.objectFile(checkpointDirectory);
            combinedRDD.persist(StorageLevel.MEMORY_AND_DISK());
            generateGlobalStatsLite(combinedRDD,e2eConfig);
            return combinedRDD;
        }
        log.info("Loading dedup checkpoints for non-entity args from multiple languages...");
        for (String checkpointDir : allDedupCheckpointDirs) {
            JavaRDD<BatchResultObject<UploadedNonEntityArguments>> checkpointedRDD = sc.objectFile
                    (checkpointDir);
            combinedRDD = combinedRDD.union(checkpointedRDD);
        }
        combinedRDD = combinedRDD.coalesce(1);
        log.info("Saving combined non-entity args checkpoint...");
        combinedRDD.persist(StorageLevel.MEMORY_AND_DISK());
        E2eUtil.saveCheckpoint(combinedRDD, sc, checkpointDirectory);
        generateGlobalStatsLite(combinedRDD, e2eConfig);
        return combinedRDD;
    }

    private JavaRDD<BatchResultObject<ImmutableMap<RelationKey, KBID>>> deduplicateRelations
            (List<String> allDedupCheckpointDirs, JavaSparkContext sc) throws Exception {
        JavaRDD<BatchResultObject<ImmutableMap<RelationKey, KBID>>> deduplicatedRelations =
                sc.emptyRDD();
        String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_RELATION_DEDUP;
        if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
            log.info("Global relation-deduplication checkpoint found, loading deduplicated relations...");
            deduplicatedRelations = sc.objectFile(checkpointDirectory);
            deduplicatedRelations.persist(StorageLevel.MEMORY_AND_DISK());
            generateGlobalStatsLite(deduplicatedRelations, e2eConfig);
            return deduplicatedRelations;
        }
        JavaRDD<BatchResultObject<UploadedRelations>> combinedRDD = sc.emptyRDD();
        log.info("Loading dedup checkpoints for relations from multiple languages...");
        for (String checkpointDir : allDedupCheckpointDirs) {
            JavaRDD<BatchResultObject<UploadedRelations>> checkpointedRDD = sc.objectFile
                    (checkpointDir);
            combinedRDD = combinedRDD.union(checkpointedRDD);
        }
        combinedRDD = combinedRDD.coalesce(1);
        log.info("Deduplicating uploaded relations from multiple languages...");
        deduplicatedRelations = combinedRDD
                .mapPartitions(new RelationDeduplicator(e2eConfig.kbParameters(), e2eConfig.isDebugMode()));
        log.info("Saving deduplicated relations checkpoint...");
        deduplicatedRelations.persist(StorageLevel.MEMORY_AND_DISK());
        E2eUtil.saveCheckpoint(deduplicatedRelations, sc, checkpointDirectory);
        if (e2eConfig.serializeRDDs()) {
            log.info("Serializing details of globally deduplicated relations...");
            E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_MULTILINGUAL_RELATION_DEDUP, sc);
            deduplicatedRelations.saveAsTextFile(outputDirectory + "/" +
                    OUTPUT_DIR_MULTILINGUAL_RELATION_DEDUP);
        }
        generateGlobalStatsLite(deduplicatedRelations,e2eConfig);
        return deduplicatedRelations;
    }

    private JavaRDD<BatchResultObject<ImmutableMap<EventKey, KBID>>> deduplicateEvents
            (List<String> allDedupCheckpointDirs, JavaSparkContext sc) throws Exception {
        JavaRDD<BatchResultObject<ImmutableMap<EventKey, KBID>>> deduplicatedEvents = sc.emptyRDD();
        String checkpointDirectory = outputDirectory + "/" + CHECKPOINT_DIR_MULTILINGUAL_EVENT_DEDUP;
        if (E2eUtil.doesHdfsDirectoryExist(checkpointDirectory, sc)) {
            log.info("Global event-deduplication checkpoint found, loading deduplicated events...");
            deduplicatedEvents = sc.objectFile(checkpointDirectory);
            deduplicatedEvents.persist(StorageLevel.MEMORY_AND_DISK());
            log.info("Not printing any stats for EventDeduplication in TAC eval mode");
            generateGlobalStatsLite(deduplicatedEvents, e2eConfig);
            return deduplicatedEvents;
        }
        JavaRDD<BatchResultObject<UploadedEvents>> combinedRDD = sc.emptyRDD();
        log.info("Loading dedup checkpoints for relations from multiple languages...");
        for (String checkpointDir : allDedupCheckpointDirs) {
            JavaRDD<BatchResultObject<UploadedEvents>> checkpointedRDD = sc.objectFile
                    (checkpointDir);
            combinedRDD = combinedRDD.union(checkpointedRDD);
        }
        combinedRDD = combinedRDD.coalesce(1);
        log.info("Deduplicating uploaded events from multiple languages...");
        deduplicatedEvents = combinedRDD
                .mapPartitions(new EventDeduplicator(e2eConfig.kbParameters(), e2eConfig.isDebugMode()));
        log.info("Saving deduplicated events checkpoint...");
        deduplicatedEvents.persist(StorageLevel.MEMORY_AND_DISK());
        E2eUtil.saveCheckpoint(deduplicatedEvents, sc, checkpointDirectory);
        if (e2eConfig.serializeRDDs()) {
            log.info("Serializing details of globally deduplicated events...");
            E2eUtil.deleteHdfsDirectory(outputDirectory + "/" + OUTPUT_DIR_MULTILINGUAL_EVENT_DEDUP, sc);
            deduplicatedEvents.saveAsTextFile(outputDirectory + "/" +
                    OUTPUT_DIR_MULTILINGUAL_EVENT_DEDUP);
        }
        generateGlobalStatsLite(deduplicatedEvents,e2eConfig);
        return deduplicatedEvents;
    }

}
