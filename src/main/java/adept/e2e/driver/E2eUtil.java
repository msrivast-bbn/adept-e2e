package adept.e2e.driver;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.common.Entity;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.KBID;
import adept.e2e.analysis.KBReportUtil;
import adept.e2e.analysis.StatsGenerator;
import adept.e2e.analysis.statslite.StatsGeneratorLite;
import adept.e2e.artifactextraction.artifactkeys.GenericThingKey;
import adept.e2e.artifactextraction.artifactkeys.NumericValueKey;
import adept.e2e.artifactextraction.artifactkeys.TemporalValueKey;
import adept.e2e.kbupload.uploadedartifacts.UploadedNonEntityArguments;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.DocumentResultObject;
import adept.e2e.stageresult.ResultObject;
import adept.kbapi.KBParameters;
import scala.Tuple2;

public final class E2eUtil {

  private static final Logger log = LoggerFactory.getLogger(E2eUtil.class);

  public static final List<String> missingEventTypes = ImmutableList.of("per:city_of_birth",
      "per:stateorprovince_of_birth", "per:country_of_birth", "per:date_of_birth","per:city_of_death",
      "per:stateorprovince_of_death", "per:country_of_death", "per:date_of_death","per:cause_of_death",
       "per:charges", "org:date_founded", "org:date_dissolved");

  public static void clearTempHdfsDirectories(String baseDirectory, JavaSparkContext sc) {
    if (!doesHdfsDirectoryExist(baseDirectory, sc)) {
      return;
    }
    try {
      org.apache.hadoop.fs.FileSystem hdfs =
          org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());
      FileStatus[] files = hdfs.listStatus(new org.apache.hadoop.fs.Path(baseDirectory));
      for (int i = 0; i < files.length; i++) {
        if (files[i].getPath().getName().contains("_temp")) {
          log.info("Clearing temp directory " + files[i].getPath().getName());
          hdfs.delete(files[i].getPath(), true);
        }
      }
    } catch (Exception ex) {
      log.error("Caught the following exception when trying to delete temp directories...", ex);
    }
  }

  public static void saveCheckpoint(JavaPairRDD output, JavaSparkContext sc, String
      checkpointDirectory) {
    output.saveAsObjectFile(checkpointDirectory + "_temp");
    deleteHdfsDirectory(checkpointDirectory, sc);
    moveHdfsDirectory(checkpointDirectory + "_temp",
        checkpointDirectory, sc);
  }

  public static void saveCheckpoint(JavaRDD output, JavaSparkContext sc,
      String checkpointDirectory) {
    output.saveAsObjectFile(checkpointDirectory + "_temp");
    deleteHdfsDirectory(checkpointDirectory, sc);
    moveHdfsDirectory(checkpointDirectory + "_temp",
        checkpointDirectory, sc);
  }

  public static boolean doesHdfsDirectoryExist(String directory, JavaSparkContext sc) {
    try {
      org.apache.hadoop.fs.FileSystem hdfs =
          org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());
      return hdfs.exists(new org.apache.hadoop.fs.Path(directory));
    } catch (Exception ex) {
      log.error("Caught the following exception when looking for directory {}...", directory, ex);
    }
    return false;
  }

  public static boolean createHdfsDirectory(String directory, JavaSparkContext sc) {
    try {
      org.apache.hadoop.fs.FileSystem hdfs =
          org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());
      return hdfs.mkdirs(new org.apache.hadoop.fs.Path(directory));
    } catch (Exception ex) {
      log.error("Caught the following exception when creating hdfs directory {}...", directory, ex);
    }
    return false;
  }


  public static boolean deleteHdfsDirectory(String directory, JavaSparkContext sc) {
    try {
      if (!doesHdfsDirectoryExist(directory, sc)) {
        return true;
      }
      org.apache.hadoop.fs.FileSystem hdfs =
          org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());
      return hdfs.delete(new org.apache.hadoop.fs.Path(directory), true);
    } catch (Exception ex) {
      log.error("Caught the following exception when trying to delete directory {}...", directory,
          ex);
    }
    return false;
  }

  public static boolean moveHdfsDirectory(String directory, String targetDirectory,
      JavaSparkContext sc) {
    try {
      org.apache.hadoop.fs.FileSystem hdfs =
          org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());
      return hdfs.rename(new org.apache.hadoop.fs.Path(directory),
          new org.apache.hadoop.fs.Path(targetDirectory));
    } catch (Exception ex) {
      log.info("Caught the following exception when trying to move directory {}...",
          directory);
    }
    return false;
  }

  /**
   * After grouping algorithm output, the resulting PairRDD has filename as the key and a list of
   * tuple as values, where each tuple in the list is a combination of algorithm-name and
   * algorithm-output.
   */
  public static <T, U> JavaPairRDD<String, ImmutableList<DocumentResultObject<T, U>>>
  groupAlgorithmOutput
  (ImmutableList<JavaPairRDD<String, DocumentResultObject<T, U>>> algorithmOutput) {
    if (algorithmOutput.size() == 0) {
      return null;
    }
    JavaPairRDD<String, ImmutableList<DocumentResultObject<T, U>>> groupedAlgorithmOutput =
        algorithmOutput.get(0).mapToPair(
            new PairFunction<Tuple2<String, DocumentResultObject<T, U>>, String,
                ImmutableList<DocumentResultObject<T, U>>>() {
              @Override
              public Tuple2<String, ImmutableList<DocumentResultObject<T, U>>> call(
                  Tuple2<String, DocumentResultObject<T, U>> t) throws Exception {
                ImmutableList.Builder<DocumentResultObject<T, U>> list =
                    ImmutableList.builder();
                list.add(t._2());
                return new Tuple2<String, ImmutableList<DocumentResultObject<T, U>>>(t._1(), list
                    .build());
              }
            });

    for (int i = 1; i < algorithmOutput.size(); i++) {
      groupedAlgorithmOutput = groupedAlgorithmOutput.join(algorithmOutput.get(i)).mapToPair(
          new PairFunction<Tuple2<String, Tuple2<ImmutableList<DocumentResultObject<T, U>>,
              DocumentResultObject<T, U>>>, String, ImmutableList<DocumentResultObject<T, U>>>() {
            @Override
            public Tuple2<String, ImmutableList<DocumentResultObject<T, U>>>
            call(
                Tuple2<String, Tuple2<ImmutableList<DocumentResultObject<T, U>>,
                    DocumentResultObject<T, U>>> t)
                throws Exception {
              List<DocumentResultObject<T, U>> algorithmOutput = new ArrayList(t._2()._1());
              algorithmOutput.add(t._2()._2());
              return new Tuple2<String, ImmutableList<DocumentResultObject<T, U>>>(t._1(),
                  ImmutableList.copyOf(algorithmOutput));
            }
          }
      );
    }
    return groupedAlgorithmOutput;
  }

  public static KBReportUtil.KBSummaryMap generateKBReports(KBParameters kbParameters, String
      corpusId, String outputDirectory) throws Exception {
    return KBReportUtil.generateReports(kbParameters, corpusId, outputDirectory);
  }

  public static void generateKBSummary(KBParameters kbParameters, String outputDirectory,
      KBReportUtil.KBSummaryMap kbSummaryMap)
      throws Exception {
    File summaryFile = new File(outputDirectory, "summary.txt");
    KBReportUtil.compareKBSummaries(Optional.absent(),
        kbSummaryMap, summaryFile);
  }

  public static class NonNullRDDFilter<T> implements Function<T, Boolean> {

    @Override
    public Boolean call(final T t) throws Exception {
      return t != null;
    }
  }

  public static class FailedResultObjectsFilter<T0,T1,T2>
      implements Function<Tuple2<T0, DocumentResultObject<T1,T2>>,
      Boolean> {

    @Override
    public Boolean call(final Tuple2<T0, DocumentResultObject<T1,T2>> resultObjectTuple) throws
                                                                                   Exception {
      return !resultObjectTuple._2().isSuccessful();
    }
  }

  public static class SuccessfulResultObjectsFilter<T0,T1,T2>
      implements Function<Tuple2<T0, DocumentResultObject<T1,T2>>,
      Boolean> {

    @Override
    public Boolean call(final Tuple2<T0, DocumentResultObject<T1,T2>> resultObjectTuple) throws
                                                                                   Exception {
      return resultObjectTuple._2().isSuccessful();
    }
  }

  public static void reAssignEntityType(Entity entity,IType newType, HltContentContainer
      hltContentContainer){
    //Before changing the entity's type, get its mappings from any maps in the HltContentContainer
    Map<KBID,Float> kbIDMap = hltContentContainer.getKBEntityMapForDocEntities().get(entity);
    //remove the mapping with old entity
    hltContentContainer.getKBEntityMapForDocEntities().remove(entity);
    IType oldType = entity.getEntityType();

    //modify the entity
    entity.setEntityType(newType);
    //since setEntityType does not modify the typeConfidences map, do that manually
    entity.addType(newType, 1.0);
    //remove the old type, since it doesn't make sense to have two types mapped with 1.0 confidence
    entity.removeType(oldType);
    //modify entity-type of the canonical mention
    entity.getCanonicalMention().setEntityType(newType);
    //update the KBEntityMapForDocEntities
    if(kbIDMap!=null) {
      for (Map.Entry<KBID,Float> entry : kbIDMap.entrySet()) {
        hltContentContainer.addEntityToKBEntityMap(entity,entry.getKey(),entry.getValue());
      }
    }
  }

  public static <T> List<String> getSuccessfulDocIdsFromBatchResult(JavaRDD<BatchResultObject<T>>
      resultObject) {
    List<List<String>> successfulDocIdsList = resultObject.map(
        new Function<BatchResultObject<T>, List<String>>() {
          @Override
          public List<String> call(final BatchResultObject<T> v1) throws Exception {
            List<String> successfulDocIds = new ArrayList<String>();
            if (!v1.isSuccessful()) {//if the batch itself failed, consider all docs to have failed
              return successfulDocIds;
            }
            if (v1.getArtifactIds().isPresent()) {
              successfulDocIds = new ArrayList<>(v1.getArtifactIds().get());
            }
            if (v1.getFailedArtifactIds().isPresent()) {
              successfulDocIds.removeAll(v1.getFailedArtifactIds().get());
            }
            return successfulDocIds;
          }
        }).collect();
    Set<String> successfulDocIds = new HashSet<String>();
    for (List<String> docIdList : successfulDocIdsList) {
      successfulDocIds.addAll(docIdList);
    }
    return new ArrayList<>(successfulDocIds);
  }

  public static <T, U> JavaPairRDD<String, DocumentResultObject<T, U>> includeNewDocsInRDD
      (JavaPairRDD<String, DocumentResultObject<T, U>> outputRDD, JavaPairRDD<String, T>
          inputRDD, JavaSparkContext sc) {

    Broadcast<List<String>> docIdsInOutputRDD = sc.broadcast(getFilePathsFromRDD(outputRDD));

    JavaPairRDD<String, DocumentResultObject<T, U>> newRDD = inputRDD.filter(
        new Function<Tuple2<String, T>, Boolean>() {
          @Override
          public Boolean call(final Tuple2<String, T> v1) throws Exception {
            return !docIdsInOutputRDD.getValue().contains(v1._1());
          }
        }).mapValues(
        (T inputRDDValue) ->
            DocumentResultObject.create(inputRDDValue));

    return outputRDD.union(newRDD);
  }

  public static <T> List<String> getFilePathsFromRDD(JavaPairRDD<String, T> inputRDD) {
    List<String> docsFromRDD = inputRDD.map(
        new Function<Tuple2<String, T>, String>() {
          @Override
          public String call(Tuple2<String, T> v1) {
            return v1._1();
          }
        }).collect();
    return docsFromRDD;
  }

  public static <T> boolean hasNewDocsToProcess(JavaPairRDD<String, T> checkpointRDD,
      List<String> inputDocsIdsOrPaths) {
    List<String> docsFromCheckpoint = getFilePathsFromRDD(checkpointRDD);
    if (!docsFromCheckpoint.containsAll(inputDocsIdsOrPaths)) {
      return true;
    }
    return false;
  }

  public static <T, U> boolean checkpointHasFailedDocs(JavaPairRDD<String, DocumentResultObject<T, U>>
      checkpointRDD) {
    return !checkpointRDD.filter(new E2eUtil.FailedResultObjectsFilter()).isEmpty();
  }

  public static <T, U> void generateStats(JavaPairRDD<String, DocumentResultObject<T, U>> moduleResults,
      E2eConfig e2eConfig, E2eConstants.LANGUAGE language)
      throws Exception {
    if (e2eConfig.gatherStatistics()) {
      StatsGenerator.getInstance(e2eConfig.statsDirectoryPath().get(), language.name()).generateStats(moduleResults);
    }
  }

  public static <T, U> void generateStats(JavaRDD<? extends ResultObject<T, U>> moduleResults,
      E2eConfig e2eConfig, E2eConstants.LANGUAGE language)
      throws Exception {
    if (e2eConfig.gatherStatistics()) {
      StatsGenerator.getInstance(e2eConfig.statsDirectoryPath().get(), language.name()).generateStats(moduleResults);
    }
  }

  public static <T, U> void generateGlobalStats(JavaPairRDD<String, DocumentResultObject<T, U>> moduleResults,
      E2eConfig e2eConfig)
      throws Exception {
    if (e2eConfig.gatherStatistics()) {
      StatsGenerator.getInstance(e2eConfig.statsDirectoryPath().get()).generateStats(moduleResults);
    }
  }

  public static <T, U> void generateGlobalStats(JavaRDD<? extends ResultObject<T, U>> moduleResults,
      E2eConfig e2eConfig)
      throws Exception {
    if (e2eConfig.gatherStatistics()) {
      StatsGenerator.getInstance(e2eConfig.statsDirectoryPath().get()).generateStats(moduleResults);
    }
  }

  public static <T,U> void updateResultObject(ResultObject<T,U> uploadResultObject,
      U outputArtifact, Optional<Multiset<String>> exceptionDetails, Map<String,Integer>
      countsMap){
    uploadResultObject.setOutputArtifact(outputArtifact);
    Map<String,Integer> propertiesMap = new HashMap<>(countsMap);
    if(exceptionDetails.isPresent()) {
      Multiset<String> exceptionsCaught = Multisets.copyHighestCountFirst(exceptionDetails.get());
      Set<String> seenExceptions = new HashSet<>();
      for (String exceptionCaught : exceptionsCaught) {
        if (seenExceptions.contains(exceptionCaught)) {
          continue;
        }
        seenExceptions.add(exceptionCaught);
        propertiesMap.put(exceptionCaught, exceptionsCaught.count
            (exceptionCaught));
      }
    }
    uploadResultObject.setPropertiesMap(ImmutableMap.copyOf(propertiesMap));
  }

  public static String getExceptionDetails(Exception e){
    List<String> exceptionDetailsArr = new ArrayList<>();
    String exceptionType = e.getClass().getName();
    String exceptionMessage = e.getMessage()!=null?e.getMessage():"";
    if(exceptionType.equals("adept.kbapi.KBUpdateException")){
      if(exceptionMessage.startsWith("Failed to insert relation")){
        exceptionMessage = "Arg Role/Type mismatch (Verbosity reduced for eval)";
      }
    }
    exceptionDetailsArr.add("-----Exception caught during upload (and its count)-----");
    exceptionDetailsArr.add(exceptionType);
    exceptionDetailsArr.add(exceptionMessage);
    if(e.getStackTrace()!=null&&!exceptionMessage.startsWith("Arg Role/Type mismatch")) {
      for (StackTraceElement elem : e.getStackTrace()) {
        exceptionDetailsArr.add(elem.toString());
      }
    }
    return Joiner.on("\n").join(exceptionDetailsArr);
  }

  public static UploadedNonEntityArguments getFinalNonEntityArgKBIDs(JavaRDD<BatchResultObject<UploadedNonEntityArguments>>
      uploadedNonEntityArguments){
    List<BatchResultObject<UploadedNonEntityArguments>> perPartitionUploadResults =
        uploadedNonEntityArguments.collect();
    Map<GenericThingKey,KBID> genericThingKeyKBIDMap = new HashMap<>();
    Map<NumericValueKey,KBID> numericValueKeyKBIDMap = new HashMap<>();
    Map<TemporalValueKey,KBID> temporalValueKeyKBIDMap = new HashMap<>();
    for(BatchResultObject<UploadedNonEntityArguments> bro : perPartitionUploadResults){
      if(bro.isSuccessful()) {
        UploadedNonEntityArguments kbIdMaps = bro.getOutputArtifact().get();
        if(kbIdMaps.uploadedGenericThings().isPresent()) {
          genericThingKeyKBIDMap.putAll(kbIdMaps.uploadedGenericThings().get());
        }
        if(kbIdMaps.uploadedNumericValues().isPresent()) {
          numericValueKeyKBIDMap.putAll(kbIdMaps.uploadedNumericValues().get());
        }
        if(kbIdMaps.uploadedTemporalValues().isPresent()) {
          temporalValueKeyKBIDMap.putAll(kbIdMaps.uploadedTemporalValues().get());
        }
      }
    }
    UploadedNonEntityArguments dedupArgsKBIDMap =
        UploadedNonEntityArguments.create(ImmutableMap.copyOf(numericValueKeyKBIDMap),ImmutableMap.copyOf(temporalValueKeyKBIDMap),
            ImmutableMap.copyOf(genericThingKeyKBIDMap));
    return dedupArgsKBIDMap;
  }

  public static <T, U> void generateStatsLite(JavaRDD<BatchResultObject<U>> moduleResults,
      E2eConfig e2eConfig, E2eConstants.LANGUAGE language)
      throws Exception {
    if (e2eConfig.gatherStatistics()) {
      StatsGeneratorLite.getInstance(e2eConfig.statsDirectoryPath().get(), language.name()).generateStats(moduleResults);
    }
  }

  public static <T, U> void generateGlobalStatsLite(JavaRDD<BatchResultObject<U>> moduleResults,
      E2eConfig e2eConfig)
      throws Exception {
    if (e2eConfig.gatherStatistics()) {
      StatsGeneratorLite.getInstance(e2eConfig.statsDirectoryPath().get()).generateStats(moduleResults);
    }
  }
}
