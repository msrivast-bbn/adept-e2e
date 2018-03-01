package adept.e2e.analysis.statslite;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import adept.e2e.analysis.BatchResultStats;
import adept.e2e.analysis.CombinedResultStats;
import adept.e2e.analysis.ResultStats;
import adept.e2e.stageresult.BatchResultObject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class StatsGeneratorLite implements Serializable{

  private static final Logger log = LoggerFactory.getLogger(StatsGeneratorLite.class);
  private static final long serialVersionUID = 1063444066285941466L;
  private static final Map<String, StatsGeneratorLite> instances = new HashMap<>();
  private final File statsFile;

  private StatsGeneratorLite(File statsFile) {
    this.statsFile = statsFile;
  }

  public static StatsGeneratorLite getInstance(String statsDirectoryPath) throws IOException {
    return getInstance(statsDirectoryPath, "");
  }

  public static StatsGeneratorLite getInstance(String statsDirectoryPath,
      String statsFileSuffix) throws IOException{
    checkNotNull(statsDirectoryPath);
    checkNotNull(statsFileSuffix);
    String statsFilePath = statsDirectoryPath + "/stats";
    if (!statsFileSuffix.isEmpty()) {
      statsFilePath = statsFilePath + "_" + statsFileSuffix;
    }
    synchronized (StatsGeneratorLite.class) {
      StatsGeneratorLite instance = instances.get(statsFilePath);
      if (instance == null) {
        File statsDirectory = new File(statsDirectoryPath);
        if (statsDirectory.exists()) {
          checkArgument(statsDirectory.isDirectory(), "Existing stats directory %s must be a directory.", statsDirectory.getAbsolutePath());
          checkArgument(statsDirectory.canWrite(), "Existing stats directory %s does not have write permissions.", statsDirectory.getAbsolutePath());
        } else {
          File parentDirectory = statsDirectory.getParentFile();
          checkArgument(parentDirectory.exists(), "Parent directory %s for stats directory does not exist.", parentDirectory.getAbsolutePath());
          checkArgument(parentDirectory.canWrite(), "Parent directory %s for stats directory does not have write permissions", parentDirectory.getAbsolutePath());
          checkArgument(statsDirectory.mkdirs(),"Could not create stats directory %s.", statsDirectory.getAbsolutePath() );
          // set the stats directory to be writable by all users, so that it will be possible to delete it later
          checkArgument(statsDirectory.setWritable(true, false));
        }
        File statsFile = new File(statsFilePath);
        if (statsFile.exists()) {
          checkArgument(statsFile.isFile(), "Existing stats file %s must be a file.", statsFile.getAbsolutePath());
          checkArgument(statsFile.canWrite(), "Existing stats file %s does not have write permissions.", statsFile.getAbsolutePath());
        } else {
          checkArgument(statsFile.createNewFile(), "Could not create stats file %s.", statsFile.getAbsolutePath());
          // set the stats file to be writable by all users, so that subsequent runs that may be under a different
          // user can successfully append to it
          checkArgument(statsFile.setWritable(true, false));
        }
        instance = new StatsGeneratorLite(statsFile);
        instances.put(statsFilePath, instance);
        instance.printHeader();
      }
    }
    return instances.get(statsFilePath);
  }

  private void printHeader() throws FileNotFoundException{
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(statsFile, true)));
    pw.println("==============================================");
    pw.println();
    pw.println("StatsGeneratorLite instantiated at: " + (new Date()));
    pw.println();
    pw.println("==============================================");
    pw.close();
  }

  private <U> JavaRDD<BatchResultObject<U>> getTrimmedModuleResults
      (JavaRDD<BatchResultObject<U>> moduleResults) {
    JavaRDD<BatchResultObject<U>> trimmedResultObjects =
        moduleResults.map(new Function<BatchResultObject<U>, BatchResultObject<U>>() {
          @Override
          public BatchResultObject<U> call(
              BatchResultObject<U> v1) {
            BatchResultObject<U> retVal = BatchResultObject.createEmpty();
            if(v1.isSuccessful()){
              retVal.markSuccessful();
            }
            retVal.setArtifactLevelProperties(v1.getArtifactLevelProperties().orNull());
            retVal.setPropertiesMap(v1.getPropertiesMap().orNull());
            retVal.setFailedArtifactIds(v1.getFailedArtifactIds().orNull());
            retVal.setArtifactIdsInvolved(v1.getArtifactIds().orNull());
            return retVal;
          }
        });
    return trimmedResultObjects;
  }

  public <U> void generateStats(JavaRDD<BatchResultObject<U>> moduleResults)
       {

    log.info("Obtaining stats from ResultObject RDDs...");
    JavaRDD<BatchResultObject<U>> trimmedModuleResults = getTrimmedModuleResults(moduleResults);
    JavaRDD<ResultStats> resultStats = trimmedModuleResults.map(new StatsCollectorLite()).filter(
        new Function<ResultStats, Boolean>() {
          @Override
          public Boolean call(ResultStats v1) throws Exception {
            return v1 != null;
          }
        });
    if (resultStats.isEmpty()) {
      log.info("resultStats RDD was found to be empty");
      return;
    }

    log.info("Aggregating properties...");
    ResultStats reducedResultStats = resultStats.reduce
        (new ResultStatsReducerLite());
    CombinedResultStats combinedResultStats = null;
    if (!(reducedResultStats instanceof CombinedResultStats)) {//this can happen if only one document
      // or batch was processed
      if (reducedResultStats instanceof BatchResultStats) {
        combinedResultStats = CombinedResultStats.builderFromBatchResultStats((BatchResultStats)
            reducedResultStats).build();
      } else {
        combinedResultStats =
            CombinedResultStats.builderFromResultStats(reducedResultStats).build();
      }
    } else {
      combinedResultStats = (CombinedResultStats) reducedResultStats;
    }

    String moduleName = combinedResultStats.moduleName();
    long totalTimeTaken = combinedResultStats.timeTaken().or(0L);
    int numSuccessful = combinedResultStats.getTotalSuccessful();
    int numFailed = combinedResultStats.getTotalFailed();
    Multiset<String> exceptionDetails =
        ((CombinedResultStats) combinedResultStats).exceptionDetails().or(HashMultiset
            .<String>create());
    Multimap<String, String> exceptionMap =
        ((CombinedResultStats) combinedResultStats).exceptionMap().or(HashMultimap
            .<String,
                String>create());
    Map<String, Integer> aggregatePropertiesCountMap = ((CombinedResultStats) combinedResultStats)
        .getPropertiesCountMap().or(new HashMap<String, Integer>());

    int numSuccessfulArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats).successfulArtifactsCount().or(0);
    int numFailedArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats).failedArtifactsCount().or(0);
    Multiset<String> exceptionDetailsForArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats)
            .exceptionDetailsForArtifacts().or(HashMultiset
            .<String>create());
    Multimap<String, String> exceptionMapForArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats)
            .exceptionMapForArtifacts().or(HashMultimap
            .<String,
                String>create());

    log.info("Dumping stats on disk...");
    FileOutputStream fos = null;
    try{fos = new FileOutputStream(statsFile, true);}catch
        (FileNotFoundException fnfe){};
    //Since statsFile's existence has already been verified in getInstance
         // method, we can be sure that fos won't be null;
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos));
    pw.println("==============================================");
    pw.println("Stats for module: " + moduleName);
    pw.println(
        "Total files (or batches, for batch-level operations) processed=" + (numSuccessful +
                                                                                 numFailed)
            + " #successful=" + numSuccessful + "" + " #failed=" + numFailed);
    if (numSuccessfulArtifactsInBatches != 0 || numFailedArtifactsInBatches != 0) {
      pw.println(
          "Total artifacts processed in various batches=" + (numSuccessfulArtifactsInBatches +
                                                                 numFailedArtifactsInBatches)
              + " #successful=" + numSuccessfulArtifactsInBatches + "" + " #failed=" +
              numFailedArtifactsInBatches);
    }
    long seconds = totalTimeTaken / 1000;
    long minutes = seconds / 60;
    long hours = minutes / 60;
    String timeTaken = hours + "h " + (minutes % 60) + "m " + (seconds % 60) + "s";
    pw.println("Total time taken=" + timeTaken);
    if (!exceptionDetails.isEmpty()) {
      pw.println("Exceptions (if any) thrown on unsuccessful documents (or by unsuccessful "
          + "batches)...");
    }
    Set<String> seenExceptionDetails = new HashSet<>();
    for (String exceptionDetail : Multisets.copyHighestCountFirst(exceptionDetails)) {
      if (seenExceptionDetails.contains(exceptionDetail)) {
        continue;
      }
      seenExceptionDetails.add(exceptionDetail);
      pw.println("Thrown a total of " + exceptionDetails.count(exceptionDetail) + " times on "
          + "the following documents: " + Joiner.on(",").join(exceptionMap.get(exceptionDetail)));
      pw.println(exceptionDetail);
      pw.println("----------------------------------------------");
    }
    if (!exceptionDetailsForArtifactsInBatches.isEmpty()) {
      pw.println("Exceptions thrown on unsuccessful artifacts in various batches...");
      Set<String> seenExceptionDetailsForArtifacts = new HashSet<>();
      for (String exceptionDetail : Multisets.copyHighestCountFirst(
          exceptionDetailsForArtifactsInBatches)) {
        if (seenExceptionDetailsForArtifacts.contains(exceptionDetail)) {
          continue;
        }
        seenExceptionDetailsForArtifacts.add(exceptionDetail);
        pw.println(
            "Thrown a total of " + exceptionDetailsForArtifactsInBatches.count(exceptionDetail)
                + " times on the following artifacts: " + Joiner.on(",").join(
                exceptionMapForArtifactsInBatches.get(exceptionDetail)));
        pw.println(exceptionDetail);
        pw.println("----------------------------------------------");
      }
    }
    if (!aggregatePropertiesCountMap.isEmpty()) {
      pw.println("Counts of other useful properties...");
      for (Map.Entry<String, Integer> entry : aggregatePropertiesCountMap.entrySet()) {
        pw.println(entry.getKey() + "=" + entry.getValue());
      }
    }
    pw.println();
    pw.close();
    return;
  }

}
