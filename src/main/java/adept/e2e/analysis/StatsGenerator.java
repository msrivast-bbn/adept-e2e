package adept.e2e.analysis;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.spark.api.java.JavaPairRDD;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.e2e.stageresult.DocumentResultObject;
import adept.e2e.stageresult.ResultObject;
import scala.Tuple2;

import static adept.e2e.driver.E2eConstants.PROPERTY_DOCID;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class StatsGenerator implements Serializable{

  private static final Logger log = LoggerFactory.getLogger(StatsGenerator.class);
  private static final long serialVersionUID = 1063444066285941466L;
  private static final Map<String,StatsGenerator> instances = new HashMap<>();
  private final File statsFile;

  private StatsGenerator(File statsFile) {
    this.statsFile = statsFile;
  }

  public static StatsGenerator getInstance(String statsDirectoryPath) throws IOException {
    return getInstance(statsDirectoryPath, "");
  }

  public static StatsGenerator getInstance(String statsDirectoryPath, String statsFileSuffix) throws IOException {
    checkNotNull(statsDirectoryPath);
    checkNotNull(statsFileSuffix);
    String statsFilePath = statsDirectoryPath + "/stats";
    if (!statsFileSuffix.isEmpty()) {
      statsFilePath = statsFilePath + "_" + statsFileSuffix;
    }
    synchronized (StatsGenerator.class) {
      StatsGenerator instance = instances.get(statsFilePath);
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
        instance = new StatsGenerator(statsFile);
        instances.put(statsFilePath, instance);
        instance.printHeader();
      }
    }
    return instances.get(statsFilePath);
  }

  public static void getCountBasedProperties(Map<String, ? extends Object> properties, Map<String,
      Integer>
      aggregateCountMap) {
    for (Map.Entry<String, ? extends Object> entry : properties.entrySet()) {
      if (!entry.getKey().equals(PROPERTY_DOCID) && entry.getValue() instanceof Integer) {
        int count = (Integer) entry.getValue();
        Integer currentCount = aggregateCountMap.get(entry.getKey());
        if (currentCount != null) {
          count += currentCount;
        }
        aggregateCountMap.put(entry.getKey(), count);
      }
    }
  }

  private void printHeader() throws FileNotFoundException {
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(statsFile, true)));
    pw.println("==============================================");
    pw.println();
    pw.println("StatsGenerator instantiated at: " + (new Date()));
    pw.println();
    pw.println("==============================================");
    pw.close();
  }

  public <T, U> void generateStats(JavaPairRDD<String,
      DocumentResultObject<T, U>> moduleResults) throws FileNotFoundException {
    JavaRDD<DocumentResultObject<T, U>> resultObjects =
        moduleResults.map(new Function<Tuple2<String,
            DocumentResultObject<T, U>>, DocumentResultObject<T, U>>() {
          @Override
          public DocumentResultObject<T, U> call(
              Tuple2<String, DocumentResultObject<T, U>> inPair) {
            DocumentResultObject<T, U> documentResultObject = inPair._2();
            return documentResultObject;
          }
        });
    generateStats(resultObjects);
  }

  public <T, U> void generateStats(JavaRDD<? extends ResultObject<T, U>>
      moduleResults) throws FileNotFoundException{

    log.info("Obtaining stats from ResultObject RDDs...");
    JavaRDD<ResultStats> resultStats = moduleResults.map(new StatsCollector()).filter(
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
        (new ResultStatsReducer());
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
    SortedSetMultimap<Long, String> timeTakenMap =
        ((CombinedResultStats) combinedResultStats).timeTakenMap()
            .or(TreeMultimap.<Long,
                String>create(
                Ordering.natural().reverse(), Ordering.natural()));
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
    SortedSetMultimap<Long, String> timeTakenMapForArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats)
            .timeTakenByArtifacts().or(TreeMultimap.<Long,
            String>create(
            Ordering.natural().reverse(), Ordering.natural()));
    Multiset<String> exceptionDetailsForArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats)
            .exceptionDetailsForArtifacts().or(HashMultiset
            .<String>create());
    Multimap<String, String> exceptionMapForArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats)
            .exceptionMapForArtifacts().or(HashMultimap
            .<String,
                String>create());
    Map<String, Map<String, Integer>> aggregatePropertiesCountMapForArtifactsInBatches =
        ((CombinedResultStats) combinedResultStats).propertiesCountForArtifacts()
            .or(new HashMap<>());

    log.info("Dumping stats on disk...");
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(statsFile, true)));
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
    pw.println("Top 5 (or less) most time-consuming documents (or batches)...");
    List<String> docIdsWithTimeTaken = new ArrayList<>();
    int numDocs = 0;
    for (long time : timeTakenMap.keySet()) {
      for (String docId : timeTakenMap.get(time)) {
        numDocs++;
        docIdsWithTimeTaken.add(docId + ": " + (time / 1000) + "s");
        if (numDocs == 5) {
          break;
        }
      }
      if (numDocs == 5) {
        break;
      }
    }
    pw.println(Joiner.on(", ").join(docIdsWithTimeTaken));
    if (!timeTakenMapForArtifactsInBatches.isEmpty()) {
      pw.println("Top 5 (or less) most time-consuming artifacts in various batches...");
      List<String> artifactIdsWithTimeTaken = new ArrayList<>();
      int numArtifacts = 0;
      for (long time : timeTakenMapForArtifactsInBatches.keySet()) {
        for (String artifactId : timeTakenMapForArtifactsInBatches.get(time)) {
          numArtifacts++;
          artifactIdsWithTimeTaken.add(artifactId + ": " + (time / 1000) + "s");
          if (numArtifacts == 5) {
            break;
          }
        }
        if (numArtifacts == 5) {
          break;
        }
      }
      pw.println(Joiner.on(", ").join(artifactIdsWithTimeTaken));
    }
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
    if (!aggregatePropertiesCountMapForArtifactsInBatches.isEmpty()) {
      pw.println("Counts of other useful properties for artifacts in various batches (capped at "
          + "10)...");
      int numArtifacts = 0;
      for (Map.Entry<String, Map<String, Integer>> entry : aggregatePropertiesCountMapForArtifactsInBatches.entrySet()) {
        Map<String, Integer> countMap = entry.getValue();
        pw.println(String.format("ArtifactId: %s" , entry.getKey()));
        for (Map.Entry<String, Integer> countMapEntry : countMap.entrySet()) {
          pw.println(String.format("\t%s=%d", countMapEntry.getKey(), countMapEntry.getValue()));
        }
        numArtifacts++;
        if (numArtifacts == 10) {
          break;
        }
      }
    }
    pw.println();
    pw.close();
    return;
  }

  private void getExceptionDetailsFromProperties(String docId, Map<String, Object> properties,
      Multiset<String> exceptionDetails, Multimap<String, String> exceptionMap) {
    List<String> exceptionDetailsArr = new ArrayList<>();
    exceptionDetailsArr.add(properties.get(PROPERTY_EXCEPTION_TYPE)!=null ? (String) properties.get
        (PROPERTY_EXCEPTION_TYPE): "");
    exceptionDetailsArr.add(properties.get(PROPERTY_EXCEPTION_MESSAGE) != null ? (String) properties
        .get(PROPERTY_EXCEPTION_MESSAGE) : "");
    if(properties.get(PROPERTY_EXCEPTION_TRACE)!=null) {
      for (StackTraceElement elem : (StackTraceElement[]) properties
          .get(PROPERTY_EXCEPTION_TRACE)) {
        exceptionDetailsArr.add(elem.toString());
      }
    }
    String exceptionDetailsStr = Joiner.on("\n").join(exceptionDetailsArr);
    exceptionDetails.add(exceptionDetailsStr);
    exceptionMap.put(exceptionDetailsStr, docId);
  }

}
