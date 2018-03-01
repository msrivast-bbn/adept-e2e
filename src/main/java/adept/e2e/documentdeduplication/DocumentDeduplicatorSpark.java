package adept.e2e.documentdeduplication;

import adept.common.HltContentContainer;
import adept.io.Reader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author dtrupian
 */

public class DocumentDeduplicatorSpark{


  private static final String defaultParamFile = "deduplication/dedup_default.par";

  private static final Logger log = LoggerFactory.getLogger(Deduplicate.class);
//  private static final long serialVersionUID = 2829643745791846848L;

  private static String outputDirectory;
  private static String inputDirectory;

  public static void main(String[] args) throws Exception {
    new DocumentDeduplicatorSpark().Run(args);
  }

  private static void setOutputDirectory(String outputDirectory) {
    DocumentDeduplicatorSpark.outputDirectory = outputDirectory;
  }

  private static void setInputDirectory(String inputDirectory) {
    DocumentDeduplicatorSpark.inputDirectory = inputDirectory;
  }

  public void Run(String[] args) throws Exception {
    DocumentDeduplicatorSpark.setInputDirectory(args[0]);
    DocumentDeduplicatorSpark.setOutputDirectory(args[1]);

    log.info("Input dir: {}", inputDirectory);
    log.info("Output dir: {}", outputDirectory);

    SparkConf conf = new SparkConf().setAppName("deduplicate");
    JavaSparkContext sc = new JavaSparkContext(conf);
    processFiles(inputDirectory, sc);
  }

  public static JavaPairRDD<String, String> processFiles(String inputDirectory,
      JavaSparkContext sc) throws Exception {
    JavaPairRDD<String, String> files = sc.wholeTextFiles(inputDirectory);
    return processFiles(files);
  }

  public static JavaPairRDD<String, String> processFiles(JavaPairRDD<String, String> files)
      throws Exception {
    Map<String, String> params;
    List<String> paramFileContents = new ArrayList<String>();
    try (InputStream is = Reader.findStreamInClasspathOrFileSystem(defaultParamFile);
         BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
      String line;
      while ((line = br.readLine()) != null) {
        paramFileContents.add(line);
      }
    }
    params = Deduplicate.readParam(paramFileContents);
    return processFiles(files, params);
  }

  public static JavaPairRDD<String, String> processFiles(JavaPairRDD<String, String> files,
      Map<String, String> params) {
    JavaPairRDD<String, String> sortedFiles =
        files.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
          @Override
          public Tuple2<String, String> call(Tuple2<String, String> item) throws Exception {
            return item.swap();
          }
        }).sortByKey(new StringLengthComparator())
            .mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
              @Override
              public Tuple2<String, String> call(Tuple2<String, String> item) throws Exception {
                return item.swap();
              }
            });

    JavaPairRDD<String, String> dedupCheckedFiles = sortedFiles.filter(new Process(params));

    return dedupCheckedFiles;
  }

  public static JavaPairRDD<String, HltContentContainer> processHltCCs(JavaPairRDD<String,
      HltContentContainer> files, Map<String, String> params) {
    JavaPairRDD<String, HltContentContainer> sortedFiles =
        files.mapToPair(new PairFunction<Tuple2<String, HltContentContainer>, HltContentContainer
            , String>() {
          @Override
          public Tuple2<HltContentContainer,String> call(Tuple2<String, HltContentContainer> item)
              throws
                                                                                   Exception {
            return item.swap();
          }
        }).sortByKey(new DocumentLengthComparator())
            .mapToPair(new PairFunction<Tuple2<HltContentContainer,String>, String,
                HltContentContainer>() {
              @Override
              public Tuple2<String, HltContentContainer> call(Tuple2<HltContentContainer, String>
                  item) throws          Exception {
                return item.swap();
              }
            });
    //To avoid race conditions during deduplication, coalesce the input RDD into a single partition
    log.info("Coalescing input files into single partition to avoid race condition during deduplication...");
    sortedFiles = sortedFiles.coalesce(1);

    JavaPairRDD<String, HltContentContainer> dedupCheckedFiles = sortedFiles.filter(new
        ProcessHltCC(params));

    return dedupCheckedFiles;
  }

  static class StringLengthComparator implements Serializable, Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
      return o1.length() - o2.length();
    }
  }

  static class DocumentLengthComparator implements Serializable, Comparator<HltContentContainer> {

    @Override
    public int compare(HltContentContainer o1, HltContentContainer o2) {
      return o1.getDocument().getValue().length() - o2.getDocument().getValue().length();
    }
  }

  static class Process implements Function<Tuple2<String, String>, Boolean> {

    private static final long serialVersionUID = 1L;
    final Map<String, String> params;

    Process(Map<String, String> params) {
      this.params = params;
    }

    @Override
    public Boolean call(Tuple2<String, String> file) throws Exception {
      try {
        log.info("Running deduplication on file: {}", file._1());
        DbHashing db = new DbHashing(params);
        boolean isOriginalDocument = Deduplicate.isOriginalDocument(file._1(), db, params);
        db.close();
        log.info("{} is original document? {}", file._1(), isOriginalDocument);
        return isOriginalDocument;
      } catch (Exception ex) {
        log.error("Caught the following exception while trying to run deduplication:", ex);
        throw ex;
      }
      //return false;
    }
  }

  static class ProcessHltCC implements Function<Tuple2<String, HltContentContainer>, Boolean> {

    private static final long serialVersionUID = 1L;
    final Map<String, String> params;

    ProcessHltCC(Map<String, String> params) {
      this.params = params;
    }

    @Override
    public Boolean call(Tuple2<String, HltContentContainer> hltCC) throws Exception {
      try {
        log.info("Running deduplication on file: {}" , hltCC._1());
        DbHashing db = new DbHashing(params);
        boolean isOriginalDocument = Deduplicate.isOriginalDocument(hltCC._2(), db, params);
        db.close();
        log.info("{} is original document? {}", hltCC._1(), isOriginalDocument);
        return isOriginalDocument;
      } catch (Exception ex) {
        log.error("Caught the following exception while trying to run deduplication:", ex);
        throw ex;
      }
      //return false;
    }
  }
}
