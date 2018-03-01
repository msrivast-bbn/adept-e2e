package adept.e2e.documentdeduplication;

import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import adept.common.Corpus;
import adept.common.HltContentContainer;
import adept.common.Passage;
import adept.common.TokenizerType;
import adept.utilities.DocumentMaker;


public class Deduplicate {

  private final static String SENTENCE_FINAL_REGEX = "[\\.\\!\\?]\\s+";
  //split on punctuation followed by whitespace
  private final static String STRIP_REGEX = "[\\W\\s]"; //remove punctuation and whitespace
  private static int min_sentence_length;
  private static int max_hash_chars;
  private static float score_threshold;
  private static Comparator<String> FileSizeComparer = new Comparator<String>() {
    @Override
    public int compare(String xstr, String ystr) {
      long x = new File(xstr).length();
      long y = new File(ystr).length();
      return (int) (x - y);
    }
  };

  public static void main(String[] args)
      throws IOException, ParserConfigurationException, SAXException, NoSuchAlgorithmException,
             SQLException {
    Map<String, String> param = readParam(args[0]);
    DbHashing db = new DbHashing(param);
    dedupBatchOfDocuments(param, db);
    db.close();
  }

  public static Map<String, String> readParam(String parFile) throws IOException {
    List<String> lineList = Files.readAllLines(Paths.get(parFile), StandardCharsets.UTF_8);
    return readParam(lineList);
  }

  public static Map<String, String> readParam(List<String> parLines) throws IOException {
    Map<String, String> param = new HashMap<String, String>();
    for (String lineIn : parLines) {
      String line = lineIn.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] piece = line.split(":", 2);
      param.put(piece[0].trim(), piece[1].trim());
    }
    return param;
  }

  public static boolean isOriginalDocument(String filePath, DbHashing db,
      Map<String, String> param) throws Exception {
    min_sentence_length = Integer.parseInt(param.get("min_sentence_length"));
    max_hash_chars = Integer.parseInt(param.get("max_hash_chars"));
    score_threshold = Float.parseFloat(param.get("score_threshold"));
    return isOriginalDocument(filePath, db);
  }

  public static boolean isOriginalDocument(HltContentContainer hltCC, DbHashing db,
      Map<String, String> param) throws Exception {
    min_sentence_length = Integer.parseInt(param.get("min_sentence_length"));
    max_hash_chars = Integer.parseInt(param.get("max_hash_chars"));
    score_threshold = Float.parseFloat(param.get("score_threshold"));
    return isOriginalDocument(hltCC, db);
  }

//  private static Document loadXMLFromString(String xml) throws Exception {
//    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//    DocumentBuilder builder = factory.newDocumentBuilder();
//    InputSource is = new InputSource(new StringReader(xml));
//    return builder.parse(is);
//  }

  private static void dedupBatchOfDocuments(Map<String, String> param, DbHashing db)
      throws IOException, ParserConfigurationException, SAXException, NoSuchAlgorithmException {
    min_sentence_length = Integer.parseInt(param.get("min_sentence_length"));
    max_hash_chars = Integer.parseInt(param.get("max_hash_chars"));
    score_threshold = Float.parseFloat(param.get("score_threshold"));

    String batch_file_in = param.get("batch_file_in");
    String batch_file_out = param.get("batch_file_out");
    String output_dir = param.get("output_dir");

    System.out.println("Input batch: " + batch_file_in);
    System.out.println("Output batch: " + batch_file_out);
    System.out.println("Output folder: " + output_dir);

    if (!(new File(output_dir).mkdirs())) {
      throw new IOException(String.format("Unable to create directory %s", output_dir));
    }

    List<String> fileList = Files.readAllLines(Paths.get(batch_file_in), StandardCharsets.UTF_8);
    List<String> newFileList = new ArrayList<String>();
    long startTime = System.currentTimeMillis();

    // sort by longest document
    Collections.sort(fileList, FileSizeComparer);

    for (String origFileName : fileList) {
      String newFileName = output_dir + (new File(origFileName)).getName();
      if (isOriginalDocument(origFileName, db)) {
        Files.copy(Paths.get(origFileName), Paths.get(newFileName));
        newFileList.add(newFileName);
      }
    }

    long processTime = System.currentTimeMillis() - startTime;
    System.out.println("Total seconds = " + (processTime / 1000));
    Files.write(Paths.get(batch_file_out), newFileList, StandardCharsets.UTF_8);
  }

  private static boolean isOriginalDocument(String filename, DbHashing db)
      throws NoSuchAlgorithmException {
    String docId = filename;
    HltContentContainer hltCC = new HltContentContainer();
    DocumentMaker.getInstance().createDocument(
        docId,
        new Corpus("dummy", "", "dummy", ""),
        "Text",
        filename,
        "English",
        filename,
        hltCC,
        TokenizerType.STANFORD_CORENLP);
    return isOriginalDocument(hltCC,db);
  }

  private static boolean isOriginalDocument(HltContentContainer hltCC, DbHashing db)
      throws NoSuchAlgorithmException {

    // Break passages into sentences
    List<Passage> passageList = hltCC.getPassages();
    //if the document is empty, just skip it
    if (passageList == null || passageList.size() == 0) {
      return true;
    }

    int sentence_count_total = 0;
    int sentence_match_total = 0;
    List<byte[]> hashes = new ArrayList<byte[]>();

    for (int i = 0; i < passageList.size(); i++) {
      String passage = passageList.get(i).getValue();
      if (passage != null && !passage.trim().isEmpty()) {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        String[] rawSentences = passage.split(SENTENCE_FINAL_REGEX);
        List<String> cleanedSentences = cleanAndCombineSentences(rawSentences);
        sentence_count_total += cleanedSentences.size();

        // If the hash of a sentence already exists in the db, it may be a duplicate.
        for (String sentence : cleanedSentences) {
          byte[] trimmedBytes = sentence
              .substring(0, sentence.length() > max_hash_chars ? max_hash_chars : sentence.length())
              .getBytes(StandardCharsets.UTF_8);
          byte[] hash = md5.digest(trimmedBytes);

          hashes.add(hash);

          if (db.checkHash(hash)) {
            sentence_match_total++;
          }
        }
      }
    }

    // if no sentences are found in this doc, just skip it
    if (sentence_count_total == 0) {
      return true;
    }
    String docId = hltCC.getDocumentId();
    // evaluate score of the duplicate detection
    float finalScore = (float) sentence_match_total / (float) sentence_count_total;
    System.out.println("For DOC " + docId + " the score is " + finalScore);
    if (finalScore < score_threshold) {
      // not a duplicate
      db.insertHashes(docId, hashes);
      return true;
    } else {
      // found a duplicate
      db.insertOverlappingDoc(docId, finalScore);
      return false;
    }
  }

  // Remove punctuation and whitespace, combine smaller sentences with larger ones.
  private static List<String> cleanAndCombineSentences(String[] passageBrokenTextList) {
    List<String> cleanedPhraseList = new ArrayList<String>();
    for (String text : passageBrokenTextList) {
      // strip out whitespace and punctuation
      String cleanedText = text.replaceAll(STRIP_REGEX, "");

      // When a sentence is shorter than the minimum we concatenate it to the prior sentence to make it longer
      if (cleanedText.length() < min_sentence_length && cleanedPhraseList.size() > 0) {
        String newSentence = cleanedPhraseList.get(cleanedPhraseList.size() - 1) + cleanedText;
        cleanedPhraseList.remove(cleanedPhraseList.size() - 1);
        cleanedPhraseList.add(newSentence);
      } else {
        if (cleanedText.length() > 0) {
          cleanedPhraseList.add(cleanedText);
        }
      }
    }

    System.out.println("Lengths:");
    for (String s : cleanedPhraseList) {
      System.out.print(" " + s.length());
    }
    System.out.println();
    return cleanedPhraseList;
  }
}
