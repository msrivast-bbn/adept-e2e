package adept.e2e.driver;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

import adept.common.Corpus;
import adept.common.HltContentContainer;
import adept.common.TokenizerType;
import adept.e2e.stageresult.DocumentResultObject;
import adept.serialization.XMLStringSerializer;
import adept.utilities.DocumentMaker;
import scala.Tuple2;

import static adept.e2e.driver.E2eConstants.PROPERTY_DOCID;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class FileToHltCCConverter implements PairFunction<Tuple2<String,
DocumentResultObject<String,HltContentContainer>>, String,
    DocumentResultObject<String,HltContentContainer>> {

  private static final Logger log = LoggerFactory.getLogger(FileToHltCCConverter.class);
//  private static final long serialVersionUID = 2689691195945522161L;
  private static XMLStringSerializer xmlStringSerializer;
  private final Corpus corpus;
  private final DocumentMaker.XMLReadMode xmlReadMode;

  public FileToHltCCConverter(Corpus corpus, DocumentMaker.XMLReadMode xmlReadMode) {
    this.corpus = corpus;
    this.xmlReadMode = xmlReadMode;
  }

  public Tuple2<String, DocumentResultObject<String,HltContentContainer>> call(Tuple2<String,
      DocumentResultObject<String,HltContentContainer>> inPair) {
    if (inPair._2().isSuccessful()){
      return inPair;  //don't re-process if this document was processed successfully last time
    }
    String filename = inPair._1();
    String fileString = inPair._2().getInputArtifact();

    if (xmlStringSerializer == null) {
      xmlStringSerializer = new XMLStringSerializer();
    }

    log.info("FileToHltcc: Creating HltCC from file: {}", filename);
//	    XMLStringSerializer xmlSerializer = new XMLStringSerializer();
    HltContentContainer hltContentContainer = new HltContentContainer();

    ArrayList<String>
        fileLines = new ArrayList<String>(Arrays.asList(fileString.split("\\r?\\n")));

    String uri = filename;
    filename = (new Path(filename)).getName();
    String docId = filename;
    String docType = "Text";
    String language = "English";
    DocumentResultObject<String,HltContentContainer> dro = DocumentResultObject.create(fileString);
    ImmutableMap.Builder propertiesMap = ImmutableMap.builder();
    long start = System.currentTimeMillis();
    try {
      DocumentMaker.getInstance().createDocument(
          docId,
          this.corpus,
          docType,
          uri,
          language,
          uri,
          hltContentContainer,
          TokenizerType.STANFORD_CORENLP, this.xmlReadMode);
      dro.setOutputArtifact(hltContentContainer);
      dro.markSuccessful();
    }catch (Exception e){
      dro.markFailed();
      propertiesMap.put(PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
      propertiesMap.put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage() != null ? e.getMessage() : "");
      propertiesMap.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
    }
    propertiesMap.put(PROPERTY_DOCID, docId);
    propertiesMap.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
    propertiesMap.put(PROPERTY_TIME_TAKEN, (System.currentTimeMillis()-start)/1000);
    dro.setPropertiesMap(propertiesMap.build());
    return new Tuple2<String, DocumentResultObject<String,HltContentContainer>>(uri, dro);
  }
}
