package adept.e2e.driver;

import adept.io.Reader;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import adept.common.Pair;
import adept.e2e.algorithms.AlgorithmSpecifications;
import adept.e2e.exceptions.InvalidConfigException;
import adept.kbapi.KBParameters;
import adept.utilities.DocumentMaker;

import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_COREF;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_ENTITY_LINKING;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_EVENT_EXTRACTION;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE_RELATION_EXTRACTION;
import static adept.e2e.driver.E2eConstants.LANGUAGE;
import static adept.e2e.driver.E2eConstants.ONTOLOGY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class E2eConfig implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(E2eConfig.class);

  private static E2eConfig instance;

  private final boolean clearKB;
  private final boolean serializedRDDs;
  private final boolean runDocumentDeduplication;
  private final String configFilePath;
  private final List<LANGUAGE> languages;
  private final Map<LANGUAGE,String> inputDirectories;
  private final Map<LANGUAGE,List<AlgorithmSpecifications>>
      algorithmSpecifications;
  private final Map<LANGUAGE,List<String>> stagesToSkip;
  private final Map<LANGUAGE,List<String>> algorithmsForEntityTypeReassignment;
  private final Map<String,String> dedupParams;
  private final KBParameters kbParameters;
  private final DocumentMaker.XMLReadMode xmlReadMode;
  private final boolean isDebugMode;
  private final boolean gatherStatistics;
  private final String statsDirectoryPath;
  private final String corpusId;
  private final boolean generateKBReports;
  private final String kbReportsOutputDir;
  private final Properties kbResolverProperties;
  private final int algorithmTimeOut;
  private Table<LANGUAGE,String,AlgorithmSpecifications> algorithmTypeToSpecsMapByLanguage;
  private Map<LANGUAGE,Boolean> useMissingEventTypesHack;

  private E2eConfig(boolean clearKB, boolean serializeRDDs, boolean runDocumentDeduplication, String configFilePath, List<LANGUAGE> languages,
      Map<LANGUAGE,String> inputDirectories, Map<LANGUAGE,
      List<AlgorithmSpecifications>> algorithmSpecifications,
      Map<LANGUAGE,List<String>> stagesToSkip, Map<LANGUAGE,List<String>> algorithmsForEntityTypeReassignment, KBParameters kbParameters, Map<String, String> dedupParams,
      DocumentMaker.XMLReadMode xmlReadMode, boolean isDebugMode, boolean gatherStatistics, Optional<String>
      statsDirectoryPath,
      String corpusId,
      boolean generateKBReports, Optional<String> kbReportsOutputDir,
      Properties kbResolverProperties, int algorithmTimeOut) {
    this.clearKB = clearKB;
    this.serializedRDDs = serializeRDDs;
    this.runDocumentDeduplication = runDocumentDeduplication;
    this.configFilePath = configFilePath;
    this.languages = languages;
    this.inputDirectories = inputDirectories;
    this.algorithmSpecifications = algorithmSpecifications;
    this.stagesToSkip = stagesToSkip;
    this.algorithmsForEntityTypeReassignment = algorithmsForEntityTypeReassignment;
    this.dedupParams = dedupParams;
    this.kbParameters = kbParameters;
    this.xmlReadMode = xmlReadMode;
    this.isDebugMode = isDebugMode;
    this.gatherStatistics = gatherStatistics;
    this.statsDirectoryPath = statsDirectoryPath.orNull();
    this.corpusId = corpusId;
    this.generateKBReports = generateKBReports;
    this.kbReportsOutputDir = kbReportsOutputDir.orNull();
    this.kbResolverProperties = kbResolverProperties;
    this.algorithmTimeOut = algorithmTimeOut;
  }

  public static E2eConfig getInstance(String configFilePath) throws
                                                             FileNotFoundException,InvalidConfigException {
    checkNotNull(configFilePath);
    synchronized (E2eConfig.class) {
      if (instance == null) {
        instance = parseConfig(configFilePath);
      }
    }
    return instance;
  }

  //TODO:
  private static ImmutableList<AlgorithmSpecifications> getAlgorithmsToRun(Node algorithmSet)
      throws InvalidConfigException{
    List<AlgorithmSpecifications> algorithmSpecificationsList =
        new ArrayList<>();
    List<String> algorithmTypesToRun = new ArrayList<>();
    NodeList algorithmNodes = algorithmSet.getChildNodes();
    List<String> algorithmTypes = new ArrayList<>();
    for(int i=0; i<algorithmNodes.getLength(); i++) {
      Node node = algorithmNodes.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if (element.getTagName().equals("algorithm")) {
          String algorithmType = element.getAttribute("type");
          String algorithmName = element.getAttribute("name");
          String providerName = element.getAttribute("providerName");
          NodeList algoDetails = node.getChildNodes();
          Class wrapperClass = null;
          Class algorithmClass = null;
          String classConfigPath = null;
          String ontologyMappingFilePath = null;
          String reverseOntologyMappingFilePath = null;
          boolean moduleUsesDeprecatedProcessCall = false;
          for(int j=0; j<algoDetails.getLength();j++){
            Node algoDetail = algoDetails.item(j);
            if(algoDetail instanceof Element){
              if(((Element)algoDetail).getTagName().equals("sparkWrapperClass")){
                String className = algoDetail.getTextContent().trim();
                try {
                  wrapperClass = Class.forName(className);
                }catch(ClassNotFoundException e){
                  throw new InvalidConfigException(e);
                }
              }
              if(((Element)algoDetail).getTagName().equals("algorithmModule")){
                String className = algoDetail.getTextContent().trim();
                try {
                  algorithmClass = Class.forName(className);
                }catch(ClassNotFoundException e){
                  throw new InvalidConfigException(e);
                }
              }else if(((Element)algoDetail).getTagName().equals("configFile")){
                classConfigPath = algoDetail.getTextContent().trim();
              }else if(((Element)algoDetail).getTagName().equals("ontologyMappingFile")){
                ontologyMappingFilePath = algoDetail.getTextContent().trim();
              }else if(((Element)algoDetail).getTagName().equals("reverseOntologyMappingFile")){
                reverseOntologyMappingFilePath = algoDetail.getTextContent().trim();
              }else if(((Element)algoDetail).getTagName().equals("moduleUsesDeprecatedProcessCall")){
                moduleUsesDeprecatedProcessCall = Boolean.parseBoolean(algoDetail.getTextContent().trim());
              }else if(((Element)algoDetail).getTagName().equals("ontology")){
                String ontologyStr = algoDetail.getTextContent().trim();
                ONTOLOGY ontology = ONTOLOGY.custom;
                try{
                  ontology = ONTOLOGY.valueOf(ontologyStr);
                }catch (IllegalArgumentException e){
                  throw new InvalidConfigException("Unrecognizable value for config "
                      + "\"ontology\". Allowed values are tac2012, rere, adept, and custom");
                }
                if(!ontology.equals(ONTOLOGY.custom)) {
                  Pair<String, String> ontologyMappingFilePaths = getMappingFilePathsFromOntology(ontology);
                  ontologyMappingFilePath = ontologyMappingFilePaths.getL();
                  reverseOntologyMappingFilePath = ontologyMappingFilePaths.getR();
                }
              }
            }
          }
          AlgorithmSpecifications algorithmSpecifications =
              AlgorithmSpecifications.create(algorithmName,providerName,
                  algorithmType,algorithmClass,
                  wrapperClass,classConfigPath,ontologyMappingFilePath,reverseOntologyMappingFilePath,moduleUsesDeprecatedProcessCall);
          algorithmSpecificationsList.add(algorithmSpecifications);
          algorithmTypes.add(algorithmType);
        } else if (element.getTagName().equals("algorithms_to_run")) {
          String textValue = element.getTextContent().trim();
          if(textValue.isEmpty()) {
            throw new InvalidConfigException("Param algorithms_to_run cannot "
                + "be empty.");
          }
          for (String algorithmToRun : textValue.split(",")) {
            if (!algorithmTypes.contains(algorithmToRun.trim())) {
              throw new InvalidConfigException(
                  "The algorithm-to-run: " + algorithmToRun + " is not configured as an "
                      + "algorithm");
            } else {
              algorithmTypesToRun.add(algorithmToRun.trim());
            }
          }
        }
      }
    }
    if(!algorithmTypes.contains(ALGORITHM_TYPE_COREF) || !algorithmTypes.contains
        (ALGORITHM_TYPE_ENTITY_LINKING)) {
      throw new InvalidConfigException("Both "+ALGORITHM_TYPE_COREF+" and "
          + ""+ALGORITHM_TYPE_ENTITY_LINKING+" must be "
          + "configured.");
    }
    List<AlgorithmSpecifications> algorithmsToRun = FluentIterable.from(algorithmSpecificationsList).filter(
            (AlgorithmSpecifications spec)->algorithmTypesToRun.contains(spec.algorithmType())).toList();
    return ImmutableList.copyOf(algorithmsToRun);
  }

  private static Pair<String,String> getMappingFilePathsFromOntology(ONTOLOGY
      ontology){
    String forwardMappingFile = null;
    String reverseMappingFile = null;
    switch (ontology){
      case tac2012:
        forwardMappingFile = "adept/kbapi/stanford-to-adept.xml";
        reverseMappingFile = "adept/kbapi/adept-to-stanford.xml";
        break;
      case rere:
        forwardMappingFile = "adept/kbapi/rere-to-adept.xml";
        reverseMappingFile = "adept/kbapi/adept-to-rere.xml";
        break;
      case adept:
        forwardMappingFile = "adept/kbapi/adept-to-adept.xml";
        reverseMappingFile = "adept/kbapi/adept-to-adept.xml";
        break;
      default:
    }
    return new Pair(forwardMappingFile,reverseMappingFile);
  }

  //Commenting out any hard-coded stuff to force the user to configure all properties for all algorithms--this will be less
  //confusing than assuming default values for a certain set of algorithms
//  private static AlgorithmSpecifications getAlgorithmSpecificationsForKnownSparkWrapper(String sparkWrapperName) throws ClassNotFoundException{
//    String moduleClassName = null;
//    String configFilePath = null;
//    String ontologyMappingFile = "adept/kbapi/rere-to-adept.xml";
//    String reverseOntologyMappingFile = "adept/kbapi/adept-to-rere.xml";
//    boolean moduleUsesNonDeprecatedProcessCall = true;
//    String algorithmName = null;
//    String providerName = null;
//
//    sparkWrapperName = sparkWrapperName.substring(sparkWrapperName.indexOf(".")+1);
//
//    if(sparkWrapperName.equals("IllinoisCorefSpark")){
//      moduleClassName = "edu.uiuc.coreference.IllinoisCoreferenceResolver";
//      configFilePath = "edu/uiuc/coreference/IllinoisCorefConfig.xml";
//      algorithmName = "IllinoisCoref";
//      providerName = "UIUC";
//    }else if(sparkWrapperName.equals("IllinoisWikifierSpark")){
//      moduleClassName = "edu.uiuc.xlwikifier.IllinoisXLWikifier";
//      configFilePath = "edu/uiuc/xlwikifier/en/xlwikifier-en.xml";
//      algorithmName = "IllinoisWikification";
//      providerName = "UIUC";
//    }else if(sparkWrapperName.equals("RPIEDLSpark")){
//      moduleClassName = "edu.rpi.blender.__main.linkipedia__.RPIEntityDiscoveryLinkingProcessor";
//      configFilePath = "edu/rpi/blender/RPIBlenderConfig.xml";
//      algorithmName = "RPI_EDL";
//      providerName = "RPI";
//    }else if(sparkWrapperName.equals("StanfordSpark")){
//      moduleClassName = "edu.stanford.nlp.StanfordCoreNlpProcessor";
//      configFilePath = "edu/stanford/nlp/StanfordCoreNlpProcessorConfig.xml";
//      ontologyMappingFile = "adept/kbapi/stanford-to-adept.xml";
//      reverseOntologyMappingFile = "adept/kbapi/adept-to-stanford.xml";
//      algorithmName = "StanfordRE";
//      providerName = "Stanford";
//    }else if(sparkWrapperName.equals("WashingtonReVerbRESpark")){
//      moduleClassName = "edu.washington.ReVerbExtractor";
//      configFilePath = "edu/washington/ReVerbExtractorConfig.xml";
//      ontologyMappingFile = null;
//      reverseOntologyMappingFile = null;
//      algorithmName = "ReVerbRE";
//      providerName = "UWash";
//    }else if(sparkWrapperName.equals("SerifEALSpark")){
//      moduleClassName = "adept.e2e.algorithms.SerifAdeptModule";
//      configFilePath = "serif_config.xml";
//      ontologyMappingFile = "serif-to-adept.xml";
//      reverseOntologyMappingFile = "adept-to-serif.xml";
//      algorithmName = "BBN_SERIF";
//      providerName = "BBN";
//    }
//    Class moduleClass = Class.forName(moduleClassName);
//    return AlgorithmSpecifications.create(algorithmName,providerName,moduleClass,configFilePath,ontologyMappingFile,reverseOntologyMappingFile,
//            moduleUsesNonDeprecatedProcessCall);
//  }
//
//  Class<? extends SparkAlgorithmComponent> getSparkWrapperClass(Class<? extends AbstractModule> algorithmModule){
//    Class<? extends SparkAlgorithmComponent> sparkWrapperClass = GenericAlgorithmSparkWrapper.class;
//    String moduleClassName = algorithmModule.getName();
//    if(moduleClassName.endsWith("IllinoisCoreferenceResolver")){
//      sparkWrapperClass = IllinoisCorefSpark.class;
//    }else if(moduleClassName.endsWith("IllinoisXLWikifier")){
//      sparkWrapperClass = IllinoisWikifierSpark.class;
//    }else if(moduleClassName.endsWith("RPIEntityDiscoveryLinkingProcessor")){
//      sparkWrapperClass = RPIEDLSpark.class;
//    }else if(moduleClassName.endsWith("StanfordCoreNlpProcessor")){
//      sparkWrapperClass = StanfordSpark.class;
//    }else if(moduleClassName.endsWith("ReVerbExtractor")){
//      sparkWrapperClass = WashingtonReVerbRESpark.class;
//    }else if(moduleClassName.endsWith("SerifAdeptModule")){
//      sparkWrapperClass = SerifEALSpark.class;
//    }
//    return sparkWrapperClass;
//  }

  private static ImmutableList<String> getStagesToSkip(Node algorithmSet){
    ImmutableList stagesToSkip = ImmutableList.of();
    NodeList algorithmNodes = algorithmSet.getChildNodes();
    for(int i=0; i<algorithmNodes.getLength(); i++) {
      Node node = algorithmNodes.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if (element.getTagName().equals("stages_to_skip")) {
          stagesToSkip = FluentIterable.of(element.getTextContent().trim().split
              (","))
              .transform((String token) -> token.trim()).filter((String
                  token)->!token.isEmpty()).toList();
          break;
        }
      }
    }
    return stagesToSkip;
  }

  private static ImmutableList<String> getAlgorithmsForEntityTypeReassignment
      (Node algorithmSet){
    ImmutableList algorithms = ImmutableList.of();
    NodeList algorithmNodes = algorithmSet.getChildNodes();
    for(int i=0; i<algorithmNodes.getLength(); i++) {
      Node node = algorithmNodes.item(i);
      if (node instanceof Element) {
        Element element = (Element) node;
        if (element.getTagName().equals("algorithms_for_entity_type_reassignment")) {
          algorithms = FluentIterable.of(element.getTextContent().trim().split
                  (","))
                  .transform(
                          new com.google.common.base.Function<String, String>() {
                            public String apply(String token) {
                              return token.trim();
                            }
                          }).toList();
          break;
        }
      }
    }
    return algorithms;
  }

  private static Map<String, String> getDeduplicationParameters(Node kbProps) {
    NodeList kbDetails = kbProps.getChildNodes();
    Map<String, String> dedupParams = new HashMap<>();
    for(int i=0;i<kbDetails.getLength();i++){
      Node kbDetail = kbDetails.item(i);
      if(kbDetail instanceof Element){
        Element element = (Element)kbDetail;
        if(element.getTagName().equals("metadata_db")){
          String dbHost = element.getAttribute("host");
          checkArgument(StringUtils.isNumeric(element.getAttribute("port")),"metadata_db port should contain only numbers");  
          String dbPort = element.getAttribute("port");
          String dbName = element.getAttribute("dbName");
          String dbUser = element.getAttribute("username");
          String dbPassword = element.getAttribute("password");
          checkArgument(!dbHost.isEmpty() && !dbPort.isEmpty() && !dbName.isEmpty() && !dbUser.isEmpty()
              && !dbPassword.isEmpty(), "No attribute of metadata_db can be empty");
          dedupParams.put("db_server", dbHost);
          dedupParams.put("db_port", dbPort);
          dedupParams.put("db_name", dbName);
          dedupParams.put("db_username", dbUser);
          dedupParams.put("db_password", dbPassword);
          dedupParams.put("db_sentence_table_name", "SentenceMD5");
          dedupParams.put("db_overlap_table_name", "OverlappingDocument");
          dedupParams.put("min_sentence_length", "20");
          dedupParams.put("max_hash_chars", "50");
          dedupParams.put("score_threshold", "0.5");
          break;
        }
      }
    }
    return dedupParams;
  }


  private static KBParameters getKBParameters(Node kbProps) {
    NodeList kbDetails = kbProps.getChildNodes();
    String tripleStoreUrl = null;
    String dbHost = null;
    String dbPort = null;
    String dbName = null;
    String dbUser = null;
    String dbPassword = null;

    for(int i=0;i<kbDetails.getLength();i++){
      Node kbDetail = kbDetails.item(i);
      if(kbDetail instanceof Element){
        Element element = (Element)kbDetail;
        if(element.getTagName().equals("triple_store")){
          tripleStoreUrl = element.getAttribute("url");
          checkArgument(!tripleStoreUrl.isEmpty(),"url for triple_store cannot be empty");
        }else if(element.getTagName().equals("metadata_db")){
          dbHost = element.getAttribute("host");
          checkArgument(StringUtils.isNumeric(element.getAttribute("port")),"metadata_db port should contain only numbers");  
          dbPort = element.getAttribute("port");
          dbName = element.getAttribute("dbName");
          dbUser = element.getAttribute("username");
          dbPassword = element.getAttribute("password");
          checkArgument(!dbHost.isEmpty() && !dbPort.isEmpty() && !dbName.isEmpty() && !dbUser.isEmpty()
              && !dbPassword.isEmpty(),"No attribute of metadata_db can be empty");
        }
      }
    }
    String metadataUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;
    log.info("Creating KBParameters with triple-store: {} and postgres: {}", tripleStoreUrl,
            metadataUrl);
    return new KBParameters(tripleStoreUrl, metadataUrl,
        dbUser, dbPassword, "/sparql", "/sparql", true);
  }

  private static E2eConfig parseConfig(String configFilePath) throws
                                                              InvalidConfigException,
                                                              FileNotFoundException {
    List<LANGUAGE> languages = new ArrayList<>();
    Map<LANGUAGE,List<AlgorithmSpecifications>> algorithmSpecifications = new HashMap<>();
    Map<LANGUAGE,List<String>> stagesToSkip = new HashMap<>();
    Map<LANGUAGE,List<String>> algorithmsForEntityTypeReassignment = new HashMap<>();
    Map<LANGUAGE,String> inputDirectories = new HashMap<>();
    boolean runDocumentDeduplication = false;
    boolean isDebugMode = false;
    boolean serializeRDDs = true;
    boolean gatherStatistics = false;
    String statsDirectoryPath = null;
    boolean generateKBReports = false;
    String kbReportsOutputDir = null;
    File configFile = new File(configFilePath);
    if(!configFile.exists()){
      throw new FileNotFoundException("Could not read file: "+configFilePath);
    }
    DocumentBuilder documentBuilder = null;
    Document doc = null;
    try {
      documentBuilder = DocumentBuilderFactory.newInstance()
          .newDocumentBuilder();
      doc = documentBuilder.parse(configFile);
    }catch (Exception e){
      throw new InvalidConfigException(e);
    }
    NodeList algorithms = doc.getElementsByTagName("algorithm_set");//.item(0).getChildNodes();
    for(int i=0;i<algorithms.getLength();i++){
      Node algorithm = algorithms.item(i);
      if(algorithm instanceof Element){
        LANGUAGE language = null;
        String languageStr = ((Element)algorithm).getAttribute("language").trim();
        try {
         language =E2eConstants.LANGUAGE.valueOf(languageStr);
        }catch(IllegalArgumentException e){
          throw new InvalidConfigException("Found an unsupported language: "+languageStr+". "
              + "Allowed values are one of "+
              ImmutableList.copyOf(E2eConstants.LANGUAGE.values()).toString());
        }
        languages.add(language);
        Node inputDirectoryNode = ((Element)algorithm).getElementsByTagName("input_directory").item(0);
        String inputDirectory = inputDirectoryNode!=null?inputDirectoryNode.getTextContent().trim():"";
        checkArgument(!inputDirectory.isEmpty(),"inputDirectory for an algorithm must be present");
        inputDirectories.put(language,inputDirectory);
        algorithmSpecifications.put(language,getAlgorithmsToRun(algorithm));
        stagesToSkip.put(language,getStagesToSkip(algorithm));
        algorithmsForEntityTypeReassignment.put(language,getAlgorithmsForEntityTypeReassignment(algorithm));
      }
    }
    Node debugProps = doc.getElementsByTagName("debug_config").item(0);
    if(debugProps instanceof Element){
      Element element = (Element)debugProps;
      String debugMode = element.getAttribute("is_debug_mode");
      if(!debugMode.isEmpty()){
        isDebugMode = Boolean.parseBoolean(debugMode);
      }
      String gatherStatisticsStr = element.getAttribute("gather_statistics");
      if(!gatherStatisticsStr.isEmpty()){
        gatherStatistics = Boolean.parseBoolean(gatherStatisticsStr);
      }
      if(gatherStatistics){
        statsDirectoryPath = element.getAttribute("stats_directory_path");
        checkArgument(!statsDirectoryPath.isEmpty(),"Absent statsDirectoryPath when gather_statistics was "
            + "true");
        File statsDirectory = new File(statsDirectoryPath);
        statsDirectory.mkdirs();
        if(!statsDirectory.exists()){
          throw new FileNotFoundException("Could not access the stats directory from path "+statsDirectoryPath+". Make sure " +
                  "that the directory exists on the shared file system");
        }
        if(!statsDirectory.canWrite()){
          throw new InvalidConfigException("stats directory path "+statsDirectoryPath+" is not a writable directory");
        }
      }
    }
    Node runDeduplication = doc.getElementsByTagName("run_document_deduplication").item(0);
    if(runDeduplication instanceof Element){
      runDocumentDeduplication =  Boolean.parseBoolean(runDeduplication.getTextContent().trim());
    }
    int algorithmTimeOutMinutes=30;
    Node algorithmTimeOut = doc.getElementsByTagName("algorithm_time_out").item(0);
    if(algorithmTimeOut instanceof Element){
      algorithmTimeOutMinutes =  Integer.parseInt(algorithmTimeOut.getTextContent().trim());
    }
    Node serializeRDDsNode = doc.getElementsByTagName("serialize_rdds_to_disk").item(0);
    if(serializeRDDsNode instanceof Element){
      String seriaLizeRDDsStr = serializeRDDsNode.getTextContent().trim();
      seriaLizeRDDsStr = seriaLizeRDDsStr.isEmpty()?"true":seriaLizeRDDsStr;
      checkArgument(seriaLizeRDDsStr.equals
          ("true")|| seriaLizeRDDsStr.equals("false"),
          "Invalid value for serialize_rdds_to_disk; allowed values are "
              + "'true' and 'false'");

      serializeRDDs =  Boolean.parseBoolean(seriaLizeRDDsStr);
    }
    Node kbProps = doc.getElementsByTagName("kb_config").item(0);
    String clearKBString =  ((Element)kbProps).getAttribute("clear_kb").trim();
    boolean clearKB = clearKBString.isEmpty()?false:Boolean.parseBoolean(clearKBString);
    String corpusId = ((Element)kbProps).getAttribute("corpus_id");
    checkArgument(!corpusId.isEmpty(), "corpus_id cannot be empty");
    Map<String,String> dedupParams = getDeduplicationParameters(kbProps);
    KBParameters kbParameters = getKBParameters(kbProps);
    Node kbReportingProps = doc.getElementsByTagName("kb_reporting_config").item(0);
    if(kbReportingProps instanceof Element){
      Element element = (Element)kbReportingProps;
      String generateReports = element.getAttribute("generate_kb_reports");
      if(!generateReports.isEmpty()){
        generateKBReports = Boolean.parseBoolean(generateReports);
      }
      if(generateKBReports){
        kbReportsOutputDir = element.getAttribute("kb_report_output_dir");
        checkArgument(!kbReportsOutputDir.isEmpty(),"Absent kb_report_output_dir when "
            + "generate_kb_reports was true");
        File kbReportsDir = new File(kbReportsOutputDir);
        kbReportsDir.mkdirs();
        if(!kbReportsDir.exists()){
          throw new FileNotFoundException("Could not access the kb-reports directory from path "+kbReportsDir+". Make sure " +
                  "that the directory exists on the shared file system");
        }
        if(!kbReportsDir.canWrite()){
          throw new InvalidConfigException("kb-reports directory path "+kbReportsDir+" is not a writable directory");
        }
      }
    }
    String xmlReadModeStr = "DEFAULT";
    NodeList nodes = doc.getElementsByTagName("xml_read_mode");
    if (nodes != null && nodes.getLength() > 0) {
      Node xmlReadMode = nodes.item(0);
      if (null != xmlReadMode && xmlReadMode instanceof Element) {
        String xrm = xmlReadMode.getTextContent();
        if (null != xrm && ! xrm.trim().isEmpty()) {
          xmlReadModeStr = xmlReadMode.getTextContent().trim();
        }
      }
      checkArgument(xmlReadModeStr.equals("DEFAULT")||xmlReadModeStr.equals("RAW_XML")||xmlReadModeStr
        .equals("RAW_XML_TAC"),"Invalid value for xml_read_mode.");
    }


    Properties kbResolver = new Properties();
    Node kbResolverConfigPath = doc.getElementsByTagName("kb_resolver_config").item(0);
    if(kbResolverConfigPath!=null && kbResolverConfigPath instanceof Element){
      String kbResolverConfigFile = kbResolverConfigPath.getTextContent().trim();
      try {
        InputStream is = Reader.findStreamInClasspathOrFileSystem(kbResolverConfigFile);
        kbResolver.loadFromXML(is);
      }catch(IOException e){
        throw new InvalidConfigException(e);
      }
      checkArgument(kbResolver.containsKey("filterTypes"),"kb_resolver_properties must specify"
          + " the filterTypes to run.");
    }

    return new E2eConfig(clearKB, serializeRDDs, runDocumentDeduplication, configFilePath, languages, inputDirectories, algorithmSpecifications, stagesToSkip, algorithmsForEntityTypeReassignment, kbParameters, dedupParams,
        DocumentMaker.XMLReadMode.valueOf(xmlReadModeStr), isDebugMode, gatherStatistics, Optional
        .fromNullable
        (statsDirectoryPath),
        corpusId,
        generateKBReports, Optional.fromNullable(kbReportsOutputDir), kbResolver,
            algorithmTimeOutMinutes);
  }

  public boolean runDocumentDeduplication(){
    return runDocumentDeduplication;
  }

  public String configFilePath() {
    return configFilePath;
  }

  public List<LANGUAGE> languages() {
    return languages;
  }

  public Map<LANGUAGE,String> inputDirectories() {
    return inputDirectories;
  }

  public List<AlgorithmSpecifications> algorithmSpecifications(LANGUAGE language) {
    return algorithmSpecifications.get(language);
  }

  public boolean useMissingEventTypesHack(LANGUAGE language) {
    if(useMissingEventTypesHack!=null){
      return useMissingEventTypesHack.get(language);
    }
    useMissingEventTypesHack = new HashMap<>();
    for(LANGUAGE lang : this.languages) {
      Optional<AlgorithmSpecifications> relationAlgorithm = getAlgorithmByType
              (lang, ALGORITHM_TYPE_RELATION_EXTRACTION);
      Optional<AlgorithmSpecifications> eventAlgorithm = getAlgorithmByType
              (lang, ALGORITHM_TYPE_EVENT_EXTRACTION);
      if(relationAlgorithm.isPresent()&&eventAlgorithm.isPresent()
          &&relationAlgorithm.get().algorithmModule().getCanonicalName()
          .contains("StanfordCoreNlpProcessor")
          &&eventAlgorithm.get().algorithmModule().getCanonicalName()
          .contains("SerifAdeptModule")){
        useMissingEventTypesHack.put(lang,Boolean.TRUE);
      }else {
        useMissingEventTypesHack.put(lang, Boolean.FALSE);
      }
    }
    return useMissingEventTypesHack.get(language);
  }

  public Optional<AlgorithmSpecifications> getAlgorithmByType(LANGUAGE language, String
      algorithmType){
    if(algorithmTypeToSpecsMapByLanguage ==null){
      algorithmTypeToSpecsMapByLanguage = HashBasedTable.create();
      for(LANGUAGE lang : this.languages) {
        for (AlgorithmSpecifications algorithmSpecifications :
            algorithmSpecifications
            .get(lang)) {
          algorithmTypeToSpecsMapByLanguage.put(lang,
              algorithmSpecifications.algorithmType(),
              algorithmSpecifications);
        }
      }
    }
    return Optional.fromNullable(algorithmTypeToSpecsMapByLanguage.get(language,algorithmType));
  }

  public List<String> stagesToSkip(E2eConstants.LANGUAGE language) {
    return stagesToSkip.get(language);
  }

  public List<String> algorithmsForEntityTypeReassignment(E2eConstants.LANGUAGE language) {
    return algorithmsForEntityTypeReassignment.get(language);
  }

  public Map<String,String> dedupParams() {
    return dedupParams;
  }

  public KBParameters kbParameters() {
    return kbParameters;
  }

  public DocumentMaker.XMLReadMode xmlReadMode() {
    return this.xmlReadMode;
  }

  public boolean serializeRDDs() {
    return serializedRDDs;
  }

  public boolean isDebugMode() {
    return isDebugMode;
  }

  public boolean gatherStatistics() {
    return gatherStatistics;
  }

  public Optional<String> statsDirectoryPath() {
    return Optional.fromNullable(statsDirectoryPath);
  }

  public String corpusId() {
    return corpusId;
  }

  public boolean generateKBReports() {
    return generateKBReports;
  }

  public Optional<String> kbReportsOutputDir() {
    return Optional.fromNullable(kbReportsOutputDir);
  }

  public Properties kbResolverProperties() {
    return this.kbResolverProperties;
  }

  public boolean ifClearKB(){
    return clearKB;
  }

  public int algorithmTimeOut(){
    return this.algorithmTimeOut;
  }

}
