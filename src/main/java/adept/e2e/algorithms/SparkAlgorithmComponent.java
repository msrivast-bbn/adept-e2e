package adept.e2e.algorithms;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SimpleTimeLimiter;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import adept.common.HltContentContainer;
import adept.e2e.exceptions.CallWithTimeoutFailedException;
import adept.e2e.exceptions.E2eException;
import adept.e2e.exceptions.ModuleActivationException;
import adept.e2e.stageresult.DocumentResultObject;
import adept.module.AbstractModule;
import adept.module.AdeptModuleException;
import adept.module.IDocumentProcessor;

import static adept.e2e.driver.E2eConstants.ALGORITHM_NAME;
import static adept.e2e.driver.E2eConstants.ALGORITHM_TYPE;
import static adept.e2e.driver.E2eConstants.ONTOLOGY_MAPPING_FILE_PATH;
import static adept.e2e.driver.E2eConstants.PROPERTY_DOCID;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;
import static adept.e2e.driver.E2eConstants.REVERSE_ONTOLOGY_MAPPING_FILE_PATH;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 10/20/16.
 */
public abstract class SparkAlgorithmComponent implements
    Function<DocumentResultObject<HltContentContainer, HltContentContainer>,
        DocumentResultObject<HltContentContainer, HltContentContainer>>{

  protected static final SimpleTimeLimiter simpleTimeLimiter = new SimpleTimeLimiter();
  protected static final Logger log = LoggerFactory.getLogger(SparkAlgorithmComponent.class);
  private static final Map<AlgorithmIdentity, AbstractModule>
      algorithmModules = new HashMap<>();
  private static final Map<AlgorithmIdentity, AlgorithmActivationStatus>
      algorithmActivationStatus = new HashMap<>();

  protected final AlgorithmSpecifications algorithmSpecifications;
  protected final int algorithmTimeOut;

  protected SparkAlgorithmComponent(AlgorithmSpecifications algorithmSpecifications, int algorithmTimeOut){
    checkNotNull(algorithmSpecifications,"algorithmSpecifications cannot be "
        + "null");
    this.algorithmSpecifications = algorithmSpecifications;
    this.algorithmTimeOut = algorithmTimeOut;
  }

  @Override
  public DocumentResultObject<HltContentContainer, HltContentContainer> call
      (DocumentResultObject<HltContentContainer, HltContentContainer> input)
      throws ModuleActivationException {
    activateModule();
    String docId = null;
    HltContentContainer hltContentContainerIn = (HltContentContainer) input.getInputArtifact();
    long timeTakenToProcess = 0L;
    ImmutableMap.Builder propertiesMap = ImmutableMap.builder();
    try {
      if (input.isSuccessful()) {
        log.info("Found successful results from checkpoint for algorithm: {} document: {}, skipping processing.",
                this.algorithmSpecifications.algorithmName(),hltContentContainerIn.getDocument().getDocId());
        return input;
      }
      //creating a copy of input HltCC using SerializationUtils' clone method, since deep-copying
      // an hltContentContainer will be tricky.
      HltContentContainer hltContentContainer = org.apache.commons.lang3.SerializationUtils.clone
          (hltContentContainerIn);
      docId = hltContentContainer.getDocument().getDocId();
      long startTime = System.currentTimeMillis();
      hltContentContainer = preProcessHltCC(hltContentContainer);
      hltContentContainer = processHltCC(hltContentContainer);
      hltContentContainer = postProcessHltCC(hltContentContainer);
      long endTime = System.currentTimeMillis();
      timeTakenToProcess = endTime - startTime;
      input.setOutputArtifact(hltContentContainer);
      input.markSuccessful();
    } catch (Exception e) {
      log.error("Caught an exception when trying to process document: {}" +
          " with {}" ,docId, this.algorithmSpecifications.algorithmName(), e);
      // Return clean copy of the hltcc before any processing had been done on it
      input.setOutputArtifact(null);
      input.markFailed();
      propertiesMap.put(PROPERTY_EXCEPTION_TYPE, e.getClass().getName());
      propertiesMap.put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage() != null ? e.getMessage() : "");
      propertiesMap.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
    }
    propertiesMap.put(PROPERTY_DOCID, docId);
    propertiesMap.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName
        ()+"("+this.algorithmSpecifications.algorithmName()+")");
    propertiesMap.put(PROPERTY_TIME_TAKEN, timeTakenToProcess);
    propertiesMap.put(ALGORITHM_NAME, this.algorithmSpecifications.algorithmName());
    propertiesMap.put(ALGORITHM_TYPE,algorithmSpecifications.algorithmType());
    propertiesMap.put(ONTOLOGY_MAPPING_FILE_PATH, this.algorithmSpecifications.ontologyMappingFilePath());
    propertiesMap.put(REVERSE_ONTOLOGY_MAPPING_FILE_PATH,this.algorithmSpecifications.reverseOntologyMappingFilePath());
    input.setPropertiesMap(propertiesMap.build());
    return input;
  }

  protected void activateModule() throws ModuleActivationException {
    synchronized (SparkAlgorithmComponent.class) {
      AlgorithmIdentity algorithmIdentity = this.algorithmSpecifications
          .algorithmIdentity();
      if (!algorithmActivationStatus.containsKey(algorithmIdentity)) {
        algorithmActivationStatus.put(algorithmIdentity,new
            AlgorithmActivationStatus());
      }
      if(!algorithmModules.containsKey(algorithmIdentity)){
        try {
          algorithmModules.put(algorithmIdentity,
              algorithmSpecifications.algorithmModule().getConstructor()
                  .newInstance());
        }catch (ReflectiveOperationException roe){
          throw new ModuleActivationException(roe);
        }
      }
      if (!algorithmActivationStatus.get(algorithmIdentity)
          .isAlgorithmActivated()) {
        AbstractModule algorithmModule = algorithmModules.get(algorithmIdentity);
        log.info("Activating Module: {}", algorithmModule.getClass()
            .getCanonicalName());
        try {
          algorithmModule.activate(this.algorithmSpecifications.configFilePath());
        } catch (IOException e) {
          throw new ModuleActivationException(e);
        } catch (AdeptModuleException e){
          throw new ModuleActivationException(e);
        }
        algorithmActivationStatus.get(algorithmIdentity).setAlgorithmAsActivated();
        Runtime.getRuntime().addShutdownHook(
            new Thread() {
              public void run() {
                try {
                  log.info("Finally Deactivating Module: {}",
                      algorithmModule.getClass().getCanonicalName());
                  algorithmModule.deactivate();
                  log.info("Module deactivated successfully.");
                } catch (AdeptModuleException e) {
                  log.error("Module deactivation threw the following exception:", e);
                }
              }
            }
        );
      }
    }
  }

  protected HltContentContainer processHltCC(HltContentContainer
      hltContentContainer) throws CallWithTimeoutFailedException {
    AbstractModule algorithmModule = algorithmModules.get(this.algorithmSpecifications.algorithmIdentity());
    long start = System.currentTimeMillis();
    String docId = hltContentContainer.getDocument().getDocId();
    log.info("{} started processing document: {}",algorithmModule.getClass().getCanonicalName(),
            docId);
    try {
      hltContentContainer = simpleTimeLimiter.callWithTimeout(
          new AlgorithmProcessor((IDocumentProcessor) algorithmModule,
              hltContentContainer,
              this.algorithmSpecifications.moduleUsesDeprecatedProcessCall()),
          this.algorithmTimeOut, TimeUnit.MINUTES, true);
    }catch (Exception e){
      throw new CallWithTimeoutFailedException(e);
    }
    hltContentContainer.setSourceAlgorithm(this.algorithmSpecifications.sourceAlgorithm());
    hltContentContainer.setAlgorithmName(this.algorithmSpecifications.algorithmName());
    log.info("{} finished processing document: {} in %%%% {}s %%%%",
            algorithmModule.getClass().getCanonicalName(), docId ,((System.currentTimeMillis() - start) / 1000));
    return hltContentContainer;
  }

  //Default implementation is a do-nothing method
  protected HltContentContainer preProcessHltCC(HltContentContainer hltContentContainer)
      throws E2eException {
    return hltContentContainer;
  }

  //Default implementation is a do-nothing method
  protected HltContentContainer postProcessHltCC(HltContentContainer hltContentContainer)
      throws E2eException {
    return hltContentContainer;
  }

}
