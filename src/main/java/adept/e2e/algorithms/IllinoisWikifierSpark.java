package adept.e2e.algorithms;

import org.apache.spark.SparkEnv;

import adept.common.AdeptException;
import adept.common.HltContentContainer;
import adept.e2e.exceptions.E2eException;
import adept.e2e.exceptions.ModuleActivationException;
import edu.illinois.cs.cogcomp.xlwikifier.postprocessing.SurfaceClustering;
import edu.uiuc.xlwikifier.XLWikifierPreprocessor;


public class IllinoisWikifierSpark extends SparkAlgorithmComponent {

  private static XLWikifierPreprocessor xlWikifierPreprocessor;
  private static AlgorithmActivationStatus adjunctAlgorithmsActivationStatus = new AlgorithmActivationStatus();

  public IllinoisWikifierSpark(AlgorithmSpecifications algorithmSpecifications, int timeOut) {

      super(algorithmSpecifications, timeOut);//Always throw exception in case algorithm module's instantiation or activation failed
    }

  @Override
  public HltContentContainer preProcessHltCC(HltContentContainer
      hltContentContainer) throws E2eException {
    HltContentContainer hltCC = null;
    try{
      hltCC = xlWikifierPreprocessor.annotate
          (hltContentContainer.getDocument());
    }catch (AdeptException e){
      throw new E2eException(e);
    }
    return hltCC;
  }

  @Override
  public void activateModule() throws ModuleActivationException {
    super.activateModule();
    synchronized (adjunctAlgorithmsActivationStatus) {
      if (!adjunctAlgorithmsActivationStatus.isAlgorithmActivated()) {
        adjunctAlgorithmsActivationStatus.setAlgorithmAsActivated();
        xlWikifierPreprocessor = new XLWikifierPreprocessor();
        try {
          xlWikifierPreprocessor
              .activate(this.algorithmSpecifications.configFilePath());
        }catch (Exception e){
          throw new ModuleActivationException(e);
        }
        // Before ultimately running nil clustering, we must avoid collisions of initial NIL IDs from the
        // wikifier generated on a given executor with those on other executors. Therefore,
        // use the executor ID to create an initial offset for the generated NIL IDs on that executor.
        int initialNilCnt = SurfaceClustering.getNilCnt();
        if (initialNilCnt != 1) {
          throw new IllegalStateException("Expected edu.illinois.cs.cogcomp.xlwikifier.postprocessing.SurfaceClustering.nil_cnt to be 1 but was " + initialNilCnt);
        }
        String executorIDString = SparkEnv.get().executorId();
        //SparkEnv.get().executorId() returns the string "driver"
        // (instead of a numeric executorId) if a2kd is run on local machine
        // and not on the cluster. That case should be handled separately
        if(executorIDString.equals("driver")){
          executorIDString = "1";
        }
        // Note that we are relying on the executorId() for executors being numerical.
        int executorID = 1;
        try{
            executorID = Integer.valueOf(executorIDString);
        }catch(NumberFormatException nfe){
         throw new ModuleActivationException("Spark returned non-numeric executorId. This "
             + "would prevent IllinoisWikifier from running properly. Please "
             + "rerun a2kd without IllinoisWikifier as the "
             + "wikification_algorithm.");
        }
        if (executorID >= 1000) {
          throw new IllegalStateException("using more than 1000 executors would result in duplicate NIL ID offsets");
        }
        // % 1000 and * 10000000 because a NIL ID string contains 10 digits and we want
        // to set the leftmost 3 as the offset; .e.g, for a desired offset of 456:
        // "NIL4560000001"
        int executorIDNilClusterOffset = (executorID % 1000) * 10000000;
        SurfaceClustering.setNilCnt(initialNilCnt + executorIDNilClusterOffset);
      }
    }
  }
}
