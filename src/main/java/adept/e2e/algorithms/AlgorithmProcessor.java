package adept.e2e.algorithms;

import adept.common.HltContentContainer;
import adept.module.IDocumentProcessor;

import java.util.concurrent.Callable;

/**
 * Created by msrivast on 9/14/16.
 */
public class AlgorithmProcessor implements Callable<HltContentContainer> {

  private IDocumentProcessor algorithmProcessor;
  private HltContentContainer hltContentContainer;
  private boolean usesDeprecatedProcessCall;

  AlgorithmProcessor(IDocumentProcessor algorithmProcessor,
      HltContentContainer hltContentContainer, boolean usesDeprecatedProcessCall) {
    this.algorithmProcessor = algorithmProcessor;
    this.hltContentContainer = hltContentContainer;
    this.usesDeprecatedProcessCall = usesDeprecatedProcessCall;
  }

  @Override
  public HltContentContainer call() throws Exception {
    if (!usesDeprecatedProcessCall) {
      algorithmProcessor.process(hltContentContainer);
    } else {
      hltContentContainer =
          algorithmProcessor.process(hltContentContainer.getDocument(), hltContentContainer);
    }
    return hltContentContainer;
  }
}
