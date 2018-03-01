package adept.e2e.algorithms;

import java.io.Serializable;

/**
 * Created by msrivast on 9/23/16.
 */
public final class AlgorithmActivationStatus implements Serializable {

  private static final long serialVersionUID = -1956938456762385183L;
  private boolean isAlgorithmActivated;

  public AlgorithmActivationStatus() {
    isAlgorithmActivated = false;
  }

  public void setAlgorithmAsActivated() {
    isAlgorithmActivated = true;
  }

  public boolean isAlgorithmActivated() {
    return isAlgorithmActivated;
  }
}
