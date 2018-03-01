package adept.e2e.exceptions;

import adept.e2e.driver.E2eConstants;

/**
 * Created by msrivast on 7/24/17.
 */
public class CannotProceedWithoutClearingKBException extends Exception{

  public CannotProceedWithoutClearingKBException() {
    super();
  }

  public CannotProceedWithoutClearingKBException(String message) {
    super(message);
  }

  public CannotProceedWithoutClearingKBException(E2eConstants.LANGUAGE language, String stageName) {
    super("Driver for language "+language+" found new or failed documents in stage "+stageName+", but clearKB property is false.");
  }
}
