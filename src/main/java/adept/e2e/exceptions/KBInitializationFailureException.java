package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class KBInitializationFailureException extends E2eException{

  public KBInitializationFailureException() {
    super();
  }

  public KBInitializationFailureException(String message) {
    super(message);
  }

  public KBInitializationFailureException(Throwable t) {
    super(t);
  }

}
