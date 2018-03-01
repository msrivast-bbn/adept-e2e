package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class ModuleFailureException extends E2eException{

  public ModuleFailureException() {
    super();
  }

  public ModuleFailureException(String message) {
    super(message);
  }

  public ModuleFailureException(Throwable t) {
    super(t);
  }

}
