package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class DriverAlreadyExistsException extends E2eException{

  public DriverAlreadyExistsException() {
    super("Cannot instantiate multiple A2KD drivers");
  }

  public DriverAlreadyExistsException(String message) {
    super(message);
  }

}
