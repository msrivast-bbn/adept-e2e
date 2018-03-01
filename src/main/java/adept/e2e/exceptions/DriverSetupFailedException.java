package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class DriverSetupFailedException extends E2eException{

  public DriverSetupFailedException() {
    super();
  }

  public DriverSetupFailedException(String message) {
    super(message);
  }

  public DriverSetupFailedException(Throwable t){
    super(t);
  }

}
