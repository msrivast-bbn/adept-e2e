package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class InvalidConfigException extends E2eException{

  public InvalidConfigException() {
    super();
  }

  public InvalidConfigException(String message) {
    super(message);
  }

  public InvalidConfigException(Throwable t) {
    super(t);
  }

}
