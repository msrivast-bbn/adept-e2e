package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class CallWithTimeoutFailedException extends E2eException{

  public CallWithTimeoutFailedException() {
    super();
  }

  public CallWithTimeoutFailedException(String message) {
    super(message);
  }

  public CallWithTimeoutFailedException(Throwable t) {
    super(t);
  }

}
