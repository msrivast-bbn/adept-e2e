package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class CannotWriteStatsException extends E2eException{

  public CannotWriteStatsException() {
    super();
  }

  public CannotWriteStatsException(String message) {
    super(message);
  }

  public CannotWriteStatsException(Throwable t) {
    super(t);
  }

}
