package adept.e2e.exceptions;

/**
 * Created by msrivast on 7/24/17.
 */
public class E2eException extends Exception{

  public E2eException() {
    super();
  }

  public E2eException(String message) {
    super(message);
  }

  public E2eException(Throwable t){
    super(t);
  }

}
