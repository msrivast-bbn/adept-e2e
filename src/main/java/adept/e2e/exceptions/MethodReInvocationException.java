package adept.e2e.exceptions;

import java.lang.reflect.Method;

/**
 * Created by msrivast on 7/24/17.
 */
public class MethodReInvocationException extends E2eException{

  public MethodReInvocationException() {
    super();
  }

  public MethodReInvocationException(Method method) {
    super("Cannot call the method "+method.getName()+" more than once on this driver.");
  }

  public MethodReInvocationException(String message) {
    super(message);
  }

  public MethodReInvocationException(Throwable t) {
    super(t);
  }

}
