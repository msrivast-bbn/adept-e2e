package adept.e2e.exceptions;

import com.google.common.collect.FluentIterable;

import java.lang.reflect.Method;

/**
 * Created by msrivast on 7/24/17.
 */
public class OutOfOrderMethodCallException extends E2eException{

  public OutOfOrderMethodCallException() {
    super();
  }

  public OutOfOrderMethodCallException(Method method, Method ... dependencies) {
    super("Cannot call the method "+method.getName()+" before calling methods " 
        + FluentIterable.from(dependencies).transform((Method dependency) ->
        dependency.getName()).toList());
  }

  public OutOfOrderMethodCallException(String message) {
    super(message);
  }

  public OutOfOrderMethodCallException(Throwable t) {
    super(t);
  }

}
