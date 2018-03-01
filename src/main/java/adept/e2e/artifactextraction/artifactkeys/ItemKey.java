package adept.e2e.artifactextraction.artifactkeys;

import java.io.Serializable;

/**
 * Created by msrivast on 9/28/16.
 */
public abstract class ItemKey implements Serializable {

  @Override
  abstract public boolean equals(Object that);

  @Override
  abstract public int hashCode();

  @Override
  abstract public String toString();

}
