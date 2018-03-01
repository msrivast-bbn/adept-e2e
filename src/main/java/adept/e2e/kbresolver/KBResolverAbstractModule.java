package adept.e2e.kbresolver;

import adept.kbapi.KBParameters;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

/**
 * Created by bmin on 11/29/16.
 */
public abstract class KBResolverAbstractModule implements
    Serializable {
  public abstract void run();

  public abstract void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) throws InvalidPropertiesFormatException;
}
