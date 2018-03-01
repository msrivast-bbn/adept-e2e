package adept.e2e.algorithms;

import java.io.Serializable;

import adept.module.AbstractModule;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 08/14/17.
 */
public class AlgorithmIdentity implements Serializable{

  private static final long serialVersionUID = 7452149920450765114L;

  private final Class<? extends AbstractModule> algorithmModule;
  private final String configFilePath;

  private AlgorithmIdentity(Class<? extends AbstractModule> algorithmModule,
                    String configFilePath) {
    checkNotNull(algorithmModule,"algorithmModule cannot be null");
    checkNotNull(configFilePath,"configFilePath cannot be null");
    checkArgument(!configFilePath.isEmpty(),"configFilePath cannot be empty");
    this.algorithmModule = algorithmModule;
    this.configFilePath = configFilePath;
  }

  public static AlgorithmIdentity getAlgorithmIdentity(Class<? extends
      AbstractModule> algorithmModule,
    String configFilePath){
    return new AlgorithmIdentity(algorithmModule,configFilePath);
  }

  public Class<? extends AbstractModule> algorithmModule(){
    return algorithmModule;
  }

  public String configFilePath(){
    return configFilePath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AlgorithmIdentity that = (AlgorithmIdentity) o;

    if (algorithmModule != null ? !algorithmModule.equals(that.algorithmModule) : that.algorithmModule
        != null)
      return false;
    return configFilePath != null ? configFilePath.equals(that.configFilePath) : that.configFilePath == null;
  }

  @Override
  public int hashCode() {
    int result = algorithmModule != null ? algorithmModule.hashCode() : 0;
    result = 31 * result + (configFilePath != null ? configFilePath.hashCode() : 0);
    return result;
  }
}
