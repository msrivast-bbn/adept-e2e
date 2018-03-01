package adept.e2e.algorithms;

import java.io.Serializable;

import adept.metadata.SourceAlgorithm;
import adept.module.AbstractModule;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 08/14/17.
 */
public class  AlgorithmSpecifications implements Serializable {

  private static final long serialVersionUID = 8920649539024533237L;

  private final AlgorithmIdentity algorithmIdentity;
  private final Class<? extends SparkAlgorithmComponent> sparkWrapperClass;
  private final String ontologyMappingFilePath;
  private final String reverseOntologyMappingFilePath;
  private final boolean usesDeprecatedProcessCall;
  private final SourceAlgorithm sourceAlgorithm;
  private final String algorithmType;


  private AlgorithmSpecifications(String algorithmName, String
      algorithmProvider, String algorithmType, Class<? extends AbstractModule>
      algorithmModule,
                                  Class<? extends SparkAlgorithmComponent> sparkWrapperClass, String configFilePath, String ontologyMappingFilePath, String reverseOntologyMappingFilePath,
                                  boolean usesDeprecatedProcessCall) {
    checkNotNull(algorithmName,"algorithm name cannot be null");
    checkArgument(!algorithmName.isEmpty(),"algorithm name cannot be empty");
    checkNotNull(algorithmProvider,"algorithm provider cannot be null");
    checkArgument(!algorithmProvider.isEmpty(),"algorithm provider cannot be"
        + " empty");
    checkNotNull(algorithmType,"algorithm type cannot be null");
    checkArgument(!algorithmType.isEmpty(),"algorithm type cannot be empty");
    checkNotNull(algorithmModule,"algorithm module cannot be null");
    checkNotNull(sparkWrapperClass,"sparkWrapperClass cannot be null");
    checkNotNull(configFilePath,"configFilePath cannot be null");
    checkArgument(!configFilePath.isEmpty(),"configFilePath cannot be empty");
    checkNotNull(ontologyMappingFilePath,"ontologyMappingFilePath cannot be "
        + "null");
    checkArgument(!ontologyMappingFilePath.isEmpty(),"ontologyMappingFilePath "
        + "cannot "
        + "be "
        + "empty");
    checkArgument(!reverseOntologyMappingFilePath.isEmpty(),
        "reverseOntologyMappingFilePath cannot be empty");
    this.algorithmIdentity = AlgorithmIdentity.getAlgorithmIdentity(algorithmModule,configFilePath);
    this.sparkWrapperClass = sparkWrapperClass;
    this.ontologyMappingFilePath = ontologyMappingFilePath;
    this.reverseOntologyMappingFilePath = reverseOntologyMappingFilePath;
    this.usesDeprecatedProcessCall = usesDeprecatedProcessCall;
    this.sourceAlgorithm = new SourceAlgorithm(algorithmName,algorithmProvider);
    this.algorithmType = algorithmType;
  }

  public static AlgorithmSpecifications create(String algorithmName, String
      algorithmProvider, String algorithmType, Class<? extends AbstractModule>
      algorithmModule,
                                  Class<? extends SparkAlgorithmComponent> sparkWrapperClass, String configFilePath, String ontologyMappingFilePath, String reverseOntologyMappingFilePath,
                                  boolean usesDeprecatedProcessCall) {
    return new AlgorithmSpecifications(algorithmName,algorithmProvider,
        algorithmType,
        algorithmModule,sparkWrapperClass,configFilePath,ontologyMappingFilePath,reverseOntologyMappingFilePath,
            usesDeprecatedProcessCall);
  }

  public String algorithmName(){
    return sourceAlgorithm.getAlgorithmName();
  }

  public String algorithmProvider(){
    return sourceAlgorithm.getContributingSiteName();
  }

  public String algorithmType(){
    return this.algorithmType;
  }

  public SourceAlgorithm sourceAlgorithm(){
    return sourceAlgorithm;
  }

  public AlgorithmIdentity algorithmIdentity(){
    return algorithmIdentity;
  }

  public Class<? extends AbstractModule> algorithmModule(){
    return algorithmIdentity.algorithmModule();
  }

  public Class<? extends SparkAlgorithmComponent> sparkWrapperClass(){
    return sparkWrapperClass;
  }

  public String configFilePath(){
    return algorithmIdentity.configFilePath();
  }

  public String ontologyMappingFilePath(){
    return ontologyMappingFilePath;
  }

  public String reverseOntologyMappingFilePath(){
    return reverseOntologyMappingFilePath;
  }

  public boolean moduleUsesDeprecatedProcessCall(){
    return usesDeprecatedProcessCall;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AlgorithmSpecifications that = (AlgorithmSpecifications) o;

    if (!algorithmType.equals(that.algorithmType)) {
      return false;
    }
    if (usesDeprecatedProcessCall != that.usesDeprecatedProcessCall) {
      return false;
    }
    if (!algorithmIdentity.equals(that.algorithmIdentity)) {
      return false;
    }
    if (!sparkWrapperClass.equals(that.sparkWrapperClass)) {
      return false;
    }
    if (!ontologyMappingFilePath.equals(that.ontologyMappingFilePath)) {
      return false;
    }
    return reverseOntologyMappingFilePath
        .equals(that.reverseOntologyMappingFilePath);
  }

  @Override
  public int hashCode() {
    int result = algorithmIdentity.hashCode();
    result = 31 * result + algorithmType.hashCode();
    result = 31 * result + sparkWrapperClass.hashCode();
    result = 31 * result + ontologyMappingFilePath.hashCode();
    result = 31 * result + reverseOntologyMappingFilePath.hashCode();
    result = 31 * result + (usesDeprecatedProcessCall ? 1 : 0);
    return result;
  }
}
