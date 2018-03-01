package adept.e2e.stageresult;


import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.e2e.driver.SerializerUtil;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Created by msrivast on 10/28/16.
 */
public class ResultObject<T, U> implements Serializable {

  private static final long serialVersionUID = 8953769160137960211L;
  protected T inputArtifact;
  protected U outputArtifact;
  protected boolean resultSuccess;
  protected Optional<ImmutableMap<String, ? extends Object>> properties;

  protected ResultObject(T inputArtifact, U outputArtifact, boolean resultSuccess,
      ImmutableMap<String, ? extends Object> properties) {
    this.inputArtifact = inputArtifact;
    this.outputArtifact = outputArtifact;
    this.resultSuccess = resultSuccess;
    this.properties = Optional.fromNullable(properties);
  }

  public static <T,U> ResultObject create(T inputArtifact){
    checkNotNull(inputArtifact);
    return new ResultObject(inputArtifact,null,false,null);
  }

  public T getInputArtifact() {
    return this.inputArtifact;
  }

//  public void setInputArtifact(T inputArtifact) {
//    checkNotNull(inputArtifact);
//    this.inputArtifact = inputArtifact;
//  }

  public Optional<U> getOutputArtifact() {
    return Optional.fromNullable(outputArtifact);
  }

  public void setOutputArtifact(U outputArtifact) {
    this.outputArtifact = outputArtifact;
  }

  public boolean isSuccessful() {
    return resultSuccess;
  }

  public void markSuccessful() {
    this.resultSuccess = true;
  }

  public void markFailed() {
    this.resultSuccess = false;
  }

  public Optional<ImmutableMap<String, ? extends Object>> getPropertiesMap() {
    return this.properties;
  }

  public void setPropertiesMap(ImmutableMap<String, ? extends Object> propertiesMap) {
    this.properties = Optional.fromNullable(propertiesMap);
  }

  public Object getProperty(String key) {
    if (properties.isPresent()) {
      return properties.get().get(key);
    }
    return null;
  }

  @Override
  public String toString() {
    String retVal = "";
    try {
      String result = Boolean.toString(this.resultSuccess);
      String properties = SerializerUtil.serialize(this.properties);
      String output = outputArtifact != null ? SerializerUtil.serialize(this.outputArtifact)
                                             : "output:null";
      retVal = "{resultSuccess=" + result + "}\n{properties=" + properties + "}\n{output=" + output
          + "}";
    } catch (Exception e) {
      retVal = "Caught exception when trying to serialize the result-object: " + e.getMessage();
    }
    return retVal;
  }

}
