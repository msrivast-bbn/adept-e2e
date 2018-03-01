package adept.e2e.stageresult;


import com.google.common.collect.ImmutableMap;

import java.io.Serializable;


/**
 * Created by msrivast on 10/28/16.
 */
public final class DocumentResultObject<T, U> extends ResultObject<T, U> implements Serializable {

  private static final long serialVersionUID = -5676765498000350410L;

  private DocumentResultObject(T inputArtifact, U outputArtifact, boolean resultSuccess,
      ImmutableMap<String, ? extends Object> properties) {
    super(inputArtifact, outputArtifact, resultSuccess, properties);
  }

  public static <T,U> DocumentResultObject create(T inputArtifact) {
    return new DocumentResultObject(inputArtifact, null, false, null);
  }

  public static <T,U> DocumentResultObject create(T inputArtifact, ImmutableMap<String, Object>
      properties) {
    return new DocumentResultObject(inputArtifact, null, false, properties);
  }

  /** Performs a shallow copy */
  public static <T,U> DocumentResultObject copyOf(DocumentResultObject<T,U> documentResultObject) {
    return new DocumentResultObject<>(
            documentResultObject.getInputArtifact(),
            documentResultObject.getOutputArtifact().orNull(),
            documentResultObject.resultSuccess,
            documentResultObject.properties.orNull()
    );
  }

//  @Override
//  public void setInputArtifact(T inputArtifact) {
//    throw new UnsupportedOperationException("Cannot set inputArtifact to an existing "
//        + "DocumentResultObject. Use create(T inputArtifact) instead");
//  }

}


