package adept.e2e.analysis;

import com.google.common.base.Optional;

import java.io.Serializable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 2/6/17.
 */
public class ResultStats implements Serializable {

  private static final long serialVersionUID = 3965627890613680270L;
  final String moduleName;
  final Optional<String> docId;
  final Long timeTaken;
  final boolean resultSuccessful;
  final String exceptionString;
  final Map<String, Integer> propertiesCountMap;

  protected ResultStats(String moduleName, Optional<String> docId, boolean resultSuccessful,
      Optional<Long> timeTaken, Optional<String> exceptionString, Optional<Map<String, Integer>>
      propertiesCountMap) {
    checkNotNull(moduleName);
    checkNotNull(docId);
    this.moduleName = moduleName;
    this.docId = docId;
    this.resultSuccessful = resultSuccessful;
    this.timeTaken = timeTaken.orNull();
    this.exceptionString = exceptionString.orNull();
    this.propertiesCountMap = propertiesCountMap.orNull();
  }

  public static ResultStats create(String moduleName, Optional<String> docId, boolean
      resultSuccessful,
      Optional<Long> timeTaken, Optional<String> exceptionString, Optional<Map<String, Integer>>
      aggregateCountMap) {
    return new ResultStats(moduleName, docId, resultSuccessful, timeTaken, exceptionString,
        aggregateCountMap);
  }

  public static Builder builder(String moduleName, Optional<String> docId, boolean
      resultSuccessful) {
    return new Builder(moduleName, docId, resultSuccessful);
  }

  public String moduleName() {
    return this.moduleName;
  }

  public Optional<String> docId() {
    return this.docId;
  }

  public boolean isResultSuccessful() {
    return this.resultSuccessful;
  }

  public Optional<Long> timeTaken() {
    return Optional.fromNullable(this.timeTaken);
  }

  public Optional<String> exceptionString() {
    return Optional.fromNullable(this.exceptionString);
  }

  public Optional<Map<String, Integer>> getPropertiesCountMap() {
    return Optional.fromNullable(this.propertiesCountMap);
  }

  public static class Builder {

    protected final String moduleName;
    protected final Optional<String> docId;
    protected final boolean resultSuccessful;
    protected Long timeTaken;
    protected String exceptionString;
    protected Map<String, Integer> propertiesCount;

    protected Builder(String moduleName, Optional<String> docId, boolean resultSuccessful) {
      checkNotNull(moduleName);
      this.moduleName = moduleName;
      this.docId = docId;
      this.resultSuccessful = resultSuccessful;
    }

    public Builder setTimeTaken(long timeTaken) {
      this.timeTaken = timeTaken;
      return this;
    }

    public Builder setExceptionString(String exceptionString) {
      this.exceptionString = exceptionString;
      return this;
    }

    public Builder setPropertiesCountMap(Map<String, Integer> propertiesCount) {
      this.propertiesCount = propertiesCount;
      return this;
    }

    public ResultStats build() {
      return new ResultStats(moduleName, docId, resultSuccessful, Optional.fromNullable(timeTaken)
          , Optional.fromNullable(exceptionString), Optional.fromNullable(propertiesCount));
    }
  }
}
