package adept.e2e.analysis;

import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SortedSetMultimap;

import java.util.Map;

/**
 * Created by msrivast on 2/6/17.
 */
public class BatchResultStats extends ResultStats {

  private static final long serialVersionUID = -7502486847843136390L;
  private final Optional<Integer> successfulArtifactsCount;
  private final Optional<Integer> failedArtifactsCount;
  private final Optional<SortedSetMultimap<Long, String>> timeTakenByArtifacts;
  private final Optional<Multiset<String>> exceptionDetailsForArtifacts;
  private final Optional<Multimap<String, String>> exceptionMapForArtifacts;
  private final Optional<Map<String, Map<String, Integer>>> propertiesCountForArtifacts;

  protected BatchResultStats(String moduleName, Optional<String> docId, boolean resultSuccessful,
      Optional<Long> timeTaken, Optional<String> exceptionString, Optional<Map<String, Integer>>
      propertiesCount, Optional<Integer> successfulArtifactsCount,
      Optional<Integer> failedArtifactsCount, Optional<SortedSetMultimap<Long, String>>
      timeTakenByArtifacts, Optional<Multiset<String>> exceptionDetailsForArtifacts,
      Optional<Multimap<String, String>> exceptionMapForArtifacts, Optional<Map<String,
      Map<String, Integer>>> propertiesCountForArtifacts) {
    super(moduleName, docId, resultSuccessful, timeTaken, exceptionString, propertiesCount);
    this.successfulArtifactsCount = successfulArtifactsCount;
    this.failedArtifactsCount = failedArtifactsCount;
    this.timeTakenByArtifacts = timeTakenByArtifacts;
    this.exceptionDetailsForArtifacts = exceptionDetailsForArtifacts;
    this.exceptionMapForArtifacts = exceptionMapForArtifacts;
    this.propertiesCountForArtifacts = propertiesCountForArtifacts;
  }

  public static BatchResultStats create(String moduleName, Optional<String> docId, boolean
      resultSuccessful,
      Optional<Long> timeTaken, Optional<String> exceptionString, Optional<Map<String, Integer>>
      propertiesCount, Optional<Integer> successfulArtifactsCount,
      Optional<Integer> failedArtifactsCount, Optional<SortedSetMultimap<Long, String>>
      timeTakenByArtifacts, Optional<Multiset<String>> exceptionDetailsForArtifacts,
      Optional<Multimap<String, String>> exceptionMapForArtifacts, Optional<Map<String,
      Map<String, Integer>>> propertiesCountForArtifacts) {
    return new BatchResultStats(moduleName, docId, resultSuccessful, timeTaken, exceptionString,
        propertiesCount, successfulArtifactsCount, failedArtifactsCount, timeTakenByArtifacts,
        exceptionDetailsForArtifacts, exceptionMapForArtifacts, propertiesCountForArtifacts);
  }

  public static BatchResultBuilder builder(String moduleName, Optional<String> docId,
      boolean resultSuccessful) {
    return new BatchResultBuilder(moduleName, docId, resultSuccessful);
  }

  public static BatchResultBuilder builderFromResultStats(ResultStats resultStats) {
    BatchResultBuilder builder = new BatchResultBuilder(resultStats.moduleName(), resultStats
        .docId(), resultStats.isResultSuccessful());
    if (resultStats.timeTaken().isPresent()) {
      builder.setTimeTaken(resultStats.timeTaken().get());
    }
    if (resultStats.exceptionString().isPresent()) {
      builder.setExceptionString(resultStats.exceptionString().get());
    }
    if (resultStats.getPropertiesCountMap().isPresent()) {
      builder.setPropertiesCountMap(resultStats.getPropertiesCountMap().get());
    }
    return builder;
  }

  public Optional<Integer> successfulArtifactsCount() {
    return this.successfulArtifactsCount;
  }

  public Optional<Integer> failedArtifactsCount() {
    return this.failedArtifactsCount;
  }

  public Optional<SortedSetMultimap<Long, String>> timeTakenByArtifacts() {
    return this.timeTakenByArtifacts;
  }

  public Optional<Multiset<String>> exceptionDetailsForArtifacts() {
    return this.exceptionDetailsForArtifacts;
  }

  public Optional<Multimap<String, String>> exceptionMapForArtifacts() {
    return this.exceptionMapForArtifacts;
  }

  public Optional<Map<String, Map<String, Integer>>> propertiesCountForArtifacts() {
    return this.propertiesCountForArtifacts;
  }

  public static class BatchResultBuilder extends Builder {

    protected Integer successfulArtifactsCount;
    protected Integer failedArtifactsCount;
    protected SortedSetMultimap<Long, String> timeTakenByArtifacts;
    protected Multiset<String> exceptionStringsForArtifacts;
    protected Multimap<String, String> exceptionMapForArtifacts;
    protected Map<String, Map<String, Integer>> countPropertiesForArtifacts;

    protected BatchResultBuilder(String moduleName, Optional<String> docId, boolean
        resultSuccessful) {
      super(moduleName, docId, resultSuccessful);
    }

    public BatchResultBuilder setSuccessfulArtifactsCount(Integer successfulArtifactsCount) {
      this.successfulArtifactsCount = successfulArtifactsCount;
      return this;
    }

    public BatchResultBuilder setFailedArtifactsCount(Integer failedArtifactsCount) {
      this.failedArtifactsCount = failedArtifactsCount;
      return this;
    }

    public BatchResultBuilder setTimeTakenByArtifacts(
        SortedSetMultimap<Long, String> timeTakenByArtifacts) {
      this.timeTakenByArtifacts = timeTakenByArtifacts;
      return this;
    }

    public BatchResultBuilder setExceptionStringsForArtifacts(Multiset<String>
        exceptionStringsForArtifacts) {
      this.exceptionStringsForArtifacts = exceptionStringsForArtifacts;
      return this;
    }

    public BatchResultBuilder setExceptionMapForArtifacts(Multimap<String, String>
        exceptionMapForArtifacts) {
      this.exceptionMapForArtifacts = exceptionMapForArtifacts;
      return this;
    }

    public BatchResultBuilder setCountPropertiesMapForArtifacts(Map<String, Map<String, Integer>>
        countPropertiesForArtifacts) {
      this.countPropertiesForArtifacts = countPropertiesForArtifacts;
      return this;
    }

    public BatchResultStats build() {
      return new BatchResultStats(moduleName, docId, resultSuccessful, Optional.fromNullable
          (timeTaken), Optional.fromNullable(exceptionString), Optional.fromNullable
          (propertiesCount), Optional.fromNullable(successfulArtifactsCount), Optional.fromNullable
          (failedArtifactsCount), Optional.fromNullable(timeTakenByArtifacts), Optional
          .fromNullable(exceptionStringsForArtifacts), Optional.fromNullable
          (exceptionMapForArtifacts), Optional.fromNullable(countPropertiesForArtifacts));
    }
  }

}
