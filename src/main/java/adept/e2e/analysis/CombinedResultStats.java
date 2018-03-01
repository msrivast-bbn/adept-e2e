package adept.e2e.analysis;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import java.util.Map;

/**
 * Created by msrivast on 2/6/17.
 */
public class CombinedResultStats extends BatchResultStats {

  private static final long serialVersionUID = 7369249596080895596L;
  private final int totalSuccessful;
  private final int totalFailed;
  private final Optional<SortedSetMultimap<Long, String>> timeTakenPerDocumentMap;
  private final Optional<Multiset<String>> exceptionDetails;
  private final Optional<Multimap<String, String>> exceptionMap;

  private CombinedResultStats(String moduleName, int totalSuccessful, int totalFailed,
      Optional<Long> combinedTimeTaken, Optional<SortedSetMultimap<Long, String>>
      timeTakenPerDocumentMap, Optional<Multiset<String>> exceptionDetails,
      Optional<Multimap<String,
          String>> exceptionMap, Optional<Map<String, Integer>>
      propertiesCount, Optional<Integer> successfulArtifactsCount,
      Optional<Integer> failedArtifactsCount, Optional<SortedSetMultimap<Long, String>>
      timeTakenByArtifacts, Optional<Multiset<String>> exceptionDetailsForArtifacts,
      Optional<Multimap<String, String>> exceptionMapForArtifacts, Optional<Map<String,
      Map<String, Integer>>> propertiesCountForArtifacts) {
    super(moduleName, Optional.absent(), false, combinedTimeTaken, Optional.absent(), propertiesCount,
        successfulArtifactsCount,
        failedArtifactsCount, timeTakenByArtifacts, exceptionDetailsForArtifacts,
        exceptionMapForArtifacts, propertiesCountForArtifacts);
    this.totalSuccessful = totalSuccessful;
    this.totalFailed = totalFailed;
    this.timeTakenPerDocumentMap = timeTakenPerDocumentMap;
    this.exceptionDetails = exceptionDetails;
    this.exceptionMap = exceptionMap;
  }

  public static CombinedResultStats create(String moduleName, int totalSuccessful, int totalFailed,
      Optional<Long> combinedTimeTaken, Optional<SortedSetMultimap<Long, String>>
      timeTakenMap, Optional<Multiset<String>> exceptionDetails, Optional<Multimap<String,
      String>> exceptionMap, Optional<Map<String, Integer>>
      propertiesCount, Optional<Integer> successfulArtifactsCount,
      Optional<Integer> failedArtifactsCount, Optional<SortedSetMultimap<Long, String>>
      timeTakenByArtifacts, Optional<Multiset<String>> exceptionDetailsForArtifacts,
      Optional<Multimap<String, String>> exceptionMapForArtifacts, Optional<Map<String,
      Map<String, Integer>>> propertiesCountForArtifacts) {
    return new CombinedResultStats(moduleName, totalSuccessful, totalFailed, combinedTimeTaken,
        timeTakenMap, exceptionDetails, exceptionMap,
        propertiesCount, successfulArtifactsCount, failedArtifactsCount, timeTakenByArtifacts,
        exceptionDetailsForArtifacts, exceptionMapForArtifacts, propertiesCountForArtifacts);
  }

  public static CombinedResultBuilder builder(String moduleName, int totalSuccessful,
      int totalFailed) {
    return new CombinedResultBuilder(moduleName, totalSuccessful, totalFailed);
  }

  public static CombinedResultBuilder builderFromResultStats(ResultStats resultStats) {
    int successful = resultStats.isResultSuccessful() ? 1 : 0;
    int failed = resultStats.isResultSuccessful() ? 0 : 1;
    CombinedResultBuilder builder = new CombinedResultBuilder(resultStats.moduleName(),
        successful, failed);
    if (resultStats.timeTaken().isPresent()) {
      builder.setTimeTaken(resultStats.timeTaken().get());
      if(resultStats.docId().isPresent()) {
        builder.addTimeTakenForDocument(resultStats.docId().get(), resultStats.timeTaken().get());
      }
    }
    if (resultStats.exceptionString().isPresent()) {
      builder.addExceptionString(resultStats.exceptionString().get());
      if(resultStats.docId().isPresent()) {
        builder.addExceptionDocIdMapping(resultStats.exceptionString().get(), resultStats.docId()
            .get());
      }
    }
    builder.setPropertiesCountMap(resultStats.getPropertiesCountMap().orNull());
    return builder;
  }

  public static CombinedResultBuilder builderFromBatchResultStats(BatchResultStats resultStats) {
    int successful = resultStats.isResultSuccessful() ? 1 : 0;
    int failed = resultStats.isResultSuccessful() ? 0 : 1;
    CombinedResultBuilder builder = new CombinedResultBuilder(resultStats.moduleName(),
        successful, failed);
    if (resultStats.timeTaken().isPresent()) {
      builder.setTimeTaken(resultStats.timeTaken().get());
      if(resultStats.docId().isPresent()) {
        builder.addTimeTakenForDocument(resultStats.docId().get(), resultStats.timeTaken().get());
      }
    }
    if (resultStats.exceptionString().isPresent()) {
      builder.addExceptionString(resultStats.exceptionString().get());
      if(resultStats.docId().isPresent()) {
        builder.addExceptionDocIdMapping(resultStats.exceptionString().get(), resultStats.docId().get());
      }
    }
    builder.setPropertiesCountMap(resultStats.getPropertiesCountMap().orNull());

    builder.setSuccessfulArtifactsCount(resultStats.successfulArtifactsCount().orNull());
    builder.setFailedArtifactsCount(resultStats.failedArtifactsCount().orNull());
    builder.setTimeTakenByArtifacts(resultStats.timeTakenByArtifacts().orNull());
    builder.setExceptionStringsForArtifacts(resultStats.exceptionDetailsForArtifacts().orNull());
    builder.setExceptionMapForArtifacts(resultStats.exceptionMapForArtifacts().orNull());
    builder.setCountPropertiesMapForArtifacts(resultStats.propertiesCountForArtifacts().orNull());

    return builder;
  }

  @Override
  public Optional<String> exceptionString() {
    throw new UnsupportedOperationException("Method invalid for CombinedResultStats");
  }

  public int getTotalSuccessful() {
    return this.totalSuccessful;
  }

  public int getTotalFailed() {
    return this.totalFailed;
  }

  public Optional<SortedSetMultimap<Long, String>> timeTakenMap() {
    return this.timeTakenPerDocumentMap;
  }

  public Optional<Multiset<String>> exceptionDetails() {
    return exceptionDetails;
  }

  public Optional<Multimap<String, String>> exceptionMap() {
    return this.exceptionMap;
  }

  public static class CombinedResultBuilder extends BatchResultBuilder {

    private int totalSuccessful;
    private int totalFailed;
    private SortedSetMultimap<Long, String> timeTakenPerDocumentMap;
    private Multiset<String> exceptionDetails;
    private Multimap<String, String> exceptionMap;

    private CombinedResultBuilder(String moduleName, int totalSuccessful, int totalFailed) {
      super(moduleName, Optional.absent(), false);
      this.totalSuccessful = totalSuccessful;
      this.totalFailed = totalFailed;
    }

    public CombinedResultBuilder addTimeTakenForDocument(String docId, Long timeTaken) {
      if (this.timeTakenPerDocumentMap == null) {
        this.timeTakenPerDocumentMap = TreeMultimap.<Long, String>create(
            Ordering.natural().reverse(), Ordering.natural());
      }
      this.timeTakenPerDocumentMap.put(timeTaken, docId);
      return this;
    }

    public CombinedResultBuilder setTimeTakenPerDocumentMap(SortedSetMultimap<Long, String>
        timeTakenMap) {
      this.timeTakenPerDocumentMap = timeTakenMap;
      return this;
    }

    public CombinedResultBuilder addExceptionString(String exceptionString) {
      if (this.exceptionDetails == null) {
        this.exceptionDetails = HashMultiset.create();
      }
      this.exceptionDetails.add(exceptionString);
      return this;
    }

    public CombinedResultBuilder setExceptionStringSet(Multiset<String> exceptionStrings) {
      this.exceptionDetails = exceptionStrings;
      return this;
    }

    public CombinedResultBuilder addExceptionStringSet(Multiset<String> exceptionStrings) {
      if (this.exceptionDetails == null) {
        this.exceptionDetails = HashMultiset.create();
      }
      this.exceptionDetails.addAll(exceptionStrings);
      return this;
    }

    public CombinedResultBuilder addExceptionDocIdMapping(String exceptionString, String docId) {
      if (this.exceptionMap == null) {
        this.exceptionMap = HashMultimap.create();
      }
      this.exceptionMap.put(exceptionString, docId);
      return this;
    }

    public CombinedResultBuilder setExceptionDocIdMap(Multimap<String, String> exceptionDocIdMap) {
      this.exceptionMap = exceptionDocIdMap;
      return this;
    }

    public CombinedResultBuilder addExceptionDocIdMap(Multimap<String, String> exceptionDocIdMap) {
      if (this.exceptionMap == null) {
        this.exceptionMap = HashMultimap.create();
      }
      this.exceptionMap.putAll(exceptionDocIdMap);
      return this;
    }

    @Override
    public Builder setExceptionString(String exceptionString) {
      throw new UnsupportedOperationException("Method invalid for CombinedResultStats");
    }

    public CombinedResultStats build() {
      return new CombinedResultStats(moduleName, totalSuccessful, totalFailed, Optional.fromNullable
          (timeTaken),
          Optional.fromNullable(timeTakenPerDocumentMap), Optional.fromNullable(exceptionDetails),
          Optional.fromNullable(exceptionMap), Optional.fromNullable
          (propertiesCount), Optional.fromNullable(successfulArtifactsCount), Optional.fromNullable
          (failedArtifactsCount), Optional.fromNullable(timeTakenByArtifacts), Optional
          .fromNullable(exceptionStringsForArtifacts), Optional.fromNullable
          (exceptionMapForArtifacts), Optional.fromNullable(countPropertiesForArtifacts));
    }
  }

}
