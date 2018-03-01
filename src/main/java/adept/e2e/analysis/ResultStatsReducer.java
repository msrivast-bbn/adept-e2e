package adept.e2e.analysis;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.function.Function2;

import java.util.Map;

/**
 * Created by msrivast on 2/6/17.
 */
public final class ResultStatsReducer implements Function2<ResultStats, ResultStats, ResultStats> {

  @Override
  public ResultStats call(ResultStats rs1, ResultStats rs2) throws Exception {

    int totalSuccessful = getSuccessfulCount(rs1) + getSuccessfulCount(rs2);
    int totalFailed = getFailedCount(rs1) + getFailedCount(rs2);

    CombinedResultStats.CombinedResultBuilder combinedResultStatsBuilder = CombinedResultStats.
        builder(rs1.moduleName(), totalSuccessful, totalFailed);

    Long timeTaken = rs1.timeTaken().orNull();
    if (rs2.timeTaken().isPresent()) {
      if (timeTaken == null) {
        timeTaken = 0L;
      }
      timeTaken += rs2.timeTaken().get();
    }
    combinedResultStatsBuilder.setTimeTaken(timeTaken);

    SortedSetMultimap<Long, String> timeTakenMap = getTimeTakenPerDocumentMap(rs1);
    SortedSetMultimap<Long, String> timeTakenMap_2 = getTimeTakenPerDocumentMap(rs2);
    if (timeTakenMap_2 != null) {
      if (timeTakenMap == null) {
        timeTakenMap = timeTakenMap_2;
      } else {
        timeTakenMap.putAll(timeTakenMap_2);
      }
    }
    combinedResultStatsBuilder.setTimeTakenPerDocumentMap(timeTakenMap);

    Multiset<String> exceptionStrings = getExceptionStrings(rs1);
    combinedResultStatsBuilder.setExceptionStringSet(exceptionStrings);
    Multiset<String> exceptionStrings2 = getExceptionStrings(rs2);
    if (exceptionStrings2 != null) {
      combinedResultStatsBuilder.addExceptionStringSet(exceptionStrings2);
    }

    Multimap<String, String> exceptionDocIdMap = getExceptionMap(rs1);
    combinedResultStatsBuilder.setExceptionDocIdMap(exceptionDocIdMap);
    Multimap<String, String> exceptionDocIdMap2 = getExceptionMap(rs2);
    if (exceptionDocIdMap2 != null) {
      combinedResultStatsBuilder.addExceptionDocIdMap(exceptionDocIdMap2);
    }

    Map<String, Integer> combinedPropertiesCountMap = combinePropertiesCountMap(rs1, rs2);
    combinedResultStatsBuilder.setPropertiesCountMap(combinedPropertiesCountMap);

    Integer numSuccessfulArtifacts = getSuccessfulArtifactsCount(rs1);
    Integer numSuccessfulArtifacts2 = getSuccessfulArtifactsCount(rs2);
    if (numSuccessfulArtifacts2 != null) {
      if (numSuccessfulArtifacts == null) {
        numSuccessfulArtifacts = 0;
      }
      numSuccessfulArtifacts += numSuccessfulArtifacts2;
    }
    combinedResultStatsBuilder.setSuccessfulArtifactsCount(numSuccessfulArtifacts);

    Integer numFailedArtifacts = getFailedArtifactsCount(rs1);
    Integer numFailedArtifacts2 = getFailedArtifactsCount(rs2);
    if (numFailedArtifacts2 != null) {
      if (numFailedArtifacts == null) {
        numFailedArtifacts = 0;
      }
      numFailedArtifacts += numFailedArtifacts2;
    }
    combinedResultStatsBuilder.setFailedArtifactsCount(numFailedArtifacts);

    combinedResultStatsBuilder.setTimeTakenByArtifacts(combineTimeTakenByArtifactsMap(rs1, rs2));

    Multiset<String> exceptionDetailsForArtifacts = getExceptionStringsForArtifacts(rs1);
    Multiset<String> exceptionDetailsForArtifacts_2 = getExceptionStringsForArtifacts(rs2);
    if (exceptionDetailsForArtifacts_2 != null) {
      if (exceptionDetailsForArtifacts == null) {
        exceptionDetailsForArtifacts = HashMultiset.create();
      }
      exceptionDetailsForArtifacts.addAll(exceptionDetailsForArtifacts_2);
    }
    combinedResultStatsBuilder.setExceptionStringsForArtifacts(exceptionDetailsForArtifacts);

    Multimap<String, String> combinedExceptionMapForArtifacts = getExceptionMapForArtifacts(rs1);
    Multimap<String, String> combinedExceptionMapForArtifacts_2 = getExceptionMapForArtifacts(rs2);
    if (combinedExceptionMapForArtifacts_2 != null) {
      if (combinedExceptionMapForArtifacts == null) {
        combinedExceptionMapForArtifacts = HashMultimap.create();
      }
      combinedExceptionMapForArtifacts.putAll(combinedExceptionMapForArtifacts_2);
    }
    combinedResultStatsBuilder.setExceptionMapForArtifacts(combinedExceptionMapForArtifacts);

    combinedResultStatsBuilder.setCountPropertiesMapForArtifacts
        (combinePropertiesCountMapForArtifacts(rs1, rs2));

    return combinedResultStatsBuilder.build();
  }

  private int getSuccessfulCount(ResultStats rs) {
    int retVal = 0;
    if (rs instanceof CombinedResultStats) {
      retVal = ((CombinedResultStats) rs).getTotalSuccessful();
    } else if (rs.isResultSuccessful()) {
      retVal = 1;
    }
    return retVal;
  }

  private int getFailedCount(ResultStats rs) {
    int retVal = 0;
    if (rs instanceof CombinedResultStats) {
      retVal = ((CombinedResultStats) rs).getTotalFailed();
    } else if (!rs.isResultSuccessful()) {
      retVal = 1;
    }
    return retVal;
  }

  private SortedSetMultimap<Long, String> getTimeTakenPerDocumentMap(ResultStats rs) {
    SortedSetMultimap<Long, String> timeTakenMap = null;
    if (rs instanceof CombinedResultStats) {
      timeTakenMap = ((CombinedResultStats) rs).timeTakenMap().orNull();
    } else if (rs.timeTaken().isPresent()&&rs.docId().isPresent()) {
      timeTakenMap = TreeMultimap.<Long, String>create(
          Ordering.natural().reverse(), Ordering.natural());
      timeTakenMap.put(rs.timeTaken().get(), rs.docId().get());
    }
    return timeTakenMap;
  }

  private Multiset<String> getExceptionStrings(ResultStats rs) {
    Multiset<String> retVal = null;
    if (rs instanceof CombinedResultStats) {
      retVal = ((CombinedResultStats) rs).exceptionDetails().orNull();
    } else if (rs.exceptionString().isPresent()) {
      retVal = HashMultiset.create();
      retVal.add(rs.exceptionString().get());
    }
    return retVal;
  }

  private Multimap<String, String> getExceptionMap(ResultStats rs) {
    Multimap<String, String> retVal = null;
    if (rs instanceof CombinedResultStats) {
      retVal = ((CombinedResultStats) rs).exceptionMap().orNull();
    } else if (rs.exceptionString().isPresent()&&rs.docId().isPresent()) {
      retVal = HashMultimap.create();
      retVal.put(rs.exceptionString().get(), rs.docId().get());
    }
    return retVal;
  }

  private Map<String, Integer> combinePropertiesCountMap(ResultStats rs1, ResultStats rs2) {
    Map<String, Integer> combinedPropertiesCountMap = rs1.getPropertiesCountMap().orNull();
    if (rs2.getPropertiesCountMap().isPresent()) {
      if (combinedPropertiesCountMap == null) {
        combinedPropertiesCountMap = rs2.getPropertiesCountMap().get();
      } else {
        for (Map.Entry<String, Integer> propertyAndCount : rs2.getPropertiesCountMap().get().
            entrySet()) {
          int count = propertyAndCount.getValue();
          if (combinedPropertiesCountMap.containsKey(propertyAndCount.getKey())) {
            count += combinedPropertiesCountMap.get(propertyAndCount.getKey());
          }
          combinedPropertiesCountMap.put(propertyAndCount.getKey(), count);
        }
      }
    }
    return combinedPropertiesCountMap;
  }

  private Integer getSuccessfulArtifactsCount(ResultStats rs) {
    if (rs instanceof BatchResultStats) {
      return ((BatchResultStats) rs).successfulArtifactsCount().orNull();
    }
    return null;//for instances of type ResultStats
  }

  private Integer getFailedArtifactsCount(ResultStats rs) {
    if (rs instanceof BatchResultStats) {
      return ((BatchResultStats) rs).failedArtifactsCount().orNull();
    }
    return null;//for instances of type ResultStats
  }

  private SortedSetMultimap<Long, String> combineTimeTakenByArtifactsMap(ResultStats rs1,
      ResultStats rs2) {
    SortedSetMultimap<Long, String> combinedTimeTakenByArtifactsMap = null;
    if (rs1 instanceof BatchResultStats) {
      combinedTimeTakenByArtifactsMap = ((BatchResultStats) rs1).timeTakenByArtifacts().orNull();
    }
    SortedSetMultimap<Long, String> combinedTimeTakenByArtifactsMap_2 = null;
    if (rs2 instanceof BatchResultStats) {
      combinedTimeTakenByArtifactsMap_2 = ((BatchResultStats) rs2).timeTakenByArtifacts().orNull();
    }
    if (combinedTimeTakenByArtifactsMap == null) {
      return combinedTimeTakenByArtifactsMap_2;
    } else if (combinedTimeTakenByArtifactsMap_2 == null) {
      return combinedTimeTakenByArtifactsMap;
    }

    //aggregate the two maps such that for any common artifact the time values get added
    for (Map.Entry<Long, String> entry : combinedTimeTakenByArtifactsMap_2.entries()) {
      Long time = entry.getKey();
      String artifactId = entry.getValue();
      if (combinedTimeTakenByArtifactsMap.containsValue(entry.getValue())) {
        //there is a common artifact
        for (Map.Entry<Long, String> entry2 : combinedTimeTakenByArtifactsMap.entries()) {
          if (entry2.getValue().equals(artifactId)) {
            Long timeToAdd = entry2.getKey();
            time += timeToAdd;//add the time
            //remove current mapping to previous time
            combinedTimeTakenByArtifactsMap.remove(timeToAdd, artifactId);
          }
        }
      }
      combinedTimeTakenByArtifactsMap.put(time, artifactId);
    }
    return combinedTimeTakenByArtifactsMap;
  }

  private Multiset<String> getExceptionStringsForArtifacts(ResultStats rs) {
    if (rs instanceof BatchResultStats) {
      return ((BatchResultStats) rs).exceptionDetailsForArtifacts().orNull();
    }
    return null;
  }

  private Multimap<String, String> getExceptionMapForArtifacts(ResultStats rs) {
    if (rs instanceof BatchResultStats) {
      return ((BatchResultStats) rs).exceptionMapForArtifacts().orNull();
    }
    return null;
  }

  private Map<String, Map<String, Integer>> combinePropertiesCountMapForArtifacts(ResultStats rs1,
      ResultStats rs2) {
    Map<String, Map<String, Integer>> combinedPropertyCountsForArtifacts = null;
    if (rs1 instanceof BatchResultStats) {
      combinedPropertyCountsForArtifacts = ((BatchResultStats) rs1).propertiesCountForArtifacts()
          .orNull();
    }
    Map<String, Map<String, Integer>> combinedPropertyCountsForArtifacts_2 = null;
    if (rs2 instanceof BatchResultStats) {
      combinedPropertyCountsForArtifacts_2 = ((BatchResultStats) rs2).propertiesCountForArtifacts()
          .orNull();
    }
    if (combinedPropertyCountsForArtifacts == null) {
      return combinedPropertyCountsForArtifacts_2;
    } else if (combinedPropertyCountsForArtifacts_2 == null) {
      return combinedPropertyCountsForArtifacts;
    }

    //if there are any common artifacts, add up their property counts
    for (Map.Entry<String, Map<String, Integer>> entry :
        combinedPropertyCountsForArtifacts_2.entrySet()) {

      String artifactId = entry.getKey();
      Map<String, Integer> countMap = entry.getValue();
      if (combinedPropertyCountsForArtifacts.containsKey(artifactId)) {
        Map<String, Integer> countMap2 = combinedPropertyCountsForArtifacts.get(artifactId);
        if (!CollectionUtils.intersection(countMap.keySet(), countMap2.keySet()).isEmpty()) {
          //if there's at least one common property between the two maps, add the count of
          // that common property
          for (Map.Entry<String, Integer> propertyAndCount : countMap.entrySet()) {
            String property = propertyAndCount.getKey();
            int count = propertyAndCount.getValue();
            if (countMap2.containsKey(property)) {
              count += countMap2.get(property);
            }
            countMap.put(property, count);
          }
        } else {
          countMap.putAll(countMap2);
        }
      }
      combinedPropertyCountsForArtifacts.put(artifactId, countMap);
    }

    return combinedPropertyCountsForArtifacts;

  }

}
