package adept.e2e.analysis.statslite;

import adept.e2e.analysis.BatchResultStats;
import adept.e2e.analysis.ResultStats;
import adept.e2e.stageresult.BatchResultObject;
import adept.e2e.stageresult.DocumentResultObject;
import adept.e2e.stageresult.ResultObject;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static adept.e2e.driver.E2eConstants.*;

/**
 * Created by msrivast on 2/6/17.
 */
public final class StatsCollectorLite<T, U> implements Function<ResultObject<T, U>,
    ResultStats> {

//  private static final long serialVersionUID = 1L;

  @Override
  public ResultStats call(ResultObject<T, U> result) {

    Map<String, Object> properties = (Map<String, Object>) result.getPropertiesMap().orNull();
    if (properties == null) {
      return null;
    }
    String moduleName = (String) result.getProperty(PROPERTY_MODULE_NAME);
    if (moduleName == null) {
      return null;
    }
    String docId = null;
    if (result instanceof DocumentResultObject) {
      docId = (String) properties.get(PROPERTY_DOCID);
    } else if (result instanceof  BatchResultObject<?>) {
      docId = ((BatchResultObject<?>) result).getArtifactIds().isPresent() ?
          Joiner.on(",").join((List<String>) (((BatchResultObject<?>) result).getArtifactIds().get()))
          : null;
    }
    Long timeTaken = (Long) properties.get(PROPERTY_TIME_TAKEN);
    String exceptionDetails = null;
    if (!result.isSuccessful()) {
      exceptionDetails = getExceptionDetailsFromProperties(properties);
    }
    Map<String, Integer> countBasedProperties = getCountBasedProperties(properties);
    ResultStats resultStats = ResultStats.create(moduleName, Optional.fromNullable(docId), result
            .isSuccessful(),
        Optional.of(timeTaken), Optional.fromNullable(exceptionDetails),
        Optional.fromNullable(countBasedProperties));

    if (result instanceof BatchResultObject && ((BatchResultObject) result).getArtifactIds()
        .isPresent()) {
      List<String> artifactIds = (List<String>) ((BatchResultObject) result).getArtifactIds()
          .get();
      List<String> failedArtifactIds = (List<String>) ((BatchResultObject) result)
          .getFailedArtifactIds().or(new ArrayList<>());
      int numSuccessfulArtifactsInBatch = artifactIds.size() - failedArtifactIds.size();
      int numFailedArtifactsInBatch = failedArtifactIds.size();
      Multiset<String> exceptionDetailsForArtifactsInBatch = HashMultiset.<String>create();
      Multimap<String, String> exceptionMapForArtifactsInBatch = HashMultimap.<String,
          String>create();
      Map<String, Map<String, Integer>> countMapForArtifactsInBatch = new HashMap<>();
      for (String artifactId : artifactIds) {
        if (((BatchResultObject) result).getPropertiesMapForArtifact(artifactId).isPresent()) {
          Map<String, Object> propertiesMapForArtifact = (Map<String, Object>) (
              (BatchResultObject) result).getPropertiesMapForArtifact(artifactId).get();
          if (failedArtifactIds.contains(artifactId)) {
            String exceptionDetailsStringForArtifact = getExceptionDetailsFromProperties(
                propertiesMapForArtifact);
            exceptionDetailsForArtifactsInBatch.add(exceptionDetailsStringForArtifact);
            exceptionMapForArtifactsInBatch.put(exceptionDetailsStringForArtifact, artifactId);
          }
          Map<String, Integer> countBasedPropertiesForArtifact = getCountBasedProperties
              (propertiesMapForArtifact);
          if(countBasedPropertiesForArtifact!=null) {
            countMapForArtifactsInBatch.put(artifactId, countBasedPropertiesForArtifact);
          }
        }
      }
      resultStats = BatchResultStats.builderFromResultStats(resultStats)
          .setSuccessfulArtifactsCount(numSuccessfulArtifactsInBatch).setFailedArtifactsCount
              (numFailedArtifactsInBatch).setExceptionStringsForArtifacts
              (exceptionDetailsForArtifactsInBatch).setExceptionMapForArtifacts
              (exceptionMapForArtifactsInBatch).setCountPropertiesMapForArtifacts
              (countMapForArtifactsInBatch).build();
    }
    return resultStats;
  }


  private String getExceptionDetailsFromProperties(Map<String, Object> properties) {
    List<String> exceptionDetailsArr = new ArrayList<>();
    String exceptionType = properties.get(PROPERTY_EXCEPTION_TYPE)!=null ? (String) properties.get
        (PROPERTY_EXCEPTION_TYPE) : "";
    String exceptionMessage = properties.get(PROPERTY_EXCEPTION_MESSAGE) != null ? (String) properties
        .get(PROPERTY_EXCEPTION_MESSAGE) : "";
    if(exceptionType.equals("adept.kbapi.KBUpdateException")){
      if(exceptionMessage.startsWith("Failed to insert relation")){
        exceptionMessage = "Arg Role/Type mismatch (Verbosity reduced for eval)";
      }
    }
    exceptionDetailsArr.add(exceptionType);
    exceptionDetailsArr.add(exceptionMessage);
    return Joiner.on("\n").join(exceptionDetailsArr);
  }

  private Map<String, Integer> getCountBasedProperties(Map<String, Object> properties) {
    Map<String, Integer> countBasedProperties = new HashMap<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      if (!entry.getKey().equals(PROPERTY_DOCID) && entry.getValue() instanceof Integer) {
        int count = (Integer) entry.getValue();
        countBasedProperties.put(entry.getKey(), count);
      }
    }
    return countBasedProperties.isEmpty() ? null : countBasedProperties;
  }
}
