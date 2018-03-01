package adept.e2e.analysis;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import adept.kbapi.KBConfigurationException;
import adept.kbapi.KBParameters;
import adept.kbapi.KBQueryException;
import adept.utilities.GenerateHumanReadableReport;
import adept.utilities.GenerateHumanReadableReport.KBSummary;
import adept.utilities.GenerateHumanReadableReport.KBSummary.ArtifactType;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 10/23/16.
 */
public final class KBReportUtil {

  public static KBSummaryMap generateReports(KBParameters kbParameters,
      String corpusId, String outputDirectory) throws
                                               KBConfigurationException,
                                               KBQueryException, IOException{
    GenerateHumanReadableReport reportGenerator = new GenerateHumanReadableReport(kbParameters,
        new File(outputDirectory), corpusId, false, 0);
    Map<ArtifactType, KBSummary> kbSummaryMap = reportGenerator.generate();
    return KBSummaryMap.from(kbSummaryMap);
  }

  /**
   * Using compareKBSummaries is not encouraged currently, partly since the information being
   * compared is trivial, and partly because in E2E we don't do iterative uploading of data.
   */
  @Deprecated
  public static void compareKBSummaries(Optional<KBSummaryMap> lastSummaryMap,
      KBSummaryMap currentSummaryMap, File outputFile) throws
                                                       FileNotFoundException{
    checkNotNull(currentSummaryMap);
    checkNotNull(outputFile);
    PrintWriter pw = new PrintWriter(new FileOutputStream(outputFile));
    for (ArtifactType artifactType : currentSummaryMap.getSummaryMap().keySet()) {
      pw.println(
          "--------------------" + artifactType.name() + " Summary" + "--------------------");
      KBSummary currentKBSummary = currentSummaryMap.getSummaryMap().get(artifactType);
      KBSummary lastKBSummary = lastSummaryMap.isPresent() ?
                                lastSummaryMap.get().getSummaryMap().get(artifactType) : null;
      pw.println("count = " + currentKBSummary.count());
      if (lastKBSummary != null) {
        pw.println("previous count = " + lastKBSummary.count());
        //Currently we're only using lastSummary for count information, since
        //1. that makes the most sense in terms of comparing the output
        //2. that's the only information being compared in current python
        //summary script too
      }
      pw.println("max mention count = " + currentKBSummary.maxMentionCount());
      pw.println("max distinct docs = " +
          currentKBSummary.maxDistinctDocs());
      pw.println("confidence range = " + currentKBSummary.minConfidence() + "-" + currentKBSummary
          .maxConfidence());
      if (artifactType.equals(ArtifactType.ENTITY)) {
        pw.println("max unique strings = " + currentKBSummary.maxUniqueStrings().get());
      } else {
        pw.println("min number of arguments = " + currentKBSummary.minNumArguments().get());
        pw.println("max number of arguments = " + currentKBSummary.maxNumArguments().get());
      }
      ImmutableList.Builder<String> types = ImmutableList.builder();
      for (KBSummary.TypeWithOptionalArgTypes type : currentKBSummary.types()) {
        String argTypes = "";
        if (type.argTypes().isPresent()) {
          argTypes = " (" + String.join(", ", type.argTypes().get()) + ")";
        }
        types.add(type.type() + argTypes);
      }
      pw.println("types = " + String.join(", ", types.build()));
      pw.println();
      pw.flush();
    }
    pw.close();
  }

  public static class KBSummaryMap {

    private final Map<ArtifactType, KBSummary> kbSummaryMap;

    private KBSummaryMap(Map<ArtifactType, KBSummary> kbSummaryMap) {
      checkNotNull(kbSummaryMap);
      this.kbSummaryMap = kbSummaryMap;
    }

    public static KBSummaryMap from(Map<ArtifactType, KBSummary> kbSummaryMap) {
      return new KBSummaryMap(kbSummaryMap);
    }

    public Map<ArtifactType, KBSummary> getSummaryMap() {
      return kbSummaryMap;
    }

    public Optional<KBSummary> getSummaryByArtifactType(ArtifactType artifactType) {
      return Optional.fromNullable(kbSummaryMap.get(artifactType));
    }
  }

}
