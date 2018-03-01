package adept.e2e.chunkalignment;

import com.google.common.collect.ImmutableList;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import adept.common.Chunk;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.e2e.stageresult.DocumentResultObject;
import scala.Tuple2;

public final class AlignedChunksCSVFormatter
    implements
        Function<DocumentResultObject<ImmutableList<DocumentResultObject
                <HltContentContainer,
                    HltContentContainer>>, ChunkQuotientSet>, String> {

  Logger log = LoggerFactory.getLogger(AlignedChunksCSVFormatter.class);

  public String call(DocumentResultObject<ImmutableList<DocumentResultObject<HltContentContainer,
          HltContentContainer>>,
          ChunkQuotientSet> inObject) {
    try {
      if (!inObject.getOutputArtifact().isPresent()) {
        return "Chunk alignment input was not available.";
      }
      ChunkQuotientSet chunkQuotientSet = inObject.getOutputArtifact().get();
      StringBuilder output = new StringBuilder();
      output.append("\n");
      output.append("mentionString,mentionEntityType,mentionCharOffset,algorithm\n");
      Iterator<ChunkEquivalenceClass> equivalenceClasses =
          chunkQuotientSet.equivalenceClasses().iterator();
      int totalIllinoisCorefMentions = 0;
      int numIllinoisCorefMentionsWithoutAlignedSerifMention = 0;

      while (equivalenceClasses.hasNext()) {
        ChunkEquivalenceClass equivalenceClass = equivalenceClasses.next();
        boolean alignedWithSerif = false;
        totalIllinoisCorefMentions++;
        for (Chunk chunk : equivalenceClass.chunks()) {
          output.append("\"" + chunk.getValue().replace("\"", "\"\"") + "\",");
          if(chunk instanceof EntityMention) {
            output.append("\"" + ((EntityMention)chunk).getEntityType().getType() + "\",");
            output.append("\"" + ((EntityMention)chunk).getMentionType().getType() + "\",");
          }
          output.append("\"(" + chunk.getCharOffset().getBegin() + ", " + chunk
              .getCharOffset().getEnd() + ")\",");
          output.append("\"" + chunk.getAlgorithmName() + "\",");
          output.append("\n");
          if(chunk.getAlgorithmName().contains("SERIF")){
            alignedWithSerif=true;
          }
        }
        if(!alignedWithSerif){
          numIllinoisCorefMentionsWithoutAlignedSerifMention++;
        }
        output.append("\n");
      }
      output.append("Stats: numCorefMentionsWithoutAlignedSerifMentions="+numIllinoisCorefMentionsWithoutAlignedSerifMention+", totalCorefMentions="+totalIllinoisCorefMentions+"\n");
      return output.toString();
    } catch (Exception e) {
      log.error("Caught the following exception when trying to write chunk-alignment output", e);
    }
    return "Exception caught when trying to write "
        + "output. Check logs for details.";
  }
}
