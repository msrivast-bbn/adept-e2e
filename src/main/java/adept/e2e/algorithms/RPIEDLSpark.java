package adept.e2e.algorithms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import adept.common.Coreference;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.HltContentContainer;
import adept.common.KBID;
import adept.common.Pair;
import adept.e2e.exceptions.E2eException;

/**
 * Created by cstokes on 12/8/2015.
 */
public class RPIEDLSpark extends SparkAlgorithmComponent {
//  private static final long serialVersionUID = 403179454183824990L;

  public RPIEDLSpark(AlgorithmSpecifications algorithmSpecifications, int timeOut) {

    super(algorithmSpecifications, timeOut);//Always throw exception in case algorithm module's instantiation or activation failed
  }

//  public static void main(String[] args) {
//    new RPIEDLSpark().Run(args);
//  }
//
//  public void Run(String[] args) {
//    String inputDirectory = args[0];
//    String outputDirectory = args[1];
//    SparkConf conf =
//        new SparkConf().setAppName("rpi-edl-coref").set("spark.executor.memory", "12g");
//    JavaSparkContext sc = new JavaSparkContext(conf);
//    processFiles(inputDirectory, outputDirectory, sc);
//  }

  @Override
  public HltContentContainer postProcessHltCC(HltContentContainer hltContentContainer)
      throws E2eException {
    //The post-processing step is required since RPI_EDL does not set entityMentions,
    // coreferences or entities to the hltContentContainer
    List<Pair<EntityMention, Map<KBID, Float>>> hltccEntityMentions =
        new ArrayList<Pair<EntityMention, Map<KBID, Float>>>();
    for (Entity entity : hltContentContainer.getKBEntityMapForDocEntities().keySet()) {
      EntityMention canonicalMention = entity.getCanonicalMention();
      // The following check/code is not required since RPI_EDL sets entityIDDistributions to all
      // canonicalMentions
//      if (canonicalMention.getEntityIdDistribution() == null
//          || canonicalMention.getEntityIdDistribution().keySet().size() == 0) {
//        Map<Long, Float> entityIdDistribution = new HashMap<Long, Float>();
//        entityIdDistribution.put(entity.getEntityId(), (float)entity.getCanonicalMentionConfidence
//            ());
//        canonicalMention.setEntityIdDistribution(entityIdDistribution);
//      }

      boolean isDuplicate = false;
      for (Pair<EntityMention, Map<KBID, Float>> entityMentionInfo : hltccEntityMentions) {
        EntityMention entityMention = entityMentionInfo.getL();
        if (canonicalMention.getValue().equals(entityMention.getValue()) &&
            canonicalMention.getEntityType().getType()
                .equals(entityMention.getEntityType().getType()) &&
            (canonicalMention.getMentionType() != null ? canonicalMention.getMentionType().getType()
                                                       : "Unknown").equals(
                (entityMention.getMentionType() != null ? entityMention.getMentionType().getType()
                                                        : "Unknown")) &&
            canonicalMention.getCharOffset().getBegin() == entityMention.getCharOffset().getBegin()
            &&
            canonicalMention.getCharOffset().getEnd() == entityMention.getCharOffset().getEnd()) {

          isDuplicate = true;
          Map<KBID, Float> kbEntity = hltContentContainer.getKBEntityMapForEntity(entity);
          if ((kbEntity != null && entityMentionInfo.getR() == null) ||
              (kbEntity == null && entityMentionInfo.getR() != null)) {
            isDuplicate = false;
          } else if (kbEntity != null && entityMentionInfo.getR() != null) {
            if (kbEntity.keySet().size() != entityMentionInfo.getR().size()) {
              isDuplicate = false;
            } else {
              for (Map.Entry<KBID, Float>entry : kbEntity.entrySet()) {
                if (!entityMentionInfo.getR().containsKey(entry.getKey())) {
                  isDuplicate = false;
                } else if (!entry.getValue().equals(entityMentionInfo.getR().get(entry.getKey()))) {
                  //isDuplicate = true;
                  //skipping this currently, since RPIEDL sets these confidences to 1.0
                  //only
                }
              }
            }
          }
          if (isDuplicate) {
            break;
          }
        }
      }
      if (!isDuplicate) {
        hltccEntityMentions.add(
            new Pair<EntityMention, Map<KBID, Float>>(entity.getCanonicalMention(),
                hltContentContainer.getKBEntityMapForEntity(entity)));
      }
    }
    List<EntityMention> entityMentions = new ArrayList<EntityMention>();
    for (Pair<EntityMention, Map<KBID, Float>> entityMentionInfo : hltccEntityMentions) {
      entityMentions.add(entityMentionInfo.getL());
    }
    hltContentContainer.setEntityMentions(entityMentions);

    Coreference coreference = new Coreference(0);
    coreference.setAlgorithmName("RPI_EDL");
    coreference.setEntities(
        new ArrayList<Entity>(hltContentContainer.getKBEntityMapForDocEntities().keySet()));
    coreference.setResolvedMentions(hltContentContainer.getEntityMentions());
    hltContentContainer.setCoreferences(Collections.singletonList(coreference));

    return hltContentContainer;
  }

}
