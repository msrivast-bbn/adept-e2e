package adept.e2e.kbresolver.pruneLowConfRelations;

import org.apache.spark.api.java.function.Function;

import adept.common.KBID;
import adept.e2e.driver.KBSingleton;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KB;
import adept.kbapi.KBParameters;
import adept.kbapi.KBProvenance;
import adept.kbapi.KBRelation;
import adept.kbapi.KBRelationArgument;
import adept.kbapi.KBTextProvenance;
import scala.Tuple2;

/**
 * Created by bmin on 12/13/16.
 */
class KBRelationToDecisions implements Function<Tuple2<String, KBID>, Boolean> {
  double minConfRelation;
  double minConfRelationTextProvenance;
  double minConfRelationArgument;

  private static KB kb;

  KBRelationToDecisions(double minConfRelation,
      double minConfRelationTextProvenance,
      double minConfRelationArgument) {
    this.minConfRelation = minConfRelation;
    this.minConfRelationTextProvenance = minConfRelationTextProvenance;
    this.minConfRelationArgument = minConfRelationArgument;
  }

  public Boolean call(Tuple2<String, KBID> tuple2) {
    try {

      KBParameters kbParameters = SerializerUtil.deserialize(tuple2._1(), KBParameters.class);
      kb = KBSingleton.getInstance(kbParameters);

      KBRelation kbRelation = kb.getRelationById(tuple2._2());

      int numArguments = kbRelation.getArguments().size();
      for (KBRelationArgument kbRelationArgument : kbRelation.getArguments()) {
        if (kbRelationArgument.getConfidence() < this.minConfRelationArgument) {
          kb.deleteKBObject(kbRelationArgument.getKBID());
          numArguments--;
        }
      }

      int numProvenances = kbRelation.getProvenances().size();
      for (KBProvenance kbProvenance : kbRelation.getProvenances()) {
        KBTextProvenance kbTextProvenance = (KBTextProvenance) kbProvenance;
        if (kbTextProvenance.getConfidence() < this.minConfRelationTextProvenance) {
          kb.deleteKBObject(kbTextProvenance.getKBID());
          numProvenances--;
        }
      }

      if (numArguments < 2 || // too few argumetns
          numProvenances <= 0 || // too few mentions
          kbRelation.getConfidence() < this.minConfRelation // low confidence
          ) {
        kb.deleteKBObject(kbRelation.getKBID());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return true;
  }
}
