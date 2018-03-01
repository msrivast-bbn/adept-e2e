package adept.e2e.kbresolver.pruneLowConfRelations;

import org.apache.spark.api.java.function.Function;

import java.util.Set;

import adept.common.KBID;
import adept.e2e.driver.KBSingleton;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KB;
import adept.kbapi.KBEntity;
import adept.kbapi.KBParameters;
import adept.kbapi.KBProvenance;
import adept.kbapi.KBTextProvenance;
import scala.Tuple2;

/**
 * Created by bmin on 12/13/16.
 */
class KBEntityToDecisions implements Function<Tuple2<String, KBID>, Boolean> {
  double minConfEntityTextProvenance;
  double minConfEntity;
  private static KB kb;

  KBEntityToDecisions(double minConfEntityTextProvenance,
      double minConfEntity) {
    this.minConfEntityTextProvenance = minConfEntityTextProvenance;
    this.minConfEntity = minConfEntity;
  }

  public Boolean call(Tuple2<String, KBID> tuple2) {
    try {
      KBParameters kbParameters = SerializerUtil.deserialize(tuple2._1(), KBParameters.class);
      kb = KBSingleton.getInstance(kbParameters);

      KBEntity kbEntity = kb.getEntityById(tuple2._2());
      Set<KBProvenance> kbProvenances = kbEntity.getProvenances();

      int deletedKBProvenance = 0;
      for (KBProvenance kbProvenance : kbProvenances) {
        if (kbProvenance instanceof KBTextProvenance) {
          KBTextProvenance kbTextProvenance =
              (KBTextProvenance) kbProvenance;

          if (kbTextProvenance.getConfidence()
              < this.minConfEntityTextProvenance) {
            // delete entity provenance
            kb.deleteKBObject(kbTextProvenance.getKBID());
            deletedKBProvenance++;
          }
        }
      }

      if (kbEntity.getConfidence() < minConfEntity || // low confidence
          deletedKBProvenance >= kbProvenances.size() // all mentions are deleted
          ) {
        // delete entity
        kb.deleteKBObject(kbEntity.getKBID());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return true;
  }
}
