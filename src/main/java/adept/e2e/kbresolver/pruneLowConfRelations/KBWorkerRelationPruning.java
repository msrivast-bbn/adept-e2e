package adept.e2e.kbresolver.pruneLowConfRelations;

import com.google.common.collect.ImmutableSet;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import adept.common.KBID;
import adept.common.OntType;
import adept.common.OntTypeFactory;
import adept.e2e.driver.KBSingleton;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KB;
import adept.kbapi.KBEntity;
import adept.kbapi.KBParameters;
import adept.kbapi.KBRelation;
import adept.kbapi.KBRelationArgument;
import scala.Tuple2;

/**
 * Created by bmin on 12/9/16.
 */
public class KBWorkerRelationPruning implements Function<Tuple2<String, String>, List<String>> {

  private static Logger log = LoggerFactory.getLogger(KBWorkerRelationPruning.class);
  //  private static KBOntologyMap kbOntologyMap;
  private static KB kb;

  boolean isReadOnly = false;

  @Override
  public List<String> call(Tuple2<String, String> kbTuple) throws Exception {
    log.info("KBWorkerRelationPruning: pruning relations...");

    KBParameters kbParameters = SerializerUtil.deserialize(kbTuple._1,KBParameters.class);
    BatchRelationCardinality batchRelationCardinality = SerializerUtil.deserialize(kbTuple._2, BatchRelationCardinality.class);

    List<String> prunedRelations = new ArrayList<String>();

    try {
      kb = KBSingleton.getInstance(kbParameters);

      KBID entityKBID = batchRelationCardinality.getEntityKBID();
      int maxCardinality = batchRelationCardinality.getMaxCardinality();
      String focus = batchRelationCardinality.getFocus();
      String role = batchRelationCardinality.getRole();
      List<KBRelation> relations = new ArrayList<KBRelation>();

      try {
        KBEntity sourceEntity = kb.getEntityById(entityKBID);
        log.info(
            "Focused entity={} maxCardinality={}"
                , sourceEntity.getCanonicalString(), maxCardinality);
        relations = kb.getRelationsByArgAndType(
            batchRelationCardinality.getEntityKBID(), batchRelationCardinality.getRelnOntType());
      } catch (Exception e) {
        e.printStackTrace();
      }

      // filter relations by roles
      List<KBRelation> relationsWithCorrectArguments = new ArrayList<KBRelation>();
      for (int relnId = 0; relnId < relations.size(); relnId++) {
        KBRelation kbRelation = relations.get(relnId);

        boolean foundRole = false;
        ImmutableSet<KBRelationArgument> kbRelationArgumentsRoles =
            kbRelation.getArgumentsByRole(
                (OntType) OntTypeFactory.getInstance().getType("adept-kb", role));
        for (KBRelationArgument kbRelationArgument : kbRelationArgumentsRoles) {
          if (!kbRelationArgument.getKBID().equals(entityKBID)) {
            foundRole = true;
            break;
          }
        }

        boolean foundFocus = false;
        ImmutableSet<KBRelationArgument> kbRelationArgumentsFocus =
            kbRelation.getArgumentsByRole((OntType) OntTypeFactory
                .getInstance().getType("adept-kb", focus));
        for (KBRelationArgument kbRelationArgument : kbRelationArgumentsFocus) {
          if (kbRelationArgument.getKBID().equals(entityKBID)) {
            foundFocus = true;
            break;
          }
        }

        if (foundFocus && foundRole) {
          continue;
        } else {
          relationsWithCorrectArguments.add(kbRelation);
        }
      }

      Collections
          .sort(relationsWithCorrectArguments, new KBRelationConfidenceComparator());
      for (int idx = 0; idx < relationsWithCorrectArguments.size(); idx++) {
        KBRelation relation = relationsWithCorrectArguments.get(idx);
        String keptOrDelete = idx < maxCardinality ? "KEEP" : "DELETE";

        if (keptOrDelete.equals("DELETE")) {
          KBID kbid = relation.getKBID();

          log.info("Delete relation: {}\t{}", kbid.getKBNamespace(),kbid.getObjectID());

          prunedRelations.add(kbid.toString());

          if(!isReadOnly)
            kb.deleteKBObject(kbid);
        }
      }

    } catch (Exception e) {
      log.error("KBWorkerRelationPruning: Could not process this partition", e);
      e.printStackTrace();
    }

    return prunedRelations;
  }
}


