package adept.e2e.kbresolver.pruneLowConfRelations;

import adept.e2e.kbresolver.KbUtil;
import adept.e2e.kbresolver.StringUtil;
import adept.e2e.kbresolver.forwardChaining.ForwardChainingInferenceRule;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import adept.common.KBID;
import adept.common.OntType;
import adept.common.OntTypeFactory;
import adept.e2e.driver.KBSingleton;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KB;
import adept.kbapi.KBEntity;
import adept.kbapi.KBParameters;
import adept.kbapi.KBProvenance;
import adept.kbapi.KBRelation;
import adept.kbapi.KBRelationArgument;
import adept.kbapi.KBTextProvenance;
import scala.Tuple2;

/**
 * Created by bmin on 12/9/16.
 */
public class KBWorkerInference implements
    Function<Tuple2<String, String>, List<String>> {

  private static KB kb;

  boolean isReadOnly = false;

  float INFERED_RELATION_DEFAULT_RELATION_CONFIDENCE = 0.3f;
  float INFERED_RELATION_DEFAULT_ARGUMENT_CONFIDENCE = 0.3f;

  private static Logger log = LoggerFactory.getLogger(KBWorkerInference.class);

  public List<String> call(Tuple2<String, String> inPair) {
    List<String> addedRelations = new ArrayList<String>();

    try {
      KBParameters kbParameters = SerializerUtil.deserialize(inPair._1, KBParameters.class);
      ForwardChainingInferenceRule forwardChainingInferenceRule =
          SerializerUtil.deserialize(inPair._2, ForwardChainingInferenceRule.class);

      kb = KBSingleton.getInstance(kbParameters);

      String query = forwardChainingInferenceRule.getQuery();
      String inferedArg1Role = forwardChainingInferenceRule.getInferedArg1Role();
      String inferedArg2Role = forwardChainingInferenceRule.getInferedArg2Role();
      String inferedRelationType = forwardChainingInferenceRule.getInferedRelationType();

      log.info("****************** query *******************\n");
      log.info(query);
      log.info("****-------------- query ---------------****\n");

      ResultSet resultset = retrieveResults(query);

      while (resultset.hasNext()) {
        QuerySolution item = resultset.next();

        RDFNode arg1 = item.get("?argument1");
        RDFNode arg2 = item.get("?argument2");
        RDFNode relation1 = item.get("?relation1");
        RDFNode relation2 = item.get("?relation2");

        KBID kbidArg1 = fromRDFNode(arg1);

        KBRelation kbRelation1 = kb.getRelationById(fromRDFNode(relation1));
        KBRelation kbRelation2 = kb.getRelationById(fromRDFNode(relation2));

        String string_relation1 = StringUtil.get_debug_info(kbRelation1);
        String string_relation2 = StringUtil.get_debug_info(kbRelation2);
        log.info("relation1: {}" , string_relation1);
        log.info("relation2: {}" , string_relation2);

        KBEntity kbEntityArg2 = kb.getEntityById(fromRDFNode(arg2));

        KBEntity kbEntityArg1 = kb.getEntityById(fromRDFNode(arg1));

        String inferredRelationInfo = "inferedRelationType: " + inferedRelationType + ", inferedArg1Role: "
            + inferedArg1Role + ", inferedArg2Role: " + inferedArg2Role;
        log.info(inferredRelationInfo + "\n");

        InstanceInferredRelation forwardChainingInferenceInputBatch =
            new InstanceInferredRelation(kbRelation1,
                kbRelation2,
                inferedRelationType,
                inferedArg1Role,
                kbEntityArg1,
                kbEntityArg2,
                inferedArg2Role);

        boolean insertionSuccess = insertRelation(forwardChainingInferenceInputBatch);
        if(insertionSuccess) {
          addedRelations.add(inferredRelationInfo);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return addedRelations;
  }

  private boolean insertRelation(InstanceInferredRelation instanceInferredRelation) {
    log.info("Add infered relation: {} + {} -> {} \tR1={}\tR2={}\t->\t{}({}, {})",
         instanceInferredRelation.kbRelation1.getType().getType(),
         instanceInferredRelation.kbRelation2.getType().getType(),
         instanceInferredRelation.inferedRelationType,
         KbUtil.getStringInfoShort(instanceInferredRelation.kbRelation1),
         KbUtil.getStringInfoShort(instanceInferredRelation.kbRelation2),
         instanceInferredRelation.inferedRelationType,
         KbUtil.getStringInfoShort(instanceInferredRelation.kbEntityArg1),
         KbUtil.getStringInfoShort(instanceInferredRelation.kbEntityArg2));

    log.info("INFER:\nR1={}\nR2={}\n\t->\t{}({}, {})\n",
        KbUtil.getStringInfoShort(instanceInferredRelation.kbRelation1),
        KbUtil.getStringInfoShort(instanceInferredRelation.kbRelation2),
        instanceInferredRelation.inferedRelationType,
        instanceInferredRelation.kbEntityArg1.getCanonicalString(),
        instanceInferredRelation.kbEntityArg2.getCanonicalString());

    try {
      KBRelation kbRelation1 = instanceInferredRelation.kbRelation1;
      KBRelation kbRelation2 = instanceInferredRelation.kbRelation2;
      String inferedRelationType = instanceInferredRelation.inferedRelationType;
      String inferedArg1Role = instanceInferredRelation.inferedArg1Role;
      KBEntity kbEntityArg1 = instanceInferredRelation.kbEntityArg1;
      KBEntity kbEntityArg2 = instanceInferredRelation.kbEntityArg2;
      String inferedArg2Role = instanceInferredRelation.inferedArg2Role;

      OntType inferedRelationTypeOnt =
          (OntType) OntTypeFactory.getInstance().getType("adept-kb", inferedRelationType);

      KBRelation.InsertionBuilder kbRelationInsertionBuilder =
          KBRelation.relationInsertionBuilder(inferedRelationTypeOnt,
              INFERED_RELATION_DEFAULT_RELATION_CONFIDENCE);

      if (doesRelationExist(kbEntityArg1.getKBID(), kbEntityArg1.getKBID(),
          inferedRelationTypeOnt)) {
        log.info("NOT INSERT because relation already exist: {}: <{}, {}>",
            inferedRelationTypeOnt.getURI(),
            kbEntityArg1.getKBID().getObjectID(),
            kbEntityArg2.getKBID().getObjectID());
        return false;
      }

      OntType inferedArg1RoleOnt =
          (OntType) OntTypeFactory.getInstance().getType("adept-kb", inferedArg1Role);
      KBRelationArgument.InsertionBuilder kbRelationArgument1InsertionBuilder =
          KBRelationArgument.insertionBuilder(inferedArg1RoleOnt,
              kbEntityArg1,
              INFERED_RELATION_DEFAULT_ARGUMENT_CONFIDENCE);
      kbRelationInsertionBuilder.addArgument(kbRelationArgument1InsertionBuilder);

      OntType inferedArg2RoleOnt =
          (OntType) OntTypeFactory.getInstance().getType("adept-kb", inferedArg2Role);
      KBRelationArgument.InsertionBuilder kbRelationArgument2InsertionBuilder =
          KBRelationArgument.insertionBuilder(inferedArg2RoleOnt,
              kbEntityArg2,
              INFERED_RELATION_DEFAULT_ARGUMENT_CONFIDENCE);
      kbRelationInsertionBuilder.addArgument(kbRelationArgument2InsertionBuilder);

      // both relations' provenances are here
      Set<KBProvenance> provenances = new HashSet<KBProvenance>();
      provenances.addAll(kbRelation1.getProvenances());
      provenances.addAll(kbRelation2.getProvenances());
      for (KBProvenance kbProvenance : provenances) {
        KBTextProvenance kbTextProvenance = (KBTextProvenance) kbProvenance;

        KBTextProvenance.InsertionBuilder kbTextProvenanceInsertionBuilder =
            fromKBTextProvenance(kbTextProvenance);

        kbRelationInsertionBuilder.addProvenance(kbTextProvenanceInsertionBuilder);
      }

      // insert new relation
      if(!isReadOnly)
        kbRelationInsertionBuilder.insert(kb);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return true;
  }

  // TODO: need a special query for efficiency
  private boolean doesRelationExist(KBID kbidArg1, KBID kbidArg2, OntType relationType) throws Exception {
    List<KBRelation> relations = kb.getRelationsByArgs(kbidArg1, kbidArg2);
    for(KBRelation kbRelation : relations) {
      if(kbRelation.getType().equals(relationType))
        return true;
    }
    return false;
  }

  private static KBTextProvenance.InsertionBuilder fromKBTextProvenance (KBTextProvenance kbTextProvenance) {
    KBTextProvenance.InsertionBuilder provenanceBuilder = KBTextProvenance.builder();

    provenanceBuilder.setBeginOffset(kbTextProvenance.getBeginOffset());
    provenanceBuilder.setEndOffset(kbTextProvenance.getEndOffset());
    provenanceBuilder.setConfidence(kbTextProvenance.getConfidence());
    provenanceBuilder.setSourceAlgorithmName(kbTextProvenance.getSourceAlgorithmName());
    provenanceBuilder.setContributingSiteName(kbTextProvenance.getContributingSiteName());
    provenanceBuilder.setDocumentURI(kbTextProvenance.getDocumentURI());
    provenanceBuilder.setDocumentID(kbTextProvenance.getDocumentID());
    provenanceBuilder.setCorpusID(kbTextProvenance.getCorpusID());
    provenanceBuilder.setCorpusName(kbTextProvenance.getCorpusName());
    provenanceBuilder.setSourceLanguage(kbTextProvenance.getSourceLanguage());
    provenanceBuilder.setValue(kbTextProvenance.getValue());

    return provenanceBuilder;
  }

  KBID fromRDFNode(RDFNode rdfNode) {
    String[] uriItems = rdfNode.asResource().getURI().trim().split("#");
    System.out.println("uriItems[1]=" + uriItems[1]);
    System.out.println("uriItems[0]=" + uriItems[0]);
    KBID kbid = new KBID(uriItems[1], uriItems[0]);
    return kbid;
  }

  ResultSet retrieveResults(String query) {
    System.out.println("****************** query *******************");
    System.out.println(query);
    System.out.println("****-------------- query ---------------****");
    ResultSet resultSet = kb.executeSelectQuery(query);
    return resultSet;
  }
}
