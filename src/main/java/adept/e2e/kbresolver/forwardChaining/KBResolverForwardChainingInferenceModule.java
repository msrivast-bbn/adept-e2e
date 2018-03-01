package adept.e2e.kbresolver.forwardChaining;

import adept.e2e.kbresolver.KBResolverAbstractModule;
import adept.e2e.kbresolver.pruneLowConfRelations.KBWorkerInference;
import adept.e2e.kbresolver.KbUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import adept.kbapi.KBParameters;

/**
 * Created by bmin on 11/30/16.
 */
public class KBResolverForwardChainingInferenceModule extends KBResolverAbstractModule {

    private static Logger log = LoggerFactory.getLogger(KBResolverForwardChainingInferenceModule.class);
    private JavaSparkContext sc;
    private static KBParameters kbParameters;
    private long numberAddedRelations;
    
    public long getNumberAddedRelations() {
        return numberAddedRelations;
    }

    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) {
        this.sc = sc;
        this.kbParameters = kbParameters;
    }

    public KBResolverForwardChainingInferenceModule() {  }

    public void run() {
        try {
            KbUtil.printKbSummaryStats(kbParameters);

            ForwardChainingInferenceHelper forwardChainingInferenceHelper = new ForwardChainingInferenceHelper(kbParameters);
            JavaPairRDD<String, String> forwardChainingInferenceRuleJavaPairRDD = sc.parallelizePairs(forwardChainingInferenceHelper.listKbParamAndRule);

            log.info("# forwardChainingInferenceRuleJavaPairRDD: {}" , forwardChainingInferenceRuleJavaPairRDD.count());

            JavaRDD<List<String>> addedRelations = forwardChainingInferenceRuleJavaPairRDD.map(new KBWorkerInference());

	    numberAddedRelations = addedRelations.count();
            log.info("# added relations: {}" , numberAddedRelations);

            KbUtil.printKbSummaryStats(kbParameters);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



  /*
  class RelationInsertionMapper implements Function<InstanceInferredRelation, Boolean> {

    public Boolean call(InstanceInferredRelation instanceInferredRelation) {
      System.out.println("Add infered relation: "
          + instanceInferredRelation.kbRelation1.getType().getType() + " + "
          + instanceInferredRelation.kbRelation2.getType().getType() + " -> "
          + instanceInferredRelation.inferedRelationType
          + "\tR1=" + KBUtility.getStringInfoShort(instanceInferredRelation.kbRelation1)
          + "\tR2=" + KBUtility.getStringInfoShort(instanceInferredRelation.kbRelation2)
          + "\t->\t" + instanceInferredRelation.inferedRelationType
          + "(" + KBUtility.getStringInfoShort(instanceInferredRelation.kbEntityArg1)
          + ", "
          + KBUtility.getStringInfoShort(instanceInferredRelation.kbEntityArg2) + ")");

      System.out.println("INFER:\n"
          + "R1=" + KBUtility.getStringInfoShort(instanceInferredRelation.kbRelation1) + "\n"
          + "R2=" + KBUtility.getStringInfoShort(instanceInferredRelation.kbRelation2) + "\n"
          + "\t->\t" + instanceInferredRelation.inferedRelationType + "("
          + instanceInferredRelation.kbEntityArg1.getCanonicalString() + ", "
          + instanceInferredRelation.kbEntityArg2.getCanonicalString() + ")\n");

        try {
          KBRelation kbRelation1 = instanceInferredRelation.kbRelation1;
          KBRelation kbRelation2 = instanceInferredRelation.kbRelation2;
          String inferedRelationType = instanceInferredRelation.inferedRelationType;
          String inferedArg1Role = instanceInferredRelation.inferedArg1Role;
          KBEntity kbEntityArg1 = instanceInferredRelation.kbEntityArg1;
          KBEntity kbEntityArg2 = instanceInferredRelation.kbEntityArg2;
          String inferedArg2Role = instanceInferredRelation.inferedArg2Role;

          OntType inferedRelationTypeOnt =
              (OntType) OntTypeFactory.getInstance().getType(inferedRelationType);
          KBRelation.InsertionBuilder kbRelationInsertionBuilder =
              KBRelation.relationInsertionBuilder(inferedRelationTypeOnt,
                  INFERED_RELATION_DEFAULT_RELATION_CONFIDENCE);

          if (doesRelationExist(kbEntityArg1.getKBID(), kbEntityArg1.getKBID(),
              inferedRelationTypeOnt)) {
            System.out.println("NOT INSERT because relation already exist: "
                + inferedRelationTypeOnt.getURI() + ": <"
                + kbEntityArg1.getKBID().getObjectID() + ", "
                + kbEntityArg2.getKBID().getObjectID() + ">");
            return false;
          }

          OntType inferedArg1RoleOnt =
              (OntType) OntTypeFactory.getInstance().getType(inferedArg1Role);
          KBRelationArgument.InsertionBuilder kbRelationArgument1InsertionBuilder =
              KBRelationArgument.insertionBuilder(inferedArg1RoleOnt,
                  kbEntityArg1,
                  INFERED_RELATION_DEFAULT_ARGUMENT_CONFIDENCE);
          kbRelationInsertionBuilder.addArgument(kbRelationArgument1InsertionBuilder);

          OntType inferedArg2RoleOnt =
              (OntType) OntTypeFactory.getInstance().getType(inferedArg2Role);
          KBRelationArgument.InsertionBuilder kbRelationArgument2InsertionBuilder =
              KBRelationArgument.insertionBuilder(inferedArg1RoleOnt,
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
          kbRelationInsertionBuilder.insert(kb);
        } catch (Exception e) {
          e.printStackTrace();
        }

      return true;
    }
  }


  class ReduceFunction implements
      Function2<Tuple2<Integer, BatchInferredRelations>, Tuple2<Integer, BatchInferredRelations>, Tuple2<Integer, BatchInferredRelations>> {
    public Tuple2<Integer, BatchInferredRelations> call(Tuple2<Integer, BatchInferredRelations> a1, Tuple2<Integer, BatchInferredRelations> a2) {

      BatchInferredRelations batchInferredRelationsMerged = new BatchInferredRelations();
      batchInferredRelationsMerged.addAll(a1._2().getInferredRelations());
      batchInferredRelationsMerged.addAll(a2._2().getInferredRelations());

      int dummyHash = a1._1();

      return new Tuple2<Integer, BatchInferredRelations>(dummyHash, batchInferredRelationsMerged);
    }
  }
  */
}
