package adept.e2e.kbresolver.pruneLowConfRelations;

import adept.e2e.kbresolver.KBResolverAbstractModule;
import adept.e2e.kbresolver.KbUtil;
import com.google.common.collect.ImmutableMultimap;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.RDFNode;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KB;
import adept.kbapi.KBParameters;
import scala.Tuple2;

/**
 * Created by bmin on 11/29/16.
 */
public class KBResolverRelationCardinalityModule extends KBResolverAbstractModule {

    private static Logger log = LoggerFactory.getLogger(KBResolverRelationCardinalityModule.class);
    private JavaSparkContext sc;
    private static KBParameters kbParameters;
    private long numPrunedRelations;
    
    public long getNumPrunedRelations() {
        return numPrunedRelations;
    }

    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) {
        this.sc = sc;
        this.kbParameters = kbParameters;
    }
    public KBResolverRelationCardinalityModule() {}

    public void run() {
        try {
            KbUtil.printKbSummaryStats(kbParameters);

            List<String> lines = writeFileOfRelationsToDelete();

            // create a RDD with each line as an element
            JavaRDD<String> files = sc.parallelize(lines);

            log.info("Pruning entities...");

            log.info("files.count(): {}" , files.count());

            BatchRelCardFunction brcFunction = new BatchRelCardFunction();
            JavaRDD<BatchRelationCardinality> batchRelationCardinalityJavaRDD = files.map(brcFunction);

            BRCToProcessPair serializerFunction = new BRCToProcessPair();
            JavaPairRDD<String, String> kbRDD = batchRelationCardinalityJavaRDD.mapToPair(serializerFunction);

            log.info("Size of kbRDD: {}" , kbRDD.count());


            JavaRDD<List<String>> prunedRelations = kbRDD.map(new KBWorkerRelationPruning());

	    numPrunedRelations = prunedRelations.count();
            log.info("# pruned relations: {}" , numPrunedRelations);

            KbUtil.printKbSummaryStats(kbParameters);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class BatchRelCardFunction implements Function<String, BatchRelationCardinality> {
        @Override
        public BatchRelationCardinality call(String relCardinalityString) {
            return BatchRelationCardinality.fromString(relCardinalityString);
        }
    }


    private static class BRCToProcessPair implements PairFunction<BatchRelationCardinality, String, String> {
        @Override
        public Tuple2<String, String> call(BatchRelationCardinality batchRelationCardinality) throws Exception {
            return new Tuple2<>(SerializerUtil.serialize(kbParameters), SerializerUtil.serialize(batchRelationCardinality));
        }
    }

    static String queryForCardinalityTemplate =
            "PREFIX adept-base: <http://adept-kb.bbn.com/adept-base#>\n"
                    + " PREFIX adept-core: <http://adept-kb.bbn.com/adept-core#>\n"
                    + " PREFIX afn: <http://jena.hpl.hp.com/ARQ/function#>\n"
                    + " PREFIX fn: <http://www.w3.org/2005/xpath-functions#>\n"
                    + " PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
                    + " PREFIX par: <http://parliament.semwebcentral.org/parliament#>\n"
                    + " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                    + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                    + " PREFIX time: <http://www.w3.org/2006/time#>\n"
                    + " PREFIX xml: <http://www.w3.org/XML/1998/namespace>\n"
                    + " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
                    + " \n"
                    + "SELECT ?arg1 (COUNT(?arg2) AS ?count) WHERE {\n"
                    + "  ?relation a adept-core:%s;\n"
                    + "   adept-core:%s ?arg1;\n"
                    + "   adept-core:%s ?arg2 .\n"
                    + "?arg1 adept-base:canonicalString ?arg1Name . \n"
                    + "?arg2 adept-base:canonicalString ?arg2Name .\n"
                    + "\n"
                    + "} GROUP BY ?arg1";

    List<String> writeFileOfRelationsToDelete() {
        List<String> lines = new ArrayList<String>();

        try {
            KB kb = new KB(kbParameters);

            int ruleId = 0;
            for (String reln : adept.e2e.kbresolver.KbUtil.reln2focus2role2maxCardinality.keySet()) {
                for (ImmutableMultimap<String, ImmutableMultimap<String, Integer>> focus2role2maxCardinality : KbUtil.reln2focus2role2maxCardinality
                        .get(reln)) {
                    for (String focus : focus2role2maxCardinality.keySet()) {
                        for (ImmutableMultimap<String, Integer> role2maxCardinality : focus2role2maxCardinality
                                .get(focus)) {
                            for (String role : role2maxCardinality.keySet()) {
                                for (int maxCardinality : role2maxCardinality.get(role)) {
                                    String queryForCardinality =
                                            String.format(queryForCardinalityTemplate, reln, focus, role);

                                    log.info("****************** query *******************\n");
                                    log.info("Process Cadinality rules #{}, reln={}, focus={}, role={} " +
                                            "maxCardinality={}", ++ruleId, reln, focus, role, maxCardinality);
                                    log.info(queryForCardinality);
                                    log.info("****-------------- query ---------------****\n");

                                    com.hp.hpl.jena.query.ResultSet resultSetOfRelationTypes =
                                            kb.executeSelectQuery(queryForCardinality);

                                    while (resultSetOfRelationTypes.hasNext()) {
                                        QuerySolution item = resultSetOfRelationTypes.next();
                                        if (item.contains("?arg1") && item.contains("?count")) {
                                            RDFNode entity = item.get("?arg1");
                                            RDFNode count = item.get("?count");
                                            String strEntity = entity.asResource().getLocalName();
                                            int numCount = count.isLiteral() ? count.asLiteral().getInt() : 0;

                                            String[] uriItems = entity.asResource().getURI().trim().split("#");

                                            if(numCount > maxCardinality) {
                                                String line = uriItems[1] + "\t" + uriItems[0] + "\t" // KBEntity
                                                        + reln + "\t" + maxCardinality + "\t"
                                                        + role + "\t" + focus;
                                                lines.add(line);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            kb.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return lines;
    }

  /*
  class BatchToDecisions implements Function<String, BatchRelationCardinality> {

    public BatchRelationCardinality call(String lineString) {

      BatchRelationCardinality batchRelationCardinality =
          BatchRelationCardinality.fromString(lineString);

      return batchRelationCardinality;
    }
  }

  class Process implements
      Function2<BatchRelationCardinality, BatchRelationCardinality, BatchRelationCardinality> {

    public BatchRelationCardinality call(BatchRelationCardinality BatchRelationCardinality1,
        BatchRelationCardinality BatchRelationCardinality2) {
      try {
        for (KBID kbid : BatchRelationCardinality1.getRelations2delete()) {
          System.out.println("DELETE relation: " + kbid.toString());
          if(!isReadOnly)
            kb.deleteKBObject(kbid);
        }
        for (KBID kbid : BatchRelationCardinality2.getRelations2delete()) {
          System.out.println("DELETE relation: " + kbid.toString());
          if(!isReadOnly)
            kb.deleteKBObject(kbid);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      return BatchRelationCardinality1;
    }
  }
  */
}
