package adept.e2e.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import adept.e2e.kbresolver.KBResolverAbstractModule;
import adept.e2e.kbresolver.KBResolverConfidenceScalerModule;
import adept.e2e.kbresolver.KBResolverEntityTypeAnalysis;
import adept.e2e.kbresolver.KBResolverRelationTypeCountModule;
import adept.e2e.kbresolver.KBResolverSplitEntityMentionOutliers;
import adept.e2e.kbresolver.KBResolverUnwantedEntityRemovalModule;
import adept.e2e.kbresolver.SimpleEventCoreference;
import adept.e2e.kbresolver.forwardChaining.KBResolverForwardChainingInferenceModule;
import adept.e2e.kbresolver.pruneLowConfRelations.KBResolverConfidenceFilterModule;
import adept.e2e.kbresolver.pruneLowConfRelations.KBResolverRelationCardinalityModule;
import adept.kbapi.KBParameters;


/**
 * Created by Bonan Min on 11/18/16.
 * use the same structure as SerifEALSpark
 */
public class KBResolverSpark implements Serializable {

    private static Logger log = LoggerFactory.getLogger(KBResolverSpark.class);

    public enum FilterTypes {ForwardChainingInference,
        EntityMentionSplit,
        RelationCardinality,
        Confidence,
        RelationTypeCount,
        EntityTypeAnalysis,
        ConfidenceScaling,
        UnwantedEntityRemoval,
        SimpleEventCoreference,
        }



    public static void main(String [] argv){
        String strFileKBParameter = argv[0];
        String strFileKBResolverParams = argv[1];
        try {
            SparkConf conf =
                    new SparkConf()
                            .setAppName("KBResolver");//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); //.set("spark.executor.memory", "12g");
            if (!conf.contains("spark.master"))
                conf.setMaster("local");

            JavaSparkContext sc = new JavaSparkContext(conf);

            KBParameters kbParameters;
            DataInputStream dis;
            if (conf.get("spark.master").equals("local")) {
                kbParameters = new KBParameters((KBResolverSpark.class).getResource(strFileKBParameter).getFile());
                dis = new DataInputStream(
                        ((KBResolverSpark.class).getResourceAsStream(strFileKBResolverParams)));
            } else {
                kbParameters = new KBParameters(strFileKBParameter);
                dis = new DataInputStream(new FileInputStream(strFileKBResolverParams));
            }
            Properties properties = new Properties();
            properties.loadFromXML(dis);


            KBResolverSpark.run(sc, kbParameters, properties);

            sc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Returns an iterable set of the modules run in KB Resolver
    public static Iterable<KBResolverAbstractModule> run(JavaSparkContext sc,
        KBParameters kbParameters, Properties properties) throws
                                                          InvalidPropertiesFormatException{
        KBResolverSparkProcess.Builder kbResolverSparkProcessBuilder = new KBResolverSparkProcess.Builder();

        String[] filterTypes = properties.getProperty("filterTypes").split(",");
        for (String filterType: filterTypes) {
            KBResolverAbstractModule module = null;
            switch (FilterTypes.valueOf(filterType)) {
                case ForwardChainingInference:
                    module = new KBResolverForwardChainingInferenceModule();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case EntityMentionSplit:
                    module = new KBResolverSplitEntityMentionOutliers();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case RelationCardinality:
                    module = new KBResolverRelationCardinalityModule();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case Confidence:
                    module = new KBResolverConfidenceFilterModule();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case RelationTypeCount:
                    module = new KBResolverRelationTypeCountModule();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case ConfidenceScaling:
                    module = new KBResolverConfidenceScalerModule();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case EntityTypeAnalysis:
                    module = new KBResolverEntityTypeAnalysis();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case UnwantedEntityRemoval:
                    module = new KBResolverUnwantedEntityRemovalModule();
                    module.initialize(sc, properties, kbParameters);
                    break;
                case SimpleEventCoreference:
                    module = new SimpleEventCoreference();
                    module.initialize(sc, properties, kbParameters);
                    break;
                default:
                    log.error("Filter type {} unknown",filterType);
            }
            if (null != module)
                kbResolverSparkProcessBuilder.withModule(module);
        }
        KBResolverSparkProcess kbResolverSparkProcess = kbResolverSparkProcessBuilder.build();
        kbResolverSparkProcess.run();
        return kbResolverSparkProcess.getModules();
    }

}
