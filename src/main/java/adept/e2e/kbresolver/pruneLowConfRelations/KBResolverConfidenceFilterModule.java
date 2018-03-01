package adept.e2e.kbresolver.pruneLowConfRelations;

import adept.e2e.kbresolver.*;
import com.google.common.base.Optional;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import adept.common.KBID;
import adept.common.OntType;
import adept.common.OntTypeFactory;
import adept.kbapi.KB;
import adept.kbapi.KBEntity;
import adept.kbapi.KBParameters;
import adept.kbapi.KBRelation;

/**
 * Created by bmin on 11/30/16.
 */
public class KBResolverConfidenceFilterModule extends KBResolverAbstractModule {

    public double minConfEntity = 1.0;
    public double minConfEntityTextProvenance = 1.0;
    public double minConfRelation = 1.0;
    public double minConfRelationTextProvenance = 1.0;
    public double minConfRelationArgument = 1.0;

    private static Logger log = LoggerFactory.getLogger(KBResolverConfidenceFilterModule.class);
    private JavaSparkContext sc;
    private static KBParameters kbParameters;
    private long numEntityDecisions;
    private long numRelationDecisions;
    
    public long getNumEntityDecisions() {
        return numEntityDecisions;
    }
    
    public long getNumRelationDecisions() {
        return numRelationDecisions;
    }

    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) throws InvalidPropertiesFormatException {
        this.sc = sc;
        this.kbParameters = kbParameters;
        try {
            this.minConfEntity = Double.valueOf(properties.getProperty("minConfEntity"));
            this.minConfEntityTextProvenance = Double.valueOf(properties.getProperty("minConfEntityTextProvenance"));
            this.minConfRelation = Double.valueOf(properties.getProperty("minConfRelation"));
            this.minConfRelationTextProvenance = Double.valueOf(properties.getProperty("minConfRelationTextProvenance"));
            this.minConfRelationArgument = Double.valueOf(properties.getProperty("minConfRelationArgument"));
        } catch (Exception e) {
            throw new InvalidPropertiesFormatException("To use ConfidenceFilter, properties must include a double value for each of" +
                    "minConfEntity, minConfEntityTextProvenance, minConfRelation, minConfRelationTextProvenance, minConfRelationArgument");
        }
    }

    public KBResolverConfidenceFilterModule() {}

    public void run() {
        try {

            List<KBID> kbEntities = getAllEntities();

            // create a RDD with each line as an element
            JavaRDD<KBID> kbEntityJavaRDD = sc.parallelize(kbEntities);//.repartition(40);

            KBIDToProcessPair serializerFunction = new KBIDToProcessPair(kbParameters);
            JavaPairRDD<String, KBID> kbEntityJavaPairRDD = kbEntityJavaRDD.mapToPair(serializerFunction);

            JavaRDD<Boolean> decisionsOnEntities = kbEntityJavaPairRDD.map(
                    new KBEntityToDecisions(this.minConfEntityTextProvenance, this.minConfEntity));

            List<KBID> kbRelations = getAllRelations();
            JavaRDD<KBID> kbRelationJavaRDD = sc.parallelize(kbRelations);//.repartition(40);
            JavaPairRDD<String, KBID> kbRelationJavaPairRDD = kbRelationJavaRDD.mapToPair(serializerFunction);

            JavaRDD<Boolean> decisionsOnRelations = kbRelationJavaPairRDD.map(
                    new KBRelationToDecisions(this.minConfRelation,
                            this.minConfRelationTextProvenance,
                            this.minConfRelationArgument));

	    numEntityDecisions = decisionsOnEntities.count();
            numRelationDecisions = decisionsOnRelations.count();
            log.info("# of decisions on entities: {}", numEntityDecisions);
            log.info("# of decisions on relations: {}", numRelationDecisions);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    List<KBID> getAllEntities() {

        List<KBID> kbEntities = new ArrayList<KBID>();
        try {
            KB kb = new KB(kbParameters);

            for (String stringEntityType : KbUtil.entityTypes) {
                log.info("Look up entity for type {}" , stringEntityType);

                Optional<OntType> ontEntityTypeOptional =
                        KbUtil.getEntityOntTypeFromString(stringEntityType);
                if (!ontEntityTypeOptional.isPresent()) {
                    log.info("Entity type {} NOT found", stringEntityType );
                    continue;
                }

                List<KBEntity> entitiesByType = kb.getEntitiesByType(ontEntityTypeOptional.get());
                log.info("search for type: {}, found #entities: {}",
                        ontEntityTypeOptional.get().getURI(), entitiesByType.size());

                for (KBEntity kbEntity : entitiesByType) {
                    kbEntities.add(kbEntity.getKBID());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return kbEntities;
    }

    List<KBID> getAllRelations() {
        List<KBID> kbRelations = new ArrayList<KBID>();
        try {
            KB kb = new KB(kbParameters);
            for (String reln : KbUtil.relationTypesAdept) {
                OntType relnOntType = (OntType) OntTypeFactory.getInstance().getType(
                        "adept-kb", reln);
                for(KBRelation kbRelation : kb.getRelationsByType(relnOntType))
                    kbRelations.add(kbRelation.getKBID());
            }
            kb.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return kbRelations;
    }
}

