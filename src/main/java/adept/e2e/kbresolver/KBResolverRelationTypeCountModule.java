package adept.e2e.kbresolver;

import adept.common.OntType;
import adept.common.OntTypeFactory;
import adept.e2e.driver.KBSingleton;
import adept.e2e.driver.SerializerUtil;
import adept.kbapi.*;
import com.google.common.base.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bmin on 11/29/16.
 * Revised by iheintz
 * Simply prints how many of each relation type exists in the current KB.
 * More of a test case for KBResolver - can you connect to the KB and get output?
 */
public class KBResolverRelationTypeCountModule extends KBResolverAbstractModule {

    private static Logger log = LoggerFactory.getLogger(KBResolverRelationTypeCountModule.class);
    private JavaSparkContext sc;
    private KBParameters kbParameters;
    private List<String> relationTypeCounts;
    
    public List<String> getRelationTypeCounts() {
        return relationTypeCounts;
    }

    public KBResolverRelationTypeCountModule() {
    }

    public void initialize(JavaSparkContext sparkContext, Properties properties, KBParameters kbParameters) throws InvalidPropertiesFormatException {
        this.sc = sparkContext;
        this.kbParameters = kbParameters;
    }

    public void run() {
        try {

            JavaRDD<String> relationTypeStringRDD = sc.parallelize(KbUtil.relationTypesTAC.asList());
            JavaRDD<Optional<OntType>> ontTypeOptionalRDD = relationTypeStringRDD.map(new StringToOntTypeOptional());
            JavaRDD<OntType> ontTypeRDD = ontTypeOptionalRDD.filter(new PresentFilter()).map(new Getter());

            log.info("Determining cardinality of {} relations",ontTypeRDD.count());

            JavaPairRDD<OntType, KBParameters> ontTypeWithKBRDD = ontTypeRDD.mapToPair(new OntTypeWithKBParams(kbParameters));
            JavaRDD<String> relationTypeCountsRDD = ontTypeWithKBRDD.map(new RelationTypeCount());
            relationTypeCounts = relationTypeCountsRDD.collect();

            FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
            Path file = new Path("relation_cardinality");
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));

            for (String relcount : relationTypeCounts) {
                bw.write(SerializerUtil.deserialize(relcount, String.class));
                bw.write("\n");
            }
            //log.info(SerializerUtil.deserialize(relcount, String.class));
            bw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class StringToOntTypeOptional implements Function<String, Optional<OntType>> {
        @Override
        public Optional<OntType> call(String s) throws Exception {
            OntType otRaw = (OntType) OntTypeFactory.getInstance().getType("TACKBP", s);
            return KBOntologyMap.getTACOntologyMap().getKBTypeForType(otRaw);
        }
    }

    private static class PresentFilter implements Function<Optional<OntType>, Boolean> {
        @Override
        public Boolean call(Optional<OntType> ontTypeOptional) throws Exception {
            if (ontTypeOptional.isPresent())
                return true;
            return false;
        }
    }

    private static class Getter implements Function<Optional<OntType>, OntType> {
        @Override
        public OntType call(Optional<OntType> ontTypeOptional) throws Exception {
            return ontTypeOptional.get();
        }
    }

    private static class RelationTypeCount implements Function<Tuple2<OntType, KBParameters>, String> {
        @Override
        public String call(Tuple2<OntType, KBParameters> tuple2) throws Exception {
            String relationTypeString = tuple2._1.getType();
            KB kb = KBSingleton.getInstance(tuple2._2);
            List<KBRelation> rels = kb.getRelationsByType(tuple2._1);
            int count = rels.size();
            String result = relationTypeString + ":" + String.valueOf(count);
            return SerializerUtil.serialize(result);
        }

    }

    private static class OntTypeWithKBParams implements PairFunction<OntType, OntType, KBParameters> {
        private KBParameters kbParameters;
        public OntTypeWithKBParams(KBParameters kbparams) {
            this.kbParameters = kbparams;
        }
        @Override
        public Tuple2 call(OntType ontType) throws Exception {
            return new Tuple2<>(ontType, kbParameters);
        }
    }
}
