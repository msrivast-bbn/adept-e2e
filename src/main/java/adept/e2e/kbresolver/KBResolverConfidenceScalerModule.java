package adept.e2e.kbresolver;

import adept.common.*;
import adept.e2e.driver.KBSingleton;
import adept.kbapi.*;
import com.google.api.client.util.Maps;
import com.google.common.base.*;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by iheintz on 5/26/17.
 */
public class KBResolverConfidenceScalerModule extends KBResolverAbstractModule {
    private static final Logger log = LoggerFactory.getLogger(KBResolverConfidenceScalerModule.class);
    private JavaSparkContext sc;
    private KBParameters kbParameters;

    private Map<String, Float> scalingParameters;
    private String logfilename;

    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) throws InvalidPropertiesFormatException {
        this.sc = sc;
        this.kbParameters = kbParameters;
        scalingParameters = Maps.newHashMap();
        List<String> scalingParams = Lists.newArrayList("allRels_ConfScaleDown",
                "unlikelyRels_ArgRelRatioThreshold",
                "unlikelyRels_ConfScaleDown",
                "likelyRels_RelCountThreshold",
                "likelyRels_ConfScaleUp");
        for (String paramKey : scalingParams) {
            Preconditions.checkArgument(properties.containsKey(paramKey) &&
                    !Float.isNaN(Float.parseFloat(properties.getProperty(paramKey))),
                    "%s requires float parameter %s", getClass().getSimpleName(), paramKey);
            scalingParameters.put(paramKey, Float.parseFloat(properties.getProperty(paramKey)));
        }

        Preconditions.checkArgument(properties.containsKey("confidenceScalingLogFile"),
                "%s requires string parameter 'confidenceScalingLogFile'", getClass().getSimpleName());
        logfilename = properties.getProperty("confidenceScalingLogFile");
    }

    @Override
    public void run() {

        try {
            Broadcast<KBParameters> kbParametersBroadcast = sc.broadcast(kbParameters);
            List<String> relationTypes = Lists.newArrayList(KbUtil.relationTypesAdept);
            JavaRDD<String> relationTypesRDD = sc.parallelize(relationTypes);
            JavaRDD<List<KBRelation>> entitiesByTypeRDD = relationTypesRDD.map(new GetRelationsByTypeString(kbParametersBroadcast));
            JavaRDD<KBID> relationIDsRDD = entitiesByTypeRDD.flatMap(new FlattenRelationLists());
            log.info("{} total relations returned",relationIDsRDD.count());

            Broadcast<Map<String, Float>> mapBroadcast = sc.broadcast(scalingParameters);
            JavaPairRDD<KBID, Float> scaledConfidences = relationIDsRDD.mapToPair(new ScaleConfidences(mapBroadcast));
            JavaPairRDD<KBID, Tuple2<Float, String>> scaledConfidencesWithLogging = scaledConfidences.mapToPair(new LogConfidenceScaling());
            scaledConfidencesWithLogging.foreach(new UpdateScaledRelations());

            Map<KBID, Tuple2<Float, String>> result = scaledConfidencesWithLogging.collectAsMap();
            List<String> logs = result.values().stream().map(Tuple2::_2).collect(Collectors.toList());
            FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());

            Path file = new Path(logfilename);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
            bw.write("Ran ConfidenceScaler with properties: ");
            for (Map.Entry<String, Float> entry: scalingParameters.entrySet()) {
                bw.write(entry.getKey());
                bw.write(": ");
                bw.write(Float.toString(entry.getValue()));
                bw.write("\n");
            }
            bw.write("RelType,RelMentions,Arg1Role,Arg1Mentions,Arg2Role,Arg2Mentions,OrigConf,NewConf\n");
            for (String s : logs)
                bw.write(s);
            bw.close();
        } catch (IOException e) {
            log.error("Cannot write to logging file, cannot find/create file");
        } catch (Exception e) {
            log.error("Cannot access kb");
            e.printStackTrace();
        }
    }

    /**
     * Return all entities associated with a given type
     */
    private static class GetRelationsByTypeString implements Function<String, List<KBRelation>> {
        private KBParameters kbParameters;
        public GetRelationsByTypeString(Broadcast<KBParameters> kbParametersBroadcast) {
            kbParameters = kbParametersBroadcast.getValue();
        }
        @Override
        public List<KBRelation> call(String typeString) throws Exception {
            KB kb = KBSingleton.getInstance(kbParameters);
            OntType otRaw = (OntType) OntTypeFactory.getInstance().getType("TACKBP", typeString);
            com.google.common.base.Optional<OntType> otOpt = KBOntologyMap.getAdeptOntologyIdentityMap().getKBTypeForType(otRaw);
            if (otOpt.isPresent())
                return kb.getRelationsByType(otOpt.get());
            return Lists.newArrayList();
        }
    }

    /**
     * Use with flatMap method to turn a list of lists of KBRelations int a single list of KBIDs
     */
    private static class FlattenRelationLists implements FlatMapFunction<List<KBRelation>, KBID> {
        @Override
        public Iterator<KBID> call(List<KBRelation> kbRelations) throws Exception {
            List<KBID> result = Lists.newArrayList();
            for (KBRelation relation: kbRelations)
                result.add(relation.getKBID());
            return result.iterator();
        }
    }


    /**
     * Use the parameters provided to scale (down or up) the confidences on a given relation
     * Just returns the relation id with a new confidence, does not change the KB
     */
    protected static class ScaleConfidences implements PairFunction<KBID, KBID, Float> {
        private Map<String, Float> scalingParams;
        public ScaleConfidences(Broadcast<Map<String, Float>> scalingParams) {
            this.scalingParams = scalingParams.value();
        }
        @Override
        public Tuple2<KBID, Float> call(KBID kbid) throws Exception {
            KB kb = KBSingleton.getInstance();
            KBRelation rel = kb.getRelationById(kbid);

            float scale1 = scalingParams.get("allRels_ConfScaleDown");

            // an unlikely relation is one in which the arguments are common but the relation itself is not
            float scale2 = 1;
            Set<KBRelationArgument> args = rel.getArguments();
            int argOccurrences = 0;
            for (KBRelationArgument arg: args) {
                argOccurrences += arg.getTarget().getProvenances().size();
            }
            Set<KBProvenance> relJustifications = rel.getProvenances();
            float justificationRatio = ((float) relJustifications.size())/((float) argOccurrences);
            if (justificationRatio < scalingParams.get("unlikelyRels_ArgRelRatioThreshold"))
                scale2 = scalingParams.get("unlikelyRels_ConfScaleDown");

            float scale3 = 1;
            if (relJustifications.size() >= scalingParams.get("likelyRels_RelCountThreshold"))
                scale3 = scalingParams.get("likelyRels_ConfScaleUp");

            float newConfidence = rel.getConfidence() * scale1 * scale2 * scale3;
            return new Tuple2<KBID, Float>(kbid, newConfidence);
        }
    }

    /**
     * Create a string that describes the changes we're making, will be written to file for post-experiment inspection
     */
    protected static class LogConfidenceScaling implements PairFunction<Tuple2<KBID, Float>, KBID, Tuple2<Float, String>> {

        @Override
        public Tuple2<KBID, Tuple2<Float, String>> call(Tuple2<KBID, Float> kbidFloatTuple2) throws Exception {
            KB kb = KBSingleton.getInstance();
            KBRelation rel = kb.getRelationById(kbidFloatTuple2._1);
            StringBuilder sb = new StringBuilder();
            sb.append(rel.getType().getType()).append(",").append(rel.getProvenances().size()).append(",");
            Set<KBRelationArgument> args = rel.getArguments();
            for (KBRelationArgument arg: rel.getArguments()) {
                sb.append(arg.getRole().getType()).append(",").append(arg.getTarget().getProvenances().size()).append(",");
            }
            sb.append(rel.getConfidence()).append(",").append(kbidFloatTuple2._2).append("\n");
            return new Tuple2<>(kbidFloatTuple2._1, new Tuple2<>(kbidFloatTuple2._2, sb.toString()));
        }
    }

    protected static class UpdateScaledRelations implements VoidFunction<Tuple2<KBID, Tuple2<Float, String>>> {

        @Override
        public void call(Tuple2<KBID, Tuple2<Float, String>> kbidTuple2Tuple2) throws Exception {
            KB kb = KBSingleton.getInstance();
            KBRelation relation = kb.getRelationById(kbidTuple2Tuple2._1);
            KBRelation.AbstractUpdateBuilder<?,?> updateBuilder = relation.updateBuilder();
            updateBuilder.setConfidence(kbidTuple2Tuple2._2._1);
            KBRelation updatedRelation = updateBuilder.update(kb);
        }
    }

}
