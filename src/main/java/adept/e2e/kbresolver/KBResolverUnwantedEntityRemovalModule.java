package adept.e2e.kbresolver;

import adept.common.KBID;
import adept.common.OntType;
import adept.e2e.driver.KBSingleton;
import adept.kbapi.*;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by iheintz on 6/7/17
 * If we know that a particular string should not be an argument of a relation, e.g. "Xinhua" is almost
 * never the employer or organization we want to see in a relation, then delete all of the
 * relations with that string as its argument
 */
public class KBResolverUnwantedEntityRemovalModule extends KBResolverAbstractModule {

    private static Logger log = LoggerFactory.getLogger(KBResolverUnwantedEntityRemovalModule.class);
    private JavaSparkContext sc;
    private static KBParameters kbParameters;
    private static List<String> argStringsToRemove;
    private static String logFilename;

    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) {
        this.sc = sc;
        this.kbParameters = kbParameters;
        Preconditions.checkArgument(properties.containsKey("unwantedEntityStrings"),
                "%s requires comma-separated string list property 'unwantedEntityStrings'", getClass().getSimpleName());
        Preconditions.checkArgument(properties.containsKey("unwantedItemsRemovedLog"),
                "%s requires string property 'unwantedItemsRemovedLog'", getClass().getSimpleName());
        this.argStringsToRemove = Arrays.asList(StringUtils.split(properties.getProperty("unwantedEntityStrings"),','));
        this.logFilename = properties.getProperty("unwantedItemsRemovedLog");
    }

    public KBResolverUnwantedEntityRemovalModule() {
    }

    public void run() {
        try {

            JavaRDD<String> argsToRemoveRDD = sc.parallelize(argStringsToRemove);
            JavaRDD<KBID> entitiesToRemove = argsToRemoveRDD.flatMap(new GetUnwantedEntities(kbParameters));
            JavaPairRDD<KBID, List<KBID>> allObjectsToRemove = entitiesToRemove.mapToPair(new GetAllRelationsWithArg());
            JavaRDD<String> removalLogsRDD = allObjectsToRemove.map(new RemoveObjects(argStringsToRemove));
            List<String> logs = removalLogsRDD.collect();

            FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
            Path file = new Path(logFilename);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
            bw.write("Ran Unwanted Entity Removal Module with strings: ");
            for (String s: argStringsToRemove) {
                bw.write(s);
                bw.write("\n");
            }
            for (String removalLog: logs) {
                bw.write(removalLog);
            }
            bw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected static class GetUnwantedEntities implements FlatMapFunction<String, KBID> {
        private final KBParameters kbParams;
        public GetUnwantedEntities(KBParameters kbParameters) {
            kbParams = kbParameters;
        }

        @Override
        public Iterator<KBID> call(String s) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams);
            List<KBID> unwantedEntityIds = kb.getEntitiesByStringReference(s).stream().map(KBEntity::getKBID).collect(Collectors.toList());
            return unwantedEntityIds.iterator();
        }
    }

    protected static class GetAllRelationsWithArg implements PairFunction<KBID, KBID, List<KBID>> {
        @Override
        public Tuple2<KBID, List<KBID>> call(KBID kbid) throws Exception {
            KB kb = KBSingleton.getInstance();
            List<KBRelation> affectedRels = kb.getRelationsByArg(kbid);
            List<KBID> affectedRelIds = affectedRels.stream().map(KBRelation::getKBID).collect(Collectors.toList());
            return new Tuple2<>(kbid, affectedRelIds);
        }
    }

    /**
     * Remove the unwanted entity along with any relations, and log those things
     * Logs will be written to the output file specified in the KBResolver parameters
     */
    protected static class RemoveObjects implements Function<Tuple2<KBID, List<KBID>>, String> {
        private final List<String> unwantedStrings;
        public RemoveObjects(List<String> unwantedStrings) {
            this.unwantedStrings = unwantedStrings;
        }
        @Override
        public String call(Tuple2<KBID, List<KBID>> kbidListTuple2) throws Exception {
            KB kb = KBSingleton.getInstance();
            KBID entityToRemoveId = kbidListTuple2._1;
            List<KBID> relationToRemoveIds = kbidListTuple2._2;
            KBEntity entityToRemove = kb.getEntityById(entityToRemoveId);
            StringBuilder sb = new StringBuilder();
            if (!unwantedStrings.contains(entityToRemove.getCanonicalString())) {
                sb.append("**** Not removing entity with canonical string: ****" + entityToRemove.getCanonicalString());
                return sb.toString();
            }
            sb.append(logEntityRemoval(kb.getEntityById(entityToRemoveId)));
            for (KBID relation: relationToRemoveIds) {
                try {
                    sb.append(logRelationRemoval(kb.getRelationById(relation), kb));
                } catch (KBQueryException e) {
                    continue; // ok, removed it before for another entity
                }
                kb.deleteKBObject(relation);
            }
            kb.deleteKBObject(entityToRemoveId);
            return sb.toString();
        }
    }

    protected static String logEntityRemoval(KBEntity entity) throws KBQueryException {
        StringBuilder sb = new StringBuilder();
        sb.append("**** DELETE ENTITY AND ALL RELATIONS WITH IT: ****\n");
        List<String> typeStrings = entity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
        String typeString = Joiner.on("/").join(typeStrings);
        sb.append(typeString).append(": ");
        sb.append(entity.getCanonicalString().replace("\n","  "));
        sb.append(" (").append(entity.getProvenances().size()+")\n");
        return sb.toString();
    }

    protected static String logRelationRemoval(KBRelation relation, KB kb) throws KBQueryException {
        StringBuilder sb = new StringBuilder();
        sb.append(relation.getType().getType()).append(" (");
        sb.append(relation.getProvenances().size()).append("): ");
        Set<KBRelationArgument> args = relation.getArguments();
        for (KBRelationArgument arg: args) {
            sb.append(arg.getRole().getType()).append("/");
            KBID tgtID = arg.getTarget().getKBID();
            try {
                KBEntity tgtEntity = kb.getEntityById(tgtID);
                sb.append(tgtEntity.getCanonicalString().replace("\n"," ")).append(" (");
                sb.append(tgtEntity.getProvenances().size()).append(") ");
            } catch (KBQueryException e) {
                sb.append("(entity not found)");
            }
        }
        sb.append("\n");
        return sb.toString();
    }

}