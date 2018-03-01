package adept.e2e.kbresolver;

import adept.common.KBID;
import adept.common.OntType;
import adept.common.OntTypeFactory;
import adept.e2e.driver.KBSingleton;
import adept.kbapi.*;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
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
 * Created by iheintz on 4/24/17.
 */
public class KBResolverEntityTypeAnalysis extends KBResolverAbstractModule {

    private static Logger log = LoggerFactory.getLogger(KBResolverEntityTypeAnalysis.class);
    private JavaSparkContext sc;
    private KBParameters kbParameters;
    private String logfile_basename;

    private enum AnalysisChoices {DumpMinorityTypes, DumpOutlierTypes}
    private String choice;
    private Float outlierPercentThreshold;

    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) throws InvalidPropertiesFormatException {
        this.sc = sc;
        this.kbParameters = kbParameters;

        Preconditions.checkArgument(properties.containsKey("logFileBasename"),
                "%s requires string parameter 'logFileBasename'", getClass().getSimpleName());
        logfile_basename = properties.getProperty("logFileBasename");
        Preconditions.checkArgument(properties.containsKey("entityAnalysisChoice") &&
                (properties.getProperty("entityAnalysisChoice").equalsIgnoreCase(AnalysisChoices.DumpMinorityTypes.toString()) ||
                        properties.getProperty("entityAnalysisChoice").equalsIgnoreCase(AnalysisChoices.DumpOutlierTypes.toString())),
                "%s requires string parameter 'entityAnalysisChoice', one of: %s", getClass().getSimpleName(), Joiner.on(",").join(AnalysisChoices.values()));
        choice = properties.getProperty("entityAnalysisChoice");
        if (AnalysisChoices.valueOf(choice).equals(AnalysisChoices.DumpOutlierTypes)) {
            Preconditions.checkArgument(properties.containsKey("entityTypeOutlierPercentThreshold") &&
                !Float.isNaN(Float.parseFloat(properties.getProperty("entityTypeOutlierPercentThreshold"))),
                    "Choice DumpOutlierTypes requires additional float parameter 'entityTypeOutlierPercentThreshold'");
            outlierPercentThreshold = Float.parseFloat(properties.getProperty("entityTypeOutlierPercentThreshold"));
        }
    }

    @Override
    public void run() {

        final Broadcast<KBParameters> broadcastKBParameters = sc.broadcast(kbParameters);

        // get all entity IDs as a flat list
        List<String> entityTypes = Lists.newArrayList(KbUtil.entityTypes);
        JavaRDD<String> entityTypesRDD = sc.parallelize(entityTypes);
        JavaRDD<List<KBEntity>> entitiesByTypeRDD = entityTypesRDD.map(new GetEntitiesByTypeString(broadcastKBParameters));
        JavaRDD<KBID> entityIDsRDD = entitiesByTypeRDD.flatMap(new FlattenEntityLists());
        log.info("{} total entities returned",entityIDsRDD.count() );

        // map KB IDs to external IDs, but the reverse mapping
        // simplify data structure to associate each external ID to a list of KBIDs
        JavaPairRDD<List<KBID>, KBID> extIDListToKBID = entityIDsRDD.mapToPair(new GetExternalKBIDs(broadcastKBParameters));
        JavaPairRDD<KBID, KBID> extIDtoKBID = extIDListToKBID.flatMapToPair(new ListsToAllPairs());
        JavaPairRDD<KBID, List<KBID>> extIDtoKBIDList = extIDtoKBID.aggregateByKey(Lists.newArrayList(), new SeqFunc(), new CombFunc());
        log.info( "{} unique externalIDs found",extIDtoKBIDList.count());

        // retain only the external IDS that have multiple KB entities
        JavaPairRDD<KBID, List<KBID>> extIDtoMultiKBID = extIDtoKBIDList.filter(new HasMultipleKBIDs());
        log.info("{} externalIDs with multiple KBIDs",extIDtoMultiKBID.count());
        JavaRDD<List<KBID>> overlappingKBIDs = extIDtoMultiKBID.values();

        JavaRDD<List<KBID>> entityListsToRemove = null;
        switch (AnalysisChoices.valueOf(choice)) {
            case DumpMinorityTypes:
                entityListsToRemove = overlappingKBIDs.map(new MinorityTypeEntities(broadcastKBParameters));
                break;
            case DumpOutlierTypes:
                final Broadcast<Float> outlierThresholdBC = sc.broadcast(outlierPercentThreshold);
                entityListsToRemove = overlappingKBIDs.map(new OutlierTypeEntities(broadcastKBParameters, outlierThresholdBC));
                break;
            default:
                log.error("Could not find appropriate entity-removal analysis for choice:{}",choice);
            }

        JavaRDD<KBID> entitiesToRemove = entityListsToRemove.flatMap(new FlattenKBIDLists());

        if (!entitiesToRemove.isEmpty()) {
            JavaPairRDD<KBID, String> relationsRemoved = entitiesToRemove.mapToPair(new RemoveRelations(broadcastKBParameters));
            JavaPairRDD<KBID, String> entitiesAndRelationsRemoved = relationsRemoved.mapToPair(new RemoveEntity(broadcastKBParameters));

            Collection<String> entitiesAndRelationsRemovedStrings = entitiesAndRelationsRemoved.collectAsMap().values();

            try {
                FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
                Path file = new Path(logfile_basename+"_entities_relations");
                if (hdfs.exists(file)) {
                    hdfs.delete(file, true);
                }
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));

                bw.write("Ran entity type analysis with parameters:\n");
                bw.write(choice);
                bw.write("\n");
                if (choice.equalsIgnoreCase("dumpoutliertypes")) {
                    bw.write("outlierPercentThreshold: ");
                    bw.write(Float.toString(outlierPercentThreshold));
                    bw.write("\n");
                }
                for (String s : entitiesAndRelationsRemovedStrings)
                    bw.write(s);

                bw.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            log.info("No entities or relations removed, no overlapping entities found.");
        }
    }


    /**
     * Return all entities associated with a given type
     */
    private static class GetEntitiesByTypeString implements Function<String, List<KBEntity>> {
        private Broadcast<KBParameters> kbParams;
        public GetEntitiesByTypeString(Broadcast<KBParameters> kbParams) {
            this.kbParams = kbParams;
        }
        @Override
        public List<KBEntity> call(String typeString) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams.value());
            OntType otRaw = (OntType) OntTypeFactory.getInstance().getType("TACKBP", typeString);
            Optional<OntType> otOpt = KBOntologyMap.getTACOntologyMap().getKBTypeForType(otRaw);
            if (otOpt.isPresent())
                return kb.getEntitiesByType(otOpt.get());
            return Lists.newArrayList();
        }
    }

    /**
     * Use with flatMap method to turn a list of lists of KBEntities int a single list of KBIDs
     */
    private static class FlattenEntityLists implements  FlatMapFunction<List<KBEntity>, KBID> {
        @Override
        public Iterator<KBID> call(List<KBEntity> kbEntities) throws Exception {
            List<KBID> result = Lists.newArrayList();
            for (KBEntity entity: kbEntities)
                result.add(entity.getKBID());
            return result.iterator();
        }
    }

    /**
     * REturn the list of external KBIDs associated with a given internal KBID
     */
    private static class GetExternalKBIDs implements PairFunction<KBID, List<KBID>, KBID> {
        private Broadcast<KBParameters> kbParams;
        public GetExternalKBIDs(Broadcast<KBParameters> kbParams) {
            this.kbParams = kbParams;
        }
        @Override
        public Tuple2<List<KBID>, KBID> call(KBID kbid) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams.value());
            return new Tuple2<>(kb.getExternalKBIDs(kbid), kbid);
        }

    }

    /**
     * We have a list of extrenal ids associated with an internal KBID
     * Return the list of all pairs extID:kbID
     */
    private static class ListsToAllPairs implements PairFlatMapFunction<Tuple2<List<KBID>, KBID>, KBID, KBID> {
        @Override
        public Iterator<Tuple2<KBID, KBID>> call(Tuple2<List<KBID>, KBID> tuple2) throws Exception {
            List<Tuple2<KBID, KBID>> result = Lists.newArrayList();
            KBID internalID = tuple2._2;
            for (KBID externalId: tuple2._1)
                result.add(new Tuple2<>(externalId, internalID));
            return result.iterator();
        }
    }


    /**
     * For use with aggregateByKey, this is how to add an item to a list
     */
    private static class SeqFunc implements Function2<List<KBID>, KBID, List<KBID>> {
        @Override
        public List<KBID> call(List<KBID> prevList, KBID kbid) throws Exception {
            prevList.add(kbid);
            return prevList;
        }
    }

    /**
     * For use with aggregateByKey, this is how to combine two lists
     */
    private static class CombFunc implements Function2<List<KBID>, List<KBID>, List<KBID>> {
        @Override
        public List<KBID> call(List<KBID> list1, List<KBID> list2) {
            list1.addAll(list2);
            return list1;
        }
    }

    /**
     * For filtering the list of external kbids for those with multiple kb entities
     */
    private static class HasMultipleKBIDs implements Function<Tuple2<KBID, List<KBID>>, Boolean> {
        @Override
        public Boolean call(Tuple2<KBID, List<KBID>> kbidListTuple2) throws Exception {
            return kbidListTuple2._2.size() > 1;
        }
    }

    /**
     * Given a list of entities pointing to the same external kb id, return a list of
     * those that are not the majority type for that entity
     */
    private static class MinorityTypeEntities implements Function<List<KBID>, List<KBID>> {
        private Broadcast<KBParameters> kbParams;
        public MinorityTypeEntities(Broadcast<KBParameters> kbParams) {
            this.kbParams = kbParams;
        }
        @Override
        public List<KBID> call(List<KBID> listKBID) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams.value());
            List<KBID> overlappingEntities = listKBID;
            List<Integer> typeCounts = Lists.newArrayList();
            for (KBID kbid: overlappingEntities) {
                KBEntity entity = kb.getEntityById(kbid);
                typeCounts.add(entity.getProvenances().size());
            }
            int majorityEntityTypeIndex = typeCounts.indexOf(Collections.max(typeCounts));
            List<KBID> result = Lists.newArrayList();
            for (int i=0; i<overlappingEntities.size(); i++)
                if (i != majorityEntityTypeIndex)
                    result.add(overlappingEntities.get(i));
            return result;
        }
    }

    /**
     * When used with the flatMap method, this will turn a list of lists into a flat list
     */
    private static class FlattenKBIDLists implements FlatMapFunction<List<KBID>, KBID> {
        @Override
        public Iterator<KBID> call(List<KBID> kbids) throws Exception {
            return kbids.iterator();
        }
    }


    /**
     * Remove relations associated with a particular entity from the kb
     * Return a string describing the relation for logging
     */
    protected static class RemoveRelations implements PairFunction<KBID, KBID, String> {
        private Broadcast<KBParameters> kbParams;
        public RemoveRelations(Broadcast<KBParameters> kbParams) {
            this.kbParams = kbParams;
        }

        @Override
        public Tuple2<KBID, String> call(KBID kbid) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams.value());
            KBID entityToRemove = kbid;
            StringBuilder result = new StringBuilder();
            List<KBRelation> relationsToRemove = kb.getRelationsByArg(entityToRemove);

            // most of this is for logging
            for (KBRelation rel: relationsToRemove) {
                result.append(rel.getType().getType());
                result.append(" (").append(rel.getProvenances().size()).append(") :\n");
                for (KBRelationArgument arg: rel.getArguments()) {
                    result.append("\t").append(arg.getRole().getType()).append("/");
                    if (arg.getTarget().getClass().equals(KBEntity.class)) {
                        KBEntity argEntity = kb.getEntityById(arg.getTarget().getKBID());
                        List<String> typeStrings = argEntity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
                        result.append(Joiner.on(",").join(typeStrings)).append("/");
                        result.append(argEntity.getCanonicalString().replace("\n", " ")).append("\n");
                    } else {
                        if (arg.getTarget().getClass().equals(KBGenericThing.class)) {
                            KBGenericThing argThing = kb.getGenericThingByID(arg.getTarget().getKBID());
                            result.append(argThing.getType().getType()).append("/");
                            result.append(argThing.getCanonicalString().replace("\n", " ")).append("\n");
                        } else
                            result.append("(can't get canonical string)").append("; ");
                    }
                }
                result.append("\n");

                // okay now actually delete the thing
                kb.deleteKBObject(rel.getKBID());
            }
            return new Tuple2<>(kbid, result.toString());
        }
    }


    /**
     * Remove the particular entity from the kb
     * Return a string describing the deleted entity for data analysis
     */
    protected static class RemoveEntity implements PairFunction<Tuple2<KBID, String>, KBID, String> {
        private Broadcast<KBParameters> kbParams;
        public RemoveEntity(Broadcast<KBParameters> kbParams) {
            this.kbParams = kbParams;
        }
        @Override
        public Tuple2<KBID, String> call(Tuple2<KBID, String> kbidStringTuple2) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams.value());
            KBID entityToRemove = kbidStringTuple2._1;
            String relationsLogging = kbidStringTuple2._2;
            StringBuilder result = new StringBuilder();
            KBEntity entity = kb.getEntityById(entityToRemove);

            List<String> typeStrings = entity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
            String typeString = Joiner.on("/").join(typeStrings);
            result.append(typeString).append(": ");
            result.append(entity.getCanonicalString().replace("\n","  "));
            result.append(" (").append(entity.getProvenances().size()+")\n");
            result.append(relationsLogging);
            kb.deleteKBObject(entityToRemove);
            return new Tuple2<>(entityToRemove, result.toString());
        }
    }

    /**
     * Return entities whose types are outliers compared to the majority type, meaning
     * there are significantly fewer mentions of this type than of the majority type
     */
    protected static class OutlierTypeEntities implements Function<List<KBID>, List<KBID>> {
        private Broadcast<KBParameters> kbParams;
        private Broadcast<Float> outlierPercentThreshold;

        public OutlierTypeEntities(Broadcast<KBParameters> broadcastKBParameters, Broadcast<Float> outlierPercentThreshold) {
            this.kbParams = broadcastKBParameters;
            this.outlierPercentThreshold = outlierPercentThreshold;
        }

        @Override
        public List<KBID> call(List<KBID> kbids) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams.value());
            List<KBID> overlappingEntities = kbids;
            List<Integer> typeCounts = Lists.newArrayList();
            for (KBID kbid: overlappingEntities) {
                KBEntity entity = kb.getEntityById(kbid);
                typeCounts.add(entity.getProvenances().size());
            }
            int majorityTypeCount = Collections.max(typeCounts);
            int outlierCountThreshold = Math.round(majorityTypeCount * outlierPercentThreshold.value());

            List<KBID> result = Lists.newArrayList();
            for (int i=0; i<overlappingEntities.size(); i++)
                if (typeCounts.get(i) < outlierCountThreshold)
                    result.add(overlappingEntities.get(i));
            return result;
        }
    }

}
