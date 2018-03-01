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
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.Collectors;


/**
 * For a given entity, find mentions for which the canonical string represents
 * less than x% of all of the canonical strings for all mentions.
 * Each of these becomes a new entity; the rest of the entity remains whole.
 * Relations are reassigned according to their provenance
 * Created by iheintz on 4/24/17.
 */
public class KBResolverSplitEntityMentionOutliers extends KBResolverAbstractModule {

    private static Logger log = LoggerFactory.getLogger(KBResolverSplitEntityMentionOutliers.class);
    private JavaSparkContext sc;
    private KBParameters kbParameters;
    private int entitySizeThreshold;
    private float provenancePercentageThreshold;
    private String logFilename;
    private KB kb;


    @Override
    public void initialize(JavaSparkContext sc, Properties properties, KBParameters kbParameters) throws InvalidPropertiesFormatException {
        this.sc = sc;
        this.kbParameters = kbParameters;
        Preconditions.checkArgument(properties.containsKey("largeEntityNumProvenancesThreshold"),
                "%s requires integer property 'largeEntityNumProvenancesThreshold'", getClass().getSimpleName());
        Preconditions.checkArgument(properties.containsKey("outlierCanonicalMentionPercentageThreshold"),
                "%s requires float property 'outlierCanonicalMentionPercentageThreshold'", getClass().getSimpleName());
        Preconditions.checkArgument(properties.containsKey("entityMentionSplitLogFile"),
                "%s requires string parameter 'entityMentionSplitLogFile'", getClass().getSimpleName());
        logFilename = properties.getProperty("entityMentionSplitLogFile");
        this.entitySizeThreshold = Integer.parseInt(properties.getProperty("largeEntityNumProvenancesThreshold"));
        this.provenancePercentageThreshold = Float.parseFloat(properties.getProperty("outlierCanonicalMentionPercentageThreshold"));
    }

    @Override
    public void run() {
//        final Broadcast<KBParameters> broadcastKBParameters = sc.broadcast(kbParameters);

        // get all entity IDs as a flat list
        List<String> entityTypes = Lists.newArrayList(KbUtil.entityTypes);
        JavaRDD<String> entityTypesRDD = sc.parallelize(entityTypes);
        JavaRDD<List<KBEntityWrapper>> entitiesByTypeRDD = entityTypesRDD.map(new GetEntitiesByTypeString(kbParameters));
        JavaRDD<KBEntityWrapper> entityIDsRDD = entitiesByTypeRDD.flatMap(new FlattenEntityLists());
        log.info("{} total entities returned",entityIDsRDD.count());

        // get the large entities, and split off the mentions with low-percentage canonical strings into their own entities
        // insert the new entities into the kb, but don't delete the old yet
        // keep track of original to new entities
        JavaRDD<KBEntityWrapper> largeEntities = entityIDsRDD.filter(new LargeEntities(entitySizeThreshold));
        log.info("{} entities to split",largeEntities.count());

        JavaPairRDD<KBEntityWrapper, Map<String, Integer>> entityToCanonStringCounts =
                largeEntities.mapToPair(new GroupProvenancesByCanonicalString());

        JavaPairRDD<KBEntityWrapper, List<List<String>>> entityToCanonStringClusters =
                entityToCanonStringCounts.mapToPair(new RegroupProvenances(provenancePercentageThreshold));

        // each original entity mapped to the list of new large and outlier entities
        JavaPairRDD<KBEntityWrapper, List<KBID>> entityWrapperToNewIDs =
                entityToCanonStringClusters.mapToPair(new CreateNewEntities());

        // each affected relation mapped to one original entity and its associated new entities
        JavaPairRDD<KBID, Tuple2<KBEntityWrapper, List<KBID>>> relationToOrigAndNewEntity =
                entityWrapperToNewIDs.flatMapToPair(new GetRelations());

        // each affected relation mapped to all original argument entities and their associated new entities
        JavaPairRDD<KBID, Map<KBEntityWrapper, List<KBID>>> relationToOrigAndNewEntities =
                relationToOrigAndNewEntity.aggregateByKey(Maps.newHashMap(), new SeqFunc(), new CombFunc());

        JavaPairRDD<Tuple2<KBID, KBID>, Map<KBEntityWrapper, List<KBID>>> relationsEntitiesOldAndNew =
                relationToOrigAndNewEntities.mapToPair(new CreateNewRelations());

        // bring everything local so that spark laziness is resolved, then go forward piecemeal
        // tried to do this within spark, but couldn't work the ordering correctly, things got deleted before getting logged
        List<String> logs = Lists.newArrayList();
        try {
            kb = KBSingleton.getInstance(kbParameters);
            Map<Tuple2<KBID, KBID>, Map<KBEntityWrapper, List<KBID>>> relationsEntitiesOldAndNewMap = relationsEntitiesOldAndNew.collectAsMap();
            Set<Tuple2<KBID, KBID>> oldRelNewRels = relationsEntitiesOldAndNewMap.keySet();
            Collection<Map<KBEntityWrapper, List<KBID>>> oldEntsNewEnts = relationsEntitiesOldAndNewMap.values();


            for (Tuple2<KBID, KBID> relPair: oldRelNewRels) {
                logs.add(removeOldRelation(relPair));
                logs.add(logNewRelation(relPair));
            }
            for (Map<KBEntityWrapper, List<KBID>> entPair: oldEntsNewEnts) {
                logs.add(removeOldEntity(entPair.keySet()));
                logs.add(logNewEntities(entPair.values()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
            Path file = new Path(logFilename);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(file)));
            for (String log: logs)
                bw.write(log);

        } catch (IOException e) {
            log.error("Can't write output, can't find filesystem");
        }

    }
    /**
     * Return all entities associated with a given type
     */
    private static class GetEntitiesByTypeString implements Function<String, List<KBEntityWrapper>> {
        private final KBParameters kbParams;
        public GetEntitiesByTypeString(KBParameters kbParams) {
            this.kbParams = kbParams;
        }
        @Override
        public List<KBEntityWrapper> call(String typeString) throws Exception {
            KB kb = KBSingleton.getInstance(kbParams);
            List<KBEntityWrapper> result = Lists.newArrayList();
            OntType otRaw = (OntType) OntTypeFactory.getInstance().getType("TACKBP", typeString);
            Optional<OntType> otOpt = KBOntologyMap.getTACOntologyMap().getKBTypeForType(otRaw);
            if (otOpt.isPresent()) {
                for (KBEntity entity: kb.getEntitiesByType(otOpt.get()))
                    result.add(new KBEntityWrapper(entity));
            }
            return result;
        }
    }

    /**
     * Use with flatMap method to turn a list of lists of KBEntities int a single list of KBEntities
     */
    protected static class FlattenEntityLists implements  FlatMapFunction<List<KBEntityWrapper>, KBEntityWrapper> {
        @Override
        public Iterator<KBEntityWrapper> call(List<KBEntityWrapper> kbEntities) throws Exception {
            return kbEntities.iterator();
        }
    }



    protected static class LargeEntities implements Function<KBEntityWrapper, Boolean> {
        private final int threshold;
        public LargeEntities(int entitySizeThreshold) {
            threshold = entitySizeThreshold;
        }
        @Override
        public Boolean call(KBEntityWrapper kbEntityWrapper) throws Exception {
            if (kbEntityWrapper.getCanonicalStrings().size() > threshold)
                return true;
            return false;
        }
    }

    /**
     * Return a mapping of canonical string to how many times that canonical string occurs within
     * the provenances of the particular entity
     */
    protected static class GroupProvenancesByCanonicalString implements PairFunction<KBEntityWrapper, KBEntityWrapper, Map<String, Integer>> {
        @Override
        public Tuple2<KBEntityWrapper, Map<String, Integer>> call(KBEntityWrapper kbEntityWrapper) throws Exception {
            Map<String, Integer> result = Maps.newHashMap();
            for (String canonicalString: kbEntityWrapper.getCanonicalStrings()) {
                int count = result.containsKey(canonicalString) ? result.get(canonicalString) : 0;
                count++;
                result.put(canonicalString, count);
            }
            return new Tuple2<>(kbEntityWrapper, result);
        }
    }

    /**
     * For each entity, make a list of two lists
     * First item has all canonical strings above the threshold
     * Second item has all canonical strings below the threshold
     *
     */
    protected static class RegroupProvenances implements PairFunction<Tuple2<KBEntityWrapper, Map<String, Integer>>, KBEntityWrapper, List<List<String>>> {
        private final float threshold;
        public RegroupProvenances(float provenancePercentageThreshold) {
            threshold = provenancePercentageThreshold;
        }
        @Override
        public Tuple2<KBEntityWrapper, List<List<String>>> call(Tuple2<KBEntityWrapper, Map<String, Integer>> kbEntityWrapperMapTuple2) throws Exception {
            Map<String, Integer> mentionCounts = kbEntityWrapperMapTuple2._2;
            int totalCount = mentionCounts.values().stream().reduce(0, Integer::sum);
            int countThreshold = Math.round(totalCount * threshold);
            List<String> mentionsForLargeEntity = Lists.newArrayList();
            List<String> mentionsForSmallEntities = Lists.newArrayList();
            for (Map.Entry<String, Integer> entry: kbEntityWrapperMapTuple2._2.entrySet()) {
                if (entry.getValue() > countThreshold)
                    mentionsForLargeEntity.add(entry.getKey());
                else
                    mentionsForSmallEntities.add(entry.getKey());
            }
            // don't bother sorting, later it's not guaranteed
            //mentionsForLargeEntity.sort(Comparator.comparing(mention -> mentionCounts.get(mention)).reversed());

            List<List<String>> result = Lists.newArrayList();
            result.add(mentionsForLargeEntity);
            result.add(mentionsForSmallEntities);
            return new Tuple2<>(kbEntityWrapperMapTuple2._1, result);
        }
    }

    /**
     * Create a single entity for all majority canonical strings and
     * individual entities for all outlier canonical strings
     */
    protected static class CreateNewEntities implements PairFunction<Tuple2<KBEntityWrapper, List<List<String>>>, KBEntityWrapper, List<KBID>> {
        @Override
        public Tuple2<KBEntityWrapper, List<KBID>> call(Tuple2<KBEntityWrapper, List<List<String>>> kbEntityWrapperListTuple2) throws Exception {
            KBEntityWrapper origEntityWrapper = kbEntityWrapperListTuple2._1;
            List<List<String>> canonicalStringClusters = kbEntityWrapperListTuple2._2;
            List<String> largeEntityCanonStrings = canonicalStringClusters.get(0);
            List<String> smallEntityCanonStrings = canonicalStringClusters.get(1);
            List<KBID> result = Lists.newArrayList();
            KB kb = KBSingleton.getInstance();
            KBEntity originalEntity = kb.getEntityById(origEntityWrapper.getKbid());
            // filtering does not guarantee original order of items in the list
            List<KBProvenance> largeEntityProvenances = originalEntity.getProvenances().stream().filter(prov -> largeEntityCanonStrings.contains(prov.getValue())).collect(Collectors.toList());
            List<KBProvenance> smallEntityProvenances = originalEntity.getProvenances().stream().filter(prov -> smallEntityCanonStrings.contains(prov.getValue())).collect(Collectors.toList());

            // make a new large entity with all the "large"  provenances together
            KBEntityMentionProvenance.InsertionBuilder canonicalMentionBuilder;
            KBEntityMentionProvenance.InsertionBuilder nextMentionBuilder;
            KBEntity.InsertionBuilder kbEntityInsertionBuilder;
            try {
                canonicalMentionBuilder = getEntityMentionProvenanceInsertionBuilder((KBEntityMentionProvenance) largeEntityProvenances.get(0));
                kbEntityInsertionBuilder = new KBEntity.InsertionBuilder(
                        originalEntity.getTypes(), canonicalMentionBuilder,
                        originalEntity.getConfidence(), ((KBTextProvenance) largeEntityProvenances.get(0)).getConfidence());
                kbEntityInsertionBuilder.addProvenance(canonicalMentionBuilder);

                for (int i = 1; i < largeEntityProvenances.size(); i++) {
                    nextMentionBuilder = getEntityMentionProvenanceInsertionBuilder((KBEntityMentionProvenance) largeEntityProvenances.get(i));
                    kbEntityInsertionBuilder.addProvenance(nextMentionBuilder);
                }
            } catch (ClassCastException e) {
                log.error("Cannot build a new entity from original {}, provenances are not KBEntityMentionProvenance",
                        originalEntity.getCanonicalString());
                throw e;
            }
            KBEntity newLargeEntity = kbEntityInsertionBuilder.insert(kb);
            result.add(newLargeEntity.getKBID());

            // group small provenances by canonical mention so we can make a new entity for each
            Map<String, List<KBProvenance>> provByCanon = Maps.newHashMap();
            for (KBProvenance prov: smallEntityProvenances) {
                String mention = prov.getValue();
                List<KBProvenance> ilist = provByCanon.containsKey(mention) ? provByCanon.get(mention) : Lists.newArrayList();
                ilist.add(prov);
                provByCanon.put(mention, ilist);
            }
            // make a new entity for each cluster of provenances
            for (String canonString: provByCanon.keySet()) {
                List<KBProvenance> clusterProvs = provByCanon.get(canonString);
                try {
                    canonicalMentionBuilder = getEntityMentionProvenanceInsertionBuilder((KBEntityMentionProvenance) clusterProvs.get(0));
                    kbEntityInsertionBuilder = new KBEntity.InsertionBuilder(
                            originalEntity.getTypes(), canonicalMentionBuilder,
                            originalEntity.getConfidence(), ((KBTextProvenance) clusterProvs.get(0)).getConfidence());
                    kbEntityInsertionBuilder.addProvenance(canonicalMentionBuilder);
                    for (int i = 1; i < clusterProvs.size(); i++) {
                            nextMentionBuilder = getEntityMentionProvenanceInsertionBuilder((KBEntityMentionProvenance) clusterProvs.get(i));
                            kbEntityInsertionBuilder.addProvenance(nextMentionBuilder);
                    }
                } catch (ClassCastException e) {
                    log.error("Cannot build new entity from original {}, provenances are not KBEntityMentionProvenance",
                            originalEntity.getCanonicalString());
                }
                KBEntity newSmallEntity = kbEntityInsertionBuilder.insert(kb);
                result.add(newSmallEntity.getKBID());
            }

            return new Tuple2<>(origEntityWrapper, result);
        }
    }


    protected static KBTextProvenance.InsertionBuilder getTextProvenanceInsertionBuilder(KBTextProvenance kbTextProvenance) {
        KBTextProvenance.InsertionBuilder provenanceBuilder = KBTextProvenance.builder();

        provenanceBuilder.setBeginOffset(kbTextProvenance.getBeginOffset());
        provenanceBuilder.setEndOffset(kbTextProvenance.getEndOffset());
        provenanceBuilder.setConfidence(kbTextProvenance.getConfidence());

        provenanceBuilder.setSourceAlgorithmName(kbTextProvenance.getSourceAlgorithmName());
        provenanceBuilder.setContributingSiteName(kbTextProvenance.getContributingSiteName());
        provenanceBuilder.setValue(kbTextProvenance.getValue());

        provenanceBuilder.setDocumentURI(kbTextProvenance.getDocumentURI());
        provenanceBuilder.setDocumentID(kbTextProvenance.getDocumentID());
        provenanceBuilder.setCorpusID(kbTextProvenance.getCorpusID());
        provenanceBuilder.setCorpusName(kbTextProvenance.getCorpusName());
        provenanceBuilder.setCorpusType(kbTextProvenance.getCorpusType());
        provenanceBuilder.setCorpusURI(kbTextProvenance.getCorpusURI());
        provenanceBuilder.setSourceLanguage(kbTextProvenance.getSourceLanguage());
        return provenanceBuilder;
    }

    protected static KBEntityMentionProvenance.InsertionBuilder getEntityMentionProvenanceInsertionBuilder(KBEntityMentionProvenance entityMentionProvenance) {
        KBEntityMentionProvenance.InsertionBuilder provenanceBuilder = KBEntityMentionProvenance.builder();

        provenanceBuilder.setBeginOffset(entityMentionProvenance.getBeginOffset());
        provenanceBuilder.setEndOffset(entityMentionProvenance.getEndOffset());
        provenanceBuilder.setConfidence(entityMentionProvenance.getConfidence());

        provenanceBuilder.setSourceAlgorithmName(entityMentionProvenance.getSourceAlgorithmName());
        provenanceBuilder.setContributingSiteName(entityMentionProvenance.getContributingSiteName());
        provenanceBuilder.setValue(entityMentionProvenance.getValue());

        provenanceBuilder.setDocumentURI(entityMentionProvenance.getDocumentURI());
        provenanceBuilder.setDocumentID(entityMentionProvenance.getDocumentID());
        provenanceBuilder.setCorpusID(entityMentionProvenance.getCorpusID());
        provenanceBuilder.setCorpusName(entityMentionProvenance.getCorpusName());
        provenanceBuilder.setCorpusType(entityMentionProvenance.getCorpusType());
        provenanceBuilder.setCorpusURI(entityMentionProvenance.getCorpusURI());
        provenanceBuilder.setSourceLanguage(entityMentionProvenance.getSourceLanguage());

        provenanceBuilder.setType(entityMentionProvenance.getType());
        return provenanceBuilder;
    }


    /**
     * Return each relation in which this entity takes part paired with the entity
     * Use KBIDs, as relations and entities are not serializable
     */
    protected static class GetRelations implements PairFlatMapFunction<Tuple2<KBEntityWrapper, List<KBID>>, KBID, Tuple2<KBEntityWrapper, List<KBID>>> {
        @Override
        public Iterator<Tuple2<KBID, Tuple2<KBEntityWrapper, List<KBID>>>> call(Tuple2<KBEntityWrapper, List<KBID>> originalAndNewEntities) throws Exception {
            KB kb = KBSingleton.getInstance();
            KBID origEntityId = originalAndNewEntities._1.getKbid();
            List<Tuple2<KBID, Tuple2<KBEntityWrapper, List<KBID>>>> result = Lists.newArrayList();
            for (KBRelation relation: kb.getRelationsByArg(origEntityId))
                result.add(new Tuple2<>(relation.getKBID(), originalAndNewEntities));
            return result.iterator();
        }
    }


    protected static class SeqFunc implements Function2<Map<KBEntityWrapper, List<KBID>>,  Tuple2<KBEntityWrapper, List<KBID>>, Map<KBEntityWrapper, List<KBID>>> {
        @Override
        public Map<KBEntityWrapper, List<KBID>> call(Map<KBEntityWrapper, List<KBID>> mapping, Tuple2<KBEntityWrapper, List<KBID>> newtuple) {
            Map<KBEntityWrapper, List<KBID>> prevmapping = mapping;
            prevmapping.put(newtuple._1, newtuple._2);
            return prevmapping;
        }
    }

    protected static class CombFunc implements Function2<Map<KBEntityWrapper, List<KBID>>, Map<KBEntityWrapper, List<KBID>>, Map<KBEntityWrapper, List<KBID>>> {
        @Override
        public Map<KBEntityWrapper, List<KBID>> call(Map<KBEntityWrapper, List<KBID>> prevmapping, Map<KBEntityWrapper, List<KBID>> nextmapping) throws Exception {
            for (Map.Entry<KBEntityWrapper, List<KBID>> entry: nextmapping.entrySet())
                prevmapping.put(entry.getKey(), entry.getValue());
            return prevmapping;
        }
    }

    /**
    * replace the argument target with the new 'large' entity representing that same thing
    * to the original argument
    * relation update builder does not work, because you can't replace the argument's *target*
     * */
    protected static class CreateNewRelations implements PairFunction<Tuple2<KBID, Map<KBEntityWrapper, List<KBID>>>, Tuple2<KBID, KBID>, Map<KBEntityWrapper, List<KBID>>> {
        @Override
        public Tuple2<Tuple2<KBID, KBID>, Map<KBEntityWrapper, List<KBID>>> call(Tuple2<KBID, Map<KBEntityWrapper, List<KBID>>> relationToOrigAndNewEntities) throws Exception {
            KBID relId = relationToOrigAndNewEntities._1;
            Map<KBEntityWrapper, List<KBID>> entitiesToReplace = relationToOrigAndNewEntities._2;
            Map<KBID, List<KBID>> entitiesToReplaceKBIDs = entitiesToReplace.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().getKbid(), entry -> entry.getValue()));

            KB kb = KBSingleton.getInstance();

            KBRelation origRel = kb.getRelationById(relId);
            KBRelation.InsertionBuilder kbRelationInsertionBuilder =
                    KBRelation.relationInsertionBuilder(origRel.getType(), origRel.getConfidence());
            for (KBRelationArgument relArg : origRel.getArguments()) {
                OntType role = relArg.getRole();
                float conf = relArg.getConfidence();
                KBPredicateArgument target = relArg.getTarget();
                KBID targetID = target.getKBID();
                if (entitiesToReplaceKBIDs.containsKey(targetID)) {
                    KBID replacementID = entitiesToReplaceKBIDs.get(targetID).get(0);
                    target = kb.getEntityById(replacementID);
                }
                KBRelationArgument.InsertionBuilder argumentBuilder =
                        KBRelationArgument.insertionBuilder(role, target, conf);
                kbRelationInsertionBuilder.addArgument(argumentBuilder);
            }
            for (KBProvenance origProvenance: origRel.getProvenances())
                kbRelationInsertionBuilder.addProvenance(getTextProvenanceInsertionBuilder((KBTextProvenance) origProvenance));
            Tuple2<KBID, KBID> rel2rel;
            try {
                KBRelation newRelation = kbRelationInsertionBuilder.insert(kb);
                rel2rel = new Tuple2<>(origRel.getKBID(), newRelation.getKBID());
            } catch (KBUpdateException e) {
                log.error("Cannot update relation, an argument is missing.");
                rel2rel = new Tuple2<>(origRel.getKBID(), origRel.getKBID());
            }
            return new Tuple2<>(rel2rel, entitiesToReplace);
        }
    }


    public String removeOldRelation(Tuple2<KBID, KBID> oldRelNewRel) throws Exception {
        StringBuilder relRemoveLog = new StringBuilder();
        KBRelation kbRelation = kb.getRelationById(oldRelNewRel._1);
        if (oldRelNewRel._1.equals(oldRelNewRel._2))
            relRemoveLog.append("NOT REMOVING RELATION, NO REPLACEMENTS DONE:\n");
        else {
            relRemoveLog.append("REMOVED RELATION: \n");
            relRemoveLog.append(kbRelation.getType().getType());
            relRemoveLog.append(" (").append(kbRelation.getProvenances().size()).append(") :\n");
            for (KBRelationArgument arg : kbRelation.getArguments()) {
                relRemoveLog.append("\t").append(arg.getRole().getType()).append("/");
                try {
                    if (arg.getTarget().getClass().equals(KBEntity.class)) {
                        KBEntity argEntity = kb.getEntityById(arg.getTarget().getKBID());
                        List<String> typeStrings = argEntity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
                        relRemoveLog.append(Joiner.on(",").join(typeStrings)).append("/");
                        relRemoveLog.append(argEntity.getCanonicalString().replace("\n", " "));
                        relRemoveLog.append(" (").append(argEntity.getProvenances().size()).append(")\n");
                    } else {
                        if (arg.getTarget().getClass().equals(KBGenericThing.class)) {
                            KBGenericThing argThing = kb.getGenericThingByID(arg.getTarget().getKBID());
                            relRemoveLog.append(argThing.getType().getType()).append("/");
                            relRemoveLog.append(argThing.getCanonicalString().replace("\n", " "));
                            relRemoveLog.append(" (").append(argThing.getProvenances().size()).append(")\n");
                        } else
                            relRemoveLog.append("(can't get canonical string)").append("; ");
                    }
                } catch (KBQueryException e) {
                    relRemoveLog.append("Exception in relation removal - Target entity not in KB; ");
                }
            }
            relRemoveLog.append("\n");

            kb.deleteKBObject(oldRelNewRel._1);
        }
        return relRemoveLog.toString();
    }


    public String logNewRelation(Tuple2<KBID, KBID> oldRelNewRel) throws Exception {
        StringBuilder relAddLog = new StringBuilder();
        KBRelation kbRelation = kb.getRelationById(oldRelNewRel._2);
        // most of this is for logging
        if (!oldRelNewRel._1.equals(oldRelNewRel._2)) {
            relAddLog.append("ADDED RELATION: \n");
            relAddLog.append(kbRelation.getType().getType());
            relAddLog.append(" (").append(kbRelation.getProvenances().size()).append(") :\n");
            for (KBRelationArgument arg : kbRelation.getArguments()) {
                relAddLog.append("\t").append(arg.getRole().getType()).append("/");
                try {
                    if (arg.getTarget().getClass().equals(KBEntity.class)) {
                        KBEntity argEntity = kb.getEntityById(arg.getTarget().getKBID());
                        List<String> typeStrings = argEntity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
                        relAddLog.append(Joiner.on(",").join(typeStrings)).append("/");
                        relAddLog.append(argEntity.getCanonicalString().replace("\n", " "));
                        relAddLog.append(" (").append(argEntity.getProvenances().size()).append(")\n");
                    } else {
                        if (arg.getTarget().getClass().equals(KBGenericThing.class)) {
                            KBGenericThing argThing = kb.getGenericThingByID(arg.getTarget().getKBID());
                            relAddLog.append(argThing.getType().getType()).append("/");
                            relAddLog.append(argThing.getCanonicalString().replace("\n", " "));
                            relAddLog.append(" (").append(argThing.getProvenances().size()).append(")\n");
                        } else
                            relAddLog.append("(can't get canonical string)").append("; \n");
                    }
                } catch (KBQueryException e) {
                    relAddLog.append("Exception in relation logging - Target entity not in KB; ");
                }
            }
            relAddLog.append("\n");
        }
        return relAddLog.toString();
    }


    public String removeOldEntity(Set<KBEntityWrapper> oldEntities) throws Exception {
        StringBuilder entRemovedLog = new StringBuilder();
        List<String> typeStrings;
        for (KBEntityWrapper wrapper: oldEntities) {
            try {
                KBEntity entity = kb.getEntityById(wrapper.getKbid());
                typeStrings = entity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
                entRemovedLog.append("REMOVED ENTITY: ");
                entRemovedLog.append(entity.getCanonicalString().replace("\n", "  "));
                entRemovedLog.append("\n\t");
                String typeString = Joiner.on("/").join(typeStrings);
                entRemovedLog.append(typeString);
                entRemovedLog.append(" (").append(entity.getProvenances().size() + ")\n\n");
                kb.deleteKBObject(wrapper.getKbid());
            } catch (KBQueryException e) {
                entRemovedLog.append("Already removed entity ").append(wrapper.getKbid()).append("\n");
            }

        }
        return entRemovedLog.toString();
    }

    public String logNewEntities(Collection<List<KBID>> newEntityLists) throws Exception {
        StringBuilder entAddLog = new StringBuilder();
        List<KBID> newEntityIDs = Lists.newArrayList();
        for (List<KBID> innerList: newEntityLists)
            newEntityIDs.addAll(innerList);
        List<String> typeStrings;
        for (KBID newEntityId: newEntityIDs) {
            KBEntity entity = kb.getEntityById(newEntityId);
            typeStrings = entity.getTypes().keySet().stream().map(OntType::getType).collect(Collectors.toList());
            entAddLog.append("ADDED ENTITY: ");
            entAddLog.append(entity.getCanonicalString().replace("\n", "  "));
            entAddLog.append("\n\t");
            String typeString = Joiner.on("/").join(typeStrings);
            entAddLog.append(typeString);
            entAddLog.append(" (").append(entity.getProvenances().size() + ")\n\n");
        }
        return entAddLog.toString();
    }

 }
