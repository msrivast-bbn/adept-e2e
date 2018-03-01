package adept.e2e.utilities;

/*-
 * #%L
 * adept-kb
 * %%
 * Copyright (C) 2012 - 2017 Raytheon BBN Technologies
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import adept.common.IType;
import adept.common.KBID;
import adept.common.OntType;
import adept.common.Pair;
import adept.kbapi.KB;
import adept.kbapi.KBConfigurationException;
import adept.kbapi.KBDate;
import adept.kbapi.KBEntity;
import adept.kbapi.KBEntityMentionProvenance;
import adept.kbapi.KBEvent;
import adept.kbapi.KBGenericThing;
import adept.kbapi.KBNumber;
import adept.kbapi.KBOntologyMap;
import adept.kbapi.KBParameters;
import adept.kbapi.KBPredicateArgument;
import adept.kbapi.KBProvenance;
import adept.kbapi.KBQueryException;
import adept.kbapi.KBRelation;
import adept.kbapi.KBRelationArgument;
import adept.kbapi.KBTextProvenance;
import adept.kbapi.KBThing;
import adept.kbapi.KBUpdateException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hp.hpl.jena.vocabulary.DCTerms.provenance;

/**
 * Utility that connects to a database and dumps some information for human analysis.
 */
public class TacFormatConverter {
    private static final String ontologyMappingFilePath = "serif-to-adept.xml";
    private static final String reverseOntologyMappingFilePath = "adept-to-serif.xml";

    /**
     * Explicit UTF-8 byte-order marks that we need to insert to make Microsoft Excel
     * automatically detect UTF-8 encoding.
     */
    private static final String bom = "\uFEFF";

    /**
     * Version of freebase we are using
     */
    private static final String freebaseVersion = "LDC2015E42";

    /**
     * Log report generation.
     */
    private static final Logger log = LoggerFactory.getLogger(TacFormatConverter.class);
    /**
     * Connection to knowledge base.
     */
    private KB kb;
    /**
     * Directory where analyses will be written.
     */
    private final File outputDirectory;
    /**
     * Analysis run name.
     */
    private final String runName;
    /**
     * Whether to resume a previously output run.
     */
    private final boolean resumeRun;
    /**
     * How many objects of each type to sample.
     */
    private final int sampleSize;
    /**
     * Maximum count of justifications to generate for each slotfill argument.
     */
    private static final int maxArgJustifications = 2;

    private static final int maxSpansPerDocument = 3;

    private final boolean isEdlOnly;

    private final boolean is2016Version;

    private final boolean serifNameLists;

    private final int maxRelationLength;

    private final boolean includeEvents;

    private final boolean doNilClustering;

    private final int maxEntityProvenanceLength = 175;

    private final int maxNominalMentionTokens = 1;

    private final int maxNamedMentionTokens = 5;

    private final double majusculeRatio = 0.3;

    private HashMap<String, String> gpeList = new HashMap<>();

    private static int stringCount = 1;

    /**
     * Map of names of types to TAC abbreviations of types
     */
    private static final Map<String, String> typeToCodeMap;
    static
    {
        typeToCodeMap = new HashMap<String, String>();
        typeToCodeMap.put("GeoPoliticalEntity", "GPE");
        typeToCodeMap.put("Person", "PER");
        typeToCodeMap.put("Organization", "ORG");
        typeToCodeMap.put("Location", "LOC");
        typeToCodeMap.put("Facility", "FAC");
    }

    private static final Pattern nomStopWords = Pattern.compile("(\\bdozen(s|\\b)|\\belement(s|\\b)|\\bflock(s|\\b)|\\bgroups\\b|"
        + "\\bhorde(s|\\b)|\\bhundred(s|\\b)|\\bmany\\b|\\bten(s|\\b)|\\bscore(s|\\b)|"
        + "\\bseries\\b|\\bseveral\\b|\\bstring(s|\\b)|\\bthousand(s|\\b)|\\bnumber(s|\\b)|\\bcrowd(s|\\b)|\\bthrong(s|\\b)|\\bpeople(s|\\b))");

    /**
     * Open a connection to the specified knowledge base.
     *
     * @param parameters A knowledge base configuration read from XML.
     * @throws KBConfigurationException
     */
    public TacFormatConverter(KBParameters parameters, File outputDirectory, String runName,
            boolean resumeRun, int sampleSize, boolean isEdlOnly, boolean is2016Version,
            boolean includeEvents, boolean doNilClustering, boolean serifNameLists)
        throws KBConfigurationException
    {
        this.kb = new KB(parameters);
        this.outputDirectory = new File(outputDirectory, runName);
        this.runName = runName;
        this.resumeRun = resumeRun;
        this.sampleSize = sampleSize;
        this.isEdlOnly = isEdlOnly;
        this.is2016Version = is2016Version;
        this.doNilClustering = doNilClustering;
        if (this.is2016Version){
            this.maxRelationLength = 150;
        } else {
            this.maxRelationLength = 200;
        }
        this.includeEvents = includeEvents;
        this.serifNameLists = serifNameLists;

    }

    /**
     * Close knowledge base connection; used when handling exceptions.
     */
    public void close() {
        try {
            if (kb != null) {
                kb.close();
            }
        } catch (KBUpdateException e) {
            // We can let close silently fail
        }
    }

    public Map<KBSummary.ArtifactType, KBSummary> generate() throws KBQueryException, IOException, URISyntaxException {

        gpeList = buildGPEReference();

        // Make sure output directory exists
        if (!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }

        // Run each kind of analysis
        log.info("Generating report for {}", runName);
        log.info("Output directory is {}", outputDirectory);
        if (resumeRun) {
            log.info("Resuming existing run");
        }

        Map<KBSummary.ArtifactType, KBSummary> kbSummaryMap = new HashMap<>();
        List<KBID> requiredRelations = new ArrayList<>();

        generateColdStart(requiredRelations);
        return kbSummaryMap;
    }

    protected HashMap<String, String> buildDbpediaToFreebase() throws IOException, URISyntaxException{
        String path = getClass().getClassLoader().getResource("adept/utilities/dbpedia2freebase.tsv").getPath();
        CSVReader reader = new CSVReader(new FileReader(path), '\t');

        HashMap<String, String> hash = new HashMap<>();

        String [] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            hash.put(nextLine[0], nextLine[1]);
        }
        return hash;
    }

    protected HashMap<String, String> buildGPEReference() throws IOException, URISyntaxException{
        String path = getClass().getClassLoader().getResource("adept/utilities/list-gpe").getPath();
        CSVReader reader = new CSVReader(new FileReader(path), '\t');

        HashMap<String, String> hash = new HashMap<>();

        String [] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            hash.put(nextLine[0], nextLine[1]);
        }
        return hash;
    }

    protected Map<String, Set<String>> getRelationJustifications(final KBPredicateArgument relation,
                                                    KBPredicateArgument argument,
                                                    KBPredicateArgument slotfill,
                                                    List<String> filteredEntityMentions) throws KBQueryException {
        Iterator<KBProvenance> provenances = relation.getProvenances().iterator();
        Map<String, Set<String>> relationJustificationsByDocument = new HashMap<>();

        // if slotfill has no provenances, the relation is bogus.  Return an empty hashmap so the relation gets dropped
        if (slotfill.getProvenances().isEmpty()){
            return relationJustificationsByDocument;
        }

        while (provenances.hasNext()) {

            KBTextProvenance textProvenance = (KBTextProvenance) provenances.next();
            String documentID = textProvenance.getDocumentID();

            if (filteredEntityMentions.contains(String.format("%s:%d-%d",
                    documentID,
                    textProvenance.getBeginOffset(),
                    textProvenance.getEndOffset()))) {
                continue;
            }

            boolean doesContainFilteredMention = false;

            //calculate beginning and end offsets
            int beginningOffset = textProvenance.getBeginOffset();
            int endOffset = textProvenance.getEndOffset() - 1;
			// Search for slotfill provenances that exist within the relation provenance
			int slotfillBeginningOffset = beginningOffset;
			int slotfillEndOffset = endOffset;
			for (KBProvenance prov : slotfill.getProvenances()){
				KBTextProvenance slotfillProv = (KBTextProvenance) prov;
				if (filteredEntityMentions.contains(String.format("%s:%d-%d",
						documentID,
						slotfillProv.getBeginOffset(),
						slotfillProv.getEndOffset()))) {
					doesContainFilteredMention = true;
					break;
				}
				if (endOffset - beginningOffset > maxRelationLength - 1 &&
						slotfillProv.getDocumentID().equals(documentID) &&
						slotfillProv.getBeginOffset() >= beginningOffset &&
						slotfillProv.getEndOffset() < endOffset) {
					slotfillBeginningOffset = slotfillProv.getBeginOffset();
					slotfillEndOffset = slotfillProv.getEndOffset();
					continue;
				}
			}

			// Search for argument provenances that exist within the relation provenance
			int argBeginningOffset = beginningOffset;
			int argEndOffset = endOffset;
			for (KBProvenance prov : argument.getProvenances()){
				KBTextProvenance argumentProv = (KBTextProvenance) prov;
				if (filteredEntityMentions.contains(String.format("%s:%d-%d",
						documentID,
						argumentProv.getBeginOffset(),
						argumentProv.getEndOffset()))) {
					doesContainFilteredMention = true;
					break;
				}
				if (endOffset - beginningOffset > maxRelationLength - 1 &&
						argumentProv.getDocumentID().equals(documentID) &&
						argumentProv.getBeginOffset() >= beginningOffset &&
						argumentProv.getEndOffset() < endOffset) {
					argBeginningOffset = argumentProv.getBeginOffset();
					argEndOffset = argumentProv.getEndOffset();
					continue;
				}
			}

			// Don't use provenance if that mention was filtered out previously
			if (doesContainFilteredMention) {
				continue;
			}

			int newEndOffset = Math.max(argEndOffset, slotfillEndOffset);
			int newBeginningOffset = Math.min(argBeginningOffset, slotfillBeginningOffset);
			if (slotfillEndOffset - argBeginningOffset > maxRelationLength && argEndOffset - slotfillBeginningOffset > maxRelationLength) {
				endOffset = newEndOffset;
				beginningOffset = endOffset - maxRelationLength + 1;
			} else {
				int len = newEndOffset-newBeginningOffset;
				int margin = (maxRelationLength - len) / 2;
				beginningOffset = Math.max(beginningOffset, newBeginningOffset - margin);
				endOffset = Math.min(endOffset, beginningOffset + maxRelationLength - 1);
//                    if (endOffset > textProvenance.getEndOffset()) {
//                        endOffset = textProvenance.getEndOffset();
//                        beginningOffset = endOffset - maxRelationLength + 1;
//                    }
			}

            // Accumulate in a HashSet to get unique justifications
            String just = formatJustification(documentID, beginningOffset, endOffset);
            if (relationJustificationsByDocument.containsKey(documentID) && relationJustificationsByDocument.get(documentID).size() <= maxEntityProvenanceLength) {
                Set<String> documentJustifications = relationJustificationsByDocument.get(documentID);
                documentJustifications.add(just);
            } else {
                Set<String> documentJustifications = new HashSet<>();
                documentJustifications.add(just);
                relationJustificationsByDocument.put(documentID, documentJustifications);
            }
        }
        return relationJustificationsByDocument;
    }

    protected String formatJustification(String documentID, int beginningOffset, int endOffset){
        String just = String.format("%s:%d-%d",
                documentID,
                beginningOffset,
                endOffset - 1);
        return just;
    }
    protected String formatJustification(KBTextProvenance provenance){
        String just = String.format("%s:%d-%d",
                provenance.getDocumentID(),
                provenance.getBeginOffset(),
                provenance.getEndOffset() - 1);
        return just;
    }

    protected HashMap<String, KBTextProvenance[]> getStringProvenances(Map<String, Set<String>> relationJustificationsByDocument, KBRelationArgument arg) throws KBQueryException{
        HashMap<String, Set<KBProvenance>> setTable = new HashMap<>();

        for ( KBProvenance provenance : arg.getProvenances() ){
            KBTextProvenance textProvenance = (KBTextProvenance) provenance;

            String docID = textProvenance.getDocumentID();

            if (relationJustificationsByDocument.containsKey(docID)) {
                for (String relationJustification : relationJustificationsByDocument.get(docID)) {

                    String doc = relationJustification.split(":")[0];

                    if (setTable.containsKey(doc)) {
                        setTable.get(doc).add(textProvenance);
                    }
                    else {
                        setTable.put(doc, new HashSet<>());
                        setTable.get(doc).add(textProvenance);
                    }
                }
            }
        }
        HashMap<String, KBTextProvenance[]> provenancesByDocument = new HashMap<>();
        for (String key : setTable.keySet()){
            provenancesByDocument.put(key, setTable.get(key).toArray(new KBTextProvenance[setTable.get(key).size()]));
        }

        return provenancesByDocument;
    }

    // Returns a set of pairs, with each pair containing the original justification (left), and the needed
    // information for the output file
    protected Set<Pair<String, List<String>>> getEntityMentionFields(final KBEntity entity,
                                                       Set<String> uniqueDocumentIDs,
                                                       Set<String> uniqueText, Set<KBID> uniqueProvenanceIDs) throws KBQueryException {
        Iterator<KBProvenance> provenances = entity.getProvenances().iterator();
        Set<Pair<String, List<String>>> uniqueJustifications = new HashSet();
        while (provenances.hasNext()) {
            KBEntityMentionProvenance textProvenance =
                (KBEntityMentionProvenance) provenances.next();
            String documentID = textProvenance.getDocumentID();
            String text = textProvenance.getValue();
            if (text == null) {
                log.warn("Provenance string for {} {} was null", entity.getClass().getName(),
                    entity.getKBID().getObjectID());
            } else if (text.isEmpty()) {
                log.warn("Provenance string for {} {} was empty", entity.getClass().getName(),
                    entity.getKBID().getObjectID());
            }

            String originalJustification = String.format("%s:%d-%d",
                    documentID,
                    textProvenance.getBeginOffset(),
                    textProvenance.getEndOffset());

            int provenanceBeginOffset = textProvenance.getBeginOffset();
            int provenanceEndOffset = textProvenance.getEndOffset();

            if (documentID.startsWith("NYT")|documentID.startsWith("ENG")) {
                String firstToken = text.toLowerCase().split("\\s")[0];
                if (text.length() > firstToken.length() && firstToken.matches("a|an|the")) {
                    provenanceBeginOffset += (firstToken.length() + 1);
                    text = text.substring(firstToken.length() + 1);
                }
            } else {
                //CHINESE STUFF GOES HERE
                assert true;
            }
            // Accumulate in a HashSet to get unique justifications
            String justification = String.format("%s:%d-%d",
                    documentID,
                    provenanceBeginOffset,
                    provenanceEndOffset - 1);
            ArrayList<String> just = new ArrayList<>();
            just.add(justification);

            just.add(normalizeJustificationText(text));

            String mentionType = textProvenance.getType().substring(0, 3).toUpperCase();
            just.add(mentionType);

            just.add(documentID);
            float confidence = Math.min(1.0f, textProvenance.getConfidence());
            confidence = Math.max(0.001f, confidence);

            just.add(String.valueOf(confidence));
            uniqueJustifications.add(new Pair<>(originalJustification, just));

            // Accumulate some combined stats
            uniqueDocumentIDs.add(documentID);
            uniqueText.add(text);
            uniqueProvenanceIDs.add(textProvenance.getKBID());

        }
            return uniqueJustifications;
    }

    protected Set<String> readExistingIDs(File outputFile, int idColumn) throws IOException {
        Set<String> existingIDs = new HashSet<String>();
        if (resumeRun) {
            try {
                CSVReader reader = new CSVReader(new FileReader(outputFile));
                String[] csvFields;
                while ((csvFields = reader.readNext()) != null) {
                    if (csvFields.length > idColumn && !csvFields[idColumn].equals("ID")) {
                        existingIDs.add(csvFields[idColumn]);
                    }
                }
                reader.close();
            } catch (FileNotFoundException e) {
                log.error("--resume specified but output doesn't exist", e);
            }
        }
        return existingIDs;
    }

    protected void generateColdStart(final List<KBID> requiredRelations) throws KBQueryException,
            IOException, URISyntaxException {
        System.out.println("Coldstart");

        HashMap<String, String> dbpediaToFreebase = buildDbpediaToFreebase();

        // Load any existing object IDs
        File outputFile = new File(outputDirectory, String.format("%s.kb", runName));
        boolean appending = resumeRun && outputFile.exists();
        Set<String> existingIDs = readExistingIDs(outputFile, 2);
        if (existingIDs.size() > 0) {
            log.info("Read {} existing entities", existingIDs.size());
        }

        // Open the output CSV file with headers
        FileWriter fileWriter = new FileWriter(outputFile, resumeRun);
        if (!appending) {
            fileWriter.write(bom);
        }
        CSVWriter writer = new CSVWriter(fileWriter, '\t', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);
        writer.writeNext(new String[] {runName});

        // Select entity IDs
        List<KBID> entityIDs = kb.getKBIDsByType("adept-base:Entity", new String[] {});
        log.info("Found {} entities", entityIDs.size());

        List<KBID> filteredEntities = new ArrayList<>();
        List<String> filteredEntityMentions = new ArrayList<>();

        int maxDistinctDocs=0;
        int maxUniqueStrings=0;
        ImmutableSet.Builder<KBSummary.TypeWithOptionalArgTypes> typesForSummary = ImmutableSet
                .builder();

        // we accumulate the NIL entities here, but only if doNilClustering is on
        final ImmutableSet.Builder<KBEntity> nilEntitiesBuilder = ImmutableSet.builder();

        // Loop over all of the entity IDs
        for (KBID entityID : entityIDs) {
            // Check if we've already loaded this slotfill
            if (existingIDs.contains(entityID.getObjectID())) {
                continue;
            }

            // Get the entity by ID
            KBEntity entity;
            try {
                entity = kb.getEntityById(entityID);
            } catch (Exception e) {
                log.error("Could not get Entity {}", entityID.getObjectID(), e);
                continue;
            }

            // we collect all NIL entities and process them later
            final boolean isNilEntity = kb.getExternalKBIDs(entityID).get(0).getObjectID().startsWith("NIL")? true : false;
            if (doNilClustering && isNilEntity) {
                nilEntitiesBuilder.add(entity);
                continue;
            }

            // Convert interesting entity fields

            // Accumulate and convert provenances
            Set<String> uniqueDocumentIDs = new HashSet<>();
            Set<String> uniqueText = new HashSet<>();
            Set<KBID> uniqueProvenanceIDs = new HashSet<>();
            Set<Pair<String, List<String>>> allEntityMentionFields = getEntityMentionFields(entity, uniqueDocumentIDs, uniqueText, uniqueProvenanceIDs);

            // Check for pronouns and remove if other mentions exist or skip entity if only mentions are pronouns
            final Pair<ImmutableSet<List<String>>, ImmutableSet<String>> entityMentionFields = cleanEntityMentions(allEntityMentionFields);

            for (String filteredEntityMention : entityMentionFields.getR()) {
                filteredEntityMentions.add(filteredEntityMention);
            }

            if (entityMentionFields.getL().isEmpty()) {
                filteredEntities.add(entityID);
                continue;
            }

            writeEntityTypeInfo(entity, writer, entity.getKBID().getObjectID().replace('-', '_'));

            List<List<String>> entityMentionFieldsArray = new ArrayList<List<String>>(entityMentionFields.getL());
            Set<String> documentIdsWithCanonicalMentions = new HashSet<String>();

            for (int i = 0; i < entityMentionFieldsArray.size(); i++) {
                List<String> justification = entityMentionFieldsArray.get(i);
                if(uniqueDocumentIDs.size()>maxDistinctDocs){
                    maxDistinctDocs=uniqueDocumentIDs.size();
                }
                if(uniqueText.size()>maxUniqueStrings){
                    maxUniqueStrings=uniqueText.size();
                }

                if (!documentIdsWithCanonicalMentions.contains(justification.get(3))) {
                    boolean addCanonicalMention = useAsCanonicalMention(justification, entityMentionFieldsArray, i);
                    if (addCanonicalMention) {
                        writeEntityCanonicalMentionInfo(entity, writer, justification, entity.getKBID().getObjectID().replace('-', '_'));

                        documentIdsWithCanonicalMentions.add(justification.get(3));
                    }
                }

                writeEntityMentionInfo(entity, writer, justification, entity.getKBID().getObjectID().replace('-', '_'));
            }

            // Freebase ID
            String externalId = kb.getExternalKBIDs(entityID).get(0).getKBNamespace();
            String externalIdTrimmed = externalId.substring(1, externalId.length() - 1);
            if (!is2016Version && isEdlOnly && dbpediaToFreebase.containsKey(externalIdTrimmed)) {
                String freebaseID = dbpediaToFreebase.get(externalIdTrimmed);
                List<String> line = new ArrayList<String>();
                line.add(String.format(":Entity_%s", entity.getKBID().getObjectID().replace('-', '_')));
                line.add("link");
                line.add("\""+freebaseVersion+":"+freebaseID+"\"");

                String[] csvFields = new String[line.size()];
                line.toArray(csvFields);
                writer.writeNext(csvFields, false);
            }
        }

        // Handle NIL Entities. We go through all NIL entities.
        // For each entity, (i) grab all canonical mentions. (ii) the first canonical mention with longest text is designated as the single best canonical mention
        // Group all NIL entities by: entityType + best_canonical_mention's_text , but putting into entitiesWithSameCanonicalTextBuilder
        final ImmutableSet<KBEntity> nilEntities = nilEntitiesBuilder.build();
        final ImmutableMultimap.Builder<String, KBEntity> entitiesWithSameCanonicalTextBuilder = ImmutableMultimap.builder();
        final ImmutableMap.Builder<KBEntity, ImmutableSet<List<String>>> entityToMentionsBuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<KBEntity, ImmutableSet<List<String>>> entityToCanonicalMentionsBuilder = ImmutableMap.builder();
        HashMap<KBEntity, String> entityToNilID = new HashMap<>();
        for(final KBEntity entity : nilEntities) {
            Set<String> uniqueDocumentIDs = new HashSet<>();    // TODO : we don't really use uniqueDocumentIDs, uniqueText, nor uniqueProvenanceIDs, do we?
            Set<String> uniqueText = new HashSet<>();
            Set<KBID> uniqueProvenanceIDs = new HashSet<>();
            Set<Pair<String, List<String>>> allEntityMentionFields = getEntityMentionFields(entity, uniqueDocumentIDs, uniqueText, uniqueProvenanceIDs);

            // Check for pronouns and remove if other mentions exist or skip entity if only mentions are pronouns
            final Pair<ImmutableSet<List<String>>, ImmutableSet<String>> entityMentionFields = cleanEntityMentions(allEntityMentionFields);

            for (String filteredEntityMention : entityMentionFields.getR()) {
                filteredEntityMentions.add(filteredEntityMention);
            }

            if (entityMentionFields.getL().isEmpty()) {
                filteredEntities.add(entity.getKBID());
                continue;
            }

            final ImmutableSet<List<String>> canonicalMentions = selectCanonicalMentions(entityMentionFields.getL());
            final List<String> canonicalMention = selectFirstMentionWithLongestText(canonicalMentions);

            final String typeAndText = getEntityType(entity) + ":" + canonicalMention.get(1);

            entitiesWithSameCanonicalTextBuilder.put(typeAndText, entity);
            entityToMentionsBuilder.put(entity, entityMentionFields.getL());
            entityToCanonicalMentionsBuilder.put(entity, canonicalMentions);
        }
        // key: entityType + ":" + text
        final ImmutableMultimap<String, KBEntity> entitiesWithSameCanonicalText = entitiesWithSameCanonicalTextBuilder.build(); // we group entities by type+":"+text
        final ImmutableMap<KBEntity, ImmutableSet<List<String>>> entityToMentions = entityToMentionsBuilder.build();
        final ImmutableMap<KBEntity, ImmutableSet<List<String>>> entityToCanonicalMentions = entityToCanonicalMentionsBuilder.build();

        for(final Map.Entry<String, Collection<KBEntity>> entry : entitiesWithSameCanonicalText.asMap().entrySet()) {
            final ImmutableSet.Builder<List<String>> mentionsBuilder = ImmutableSet.builder();
            final ImmutableSet.Builder<List<String>> canonicalMentionsBuilder = ImmutableSet.builder();

            String representativeId = null;
            for(final KBEntity entity : entry.getValue()) {
                if (representativeId == null)
                    representativeId = entity.getKBID().getObjectID().replace('-', '_');

                mentionsBuilder.addAll(entityToMentions.get(entity));
                canonicalMentionsBuilder.addAll(entityToCanonicalMentions.get(entity));

                entityToNilID.put(entity, representativeId);
            }
            final ImmutableSet<List<String>> mentions = mentionsBuilder.build();
            final ImmutableSet<List<String>> canonicalMentions = canonicalMentionsBuilder.build();
            final ImmutableSet<List<String>> canonicalMentionsFinal = pruneCanonicalMentions(canonicalMentions);

            final KBEntity e = (KBEntity) entry.getValue().toArray()[0];

            writeEntityTypeInfo(e, writer, representativeId);
            for(final List<String> justification : canonicalMentionsFinal) {
                writeEntityCanonicalMentionInfo(e, writer, justification, representativeId); // write canonical mention
            }
            for(final List<String> justification : mentions) {
                writeEntityMentionInfo(e, writer, justification, representativeId);  // write entity mention info
            }

        }

        if (!isEdlOnly) {
            // Now it's time to print the relations and events
            printRelations(writer, "adept-base:Relation",
                new String[]{
                        "adept-base:Event",
                        "adept-base:TemporalSpan",
                        "adept-core:Belief",
                        "adept-core:Sentiment",
                }, filteredEntities, filteredEntityMentions, entityToNilID);
            if (includeEvents) {
                printEventsAsRelations(writer, filteredEntities, filteredEntityMentions, entityToNilID);
            }
        }

        // Done
        writer.close();
    }

    // canonicalMentions can contain more than one per document, so we remove all but the best
    // for each document
    protected ImmutableSet<List<String>> pruneCanonicalMentions(final ImmutableSet<List<String>> canonicalMentions) {


        HashMap<String, List<String>> documentToBestCanonicalMention = new HashMap<>();

        for(final List<String> justification : canonicalMentions) {
            String docid = justification.get(0).split(":")[0];
            if (!documentToBestCanonicalMention.containsKey(docid)) {
                documentToBestCanonicalMention.put(docid, justification);
                continue;
            }

            // compare justifications prefer names. After that, prefer longer mentions.
            // Looks like the way to check for a name mention is to ask whether it's not a
            // NOM mention, as things like "FAC" and "GPE" can be in the justification.get(2)
            // spot.
            List<String> currentBestJustification = documentToBestCanonicalMention.get(docid);
            if (!currentBestJustification.get(2).equals("NAM") &&
                justification.get(2).equals("NAM")) {
                documentToBestCanonicalMention.put(docid, justification);
                continue;
            }

            if (currentBestJustification.get(2).equals("NAM") &&
                !justification.get(2).equals("NAM"))
                continue;

            if (currentBestJustification.get(1).length() < justification.get(1).length()) {
                documentToBestCanonicalMention.put(docid, justification);
                continue;
            }
        }
        final ImmutableSet.Builder<List<String>> finalCanonicalMentionsBuilder = ImmutableSet.builder();
        for (List<String> justification : documentToBestCanonicalMention.values())
            finalCanonicalMentionsBuilder.add(justification);

        return finalCanonicalMentionsBuilder.build();
    }

    // this allows one canonical mention per document
    protected ImmutableSet<List<String>> selectCanonicalMentions(final ImmutableSet<List<String>> entityMentionFields) {
        final ImmutableSet.Builder<List<String>> canonicalMentionBuilder = ImmutableSet.<List<String>>builder();

        List<List<String>> entityMentionFieldsArray = new ArrayList<>(entityMentionFields);
        Set<String> documentIdsWithCanonicalMentions = new HashSet<>();

        for (int i = 0; i < entityMentionFieldsArray.size(); i++) {         // loop through all entity mentions
            List<String> justification = entityMentionFieldsArray.get(i);   // current entity mention to examine
            if (!documentIdsWithCanonicalMentions.contains(justification.get(3))) {
                if(useAsCanonicalMention(justification, entityMentionFieldsArray, i)) {
                    canonicalMentionBuilder.add(justification);
                    documentIdsWithCanonicalMentions.add(justification.get(3));
                }
            }
        }

        return canonicalMentionBuilder.build();
    }

    protected boolean useAsCanonicalMention(final List<String> justification, final List<List<String>> entityMentionFieldsArray, final int startingIndex) {
        // If named entity mention, just use it as the canonical mention for now.
        boolean addCanonicalMention = justification.get(2).equals("NAM");

        // If it's a nominal mention and no future mentions from the document
        // are named mentions, go ahead and use it as a canonical mention.
        if (!addCanonicalMention) {
            addCanonicalMention = true;
            for (int j = startingIndex+1; j < entityMentionFieldsArray.size(); j++) {
                if (justification.get(3).equals(entityMentionFieldsArray.get(j).get(3)) && entityMentionFieldsArray.get(j).get(2).equals("NAM")) {
                    addCanonicalMention = false;
                    break;
                }
            }
        }

        return addCanonicalMention;
    }


    protected List<String> selectFirstMentionWithLongestText(final ImmutableSet<List<String>> mentions) {
        List<String> ret = Lists.newArrayList();
        int maxLength = 0;

        for(final List<String> mention : mentions) {
            if(mention.get(1).length() > maxLength) {
                maxLength = mention.get(1).length();
                ret = mention;
            }
        }

        return ret;
    }

    // Returns a pair of sets of entity mentions. The left set are the clean mentions, the right are the removed
    // mentions with original justification information
    protected Pair<ImmutableSet<List<String>>, ImmutableSet<String>> cleanEntityMentions(final Set<Pair<String, List<String>>> allEntityMentionFields) {
        Iterator<Pair<String, List<String>>> entityMentionsIterator = allEntityMentionFields.iterator();
        final ImmutableSet.Builder<List<String>> entityMentionFieldsBuilder = ImmutableSet.builder();
        final ImmutableSet.Builder<String> removedEntityMentionFieldsBuilder = ImmutableSet.builder();

        while (entityMentionsIterator.hasNext()) {
            Pair<String, List<String>> entityMentionFull = entityMentionsIterator.next();
            List<String> entityMention = entityMentionFull.getR();
            String justification = entityMention.get(0);

            String mentionType = entityMention.get(2);
            String text = entityMention.get(1).replace("\"", "");

            //drop too-long justifications
            if (text.length() > maxEntityProvenanceLength){
                continue;
            }
            String lowerCaseText = text.toLowerCase();



            //English path
            if (justification.contains("ENG")) {

                //strip out pronoun mentions
                if (mentionType.equals("PRO") || mentionType.equals("LIS") || mentionType.equals("PAR")) {
                    removedEntityMentionFieldsBuilder.add(entityMentionFull.getL());
                    continue;
                } else if (mentionType.equals("DES") || mentionType.equals("APP")) {
                    entityMention.set(2, "NOM");
                }
                String[] splitText = StringUtils.split(text);
                if (entityMention.get(2).equals("NOM")) {
                    //government is always nominal
                    //drop plural nominals
                    if (lowerCaseText.endsWith("s") || lowerCaseText.endsWith("es") && (!lowerCaseText.endsWith("ss") && !lowerCaseText.endsWith("'s"))) {
                        removedEntityMentionFieldsBuilder.add(entityMentionFull.getL());
                        continue;
                    }
                    // if all the words in it are capitalized, make it a named mention
                    boolean capsFlag = true;
                    for (String word : splitText) {
                        if (!java.lang.Character.isUpperCase(word.charAt(0))) {
                            capsFlag = false;
                        }
                    }
                    if (capsFlag) {
                        entityMention.set(2, "NAM");
                        //else if this entity contains more than num tokens, remove it from the output
                    } else if (splitText.length > maxNominalMentionTokens) {
                        removedEntityMentionFieldsBuilder.add(entityMentionFull.getL());
                        continue;
                    }
                    //if this is a nominal mention and contains a nomStopWord, remove it from the output
                    Matcher m = nomStopWords.matcher(entityMention.get(1));
                    if (m.find()) {
                        removedEntityMentionFieldsBuilder.add(entityMentionFull.getL());
                        continue;
                    }

                } else {
                    if (splitText.length > maxNamedMentionTokens) {
                        double caps = 0;
                        for (String word : splitText) {
                            if ((java.lang.Character.isUpperCase(word.charAt(0)))) {
                                caps += 1.0;
                            }
                        }
                        double ratio = caps / splitText.length;
                        if (ratio < majusculeRatio) {
                            removedEntityMentionFieldsBuilder.add(entityMentionFull.getL());
                            continue;
                        }
                    }
                    if (lowerCaseText.equals("government")) {
                        entityMention.set(2, "NOM");
                    }
                }
            } else if (justification.startsWith("CMN")){
                //CHINESE HACKS GO HERE
                assert true;
            } else {
                System.out.println(String.format("Invalid language: %s", justification));
            }
            //strip out URLs and mentions with excessive whitespace, a sign that the span contains HTML
            if (lowerCaseText.startsWith("http") | lowerCaseText.contains("  ") | lowerCaseText.contains("\t") | lowerCaseText.contains("\r")) {
                removedEntityMentionFieldsBuilder.add(entityMentionFull.getL());
                continue;
            }

            entityMentionFieldsBuilder.add(entityMention);
        }
        return new Pair<>(entityMentionFieldsBuilder.build(), removedEntityMentionFieldsBuilder.build());
    }

    protected void writeEntityTypeInfo(final KBEntity entity, CSVWriter writer, final String id) {
        List<String> line1 = new ArrayList<>();
        line1.add(String.format(!is2016Version ? ":Entity_%s" : ":%s", id));
        line1.add("type");
        line1.add(getEntityType(entity));

        String[] csvFields = new String[line1.size()];
        line1.toArray(csvFields);
        writer.writeNext(csvFields, false);
    }

    protected void writeEntityCanonicalMentionInfo(final KBEntity entity, CSVWriter writer, List<String> justification, final String id) {
        List<String> line = new ArrayList<>();
        line.add(String.format(!is2016Version ? ":Entity_%s" : ":%s", id));
        line.add("canonical_mention");
        line.add(justification.get(1));
        line.add(justification.get(0));
        if (!is2016Version && isEdlOnly) {
            line.add(justification.get(4));
        }

        String[] csvFields = new String[line.size()];
        line.toArray(csvFields);
        writer.writeNext(csvFields, false);
    }

    protected void writeEntityMentionInfo(final KBEntity entity, CSVWriter writer, List<String> justification, String id) {
        List<String> line = new ArrayList<>();
        line.add(String.format(!is2016Version ? ":Entity_%s" : ":%s", id));
        line.add(justification.get(2).equals("NOM") ? "nominal_mention" : "mention");
        line.add(justification.get(1));
        line.add(justification.get(0));
        if (!is2016Version && isEdlOnly) {
            line.add(justification.get(4));
        }

        String[] csvFields = new String[line.size()];
        line.toArray(csvFields);
        writer.writeNext(csvFields, false);
    }

    protected String normalizeJustificationText(String justificationText){
        return String.format("\"%s\"", justificationText.replace('\n', ' ').replace("\\", "\\\\").replace("\"", "\\\""));
    }


    protected String getEntityType(KBEntity entity){
        if (gpeList.containsKey(entity.getCanonicalString())) {
            return gpeList.get(entity.getCanonicalString());
        }
        List<String> types = new ArrayList<>();
        for (Map.Entry<OntType, Float> entry : entity.getTypes().entrySet()) {
            types.add(entry.getKey().getType());
        }
        String t = types.get(0);
        if (types.size() > 1){
            for ( String type : types) {
                if (typeToCodeMap.containsKey(type)) {
                    t = type;
                    break;
                }
            }
        }
        return typeToCodeMap.get(t);
    }

    protected HashMap<Optional<String>, KBRelationArgument> getRelationFields(KBOntologyMap ontologyMap, KBRelation slotfill){
        HashMap<Optional<String>, KBRelationArgument> hash = new HashMap<>();
        Integer count = 0;
        for ( KBRelationArgument arg : slotfill.getArguments() ) {
            if (!(ontologyMap.getTypeforKBRole(slotfill.getType(), arg.getRole()).equals(Optional.of("arg-1|arg-2")))) {
                hash.put(ontologyMap.getTypeforKBRole(slotfill.getType(), arg.getRole()), arg);
            } else {
                count++;
                hash.put(Optional.of(String.format("arg-%s", count.toString())), arg);
            }
        }
        return hash;
    }

    protected void printEventsAsRelations(CSVWriter writer,  final List<KBID> removedEntities,
        final List<String> filteredEntityMentions, final HashMap<KBEntity, String> entityToNilID)
        throws KBQueryException, IOException {

        List<KBID> eventIds = kb.getKBIDsByType("adept-base:Event", new String[0]);
        log.info("Found {} events", eventIds.size());

        for (KBID id : eventIds) {
            KBEvent event;
            try {
                event = kb.getEventById(id);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.error("Could not get KB object {}", id.getObjectID().replace('-', '_'), e);
                continue;
            }

            IType eventType = event.getType();
            List<String> relationTypes = EventToRelationConverter.getRelationTypes(eventType);
            for (String relationType : relationTypes) {
                KBRelationArgument subject = EventToRelationConverter.getSubject(event, relationType);
                KBRelationArgument object = EventToRelationConverter.getObject(event, relationType);

                if (subject == null || object == null)
                    continue;

                String confidence = Double.toString(Math.min(Math.max(0.001, event.getConfidence()), 1.0));
                printRelation(event, relationType, subject, object, removedEntities, filteredEntityMentions,
                        writer, confidence, entityToNilID);
            }
        }
    }


    protected void printEvents(CSVWriter writer)
            throws KBQueryException, IOException {
        KBOntologyMap ontologyMap = KBOntologyMap.loadOntologyMap(ontologyMappingFilePath,
              reverseOntologyMappingFilePath);

        List<KBID> eventIds = kb.getKBIDsByType("adept-base:Event", new String[0]);
        log.info("Found {} events", eventIds.size());

        for (KBID id : eventIds) {
            KBEvent event;
            try {
                event = kb.getEventById(id);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.error("Could not get KB object {}", id.getObjectID().replace('-', '_'), e);
                continue;
            }

            ImmutableMap<OntType, Float> typeMap = event.getRealisTypes();
            OntType realisType = null;
            float highestConfidence = -1;
            for (OntType type : typeMap.keySet()) {
                if (typeMap.get(type) > highestConfidence) {
                    realisType = type;
                    highestConfidence = typeMap.get(type);
                }
            }

            // Default to realis type "actual" if type map was empty
            if (realisType == null) {
               realisType = new OntType("", "actual");
            }

            List<List<String>> linesToWrite = new ArrayList<List<String>>();

            // event type
            IType fullEventType = event.getType();
            if (ontologyMap.getTypeForKBType(event.getType()).isPresent()) {
                fullEventType = ontologyMap.getTypeForKBType(event.getType()).get();
            }
            List<String> typeLine = new ArrayList<String>();
            typeLine.add(String.format(":Event_%s", event.getKBID().getObjectID().replace('-', '_')));
            typeLine.add("type");
            typeLine.add(fullEventType.getType().toUpperCase());

            linesToWrite.add(typeLine);

            // event mentions
            boolean wroteCanonicalMention = false;
            for (KBProvenance provenance : event.getProvenances()) {
                KBTextProvenance textProvenance = (KBTextProvenance)provenance;

                List<String> mentionLine = new ArrayList<String>();
                mentionLine.add(String.format(":Event_%s", event.getKBID().getObjectID().replace('-', '_')));
                mentionLine.add("mention."+realisType.getType());
                mentionLine.add("\""+textProvenance.getValue()+"\"");
                mentionLine.add(String.format("%s:%d-%d",
                        textProvenance.getDocumentID(),
                        textProvenance.getBeginOffset(),
                        textProvenance.getEndOffset() - 1));
                mentionLine.add(Float.toString(textProvenance.getConfidence()));

                linesToWrite.add(mentionLine);

                if (!wroteCanonicalMention) {
                    List<String> canonicalMentionLine = new ArrayList<String>();
                    canonicalMentionLine.add(String.format(":Event_%s", event.getKBID().getObjectID().replace('-', '_')));
                    canonicalMentionLine.add("canonical_mention."+realisType.getType());
                    canonicalMentionLine.add(String.format("\"%s\"", textProvenance.getValue()));
                    canonicalMentionLine.add(String.format("%s:%d-%d",
                            textProvenance.getDocumentID(),
                            textProvenance.getBeginOffset(),
                            textProvenance.getEndOffset() - 1));
                    canonicalMentionLine.add(Float.toString(textProvenance.getConfidence()));

                    linesToWrite.add(canonicalMentionLine);

                    wroteCanonicalMention = true;
                }
            }

            // arguments
            for (KBRelationArgument argument : event.getArguments()) {
                List<String> argumentLine = new ArrayList<>();
                argumentLine.add(String.format(":Event_%s", event.getKBID().getObjectID().replace('-', '_')));
                argumentLine.add(fullEventType.getType().toLowerCase()+":"+argument.getRole().getType()+"."+realisType.getType());
                if (argument.getTarget() instanceof KBEntity) {
                    argumentLine.add(String.format(":Entity_%s", argument.getTarget().getKBID().getObjectID().replace('-', '_')));
                } else if (argument.getTarget() instanceof KBGenericThing) {
                    argumentLine.add(String.format("\"%s\"", ((KBGenericThing)argument.getTarget()).getCanonicalString().replace('\n', ' ')));
                } else if (argument.getTarget() instanceof KBNumber) {
                    argumentLine.add(String.format("\"%s\"", ((KBNumber)argument.getTarget()).getNumber()));
                } else {
                    argumentLine.add(String.format("\"%s\"", ((KBThing)argument.getTarget()).getCanonicalString().replace('\n', ' ')));
                }
                String argProvenances = "";
                for (KBProvenance provenance : argument.getProvenances()) {
                    KBTextProvenance textProvenance = (KBTextProvenance)provenance;
                    argProvenances += String.format("%s:%d-%d;",
                        textProvenance.getDocumentID(),
                        textProvenance.getBeginOffset(),
                        textProvenance.getEndOffset() - 1);
                }
                argumentLine.add(argProvenances.length() > 0 ? argProvenances.substring(0, argProvenances.length()-1) : "");
                argumentLine.add(Float.toString(argument.getConfidence()));

                linesToWrite.add(argumentLine);
            }

            for (List<String> line : linesToWrite) {
                String[] csvFields = new String[line.size()];
                line.toArray(csvFields);
                writer.writeNext(csvFields, false);
            }
        }
    }

    protected void printRelations(CSVWriter writer, String
            slotfillType, final String[] ignoredTypes, final List<KBID> removedEntities,
            final List<String> filteredEntityMentions, final HashMap<KBEntity, String> entityToNilID)
        throws KBQueryException, IOException, URISyntaxException {

        // Select entity IDs
        String KBExtractionType = "relations";
        List<KBID> ids = kb.getKBIDsByType(slotfillType, ignoredTypes);
        log.info("Found {} {}", ids.size(), KBExtractionType);

        // Loop over all of the relation IDs
        for (KBID id : ids) {
            // Get the relation by ID
            KBRelation relation;
            try {
                relation = kb.getRelationById(id);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.error("Could not get KB object {}", id.getObjectID().replace('-', '_'), e);
                continue;
            }

            // Convert interesting relation fields
            KBOntologyMap ontologyMap = KBOntologyMap.getTACOntologyMap();
            HashMap<Optional<String>, KBRelationArgument> argumentHashMap =
                getRelationFields(ontologyMap, relation);

            KBRelationArgument arg1 = argumentHashMap.get(Optional.of("arg-1"));
            KBRelationArgument arg2 = argumentHashMap.get(Optional.of("arg-2"));

            if (arg1 == null || arg2 == null)
                continue;

            // Calculate relation type string
            String relationTypeString = "UNKNOWN";
            KBEntity entity = null;
            try {
                if (arg2.getTarget() instanceof KBEntity) {
                    entity = (KBEntity) arg2.getTarget();
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.error("Could not get KB object {}", id.getObjectID(), e);
                continue;
            }
            if (ontologyMap.getTypeStringForKBType(relation.getType()).isPresent()) {
                if (relation.getType().getType().equals("OrgHeadquarter")) {
                    for (OntType type : entity.getTypes().keySet()) {
                        if (type.getType().equals("City")) {
                            relationTypeString = "org:city_of_headquarters";
                            break;
                        } else if (type.getType().equals("StateProvince")) {
                            relationTypeString = "org:stateorprovince_of_headquarters";
                            break;
                        } else if (type.getType().equals("Country")) {
                            relationTypeString = "org:country_of_headquarters";
                            break;
                        }
                    }
                } else if (relation.getType().getType().equals("Resident")) {
                    for (OntType type : entity.getTypes().keySet()) {
                        if (type.getType().equals("City")) {
                            relationTypeString = "per:cities_of_residence";
                            break;
                        } else if (type.getType().equals("StateProvince")) {
                            relationTypeString = "per:statesorprovinces_of_residence";
                            break;
                        } else if (type.getType().equals("Country")) {
                            relationTypeString = "per:countries_of_residence";
                            break;
                        }
                    }
                } else {
                    relationTypeString =
                        ontologyMap.getTypeStringForKBType(relation.getType()).get();
                }
            } else {
                System.out.println(String.format("Type missing from adept-to-tac.xml: %s",
                    relation.getType().getType()));
            }
            String confidence = Double.toString(Math.min(Math.max(0.001, relation.getConfidence()), 1.0));

            printRelation(relation, relationTypeString, arg1, arg2, removedEntities, filteredEntityMentions, writer,
                confidence, entityToNilID);
        }
    }

    void printRelation(KBPredicateArgument relation, String relationTypeString,
        KBRelationArgument arg1, KBRelationArgument arg2, List<KBID> removedEntities,
        List<String> filteredEntityMentions, CSVWriter writer, String confidence,
        final HashMap<KBEntity, String> entityToNilID)
            throws KBQueryException, IOException
    {
        HashMap<String, String> TACInverseMap = KBOntologyMap.getTACInverseMap();

        List<String> fields = new ArrayList<String>();
        String arg1Id;
        String arg2Id;

        try {
            arg1Id = arg1.getTarget().getKBID().getObjectID().replace('-', '_');
            arg2Id = arg2.getTarget().getKBID().getObjectID().replace('-', '_');
        } catch (NullPointerException e) {
            return;
        }

        // Handle nil entity id switching
        if (arg1.getTarget() instanceof KBEntity) {
            KBEntity arg1Target = (KBEntity) arg1.getTarget();
            if (entityToNilID.containsKey(arg1Target))
                arg1Id = entityToNilID.get(arg1Target);
        }
        if (arg2.getTarget() instanceof KBEntity) {
            KBEntity arg2Target = (KBEntity) arg2.getTarget();
            if (entityToNilID.containsKey(arg2Target))
                arg2Id = entityToNilID.get(arg2Target);
        }

        boolean wasRemovedArgFound = false;
        for (KBID removedEntity : removedEntities) {
            if (arg1.getTarget().getKBID().getObjectID().equals(removedEntity.getObjectID()) ||
                arg2.getTarget().getKBID().getObjectID().equals(removedEntity.getObjectID())) {
                wasRemovedArgFound = true;
                break;
            }
        }
        if (wasRemovedArgFound) return;

        //First field: unique main argument's ID
        fields.add(String.format((arg1.getTarget() instanceof KBEntity && !is2016Version)
                                 ? ":Entity_%s" : ":%s", arg1Id));

        //Second field: type of relation
        fields.add(relationTypeString);
        KBPredicateArgument slotfill = arg2.getTarget();

        String stringID = null;
        String string= "";
        //Third field: Secondary argument's unique ID
        if (arg2.getTarget() instanceof KBGenericThing){
            if (!is2016Version) {
                stringID = String.format(":String_%06d", stringCount);
                stringCount++;
                fields.add(stringID);
            } else {
                string = ((KBGenericThing) slotfill).getCanonicalString().replaceAll("\\s", " ");
                fields.add(String.format("\"%s\"", string));
            }
        } else if (arg2.getTarget() instanceof KBNumber) {
            if (!is2016Version) {
                stringID = String.format(":String_%06d", stringCount);
                stringCount++;
                fields.add(stringID);
            } else {
                fields.add(String.format("\"%s\"", ((KBNumber) slotfill).getNumber()));
            }
        } else if (arg2.getTarget() instanceof KBDate) {
            if (!is2016Version) {
                stringID = String.format(":String_%06d", stringCount);
                stringCount++;
                fields.add(stringID);
            } else {
                fields.add(String.format("\"%s\"", getCanonicalDateStringForKBDate((KBDate) slotfill)));
            }
        } else if (fields.get(fields.size()-1).equals("per:origin")) {
            if (!is2016Version) {
                stringID = String.format(":String_%06d", stringCount);
                stringCount++;
                fields.add(stringID);
            } else {
                string = ((KBThing) slotfill).getCanonicalString().replaceAll("\\s", " ");
                fields.add(String.format("\"%s\"", string));
            }
        } else if (fields.get(fields.size()-1).equals("per:title")) {
            if (!is2016Version) {
                stringID = String.format(":String_%06d", stringCount);
                stringCount++;
                fields.add(stringID);
            } else {
                string = ((KBThing) slotfill).getCanonicalString().replaceAll("\\s", " ");
                fields.add(String.format("\"%s\"", string));
            }
        } else {
            fields.add(String.format(!is2016Version ? ":Entity_%s" : ":%s", arg2Id));
        }

        // Provenances
        Map<String, Set<String>> uniqueJustificationsByDocument = getRelationJustifications(relation, arg1, arg2, filteredEntityMentions);
        List<String> allRelationJustifications = new ArrayList<>();
        if (uniqueJustificationsByDocument.isEmpty()){
//            System.out.println(String.format("Dropped relation %s due to no mentions in range", relation.getKBID().getObjectID()));
            return;
        }

        for (String documentId : uniqueJustificationsByDocument.keySet()) {
            allRelationJustifications.addAll(uniqueJustificationsByDocument.get(documentId));
        }

        // Limit to 3 provenances per relation
        allRelationJustifications = new ArrayList<>(allRelationJustifications.subList(0, Math.min(allRelationJustifications.size(), 3)));

        String join = StringUtils.join(allRelationJustifications, ",");
        fields.add(join);

        // confidence

        fields.add(confidence);

        if (is2016Version) {
            // Convert to CSV
            String[] csvFields = new String[fields.size()];
            fields.toArray(csvFields);
            writer.writeNext(csvFields, false);

            if (TACInverseMap.containsKey(fields.get(1))){
                fields = relationReverser(fields, TACInverseMap, (KBEntity) arg2.getTarget());
                fields.toArray(csvFields);
                writer.writeNext(csvFields, false);
            }
        } else {
            // This block is only needed for relations with string slots, but it shouldn't hurt
            // for other relations
            HashMap<String, KBTextProvenance[]> documentsToStringProvenances = getStringProvenances(uniqueJustificationsByDocument, arg2);
            // drop relation and string if string has no justifications.
            if (documentsToStringProvenances.values().isEmpty()){
//                System.out.println(stringID);
                return;
            }
            boolean wroteStringLine = false;

            for (String documentId : uniqueJustificationsByDocument.keySet()) {
                Set<String> uniqueJustifications = uniqueJustificationsByDocument.get(documentId);
                Set<String> usableJustifications = new HashSet<>();

                Iterator<String> iter = uniqueJustifications.iterator();
                int count = 0;
                while (count < maxSpansPerDocument && iter.hasNext()) {
                    usableJustifications.add(iter.next());
                    count++;
                }
                String joinedJustifications = StringUtils.join(usableJustifications, ",");

                List<String> documentFields = new ArrayList<>();
                for (int i = 0; i < fields.size() - 2; i++) {
                    documentFields.add(fields.get(i));
                }
                String[] csvFields;
                try {
                    if (stringID != null && documentsToStringProvenances.get(documentId) == null) {
                        // relation has document in provenance, but the string argument
                        // provenances do not contain that document
                        continue;
                    }

                    if (stringID != null) {
                        KBTextProvenance[] stringProvenances = documentsToStringProvenances.get(documentId);

                        // It's only now we know we need to print out the string line
                        if (!wroteStringLine) {
                            List<String> nodeFields = stringTypeNodeMaker(stringID);
                            String[] stringNodeFields = new String[nodeFields.size()];
                            nodeFields.toArray(stringNodeFields);
                            writer.writeNext(stringNodeFields, false);
                            wroteStringLine = true;
                        }

                        int canonicalMentionIndex = getIndexOfCanonicalMention(stringProvenances, arg2.getTarget());
                        documentFields.add(String.join(";", formatJustification(stringProvenances[canonicalMentionIndex]), joinedJustifications));
                        documentFields.add(confidence);
                        csvFields = new String[documentFields.size()];
                        documentFields.toArray(csvFields);
                        writer.writeNext(csvFields, false);

                        //print string canonical mention line\
                        String canonicalMentionJustification = formatJustification(stringProvenances[canonicalMentionIndex]);
                        List<String> canonicalMentionFields = stringCanonicalNodeMaker(stringProvenances[canonicalMentionIndex], stringID, canonicalMentionJustification, fields);
                        csvFields = new String[canonicalMentionFields.size()];
                        canonicalMentionFields.toArray(csvFields);
                        writer.writeNext(csvFields, false);

                        if (documentsToStringProvenances.containsKey(documentId)) {
                            for (KBTextProvenance provenance : documentsToStringProvenances.get(documentId)) {
                                //print regular mention lines
                                String justification = formatJustification(provenance);
                                List<String> mentionFields = stringMentionNodeMaker(provenance, stringID, justification, fields);
                                csvFields = new String[mentionFields.size()];
                                mentionFields.toArray(csvFields);
                                writer.writeNext(csvFields, false);

                                if (arg2.getTarget() instanceof KBDate) {
                                    String mentionText = normalizeJustificationText(provenance.getValue());
                                    if (mentionText.startsWith("\""))
                                        mentionText = mentionText.substring(1);
                                    if (mentionText.endsWith("\""))
                                        mentionText = mentionText.substring(0, mentionText.length() - 1);
                                    String normalizedDate = SimpleDateNormalizer.normalizeDate(documentId, mentionText);
                                    if (normalizedDate != null) {
                                        List<String> normalizedMentionFields =
                                            stringNormalizedNodeMaker(normalizedDate, stringID,
                                                justification, fields);
                                        csvFields = new String[normalizedMentionFields.size()];
                                        normalizedMentionFields.toArray(csvFields);
                                        writer.writeNext(csvFields, false);
                                    }
                                }
                            }
                        }

                    } else {
                        //Handles relations without string fills
                        documentFields.add(joinedJustifications);
                        documentFields.add(confidence);
                        csvFields = new String[documentFields.size()];
                        documentFields.toArray(csvFields);
                        writer.writeNext(csvFields, false);
                    }
                } catch (NullPointerException e){
//                    System.out.println(fields[2]);
//                    System.out.println(documentId);
//                    System.out.println(e);
                    continue;
                }
//                if (fields.get(0).equals(":Entity_72122c7c_4a5f_4504_8bb7_b7d0f7fb2598")) {
//                    System.out.println(String.format("found relation containing %s", arg1Id));
//                    System.out.println(String.format("Relation field: %s", fields.get(1)));
//                    System.out.println(String.format("Inverse: %s", TACInverseMap.get(fields.get(1))));
//                }
                if (TACInverseMap.containsKey(documentFields.get(1))){
//                    if (fields.get(0).equals(":Entity_72122c7c_4a5f_4504_8bb7_b7d0f7fb2598")) {
//                        System.out.println(String.format("passed relation reverser flag"));
//                    }
                    documentFields = relationReverser(documentFields, TACInverseMap, (KBEntity) arg2.getTarget());
                    documentFields.toArray(csvFields);
                    writer.writeNext(csvFields, false);
                }
            }
        }
    }

    protected int getIndexOfCanonicalMention(KBTextProvenance[] stringProvenances, KBPredicateArgument arg) {
        String canonicalString;
        if (arg instanceof KBThing)
            canonicalString = "\"" + ((KBThing) arg).getCanonicalString() + "\"";
        else
            return 0;
        int count = 0;
        for (KBTextProvenance prov : stringProvenances) {
            //System.out.println("Comparing " + canonicalString + " to " + normalizeJustificationText(prov.getValue()));
            if (canonicalString.equals(normalizeJustificationText(prov.getValue())))
                return count;
            count += 1;
        }

        return 0;
    }

    protected  List<String> stringMentionNodeMaker(KBTextProvenance provenance, String stringID, String justification, List<String> fields){
        List<String> newFields = new ArrayList<>();
        newFields.add(stringID);
        newFields.add("mention");
        newFields.add(normalizeJustificationText(provenance.getValue()));
        newFields.add(justification);
        String confidence = fields.get(fields.size()-1);
        newFields.add(confidence);
        return newFields;
    }


    protected  List<String> stringNormalizedNodeMaker(String normalizedDate, String stringID, String justification, List<String> fields){
        List<String> newFields = new ArrayList<>();
        newFields.add(stringID);
        newFields.add("normalized_mention");
        newFields.add("\"" + normalizedDate + "\"");
        newFields.add(justification);

        String confidence = fields.get(fields.size()-1);
        newFields.add(confidence);
        return newFields;
    }

    protected String getCanonicalDateStringForKBDate(KBDate kbDate) {
        String originalString = kbDate.getCanonicalString().replace('\n', ' ');
        return originalString.replaceAll("<TIMEX3.*?>(.*?)</TIMEX3>", "$1");
    }

    protected  List<String> stringTypeNodeMaker(String stringID){
        List<String> fields = new ArrayList<>();
        fields.add(stringID);
        fields.add("type");
        fields.add("STRING");
        return fields;
    }


    protected  List<String> stringCanonicalNodeMaker(KBTextProvenance provenance, String stringID, String justification, List<String> fields){
        List<String> localFields = new ArrayList<String>();
        localFields.add(stringID);
        localFields.add("canonical_mention");
        localFields.add(normalizeJustificationText(provenance.getValue()));

         try {
             localFields.add(justification);
         } catch (Exception e) {
             System.out.print(e.getMessage());
         }
        String confidence = Double.toString(Math.min(Double.parseDouble(fields.get(fields.size()-1)), 1.0));
        localFields.add(confidence);
        return localFields;
    }


    protected List<String> relationReverser(List<String> fields, HashMap<String, String> tacInverseMap, KBEntity entity){
        String entityType;
        entityType = getEntityType(entity);

        List<String> reversedFields = new ArrayList<String>();
        reversedFields.add(fields.get(2));

        String originalType = fields.get(1);
        String relationType = tacInverseMap.get(originalType);

        if (relationType.equals("org|gpe:employees_or_members")) {
            if (entityType.equals("ORG")) {
                relationType = "org:employees_or_members";
            } else {
                relationType = "gpe:employees_or_members";
            }
        } else if (relationType.equals("org:members")){
                if (entityType.equals("ORG")){
                    relationType = "org:members";
                } else {
                    relationType = "gpe:members";
                }
        } else if (relationType.equals("per|org|gpe:holds_shares_in")){
            if (entityType.equals("PER")){
                relationType = "per:holds_shares_in";
            } else if (entityType.equals("ORG")){
                relationType = "org:holds_shares_in";
            } else {
                relationType = "gpe:holds_shares_in";
            }
        } else if (relationType.equals("per|org|gpe:organizations_founded")){
            if (entityType.equals("PER")){
                relationType = "per:organizations_founded";
            } else if (entityType.equals("ORG")){
                relationType = "org:organizations_founded";
            } else {
                relationType = "gpe:organizations_founded";
            }
        }else if (relationType.equals("org:member_of|gpe:member_of")) {
            if (entityType.equals("ORG")) {
                relationType = "org:member_of";
            } else if (entityType.equals("GPE")) {
                relationType = "gpe:member_of";
            }
        } else if (relationType.equals("org:subsidiaries|gpe:subsidiaries")) {
            if (entityType.equals("ORG")) {
                relationType = "org:subsidiaries";
            } else if (entityType.equals("GPE")) {
                relationType = "gpe:subsidiaries";
            }
        }

        reversedFields.add(relationType);
        reversedFields.add(fields.get(0));
        for (int i = 3; i < fields.size(); i++) {
            reversedFields.add(fields.get(i));
        }
        return reversedFields;
    }

    /**
     * Entry point for {@link TacFormatConverter} that loads configuration from
     * command line or default configuration, connects to knowledge base, and generates reports.
     */
    public static void main(String[] args) throws Exception {
        TacFormatConverter reportGenerator = null;
        try {
            // Define command line options
            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            Options options = new Options();
            options.addOption("r", "resume", false, "do no requery objects that already exist in output");
            options.addOption(Option.builder("p")
                    .longOpt("params")
                    .argName("kb parameter file")
                    .hasArg()
                    .desc("optional path to KB parameter file")
                    .build());
            options.addOption(Option.builder("s")
                    .longOpt("sample")
                    .argName("N objects")
                    .hasArg()
                    .desc("number of objects to randomly sample from the KB")
                    .build());
            options.addOption(Option.builder("e")
                    .longOpt("edl_only")
                    .desc("only produce output with entities for EDL use")
                    .build());
            options.addOption(Option.builder("o")
                    .longOpt("old")
                    .desc("if specified, use old 2016 version")
                    .build());
            options.addOption(Option.builder("v")
                    .longOpt("events")
                    .desc("if specified, include events in output")
                    .build());
            options.addOption(Option.builder("n")
                    .longOpt("namelists")
                    .desc("if specified, use SERIF namelists")
                    .build());
            options.addOption(Option.builder("c")
                .longOpt("nil_clustering")
                .desc("if specified, do nil clustering")
                .build());

            // Read parameters
            CommandLine line = parser.parse(options, args);
            if (line.getArgs().length != 2) {
                formatter.printHelp("TacFormatConverter <output dir> <run name>", options);
                System.exit(1);
            }
            File outputDirectory = new File(line.getArgs()[0]);
            String systemName = line.getArgs()[1];
            final KBParameters kbParameters;
            if (line.hasOption("params")) {
                kbParameters = new KBParameters(line.getOptionValue("params"));
            } else {
                kbParameters = new KBParameters();
            }
            boolean isEdlOnly = line.hasOption("edl_only");
            boolean use2016version = line.hasOption("old");
            boolean resumeRun = line.hasOption("resume");
            boolean includeEvents = line.hasOption("events");
            boolean serifNameLists = line.hasOption("namelists");
            boolean doNilClustering = line.hasOption("nil_clustering");
            int sampleSize = 0;

            // Initialize connections
            reportGenerator = new TacFormatConverter(kbParameters, outputDirectory, systemName, resumeRun, sampleSize, isEdlOnly, use2016version, includeEvents, doNilClustering, serifNameLists);

            // Generate reports
            reportGenerator.generate();
        } catch (Exception e) {
            System.err.println("Could not generate reports");
            e.printStackTrace(System.err);
        } finally {
            if (reportGenerator != null) {
                reportGenerator.close();
            }
        }
    }

    public static class KBSummary{
        public enum ArtifactType{BELIEF,SENTIMENT,RELATION,EVENT,ENTITY};

        public static class TypeWithOptionalArgTypes{
            final String type;
            final Optional<ImmutableSet<String>> argTypes;

            private TypeWithOptionalArgTypes(String type,Optional<ImmutableSet<String>> argTypes){
                this.type = type;
                this.argTypes = argTypes;

            }
            static TypeWithOptionalArgTypes create(String type,Optional<ImmutableSet<String>> argTypes){
                checkNotNull(type);
                return new TypeWithOptionalArgTypes(type,argTypes);
            }

            public String type(){
                return type;
            }

            public Optional<ImmutableSet<String>> argTypes(){
                return argTypes;
            }

            @Override
            public boolean equals(Object that){
                if(that!=null && that instanceof TypeWithOptionalArgTypes &&
                        this.type.equals(((TypeWithOptionalArgTypes) that).type()) &&
                        this.argTypes.equals(((TypeWithOptionalArgTypes)that).argTypes())){
                    return true;
                }
                return false;
            }

            @Override
            public int hashCode(){
                return Objects.hashCode(this.type,this.argTypes);
            }
        }

        final ArtifactType artifactType;
        final int count;
        final int maxMentionCount;
        final int maxDistinctDocs;
        final float minConfidence;
        final float maxConfidence;
        final Optional<Integer> maxUniqueStrings; //for entities
        final Optional<Integer> minNumArguments; //for non-entities
        final Optional<Integer> maxNumArguments; //for non-entities
        final ImmutableSet<TypeWithOptionalArgTypes> types;

        private KBSummary(ArtifactType artifactType, int count, int maxMentionCount, int
                maxDistinctDocs, float minConfidence, float maxConfidence, Optional<Integer> maxUniqueStrings,
                          Optional<Integer> minNumArguments, Optional<Integer> maxNumArguments,
                          ImmutableSet<TypeWithOptionalArgTypes> types){
            this.artifactType=artifactType;
            this.count=count;
            this.maxMentionCount=maxMentionCount;
            this.maxDistinctDocs=maxDistinctDocs;
            this.minConfidence=minConfidence;
            this.maxConfidence=maxConfidence;
            this.maxUniqueStrings=maxUniqueStrings;
            this.minNumArguments=minNumArguments;
            this.maxNumArguments=maxNumArguments;
            this.types=types;

        }

        static KBSummary create(ArtifactType artifactType, int count, int maxMentionCount, int
                maxDistinctDocs, float minConfidence, float maxConfidence, Optional<Integer> maxUniqueStrings,
                                Optional<Integer> minNumArguments, Optional<Integer> maxNumArguments,
                                ImmutableSet<TypeWithOptionalArgTypes> types){
            checkNotNull(artifactType);
            return new KBSummary(artifactType, count, maxMentionCount, maxDistinctDocs, minConfidence,
                    maxConfidence, maxUniqueStrings, minNumArguments, maxNumArguments, types);
        }

        public ArtifactType artifactType(){
            return artifactType;
        }

        public int count(){
            return count;
        }

        public int maxMentionCount(){
            return maxMentionCount;
        }

        public int maxDistinctDocs(){
            return maxDistinctDocs;
        }

        public float minConfidence(){
            return minConfidence;
        }

        public float maxConfidence(){
            return maxConfidence;
        }

        public Optional<Integer> maxUniqueStrings(){
            return maxUniqueStrings;
        }

        public Optional<Integer> minNumArguments(){
            return minNumArguments;
        }

        public Optional<Integer> maxNumArguments(){
            return maxNumArguments;
        }

        public ImmutableSet<TypeWithOptionalArgTypes> types(){
            return types;
        }

    }




}
