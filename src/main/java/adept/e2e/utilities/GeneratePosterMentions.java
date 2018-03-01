package adept.e2e.utilities;

import adept.common.ID;
import adept.common.Pair;
import adept.io.Reader;
import adept.kbapi.KBConfigurationException;
import adept.kbapi.KBQueryException;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GeneratePosterMentions {
    private static final String posterAttributeFormat = " author=\"%s\"";
    private static final String idAttributeFormat = " id=\"%s\"";
    private final String tacFormatConverterFile;
    private final String[] inputDirectories;
    private final String outputFile;

    private static int posterNamesEscaped = 0;
    private static int stillErroneousPosterNames = 0;
    
    public GeneratePosterMentions(String tacFormatConverterFile, String outputFile,
            String ... inputDirectories) throws InvalidPropertiesFormatException, IOException, KBConfigurationException {
        this.tacFormatConverterFile = tacFormatConverterFile;
        this.outputFile = outputFile;
        this.inputDirectories = inputDirectories;
    }
    
    public void generate() throws IOException, ParserConfigurationException, KBQueryException {
        System.out.println("Input KB file: "+tacFormatConverterFile);
        System.out.println("Output KB file: "+outputFile);
        Multimap<String,Poster> posters = HashMultimap.create();
        for(String inputDirectory : inputDirectories){
            System.out.println("Getting poster-names from directory "+inputDirectory);
            getPosters(inputDirectory,posters);
        }
        System.out.println("Extracted a total of "+posters.values().size()+" poster-mentions");
        System.out.println("Reading input KB file...");
        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(tacFormatConverterFile), "UTF-8"), 
                '\t', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);
        Set<Poster> existingPosters = new HashSet<>();
        List<String[]> newKBLines = new ArrayList<>();
        String [] currentLine;
        String currentEntityType = "";
        System.out.println("Iterating over KB file lines...");
        int totalLinesRead = 0;
        while ((currentLine = reader.readNext()) != null) {
            if(currentLine.length==4
                    && currentLine[1].equals("type") && currentLine[3].equals("")){
                currentLine = new String[]{currentLine[0],currentLine[1],currentLine[2]};
            }
            String[] columns = currentLine;
            newKBLines.add(currentLine);
            totalLinesRead++;
            if (columns.length == 3 && columns[1].equals("type")) {
                currentEntityType = columns[2];
            } else if (columns.length > 2
                    && currentEntityType.equals("PER")
                    && columns[1].equals("mention")) {
                String entityMention = columns[2];
                String mentionProvenance = columns[3];
                String docId = mentionProvenance.split(":")[0];
                for (Poster poster : posters.get(docId)) {
                    if (existingPosters.contains(poster)) {
                        continue;
                    }
                    if (entityMention.equals("\"" + poster.getName() + "\"")) {
                        for (Poster.Mention posterMention : poster.getMentions()) {
                            List<String> mentionLine = new ArrayList<>();
                            mentionLine.add(columns[0]);    //entity Id
                            mentionLine.add("mention");
                            mentionLine.add('\"' + poster.getName() + '\"');
                            String provenance = String.format("%s:%d-%d",
                                    poster.getDocumentId(),
                                    posterMention.getBeginOffset(),
                                    posterMention.getEndOffset() - 1);
                            if(provenance.equals(mentionProvenance)){
                                continue;//avoid adding a duplicate mention
                            }
                            mentionLine.add(provenance);
                            mentionLine.add("1.0");
                            String[] csvFields = new String[mentionLine.size()];
                            mentionLine.toArray(csvFields);
                            newKBLines.add(csvFields);
                        }
                        existingPosters.add(poster);
                    }
                }
            }
            if(totalLinesRead%100000==0){
                System.out.println("...lines read so far: "+totalLinesRead);
            }
        }//all the line in input KB file have been processed and added to newKBLines
        System.out.println("Added poster-mentions to "+existingPosters.size()+" existing entities...");
        List<String[]> newPosterEntityLines = new ArrayList<>();
        List<Poster> newPosters = FluentIterable.from(posters.values()).filter(new Predicate<Poster>() {
            @Override
            public boolean apply(@Nullable Poster poster) {
                return !existingPosters.contains(poster);
            }
        }).toList();
        System.out.println("Appending remaining "+newPosters.size()+" poster-mentions...");
        for(Poster poster : newPosters) {
                // Create new entity and add as mention
                String newEntityId = String.format(":Entity_%s", new ID().getIdString().replace('-', '_'));
                
                List<String> typeLine = new ArrayList<>();
                typeLine.add(newEntityId);
                typeLine.add("type");
                typeLine.add("PER");
                String[] csvFields = new String[typeLine.size()];
                typeLine.toArray(csvFields);
                newPosterEntityLines.add(csvFields);
                for (int i = 0; i < poster.getMentions().size(); i++) {
                    // Default to making the first mention the canonical mention
                    if (i == 0) {
                        List<String> canonicalMentionLine = new ArrayList<>();
                        canonicalMentionLine.add(newEntityId);
                        canonicalMentionLine.add("canonical_mention");
                        canonicalMentionLine.add('\"' + poster.getName() + '\"');
                        canonicalMentionLine.add(String.format("%s:%d-%d",
                                        poster.getDocumentId(),
                                        poster.getMentions().get(i).getBeginOffset(),
                                        poster.getMentions().get(i).getEndOffset() - 1));
                        canonicalMentionLine.add("1.0");
                        csvFields = new String[canonicalMentionLine.size()];
                        canonicalMentionLine.toArray(csvFields);
                        newPosterEntityLines.add(csvFields);
                    }
                    
                    List<String> mentionLine = new ArrayList<>();
                    mentionLine.add(newEntityId);
                    mentionLine.add("mention");
                    mentionLine.add('\"' + poster.getName() + '\"');
                    mentionLine.add(String.format("%s:%d-%d",
                                poster.getDocumentId(),
                                poster.getMentions().get(i).getBeginOffset(),
                                poster.getMentions().get(i).getEndOffset() - 1));
                    mentionLine.add("1.0");
                    csvFields = new String[mentionLine.size()];
                    mentionLine.toArray(csvFields);
                    newPosterEntityLines.add(csvFields);
                }
        }
        newKBLines.addAll(newPosterEntityLines);
        System.out.println("Writing new KB lines to output file....");
        // Write the final output
        CSVWriter writer = new CSVWriter(new FileWriter(new File(outputFile), false), '\t', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER);          
        for (String[] line : newKBLines) {
            writer.writeNext(line, false);
        }
        writer.close();
        System.out.println("Finished running GeneratePosterMentions! Summary:");
        System.out.println("#of poster-names extracted: "+ posters.values().size());
        System.out.println("#of erroneous posternames dropped: "+stillErroneousPosterNames);
        System.out.println("#of poster-mentions added to existing entities: "+existingPosters.size());
        System.out.println("#of new poster-mentions: "+newPosters.size());
        System.out.println("----------------------------------------------------");
    }
    
    public void getPosters(String inputFileDirectory, Multimap<String,Poster> postersMap) throws IOException, ParserConfigurationException {

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        
        try (Stream<Path> filePaths = Files.walk(Paths.get(inputFileDirectory))) {
            List<Path> paths = filePaths.filter(Files::isRegularFile).collect(Collectors.toList());
            int totalFilesToRead = paths.size();
            int filesRead = 0;
            for (Path filePath : paths) {
                String docId = null;
                filesRead++;
                try {
                    File file = new File(filePath.toString());                
                    Document doc = dBuilder.parse(file);
                    NodeList docNodes = doc.getElementsByTagName("doc");
                    for (int i = 0; i < docNodes.getLength(); i++) {
                        Node docNode = docNodes.item(i);
                        if (docNode.getNodeType() == Node.ELEMENT_NODE) {
                            Element docElement = (Element)docNode;
                            if (docElement.hasAttribute("id")) {
                                docId = docElement.getAttribute("id");
                                break;
                            }
                        }
                    }

                    NodeList postNodes = doc.getElementsByTagName("post");
                    List<Poster> postersInCurrentDoc = new ArrayList<>();
                    for (int i = 0; i < postNodes.getLength(); i++) {
                        Node postNode = postNodes.item(i);
                        if (postNode.getNodeType() == Node.ELEMENT_NODE) {
                            Element postElement = (Element)postNode;
                            if (postElement.hasAttribute("author")) {
                                String posterName = postElement.getAttribute("author");
                                if(posterName.isEmpty()){
                                    continue;
                                }
                                String postId = postElement.getAttribute("id");
                                Pair<Integer, Integer> offsets = null;
                                try {
                                    offsets = getOffsetsForPosterName(posterName, postId, file);
                                }catch (BadPosterNameOrOffsetsException e){
                                    continue;
                                }
                                // If poster name already exists for document, just add additional mention
                                boolean doesPosterExist = false;
                                for (Poster poster : postersInCurrentDoc) {
                                    if (poster.getName().equals(posterName)) {
                                        poster.addMention(offsets.getL(), offsets.getR());
                                        doesPosterExist = true;
                                    }
                                }
                                if (!doesPosterExist) {
                                    Poster newPoster = new Poster(posterName, docId);
                                    newPoster.addMention(offsets.getL(), offsets.getR());
                                    postersInCurrentDoc.add(newPoster);
                                }
                            }
                        }
                    }
                    if(!postersInCurrentDoc.isEmpty()) {
                        postersMap.putAll(docId, postersInCurrentDoc);
                    }
                    if(postNodes.getLength()==0){
                        getAuthorFromSGMDocument(file,doc,postersMap);
                    }
                } catch (Exception e) {
//                    throw new RuntimeException("Unable to parse file " + filePath.toString(), e);
                    System.out.println("Exeption caught "+e.getMessage()+" for docId: "+docId+"...ignoring...");
                }
                if(filesRead%1000==0){
                    System.out.println("Files read: "+filesRead+"/"+totalFilesToRead);
                }
            }
        }
    }

    private void getAuthorFromSGMDocument(File file, Document doc, Multimap<String,Poster> postersMap) throws Exception{
        List<Poster> posters = new ArrayList<>();
        String docId = null;
        Node docNode = doc.getElementsByTagName("DOC").item(0);
        if (docNode != null && docNode instanceof Element && ((Element) docNode)
               .hasAttribute("id")) {
            docId = ((Element) docNode).getAttribute("id");
        }else{
            return;
        }

        Node authorNode = doc.getElementsByTagName("AUTHOR").item(0);
        if (authorNode == null) {
            authorNode = doc.getElementsByTagName("POSTER").item(0);
        }
        if (authorNode != null && authorNode.getNodeType() == Node.ELEMENT_NODE) {
            String authorNameText = authorNode.getTextContent().replace("\n", "").trim();
            if(authorNameText.isEmpty()){
                return;
            }
            String[] authorNames = authorNameText.split(",");
            for(String authorName : authorNames){
                authorName = authorName.trim();
                try {
                    Pair<Integer, Integer> offsets = null;
                    try{
                        offsets = getOffsetsForAuthorName(authorName, file);
                    }catch(BadPosterNameOrOffsetsException e){
                        continue;
                    }
                    Poster author = new Poster(authorName, docId);
                    author.addMention(offsets.getL(), offsets.getR());
                    posters.add(author);
                }catch(Exception e){
                    System.out.println("Failed getting authorname for: "+docId+"...ignoring...");
                }
            }
        }
        postersMap.putAll(docId,posters);
    }

    private Pair<Integer, Integer> getOffsetsForAuthorName(String authorName, File file) throws IOException, BadPosterNameOrOffsetsException {
        Pair<Integer, Integer> offsets = new Pair<>(-1, -1);

        String rawDocumentValue = Reader.readRawFile(file.getAbsolutePath());
        rawDocumentValue = rawDocumentValue.substring(Reader.findXMLTag(rawDocumentValue, "DOC"));
        String authorNameUnescaped = authorName.replace("&","&amp;").replace("\"","&quot;").replace("'","&apos;").replace("<","&lt;").replace(">","&gt;");

        boolean authorBegin = false;
        boolean authorEnd = false;
        boolean lineContainsAuthorTag = false;
        String authorElementText = "";

        for (String line : Files.readAllLines(file.toPath())) {
            // Ensure the post id as well as author match
            if (line.contains("<AUTHOR>") || line.contains("<POSTER>")) {
                authorBegin = true;
                lineContainsAuthorTag = true;
                String authorString = "<AUTHOR>";
                if(line.contains("<POSTER>")) {
                    authorString = "<POSTER>";
                }
                authorElementText = line.substring(line.indexOf(authorString)+authorString.length());
                if(authorElementText.isEmpty()){
                    authorElementText+="\n";
                }
                line = authorElementText;
            }
            if(line.contains("</AUTHOR>")||line.contains("</POSTER>")) {
                authorEnd = true;
                authorBegin = false;
                String authorString = "</AUTHOR>";
                if(line.contains("</POSTER>")) {
                    authorString = "</POSTER>";
                }
                String lineWithoutAuthorString = line.substring(0,line.indexOf(authorString));
                if(!authorElementText.contains(authorString)) {
                  authorElementText += lineWithoutAuthorString;
                }else{
                    authorElementText = lineWithoutAuthorString;
                }
            }
            if(authorEnd) {
                if(!authorElementText.contains(authorName)){
                    authorName = authorNameUnescaped;
                    posterNamesEscaped++;
                }
                int beginOffset = rawDocumentValue.indexOf(authorElementText)+authorElementText.indexOf(authorName);

                int endOffset = beginOffset + authorName.length();
                offsets = new Pair<>(beginOffset, endOffset);
                break;
            }else if (authorBegin&&!lineContainsAuthorTag){
                authorElementText+=(line+"\n");
            }
            lineContainsAuthorTag = false;
        }
        if(offsets.getL()>offsets.getR()|| offsets.getL()<0 || offsets.getR()<0 || authorName.isEmpty()){
            System.out.println("Bad offsets or postername:  "+authorName+".."+offsets.getL()+","+offsets.getR());
            stillErroneousPosterNames++;
            throw new BadPosterNameOrOffsetsException("Bad offsets or postername:  "+authorName+".."+offsets.getL()+","+offsets.getR());
        }
        return offsets;
    }
    
    private Pair<Integer, Integer> getOffsetsForPosterName(String posterName, String postId, File file) throws IOException,
    BadPosterNameOrOffsetsException{
        Pair<Integer, Integer> offsets = new Pair<>(-1, -1);

        String posterNameUnescaped = posterName.replace("&","&amp;").replace("\"","&quot;").replace("'","&apos;").replace("<","&lt;").replace(">","&gt;");

        String rawDocumentValue = Reader.readRawFile(file.getAbsolutePath());
        rawDocumentValue = rawDocumentValue.substring(Reader.findXMLTag(rawDocumentValue, "doc"));

        for (String line : Files.readAllLines(file.toPath())) {
            // Ensure the post id as well as author match
            if (!posterNameUnescaped.equals(posterName)&&line.contains(String.format(posterAttributeFormat, posterNameUnescaped))&&
                    line.contains(String.format(idAttributeFormat, postId))) {
                posterName = posterNameUnescaped;
                posterNamesEscaped++;
            }
            if (line.contains(String.format(posterAttributeFormat, posterName))&&
                    line.contains(String.format(idAttributeFormat, postId))) {
                int beginOffset = rawDocumentValue.indexOf(line) + line.indexOf(posterName);                                
                int endOffset = beginOffset + posterName.length();                
                offsets = new Pair<>(beginOffset, endOffset);
                break;
            }
        }
        if(offsets.getL()>offsets.getR()|| offsets.getL()<0 || offsets.getR()<0 || posterName.isEmpty()){
            stillErroneousPosterNames++;
            System.out.println("Bad offsets or postername:  "+posterName+".."+offsets.getL()+","+offsets.getR());
            throw new BadPosterNameOrOffsetsException("Bad offsets or postername:  "+posterName+".."+offsets.getL()+","+offsets.getR());
        }
        return offsets;
    }
    
    public static void main(String[] args) throws Exception {
        // TODO: check for correct args
        // 1. TacFormatConverter output
        // 2. File directory
        // 3. KBParameters.xml
        // 4. Output file
        String tacFormatConverterFile = args[0];
        String outputFile = args[1];
        String[] inputDirectories = Arrays.copyOfRange(args,2,args.length);
        
        GeneratePosterMentions generator = new GeneratePosterMentions(tacFormatConverterFile,
                outputFile, inputDirectories);
        
        generator.generate();
    }
    
    protected class Poster {
        final private String name;
        final private String documentId;
        private List<Mention> mentions;
        
        public String getName() {
            return name;
        }
        
        public String getDocumentId() {
            return documentId;
        }
        
        public List<Mention> getMentions() {
            return mentions;
        }
        
        public void addMention(int beginOffset, int endOffset) {
            mentions.add(new Mention(beginOffset, endOffset));
        }

        public Poster(String name, String documentId) {
            name = name.replace("\""," ");
            this.name = name;
            this.documentId = documentId;
            mentions = new ArrayList<>();
        }
        
        protected class Mention {
            final private int beginOffset;
            final private int endOffset; 
            
            public int getBeginOffset() {
                return beginOffset;
            }
            
            public int getEndOffset() {
                return endOffset;
            }
            
            public Mention(int beginOffset, int endOffset) {
                this.beginOffset = beginOffset;
                this.endOffset = endOffset;
            }
            @Override
            public String toString(){
                return this.beginOffset+":"+this.endOffset;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Mention mention = (Mention) o;

                if (beginOffset != mention.beginOffset) return false;
                return endOffset == mention.endOffset;
            }

            @Override
            public int hashCode() {
                int result = beginOffset;
                result = 31 * result + endOffset;
                return result;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Poster poster = (Poster) o;

            if (!documentId.equals(poster.documentId)) return false;
            return mentions.equals(poster.mentions);
        }

        @Override
        public int hashCode() {
            int result = documentId.hashCode();
            result = 31 * result + mentions.hashCode();
            return result;
        }
    }

    protected static class BadPosterNameOrOffsetsException extends Exception{
        protected BadPosterNameOrOffsetsException(){
            super();
        }
        protected BadPosterNameOrOffsetsException(String message){
            super(message);
        }
    }
}
