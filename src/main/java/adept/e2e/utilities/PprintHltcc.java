package adept.e2e.utilities;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import adept.common.Chunk;
import adept.common.ConversationElement;
import adept.common.Coreference;
import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.EntityMention;
import adept.common.GenericThing;
import adept.common.HltContentContainer;
import adept.common.KBID;
import adept.common.NumericValue;
import adept.common.Pair;
import adept.common.RelationMention;
import adept.common.TemporalValue;
import adept.e2e.driver.SerializerUtil;
import adept.io.Reader;
import adept.io.Writer;
import adept.serialization.XMLStringSerializer;

//import adept.io.Writer;
//import edu.stanford.nlp.StanfordCoreNlpProcessor;

public class PprintHltcc {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println(
          "Usage: pprint_hltcc <hltcc XMl file path or partitions directory> <pprint out file or directory path>");
      System.exit(1);
    }
    String inPath = args[0];
    String outPath = args[1];
    File inFile = new File(inPath);
    if (!inFile.exists()) {
      System.out.println("Input file/directory path does not exist");
      System.exit(1);
    }
    if (inFile.isFile()) {
      File outFile = new File(outPath);
      if (outFile.exists()) {
        Preconditions.checkArgument(outFile.isFile(),
            "Input path points to a file, but output path points to an existing directory");
      }
      pprintHltCC(inPath, outPath);
    } else {
      File outDir = new File(outPath);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      System.out.println("Reading part files from directory: " + inPath);
      File[] partFiles = (new File(inPath)).listFiles();
      int totalSize = partFiles.length;
      int count = 0;
      for (File partFile : partFiles) {
        count++;
        if (!partFile.getName().contains("part-") || partFile.getName()
            .contains(".")) {
          continue;
        }
        System.out.println(
            "Getting HltCCs from part file: " + partFile.getName() + "..."
                + count + "/" + totalSize);
        List<Pair<String, Optional<HltContentContainer>>> hltCCS =
            getHltCCsFromPartFile(partFile);
        for (Pair<String, Optional<HltContentContainer>> pair : hltCCS) {
          String pprintHltCC = getPprintOutput(pair.getR());
          System.out.println("\t\tpretty-printing HltCC for: " + pair.getL());
          String filePath = outPath + "/" + pair.getL();
          (new File(filePath)).createNewFile();
          Writer.getInstance().writeToFile(filePath, pprintHltCC);
        }
      }
    }
    System.out.println("Finished pretty-printing HltCCs in " + outPath);
  }

  /**
   * Gets a list of HltContentContainers from all the part files contained
   * in a directory. The part files need to have a "part-" prefix, which is
   * usually the case if these files are created by Spark.
   *
   * @param partFileDirPath Absolute file system path to the directory
   *                        containing the part files
   * @return List of HltContentContainers serialized in the part files
   * @throws IOException if the input directory path could not be found, or if
   * any of the input part files could not be read or deserialized
   */

  public static List<HltContentContainer> getHltCCsFromPartFileDir(String
      partFileDirPath) throws IOException{
    File dir = new File(partFileDirPath);
    if(!dir.exists()){
      throw new FileNotFoundException(partFileDirPath+ "does not exist");
    }
    File[] partFiles = dir.listFiles((File
        file) -> file.getName().startsWith("part-"));
    List<HltContentContainer> returnList = new ArrayList<>();
    for (File partFile : partFiles) {
      List<HltContentContainer> validHltCCsFromFile = (FluentIterable.from
          (getHltCCsFromPartFile(partFile)
      ).filter(
          ( (Pair<String,Optional<HltContentContainer>> result )-> result.getR()
              .isPresent())).transform((Pair<String,
          Optional<HltContentContainer>> pair )-> pair.getR().get()).toList());
      returnList.addAll(validHltCCsFromFile);
    }
    return returnList;
  }

  /**
   * Gets a list of HltContentContainers from a single part file.
   *
   * @param partFilePath Absolute file system path to the part file
   * @return List of HltContentContainers serialized in the part file
   * @throws IOException if the part file could not be read or deserialized
   */

  public static List<HltContentContainer> getHltCCsFromPartFile(String
      partFilePath) throws IOException{
    File partFile = new File(partFilePath);
    return (FluentIterable.from
        (getHltCCsFromPartFile(partFile)
        ).filter(
        ( (Pair<String,Optional<HltContentContainer>> result )-> result.getR()
            .isPresent())).transform((Pair<String,
        Optional<HltContentContainer>> pair )-> pair.getR().get()).toList());
  }

  private static List<Pair<String, Optional<HltContentContainer>>>
  getHltCCsFromPartFile(File partFile) throws IOException {

    List<Pair<String, Optional<HltContentContainer>>> fileNameAndHltCCList =
        new ArrayList<>();

    List<String> content = new ArrayList<>();
    Reader.getInstance().readFileIntoLines(partFile.getAbsolutePath(), content);
    StringBuilder hltCCString = new StringBuilder();
    String originalFileName = null;
    boolean hltCCEnded = false;
    boolean isOptionalObject = false;

    for (String line : content) {
      if (line.startsWith("(hdfs") || line.startsWith("(file")) {
        originalFileName = line.substring(0, line.indexOf(","));
        originalFileName =
            originalFileName.substring(originalFileName.lastIndexOf("/") + 1);
        line = line.substring(line.indexOf(",") + 1);
        if (line.endsWith("Present>")) {
          isOptionalObject = true;
        } else if (line.endsWith("Absent/>)") || line.endsWith("null/>)")) {
          line = line.substring(0, line.length() - 1);
          hltCCEnded = true;
        }
      } else if (line.endsWith("Present>)") || line
          .endsWith("HltContentContainer>)")) {
        line = line.substring(0, line.length() - 1);
        hltCCEnded = true;
      }
      hltCCString.append(line);
      if (hltCCEnded) {
        String hltCCContent = removeShadedReferences(hltCCString.toString());
        Optional<HltContentContainer> optionalHltCC = Optional.absent();
        if (hltCCContent.endsWith("Absent/>") || hltCCContent
            .endsWith("null/>")) {
          //do nothing
        } else if (isOptionalObject) {
          optionalHltCC =
              SerializerUtil.deserialize(hltCCContent, Optional.class);
        } else {
          optionalHltCC = Optional.of(SerializerUtil
              .deserialize(hltCCContent, HltContentContainer.class));
        }
        fileNameAndHltCCList.add(new Pair<>(originalFileName, optionalHltCC));
        hltCCEnded = false;
        hltCCString = new StringBuilder();
        originalFileName = null;
        isOptionalObject = false;
      }
    }
    return fileNameAndHltCCList;
  }

  public static void pprintHltCC(String inFilePath, String outFilePath)
      throws IOException {

    // Loading the HLTCC from a file

    //HLTConcentContainer hltcc = Reader.getInstance().readFromFile(inFilePath, new XMLStringSerializer().deserializeFromString(hltcc, HltContentContainer));
    XMLStringSerializer DESERIALIZER = new XMLStringSerializer();
    inFilePath = (new File(inFilePath)).getAbsolutePath();
    outFilePath = (new File(outFilePath)).getAbsolutePath();
    String fileString = Reader.getInstance().readFileIntoString(inFilePath);
    fileString = removeShadedReferences(fileString);
    HltContentContainer hltcc = null;
    if (fileString.indexOf("<adept.common.HltContentContainer>") == 0) {
      hltcc = (HltContentContainer) DESERIALIZER
          .deserializeFromString(fileString, HltContentContainer.class);
    } else if (fileString.indexOf("<com.google.common.base.Present>") == 0) {
      com.google.common.base.Optional optObj =
          (com.google.common.base.Optional) DESERIALIZER
              .deserializeFromString(fileString,
                  com.google.common.base.Optional.class);

      if (optObj.isPresent()) {
        hltcc = (HltContentContainer) optObj.get();
      } else {
        System.err.println("Found Optional element not containing an HLTCC.");
        System.exit(-1);
      }
    } else {
      System.err.println("Unknown type found in part file");
      System.exit(-1);
    }

    PrintWriter writer = new PrintWriter(outFilePath, "UTF-8");
    writer.write(getPprintOutput(hltcc));
    writer.flush();
    writer.close();
  }

  private static String removeShadedReferences(String fileString) {
    fileString =
        fileString.replaceAll("shadedgoogle\\.com\\.google", "com.google");
    fileString = fileString
        .replaceAll("shadedfasterxml\\.com\\.fasterxml", "com.fasterxml");
    return fileString;
  }

  public static String getPprintOutput(Optional<HltContentContainer> hltcc) {
    return getPprintOutput(hltcc.orNull());
  }

  public static String getPprintOutput(HltContentContainer hltcc) {
    if (hltcc == null) {
      return "null";
    }
    List<String> pprintOutput = new ArrayList();
    pprintOutput.add(String.format("DocumentId: %s\n", hltcc.getDocumentId()));
    pprintOutput.add("Starting on coreferences...");
    Set<EntityMention> resolvedMentionsSet = new HashSet<>();
    // (StanfordCoreNlp outputs a single coreference object; other algorithms may output multiple)
    for (int i = 0; i < hltcc.getCoreferences().size(); i++) {
      pprintOutput.add("");
      pprintOutput.add(String.format("hltcc->coreferences->[%d]\n", i));
      Coreference c = hltcc.getCoreferences().get(i);
      for (Entity e : c.getEntities()) {
        pprintOutput.add(String.format(
            "Coreferential entity has id %d, type %s, and value '%s' [%s]\n",
            e.getEntityId(),
            e.getEntityType().getType(),
            e.getValue() != null ? e.getValue().replace("\n", " ") : null,
            e.getAlgorithmName()
        ));
      }

      if (c.getResolvedMentions() == null) {
        pprintOutput.add("No ResolvedMentions found");
      } else {
        for (EntityMention em : c.getResolvedMentions()) {
          pprintOutput.add(String
              .format("Mention '%s' (entity %d) has char offset [%d,%d] [%s]\n",
                  em.getValue().replace("\n", " "),
                  em.getBestEntityId(),
                  em.getCharOffset().getBegin(),
                  em.getCharOffset().getEnd(),
                  em.getAlgorithmName()
              ));
          //wikifier adds KBIDs to mention-attributes; print them if they're present
          for (Map.Entry<String, String> attr : em.getAttributes().entrySet()) {
            pprintOutput.add(
                String.format("\t %s:%s\n", attr.getKey(), attr.getValue()));
          }
          resolvedMentionsSet.add(em);
        }
      }
    }
    pprintOutput.add("");
    Set<EntityMention> unResolvedMentionsSet = new HashSet<>();
    if (hltcc.getEntityMentions() != null) {
      unResolvedMentionsSet =
          Sets.difference(new HashSet<>(hltcc.getEntityMentions()),
              resolvedMentionsSet);
    }
    if (unResolvedMentionsSet.isEmpty()) {
      pprintOutput.add("No unresolved mentions found.");
    } else {
      pprintOutput.add("hltcc->unresolvedMentions");
      for (EntityMention em : unResolvedMentionsSet) {
        pprintOutput.add(String
            .format("Mention '%s' (entity %d) has char offset [%d,%d] [%s]\n",
                em.getValue().replace("\n", " "),
                em.getBestEntityId(),
                em.getCharOffset().getBegin(),
                em.getCharOffset().getEnd(),
                em.getAlgorithmName()
            ));
      }
    }
    if (hltcc.getNamedEntities() == null) {
      pprintOutput.add("No named entities found.");
    } else {
      pprintOutput.add("hltcc->namedEntities");
      for (EntityMention em : hltcc.getNamedEntities()) {
        String mentType = "unset";
        if (em.getMentionType() != null) {
          mentType = em.getMentionType().getType();
        }
        pprintOutput.add(String.format(
            "Entity mention '%s' (entity %d) has char offset [%d,%d), mention type %s, and entity type %s [%s]\n",
            em.getValue().replace("\n", " "),
            em.getBestEntityId(),
            em.getCharOffset().getBegin(),
            em.getCharOffset().getEnd(),
            //em.getTokenOffset().getBegin(),
            //em.getTokenOffset().getEnd(),
            mentType,
            em.getEntityType().getType(),
            em.getAlgorithmName()
        ));
      }
    }

    pprintOutput.add("");
    if (hltcc.getDocumentRelations() == null || hltcc.getDocumentRelations()
        .isEmpty()) {
      pprintOutput.add("No relations found.");
    } else {
      pprintOutput.add("hltcc->documentRelations");
      for (DocumentRelation dr : hltcc.getDocumentRelations()) {
        Set<DocumentRelationArgument> arguments = dr.getArguments();
        pprintOutput.add(String
            .format("Document relation has type %s and %d arguments [%s]\n",
                dr.getRelationType().getType(),
                arguments.size(),
                dr.getAlgorithmName()
            ));
        for (DocumentRelationArgument dra : arguments) {
          String role = dra.getRole().getType();
          DocumentRelationArgument.Filler filler = dra.getFiller();
          if (filler.asEntity().isPresent()) {
            Entity argument = filler.asEntity().get();
            pprintOutput.add(String.format(
                "  Discovered entity-type argument with role '%s', entity id %d and entity value '%s' [%s]\n",
                role,
                argument.getEntityId(),
                argument.getValue().replace("\n", " "),
                argument.getAlgorithmName()
            ));
          }
          if (filler.asTemporalValue().isPresent()) {
            TemporalValue argument = filler.asTemporalValue().get();
            pprintOutput.add(String.format(
                "  Discovered temporal-value-type argument with role '%s', string value '%s'\n",
                role,
                argument.asString()
            ));
          }
          if (filler.asNumericValue().isPresent()) {
            NumericValue argument = filler.asNumericValue().get();
            pprintOutput.add(String.format(
                "  Discovered numeric-value-type argument with role '%s', number value %f\n",
                role,
                argument.asNumber().floatValue()
            ));
          }
          if (filler.asGenericThing().isPresent()) {
            GenericThing argument = filler.asGenericThing().get();
            pprintOutput.add(String.format(
                "  Discovered generic-thing-type argument with role '%s', generic thing type '%s', generic thing value '%s'\n",
                role,
                argument.getType().getType(),
                argument.getValue()
            ));
          }
          if (dra.getProvenances() != null) {
            for (RelationMention.Filler argProvenance : dra.getProvenances()) {
              if (argProvenance.asChunk().isPresent()) {
                Chunk chunk = argProvenance.asChunk().get();
                pprintOutput.add(String.format(
                    "    arg-provenance found: %s [%d,%d] [algorithmName:%s]",
                    chunk.getValue(), chunk.getCharOffset().getBegin(),
                    chunk.getCharOffset().getEnd(), chunk.getAlgorithmName()));
              }
            }
          }
        }
      }
    }

    pprintOutput.add("");
    if (hltcc.getDocumentEvents() == null || hltcc.getDocumentEvents()
        .isEmpty()) {
      pprintOutput.add("No events found.");
    } else {
      pprintOutput.add("hltcc->documentEvents");
      for (DocumentEvent de : hltcc.getDocumentEvents()) {
        Set<DocumentEventArgument> arguments = de.getArguments();
        pprintOutput.add(
            String.format("Document event has type %s and %d arguments [%s]\n",
                de.getEventType().getType(),
                arguments.size(),
                de.getAlgorithmName()
            ));
        for (DocumentEventArgument dea : arguments) {
          String role = dea.getRole().getType();
          String argumentEventType = dea.getEventType().getType();
          DocumentEventArgument.Filler filler = dea.getFiller();
          if (filler.asEntity().isPresent()) {
            Entity argument = filler.asEntity().get();
            pprintOutput.add(String.format(
                "  Entity-type arg with role '%s', entity id %d and entity value '%s' [%s]\n",
                role,
                argument.getEntityId(),
                argument.getValue().replace("\n", " "),
                argument.getAlgorithmName()
            ));
          }
          if (filler.asTemporalValue().isPresent()) {
            TemporalValue argument = filler.asTemporalValue().get();
            pprintOutput.add(String.format(
                "  Temporal-value-type argwith role '%s', event type '%s', string value '%s'\n",
                role,
                argumentEventType,
                argument.asString()
            ));
          }
          if (filler.asGenericThing().isPresent()) {
            GenericThing argument = filler.asGenericThing().get();
            pprintOutput.add(String.format(
                "  Generic-thing-type arg with role '%s', event type '%s', generic thing type '%s', generic thing value '%s'\n",
                role,
                argumentEventType,
                argument.getType().getType(),
                argument.getValue()
            ));
          }
          if (dea.getProvenances() != null) {
            for (DocumentEventArgument.Provenance argProvenance : dea
                .getProvenances()) {
              Chunk chunk = argProvenance.getEventMentionArgument().getFiller();
              pprintOutput.add(String.format(
                  "    arg-provenance found: %s [%d,%d] [algorithmName:%s]",
                  chunk.getValue(), chunk.getCharOffset().getBegin(),
                  chunk.getCharOffset().getEnd(), chunk.getAlgorithmName()));
            }
          }
        }
      }
    }
    pprintOutput.add("");
    if (hltcc.getKBEntityMapForDocEntities() != null && !hltcc
        .getKBEntityMapForDocEntities().isEmpty()) {
      pprintOutput.add("hltcc->kbEntityMapForDocEntities");
      for (Map.Entry<Entity, Map<KBID, Float>> entry : hltcc
          .getKBEntityMapForDocEntities().entrySet()) {
        Entity entity = entry.getKey();
        pprintOutput.add(String
            .format("Entity (id=%s) '%s' has following KBIDs:\n",
                entity.getEntityId(),
                entity.getValue()));
        for (Map.Entry<KBID, Float> kbEntry : entry.getValue().entrySet()) {
          pprintOutput.add(String
              .format("\t%s  confidence=%f\n", kbEntry.getKey().getObjectID(),
                  kbEntry.getValue().floatValue()));
        }
      }
    } else {
      pprintOutput.add("No documentEntityToKBentityMap found");
    }
    pprintOutput.add("");
    if (hltcc.getConversationElements() == null || hltcc
        .getConversationElements().isEmpty()) {
      pprintOutput.add("No conversation elements found");
    } else {
      pprintOutput.add("hltcc->conversationElements");
      for (ConversationElement ce : hltcc.getConversationElements()) {
        pprintOutput.add(String.format(
            "\n>>> Conversation element has author id '%s', authoredTime '%s', char offset [%d, %d), token offset [%d, %d], and text value:\n'%s'\n",
            ce.getAuthorId(),
            ce.getAuthoredTime(),
            ce.getMessageChunk().getCharOffset().getBegin(),
            ce.getMessageChunk().getCharOffset().getEnd(),
            ce.getMessageChunk().getTokenOffset().getBegin(),
            ce.getMessageChunk().getTokenOffset().getEnd(),
            ce.getMessageChunk().getValue()
        ));
      }

      //String outFilePath = "demo-parse-hltcc-hltcontentcontainer-output.xml";
      //Writer.getInstance().writeToFile(outFilePath, new XMLStringSerializer().serializeToString(hltcc));
    }
    return Joiner.on("\n").join(pprintOutput);
  }
}
