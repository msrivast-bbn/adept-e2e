package adept.e2e.driver;

/**
 * Created by msrivast on 10/20/16.
 */
public final class E2eConstants {

  public static enum LANGUAGE {EN, ZH};
  public static enum ONTOLOGY {tac2012, rere, adept, custom};
  public static final String ALGORITHM_TYPE = "algorithm_type";
  public static final String ALGORITHM_NAME = "algorithm_name";
  public static final String ONTOLOGY_MAPPING_FILE_PATH = "ontology_mapping_file_path";
  public static final String REVERSE_ONTOLOGY_MAPPING_FILE_PATH =
      "reverse_ontology_mapping_file_path";
  public static final String ALGORITHM_TYPE_COREF = "coref_algorithm";
  public static final String ALGORITHM_TYPE_ENTITY_LINKING = "entity_linking_algorithm";
  public static final String ALGORITHM_TYPE_NIL_CLUSTERING =
      "nil_clustering_algorithm";
  public static final String ALGORITHM_TYPE_RELATION_EXTRACTION =
      "relation_extraction_algorithm";
  public static final String ALGORITHM_TYPE_EVENT_EXTRACTION = "event_extraction_algorithm";
  public static final String STAGE_NAME_DOCUMENT_DEDUPLICATION = "document_deduplication";
  public static final String STAGE_NAME_HLTCC_CREATION = "hltcc_creation";
  public static final String STAGE_NAME_NIL_CLUSTERING = "nil_clustering";
  public static final String STAGE_NAME_CHUNK_ALIGNMENT = "chunk_alignment";
  public static final String STAGE_NAME_COREF_DTRA_UPDATE = "coref_dtra_update";
  public static final String STAGE_NAME_TYPE_REASSIGNMENT = "entity_type_reassignment";
  public static final String STAGE_NAME_CONTAINER_MERGING = "master_container_creation";
  public static final String STAGE_NAME_ARTIFACT_EXTRACTION = "batch_level_artifact_extraction";
  public static final String STAGE_NAME_CBRNE_EXTRACTION = "cbrne_extraction";
  public static final String PROPERTY_DOCID = "DOCID";
  public static final String PROPERTY_EXCEPTION_TYPE = "Exception_Type";
  public static final String PROPERTY_EXCEPTION_TRACE = "Exception_Trace";
  public static final String PROPERTY_EXCEPTION_MESSAGE = "Exception_Message";
  public static final String PROPERTY_MODULE_NAME = "Class/Module";
  public static final String PROPERTY_TIME_TAKEN = "Time_Taken";

  public static final String CHECKPOINT_DIR_DOCUMENT_DEDUP =
      "document_deduplication_checkpoint";
  public static final String CHECKPOINT_DIR_HLTCC = "hltcc_creation_checkpoint";
  public static final String CHECKPOINT_DIR_NIL_CLUSTERING =
      "nil_clustering_checkpoint";
  public static final String CHECKPOINT_DIR_POST_NIL_CLUSTERING_ALGORITHMS =
      "post_nil_clustering_algos_checkpoint";
  public static final String CHECKPOINT_DIR_POST_COREF_DTRA_UPDATE = "post_coref_dtra_update_algos_checkpoint";
  public static final String CHECKPOINT_DIR_CHUNK_ALIGNMENT = "chunk_alignment_checkpoint";
  public static final String CHECKPOINT_DIR_ENTITY_TYPE_REASSIGNMENT =
      "entity_type_reassignment_checkpoint";
  public static final String CHECKPOINT_DIR_MASTER_CONTAINERS = "master_containers_checkpoint";
  public static final String CHECKPOINT_DIR_ARTIFACT_EXTRACTION = "artifact_extraction_checkpoint";
  public static final String CHECKPOINT_DIR_CBRNE_EXTRACTION = "cbrne_extraction_checkpoint";
  public static final String CHECKPOINT_DIR_ENTITY_UPLOAD = "batch_level_entity_upload_checkpoint";
  public static final String CHECKPOINT_DIR_NON_ENTITY_ARG_UPLOAD = "non_entity_argument_upload_checkpoint";
  public static final String CHECKPOINT_DIR_ARTIFACT_FILTERING = "artifact_drop_checkpoint";
  public static final String CHECKPOINT_DIR_RELATION_UPLOAD =
      "batch_level_relation_upload_checkpoint";
  public static final String CHECKPOINT_DIR_EVENT_UPLOAD = "batch_level_event_upload_checkpoint";
  public static final String CHECKPOINT_DIR_ENTITY_DEDUP = "entity_deduplication_checkpoint";
  public static final String CHECKPOINT_DIR_RELATION_DEDUP = "relation_deduplication_checkpoint";
  public static final String CHECKPOINT_DIR_EVENT_DEDUP = "event_deduplication_checkpoint";
  public static final String CHECKPOINT_DIR_TEXT_UPLOAD = "document_text_upload_checkpoint";

  public static final String CHECKPOINT_DIR_MULTILINGUAL_ENTITY_DEDUP = "multilingual_entity_dedup_checkpoint";
  public static final String CHECKPOINT_DIR_MULTILINGUAL_NON_ENTITY_ARG_DEDUP = "multilingual_non_entity_argument_union_checkpoint";
  public static final String CHECKPOINT_DIR_MULTILINGUAL_RELATION_DEDUP = "multilingual_relation_dedup_checkpoint";
  public static final String CHECKPOINT_DIR_MULTILINGUAL_EVENT_DEDUP = "multilingual_event_dedup_checkpoint";

  public static final String OUTPUT_DIR_NIL_CLUSTERING = "nil_clustering";
  public static final String OUTPUT_DIR_CHUNK_ALIGNMENT = "alignedChunksCSV";
  public static final String OUTPUT_DIR_POST_COREF_DTRA_UPDATE = "corefOutputPostDTRACBRNEUpdate";
  public static final String OUTPUT_DIR_TYPE_REASSIGNMENT = "entity_type_reassignment_details";
  public static final String OUTPUT_DIR_MASTER_CONTAINERS = "masterContainers";
  public static final String OUTPUT_DIR_ARTIFACT_FILTERING = "filteredArtifacts";
  public static final String OUTPUT_DIR_CBRNE_EXTRACTION = "masterContainersPostCBRNEExtraction";
  public static final String OUTPUT_DIR_ARTIFACT_EXTRACTION = "extractedArtifacts";
  public static final String OUTPUT_DIR_ENTITY_UPLOAD = "batch_level_uploaded_entities";
  public static final String OUTPUT_DIR_NON_ENTITY_ARG_UPLOAD = "final_non_entity_arguments";
  public static final String OUTPUT_DIR_RELATION_UPLOAD = "batch_level_uploaded_relations";
  public static final String OUTPUT_DIR_EVENT_UPLOAD = "batch_level_uploaded_events";
  public static final String OUTPUT_DIR_ENTITY_DEDUP = "final_entities";
  public static final String OUTPUT_DIR_RELATION_DEDUP = "final_relations";
  public static final String OUTPUT_DIR_EVENT_DEDUP = "final_events";

  public static final String OUTPUT_DIR_MULTILINGUAL_ENTITY_DEDUP = "multilingual_final_entities";
  public static final String OUTPUT_DIR_MULTILINGUAL_RELATION_DEDUP = "multilingual_final_relations";
  public static final String OUTPUT_DIR_MULTILINGUAL_EVENT_DEDUP = "multilingual_final_events";

}
