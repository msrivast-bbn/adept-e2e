package adept.e2e.artifactextraction.artifactfilters.additionalargtypefilter;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import adept.common.OntType;
import adept.e2e.artifactextraction.ExtractedArtifacts;
import adept.e2e.artifactextraction.artifactkeys.ArgumentKey;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;
import adept.e2e.artifactextraction.artifactkeys.EventKey;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentEvent;
import adept.e2e.artifactextraction.mergedartifacts.MergedDocumentRelation;
import adept.e2e.stageresult.BatchResultObject;

import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_MESSAGE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TRACE;
import static adept.e2e.driver.E2eConstants.PROPERTY_EXCEPTION_TYPE;
import static adept.e2e.driver.E2eConstants.PROPERTY_MODULE_NAME;
import static adept.e2e.driver.E2eConstants.PROPERTY_TIME_TAKEN;

public class EntityKeyToAdditionalTypesMapper implements
    FlatMapFunction<Iterator<BatchResultObject<ExtractedArtifacts>>,
        BatchResultObject<ImmutableMultimap<EntityKey,OntType>>> {

  private static final long serialVersionUID = 1L;
  private static Logger log = LoggerFactory.getLogger(EntityKeyToAdditionalTypesMapper.class);
  private final boolean throwExceptions;

  public EntityKeyToAdditionalTypesMapper(boolean throwExceptions) {
    this.throwExceptions = throwExceptions;
  }

  @Override
  public Iterator<BatchResultObject<ImmutableMultimap<EntityKey,OntType>>> call
      (Iterator<BatchResultObject<ExtractedArtifacts>> inPart) throws Exception {

    log.info("Processing extractedArtifacts in batch...");
    List<BatchResultObject<ImmutableMultimap<EntityKey, OntType>>> retVal = new ArrayList<>();
    while (inPart.hasNext()) {
      BatchResultObject<ExtractedArtifacts> inputBRO = inPart.next();
      if (!inputBRO.isSuccessful()) {
        continue;
      }
      ImmutableMap.Builder batchLevelProperties = ImmutableMap.builder();
      batchLevelProperties.put(PROPERTY_MODULE_NAME, this.getClass().getCanonicalName());
      long batchStartTime = System.currentTimeMillis();
      BatchResultObject<ImmutableMultimap<EntityKey, OntType>> batchResultObject = BatchResultObject
          .createEmpty();
      ExtractedArtifacts extractedArtifacts = inputBRO.getOutputArtifact().get();
      ImmutableMultimap.Builder entityKeyToAddnlTypes = ImmutableMultimap.builder();
      try {
        ImmutableMap<RelationKey, MergedDocumentRelation> mergedRelations = extractedArtifacts
            .mergedRelations();
        for (RelationKey key : mergedRelations.keySet()) {
          for (ArgumentKey argKey : key.getArgumentKeys()) {
            if (argKey.additionalArgType().isPresent()) {
              entityKeyToAddnlTypes.put((EntityKey) argKey.fillerKey(),
                  argKey.additionalArgType().get());
            }
          }
        }
        ImmutableMap<EventKey, MergedDocumentEvent> mergedEvents = extractedArtifacts
            .mergedEvents();
        for (EventKey key : mergedEvents.keySet()) {
          for (ArgumentKey argKey : key.getArgumentKeys()) {
            if (argKey.additionalArgType().isPresent()) {
              entityKeyToAddnlTypes.put((EntityKey) argKey.fillerKey(),
                  argKey.additionalArgType().get());
            }
          }
        }
        ImmutableMultimap<EntityKey,OntType> builtMap = entityKeyToAddnlTypes.build();
        batchResultObject.setOutputArtifact(builtMap);
        batchResultObject.markSuccessful();
//        setTypeCountsAsProperties(batchLevelProperties,builtMap);
      } catch (Exception e) {
        log.error("Could not create EntityKey to additionalTypes map", e);
        if (throwExceptions) {
          throw e;
        }
        batchResultObject.markFailed();
        batchLevelProperties.put(PROPERTY_EXCEPTION_TYPE, e.getClass()
            .getName());
        batchLevelProperties
            .put(PROPERTY_EXCEPTION_MESSAGE, e.getMessage() != null ? e.getMessage() : "");
        batchLevelProperties.put(PROPERTY_EXCEPTION_TRACE, e.getStackTrace());
      }
      long batchEndTime = System.currentTimeMillis();
      batchLevelProperties.put(PROPERTY_TIME_TAKEN, (batchEndTime - batchStartTime));
      batchResultObject.setPropertiesMap(batchLevelProperties.build());
      retVal.add(batchResultObject);
    }
    return retVal.iterator();
  }

  private void setTypeCountsAsProperties(ImmutableMap.Builder batchLevelProperties,
      ImmutableMultimap<EntityKey,OntType> entityKeyToTypesMap) {
    for (EntityKey entityKey : entityKeyToTypesMap.keySet()) {
      Multiset<OntType> typeMultiSet = HashMultiset.create(entityKeyToTypesMap.get
          (entityKey));
      typeMultiSet = Multisets.copyHighestCountFirst(typeMultiSet);
      Set<OntType> seenTypes = new HashSet<>();
      for (OntType type : typeMultiSet) {
        if (seenTypes.contains(type)) {
          continue;
        }
        seenTypes.add(type);
        batchLevelProperties
            .put(entityKey + "; AdditionalType=" + type.getType(), typeMultiSet.count
                (type));
      }
    }
  }

}


