package adept.e2e.artifactextraction.artifactkeys;

import com.google.common.base.Optional;

import java.util.Map;
import java.util.Objects;

import adept.common.Entity;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.KBID;
import adept.common.OntType;
import adept.kbapi.KBOntologyMap;

/**
 * Created by msrivast on 9/28/16.
 */
public final class EntityKey extends ItemKey {

  private static final long serialVersionUID = 8016097244424328414L;
  KBID kbID;
  OntType entityType;

  private EntityKey(KBID kbID, OntType entityType) {
    this.kbID = kbID;
    this.entityType = entityType;
  }

  public static Optional<EntityKey> getKey(Entity entity, HltContentContainer hltCC,
      KBOntologyMap kbOntologyMap) throws
    Exception {

    Map<KBID, Float> kbIDDist = hltCC.getKBEntityMapForEntity(entity);
    if (kbIDDist == null) {
      return Optional.absent();
    }
    KBID bestKBID = null;
    float bestConf = -1.0f;
    for (Map.Entry<KBID, Float> entry : kbIDDist.entrySet()) {
      float conf = entry.getValue();
      //TODO: Implement tie-breaker
      if (conf > bestConf) {
        bestConf = conf;
        bestKBID = entry.getKey();
      }
    }
    if (bestKBID == null) {
      return Optional.absent();
    }
    return Optional.fromNullable(new EntityKey(bestKBID,getKBTypeForEntityType(entity
        .getEntityType(),kbOntologyMap)));
  }

  public KBID kbID() {
    return this.kbID;
  }

  public OntType type(){
    return this.entityType;
  }

  public static OntType getKBTypeForEntityType(IType entityType, KBOntologyMap kbOntologyMap) throws
                                                                                    Exception{
    return kbOntologyMap.getKBTypeForType(entityType).get();
  }

  @Override
  public boolean equals(Object that) {
    return (that != null && that instanceof EntityKey && this.kbID.equals(((EntityKey) that)
        .kbID) && this.entityType.equals(((EntityKey) that).entityType));
  }

  @Override
  public int hashCode() {
    return Objects.hash(kbID.hashCode(),entityType.hashCode());
  }

  @Override
  public String toString() {
    return String.format("{EntityKey: kbID=%s %s entityType=%s}", kbID.getKBNamespace()
            .toString(), kbID.getObjectID(), entityType.getType());
  }
}
