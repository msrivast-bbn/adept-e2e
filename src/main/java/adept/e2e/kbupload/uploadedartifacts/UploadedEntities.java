package adept.e2e.kbupload.uploadedartifacts;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.common.KBID;
import adept.e2e.artifactextraction.artifactkeys.EntityKey;

/**
 * Created by msrivast on 10/12/16.
 */
public final class UploadedEntities implements Serializable {

  private static final long serialVersionUID = 4272418023007536399L;
  final ImmutableMap<EntityKey, KBID> uploadedEntities;

  private UploadedEntities(
      ImmutableMap<EntityKey, KBID> uploadedEntities) {
    this.uploadedEntities = uploadedEntities;
  }

  public static UploadedEntities create(
      ImmutableMap<EntityKey, KBID> uploadedEntities) {
    return new UploadedEntities(uploadedEntities);
  }

  public ImmutableMap<EntityKey, KBID> uploadedEntities() {
    return uploadedEntities;
  }

}
