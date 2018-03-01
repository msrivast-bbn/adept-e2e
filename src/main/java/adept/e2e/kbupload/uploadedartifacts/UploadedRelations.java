package adept.e2e.kbupload.uploadedartifacts;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.common.KBID;
import adept.e2e.artifactextraction.artifactkeys.RelationKey;

/**
 * Created by msrivast on 10/12/16.
 */
public final class UploadedRelations implements Serializable {

  private static final long serialVersionUID = -5455037887774647292L;
  final ImmutableMap<RelationKey, KBID> uploadedRelations;

  private UploadedRelations(
      ImmutableMap<RelationKey, KBID> uploadedRelations) {
    this.uploadedRelations = uploadedRelations;
  }

  public static UploadedRelations create(
      ImmutableMap<RelationKey, KBID> uploadedRelations) {
    return new UploadedRelations(uploadedRelations);
  }

  public ImmutableMap<RelationKey, KBID> uploadedRelations() {
    return uploadedRelations;
  }

}
