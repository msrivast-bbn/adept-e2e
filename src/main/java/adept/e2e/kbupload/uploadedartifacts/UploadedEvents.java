package adept.e2e.kbupload.uploadedartifacts;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import adept.common.KBID;
import adept.e2e.artifactextraction.artifactkeys.EventKey;

/**
 * Created by msrivast on 10/12/16.
 */
public final class UploadedEvents implements Serializable {

  private static final long serialVersionUID = 1564297066376368282L;
  final ImmutableMap<EventKey, KBID> uploadedEvents;

  private UploadedEvents(ImmutableMap<EventKey, KBID> uploadedEvents) {
    this.uploadedEvents = uploadedEvents;
  }

  public static UploadedEvents create(
      ImmutableMap<EventKey, KBID> uploadedEvents) {
    return new UploadedEvents(uploadedEvents);
  }

  public ImmutableMap<EventKey, KBID> uploadedEvents() {
    return uploadedEvents;
  }

}
