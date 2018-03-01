package adept.e2e.artifactextraction.artifactkeys;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import adept.common.GenericThing;
import adept.common.OntType;
import adept.kbapi.KBOntologyMap;

/**
 * Created by msrivast on 9/28/16.
 */
public final class GenericThingKey extends ItemKey {

  private static final long serialVersionUID = -4598591100471024040L;
  GenericThing genericThing;

  private GenericThingKey(OntType kbType, String value) {
    this.genericThing = new GenericThing(kbType,value);
  }

  public static Optional<GenericThingKey> getKey(GenericThing genericThing)
      throws Exception {
    Optional<OntType> kbType = Optional.absent();
    try {
      kbType = KBOntologyMap.getTACOntologyMap().getKBTypeForType
          (genericThing.getType());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    if (!kbType.isPresent()) {
      return Optional.absent();
    }
    return Optional.of(new GenericThingKey(kbType.get(), genericThing.getValue().toLowerCase()));
  }

  @Override
  public boolean equals(Object that) {
    return (that != null && that instanceof GenericThingKey
                && this.genericThing.getType().getType().equals(((GenericThingKey) that).genericThing.getType().getType())
                && this.genericThing.getValue().equals(((GenericThingKey) that).genericThing.getValue()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.genericThing.getType(), this.genericThing.getValue());
  }

  @Override
  public String toString() {
    return "{GenericThingKey: kbType=" + this.genericThing.getType().getType() + ", value=" + this.genericThing.getValue()+ "}";
  }

  public OntType type() {
    return (OntType) this.genericThing.getType();
  }

  public String value() {
    return this.genericThing.getValue();
  }

  public GenericThing genericThing(){
    return this.genericThing;
  }
}
