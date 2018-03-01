package adept.e2e.artifactextraction.artifactkeys;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.io.Serializable;

import adept.common.OntType;

/**
 * Created by msrivast on 9/28/16.
 */
public class ArgumentKey implements Serializable {

  private static final long serialVersionUID = 4058351584434647716L;
  OntType argKBRole;
  ItemKey fillerKey;
  Optional<OntType> additionalArgType;//additional type to use for entity args (e.g. to determine if
  // a place arg is a City or a Country

  private ArgumentKey(OntType argKBRole, ItemKey fillerKey, Optional<OntType> additionalArgType) {
    this.argKBRole = argKBRole;
    this.fillerKey = fillerKey;
    this.additionalArgType = additionalArgType;
  }

  public static ArgumentKey getKey(OntType argKBRole, ItemKey fillerKey, Optional<OntType>
      additionalArgType) {
    return new ArgumentKey(argKBRole, fillerKey, additionalArgType);
  }

  public OntType kbRole() {
    return this.argKBRole;
  }

  public ItemKey fillerKey() {
    return this.fillerKey;
  }

  public Optional<OntType> additionalArgType() {
    return this.additionalArgType;
  }

  @Override
  public boolean equals(Object that) {
    return that != null && that instanceof ArgumentKey
        && this.argKBRole.equals(((ArgumentKey) that).argKBRole)
        && this.fillerKey.equals(((ArgumentKey) that).fillerKey)
        && this.additionalArgType.equals(((ArgumentKey) that).additionalArgType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(argKBRole, fillerKey, additionalArgType);
  }

  @Override
  public String toString() {
    return "{ArgumentKey: argKBRole=" + this.argKBRole.getType() + ", fillerKey=" + this.fillerKey
        .toString() + (this.additionalArgType.isPresent()?" additionalArgType="+this
        .additionalArgType.get().getType():"")+"}";
  }
}
