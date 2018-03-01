package adept.e2e.artifactextraction.mergedartifacts;

import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by msrivast on 9/28/16.
 */
public class ItemWithProvenances<ItemType,ProvenanceType> implements Serializable {

  private static final long serialVersionUID = 1791421798527657311L;
  ItemType item;
  ImmutableSet<ProvenanceType> provenances;

  private ItemWithProvenances(ItemType item, ImmutableSet<ProvenanceType> provenances) {
    this.item = item;
    this.provenances = provenances;
  }

  public static <ItemType,ProvenanceType> ItemWithProvenances create(ItemType item,
      ImmutableSet<ProvenanceType> provenances) {
    return new ItemWithProvenances(item, provenances);
  }

  public ItemType item() {
    return item;
  }

  public Set<ProvenanceType> getProvenances() {
    return provenances;
  }

}
