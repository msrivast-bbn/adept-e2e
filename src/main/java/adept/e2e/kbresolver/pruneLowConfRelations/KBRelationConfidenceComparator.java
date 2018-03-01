package adept.e2e.kbresolver.pruneLowConfRelations;

import java.util.Comparator;

import adept.kbapi.KBRelation;

/**
 * Created by bmin on 6/15/16.
 */
public class KBRelationConfidenceComparator implements Comparator<KBRelation> {
  // rank reversely by confidence
  public int compare(KBRelation a, KBRelation b) {
    return Float.compare(b.getConfidence(), a.getConfidence());
  }
}
