package adept.e2e.kbresolver.forwardChaining;

import adept.e2e.kbresolver.pruneLowConfRelations.InstanceInferredRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by bmin on 11/30/16.
 */
public class BatchInferredRelations {
  List<InstanceInferredRelation> inferredRelations;

  BatchInferredRelations() {
    inferredRelations = new ArrayList<InstanceInferredRelation>();
  }

  public void add(InstanceInferredRelation instanceInferredRelation) {
    inferredRelations.add(instanceInferredRelation);
  }

  public void addAll(Collection<InstanceInferredRelation> instanceInferredRelations) {
    inferredRelations.addAll(instanceInferredRelations);
  }

  public List<InstanceInferredRelation> getInferredRelations() {
    return inferredRelations;
  }
}
