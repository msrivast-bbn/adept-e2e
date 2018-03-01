package adept.e2e.kbresolver.pruneLowConfRelations;

import adept.kbapi.KBEntity;
import adept.kbapi.KBRelation;

/**
 * Created by bmin on 11/30/16.
 */
public class InstanceInferredRelation {
  public KBRelation kbRelation1;
  public KBRelation kbRelation2;
  public String inferedRelationType;
  public String inferedArg1Role;
  public KBEntity kbEntityArg1;
  public KBEntity kbEntityArg2;
  public String inferedArg2Role;

  public InstanceInferredRelation(KBRelation kbRelation1,
      KBRelation kbRelation2,
      String inferedRelationType,
      String inferedArg1Role,
      KBEntity kbEntityArg1,
      KBEntity kbEntityArg2,
      String inferedArg2Role) {
    this.kbRelation1 = kbRelation1;
    this.kbRelation2 = kbRelation2;
    this.inferedRelationType = inferedRelationType;
    this.inferedArg1Role = inferedArg1Role;
    this.kbEntityArg1 = kbEntityArg1;
    this.kbEntityArg2 = kbEntityArg2;
    this.inferedArg2Role = inferedArg2Role;
  }
}
