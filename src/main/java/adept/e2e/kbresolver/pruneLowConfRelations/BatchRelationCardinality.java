package adept.e2e.kbresolver.pruneLowConfRelations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import adept.common.KBID;
import adept.common.OntType;
import adept.common.OntTypeFactory;

public class BatchRelationCardinality implements Serializable {
  KBID entityKBID;
  String relnType;

  int maxCardinality;

  String focus;
  String role;

  List<KBID> relations2delete;

  private static Logger log = LoggerFactory.getLogger(BatchRelationCardinality.class);


  BatchRelationCardinality(KBID entityKBID,
      String relnType,
      int maxCardinality,
      String focus,
      String role) {
    this.entityKBID = entityKBID;
    this.relnType = relnType;
    this.maxCardinality = maxCardinality;
    this.focus = focus;
    this.role = role;

    relations2delete = new ArrayList<KBID>();
  }

  public KBID getEntityKBID() {
    return entityKBID;
  }

  public OntType getRelnOntType() {
    OntType relnOntType = (OntType) OntTypeFactory.getInstance().getType(
        "adept-kb", relnType);
    return relnOntType;
  }

  public int getMaxCardinality() { return maxCardinality; }

  public String getRole() {
    return role;
  }

  public String getFocus() {
    return focus;
  }

  public void addRelationToDelete(KBID relation) {
    relations2delete.add(relation);
  }

  public Iterable<KBID> getRelations2delete() {
    return relations2delete;
  }

  public static BatchRelationCardinality fromString(String sline) {
    log.info("read: {}", sline);

    String [] items = sline.split("\t");

    String uriItems_1 = items[0];
    String uriItems_0 = items[1];
    KBID entityKBID = new KBID(uriItems_1, uriItems_0);

    String relnType = items[2];

    int maxCardinality = Integer.parseInt(items[3]);
    String role = items[4];
    String focus = items[5];


    return new BatchRelationCardinality(entityKBID, relnType, maxCardinality, focus, role);
  }
}
