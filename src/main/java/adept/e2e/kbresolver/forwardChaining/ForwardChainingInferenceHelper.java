package adept.e2e.kbresolver.forwardChaining;

import adept.e2e.kbresolver.pruneLowConfRelations.BatchRelationCardinality;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import adept.e2e.driver.SerializerUtil;
import adept.kbapi.KBParameters;
import scala.Tuple2;

/**
 * Created by bmin on 11/30/16.
 */
public class ForwardChainingInferenceHelper {

  List<Tuple2<String, String>> listKbParamAndRule;

  private static Logger log = LoggerFactory.getLogger(BatchRelationCardinality.class);

  ForwardChainingInferenceHelper(KBParameters kbParameters) {
    listKbParamAndRule = new ArrayList<Tuple2<String, String>>();

    try {
      for(ImmutableList<String> ruleInfo : ruleInfos) {
        ForwardChainingInferenceRule forwardChainingInferenceRule =
            new ForwardChainingInferenceRule(ruleInfo);

        StringBuilder sb = new StringBuilder();
        for(String rule : ruleInfo)
          sb.append(rule+" ");
        log.info("rule: {}" , sb.toString().trim());

        listKbParamAndRule.add(new Tuple2<String, String>(
            SerializerUtil.serialize(kbParameters), SerializerUtil.serialize(forwardChainingInferenceRule)));
      }
    } catch (Exception e) {
        e.printStackTrace();
    }
  }

  public int getNumInferenceRules() {
    return listKbParamAndRule.size();
  }

  /*
  static String queryTemplateTwoRelationRule =
      "PREFIX adept-base: <http://adept-kb.bbn.com/adept-base#>\n"
          + "PREFIX adept-core: <http://adept-kb.bbn.com/adept-core#>\n"
          + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
          + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
          + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
          + "\n"
          + "SELECT * WHERE {\n"
          + "    ?relation1 a adept-core:%s ;\n"
          + "        adept-core:%s ?argument1 ;\n"
          + "        adept-core:%s ?sharedArgument .\n"
          + "    FILTER (?argument1 != ?sharedArgument)\n"
          + "    ?relation2 a adept-core:%s ;\n"
          + "        adept-core:%s ?sharedArgument ;\n"
          + "        adept-core:%s ?argument2 .\n"
          + "}";

  static String queryTemplateOneRelationRule =
      "PREFIX adept-base: <http://adept-kb.bbn.com/adept-base#>\n"
          + "PREFIX adept-core: <http://adept-kb.bbn.com/adept-core#>\n"
          + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
          + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
          + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
          + "\n"
          + "SELECT * WHERE {\n"
          + "    ?relation1 a adept-core:%s ;\n"
          + "        adept-core:%s ?argument1 ;\n"
          + "        adept-core:%s ?argument2 .\n"
          + "}";
*/

  static String queryTemplateTwoRelationRule =
      "PREFIX adept-base: <http://adept-kb.bbn.com/adept-base#>\n"
          + "PREFIX adept-core: <http://adept-kb.bbn.com/adept-core#>\n"
          + "PREFIX afn: <http://jena.hpl.hp.com/ARQ/function#>\n"
          + "PREFIX fn: <http://www.w3.org/2005/xpath-functions#>\n"
          + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
          + "PREFIX par: <http://parliament.semwebcentral.org/parliament#>\n"
          + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
          + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
          + "PREFIX time: <http://www.w3.org/2006/time#>\n"
          + "PREFIX xml: <http://www.w3.org/XML/1998/namespace>\n"
          + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
          + "\n"
          + "SELECT * WHERE {\n"
          + "    ?relation1 a adept-core:%s ;\n"
          + "        adept-core:%s ?argument1 ;\n"
          + "        adept-core:%s ?sharedArgument .\n"
          + "    FILTER (?argument1 != ?sharedArgument)\n"
          + "    ?relation2 a adept-core:%s ;\n"
          + "        adept-core:%s ?sharedArgument ;\n"
          + "        adept-core:%s ?argument2 .\n"
          + "}";

  static String queryTemplateOneRelationRule =
      "PREFIX adept-base: <http://adept-kb.bbn.com/adept-base#>\n"
          + "PREFIX adept-core: <http://adept-kb.bbn.com/adept-core#>\n"
          + "PREFIX afn: <http://jena.hpl.hp.com/ARQ/function#>\n"
          + "PREFIX fn: <http://www.w3.org/2005/xpath-functions#>\n"
          + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
          + "PREFIX par: <http://parliament.semwebcentral.org/parliament#>\n"
          + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
          + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
          + "PREFIX time: <http://www.w3.org/2006/time#>\n"
          + "PREFIX xml: <http://www.w3.org/XML/1998/namespace>\n"
          + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
          + "\n"
          + "SELECT * WHERE {\n"
          + "    ?relation1 a adept-core:%s ;\n"
          + "        adept-core:%s ?argument1 ;\n"
          + "        adept-core:%s ?argument2 .\n"
          + "}";


  static List<ImmutableList<String>> ruleInfos = ImmutableList.of(
      /** per:parents per:spouse -> per:parents **/
      /** per:spouse per:children -> per:children **/
      ImmutableList.of(
          "SpousalRelationship", "person", "person",
          "ParentChildRelationship", "parent", "child",
          "ParentChildRelationship", "parent", "child"
      ),

      /** per:children per:children -> per:other_family **/
      ImmutableList.of(
          "ParentChildRelationship", "parent", "child",
          "ParentChildRelationship", "parent", "child",
          "FamilyRelationship", "person", "person"
      ),

      /** per:children per:siblings -> per:children **/
      ImmutableList.of(
          "ParentChildRelationship", "parent", "child",
          "SiblingRelationship", "person", "person",
          "ParentChildRelationship", "parent", "child"
      ),

      /** per:children per:spouse -> per:other_family **/
      ImmutableList.of(
          "ParentChildRelationship", "parent", "child",
          "SpousalRelationship", "person", "person",
          "FamilyRelationship", "person", "person"
      ),

      /** per:parents per:parents -> per:other_family **/
      ImmutableList.of(
          "ParentChildRelationship", "child", "parent",
          "ParentChildRelationship", "child", "parent",
          "FamilyRelationship", "person", "person"
      ),

      /** per:parents per:siblings -> per:other_family **/
      ImmutableList.of(
          "ParentChildRelationship", "child", "parent",
          "SiblingRelationship", "person", "person",
          "FamilyRelationship", "person", "person"
      ),

      /** per:siblings per:children -> per:other_family **/
      ImmutableList.of(
          "SiblingRelationship", "person", "person",
          "ParentChildRelationship", "parent", "child",
          "FamilyRelationship", "person", "person"
      ),

      /** per:siblings per:parents -> per:parents  **/
      ImmutableList.of(
          "SiblingRelationship", "person", "person",
          "ParentChildRelationship", "child", "parent",
          "ParentChildRelationship", "child", "parent"
      ),

      /** per:siblings per:siblings -> per:siblings **/
      ImmutableList.of(
          "SiblingRelationship", "person", "person",
          "SiblingRelationship", "person", "person",
          "SiblingRelationship", "person", "person"
      ),

      /** per:siblings per:spouse -> per:other_family **/
      ImmutableList.of(
          "SiblingRelationship", "person", "person",
          "SpousalRelationship", "person", "person",
          "FamilyRelationship", "person", "person"
      ),

      /** per:spouse per:parents -> per:other_family **/
      ImmutableList.of(
          "SpousalRelationship", "person", "person",
          "ParentChildRelationship", "child", "parent",
          "FamilyRelationship", "person", "person"
      ),

      /** per:spouse per:siblings -> per:other_family **/
      ImmutableList.of(
          "SpousalRelationship", "person", "person",
          "SiblingRelationship", "person", "person",
          "FamilyRelationship", "person", "person"
      ),

      /** per:schools_attended org:stateorprovince_of_headquarters -> per:statesorprovinces_of_residence **/
      ImmutableList.of(
          "StudentAlum", "person", "organization",
          "organization", "location", "OrgHeadquarter",
          "Resident", "person", "location"
      ),

      /** per:top_member_employee_of -> per:employee_or_member_of **/
      ImmutableList.of(
          "Leadership", "leader", "organization",
          "", "", "",
          "EmploymentMembership", "person", "organization"
      )
  );


  /******* The following rules will not be supported because of the ADEPT ontology doesn't support gpe_contains relations *******/
  /** org:city_of_headquarters country:gpe_contains:city-1 -> org:country_of_headquarters **/
  /** org:city_of_headquarters state-or-province:gpe_contains:city-1 -> org:stateorprovince_of_headquarters **/
  /** org:stateorprovince_of_headquarters country:gpe_contains:state-or-province-1 -> org:country_of_headquarters **/
  /** per:cities_of_residence country:gpe_contains:city-1 -> per:countries_of_residence **/
  /** per:cities_of_residence state-or-province:gpe_contains:city-1 -> per:statesorprovinces_of_residence **/

  /** per:city_of_birth country:gpe_contains:city-1 -> per:country_of_birth **/
  /** per:city_of_birth state-or-province:gpe_contains:city-1 -> per:stateorprovince_of_birth **/
  /** per:city_of_death country:gpe_contains:city-1 -> per:country_of_death **/
  /** per:city_of_death state-or-province:gpe_contains:city-1 -> per:stateorprovince_of_death **/

  /** per:stateorprovince_of_birth country:gpe_contains:state-or-province-1 -> per:country_of_birth **/
  /** per:stateorprovince_of_death country:gpe_contains:state-or-province-1 -> per:country_of_death **/
  /** per:statesorprovinces_of_residence country:gpe_contains:state-or-province-1 -> per:countries_of_residence **/
  /*******************************************************************************************************************************/

}
