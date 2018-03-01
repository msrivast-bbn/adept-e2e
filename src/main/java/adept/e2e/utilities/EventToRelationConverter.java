package adept.e2e.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import adept.common.IType;
import adept.common.OntType;
import adept.kbapi.KBEntity;
import adept.kbapi.KBEvent;
import adept.kbapi.KBRelationArgument;

/**
 * Helper class containing the logic for converting KBEvent objects
 * into lines in the KB file.
 */
public final class EventToRelationConverter {

  // Mapping from database event type to possible generated relation types.
  // We'll check to make sure the necessary event slots for each relation
  // are present before creating a relation.
  // Reversed relations e.g. per:city_of_death -> gpe:deaths_in_city are added
  // using deft/adept/adept-kb/src/main/resources/adept/kbapi/tac-to-inverse.xml
  static HashMap<String, String[]> eventTypeToRelationType = new HashMap<>();
  static {
    eventTypeToRelationType.put("Die",
        new String[]{"per:city_of_death", "per:country_of_death", "per:stateorprovince_of_death",
            "per:date_of_death", "per:cause_of_death"});
    eventTypeToRelationType.put("BeBorn",
        new String[]{"per:city_of_birth", "per:country_of_birth", "per:stateorprovince_of_birth",
            "per:date_of_birth"});
    eventTypeToRelationType.put("ChargeIndict", new String[]{"per:charges"});
    eventTypeToRelationType.put("StartOrganization", new String[]{"org:date_founded"});
    eventTypeToRelationType.put("EndOrganization", new String[]{"org:date_dissolved"});
  }

  // Mapping from relation type to event argument role(s) that should can the subject slot of the
  // generated relation.
  static HashMap<String, String> relationTypeToSubjectRole = new HashMap<>();
  static {
    relationTypeToSubjectRole.put("per:city_of_death", "victim|person");
    relationTypeToSubjectRole.put("per:country_of_death", "victim|person");
    relationTypeToSubjectRole.put("per:stateorprovince_of_death", "victim|person");
    relationTypeToSubjectRole.put("per:date_of_death", "victim|person");
    relationTypeToSubjectRole.put("per:cause_of_death", "victim|person");

    relationTypeToSubjectRole.put("per:city_of_birth", "person");
    relationTypeToSubjectRole.put("per:country_of_birth", "person");
    relationTypeToSubjectRole.put("per:stateorprovince_of_birth", "person");
    relationTypeToSubjectRole.put("per:date_of_birth", "person");

    relationTypeToSubjectRole.put("per:charges", "defendant");

    relationTypeToSubjectRole.put("org:date_founded", "organization");

    relationTypeToSubjectRole.put("org:date_dissolved", "organization");
  }

  // Mapping from relation type to event argument role(s) that can fill the object slot of the
  // generated relation.
  static HashMap<String, String> relationTypeToObjectRole = new HashMap<>();
  static {
    relationTypeToObjectRole.put("per:city_of_death", "place");
    relationTypeToObjectRole.put("per:country_of_death", "place");
    relationTypeToObjectRole.put("per:stateorprovince_of_death", "place");
    relationTypeToObjectRole.put("per:date_of_death", "time");
    relationTypeToObjectRole.put("per:cause_of_death", "causeOfDeath");

    relationTypeToObjectRole.put("per:city_of_birth", "place");
    relationTypeToObjectRole.put("per:country_of_birth", "place");
    relationTypeToObjectRole.put("per:stateorprovince_of_birth", "place");
    relationTypeToObjectRole.put("per:date_of_birth", "time");

    relationTypeToObjectRole.put("per:charges", "crime");

    relationTypeToObjectRole.put("org:date_founded", "time");

    relationTypeToObjectRole.put("org:date_dissolved", "time");
  }

  // If a relation type appears as a key in this map, the subject of that relation type
  // must have the value as an ont type
  static HashMap<String, String> relationTypeToSubjectOntType = new HashMap<>();
  static {

  }

  // If a relation type appears as a key in this map, the object of that relation type
  // must have the value as an ont type
  static HashMap<String, String> relationTypeToObjectOntType = new HashMap<>();
  static {
    relationTypeToObjectOntType.put("per:city_of_death", "City");
    relationTypeToObjectOntType.put("per:country_of_death", "Country");
    relationTypeToObjectOntType.put("per:stateorprovince_of_death", "StateProvince");

    relationTypeToObjectOntType.put("per:city_of_birth", "City");
    relationTypeToObjectOntType.put("per:country_of_birth", "Country");
    relationTypeToObjectOntType.put("per:stateorprovince_of_birth", "StateProvince");
  }

  static List<String> getRelationTypes(IType eventType) {
    //System.out.println("eventType: " + eventType.getType());

    if (!eventTypeToRelationType.containsKey(eventType.getType()))
      return new ArrayList<>();
    //System.out.println("Returning list");
    return new ArrayList<>(Arrays.asList(eventTypeToRelationType.get(eventType.getType())));

  }

  static KBRelationArgument getSubject(KBEvent event, String relationType) {
    String[] roles = relationTypeToSubjectRole.get(relationType).split("\\|");
    //System.out.println("Subject role: " + roles);
    return getArgumentThatFillsRole(event, roles, relationType, "SUBJECT");
  }

  static KBRelationArgument getObject(KBEvent event, String relationType) {
    String[] roles = relationTypeToObjectRole.get(relationType).split("\\|");
    //System.out.println("Object role: " + roles);
    return getArgumentThatFillsRole(event, roles, relationType, "OBJECT");
  }

  static KBRelationArgument getArgumentThatFillsRole(KBEvent event, String[] roles,
      String relationType, String argumentType)
  {
    for (KBRelationArgument argument : event.getArguments()) {
      for (String role : roles) {
        //System.out.println("Comparing " + role + " to " + argument.getRole().getType());
        if (!argument.getRole().getType().equals(role))
          continue;

        if (argumentIsCompatibleWithRelationType(argument, relationType, argumentType)) {
          //System.out.print("Returning argument");
          return argument;
        }
      }
    }
    return null;
  }

  static boolean argumentIsCompatibleWithRelationType(KBRelationArgument argument,
      String relationType, String argumentType)
  {
    if (!(argument.getTarget() instanceof KBEntity))
      return true;
    KBEntity entity = (KBEntity) argument.getTarget();

    HashMap<String, String> relationTypeToOntTypeMap;
    if (argumentType.equals("SUBJECT"))
      relationTypeToOntTypeMap = relationTypeToSubjectOntType;
    else if (argumentType.equals("OBJECT"))
      relationTypeToOntTypeMap = relationTypeToObjectOntType;
    else return true;

    if (!relationTypeToOntTypeMap.containsKey(relationType))
      return true;

    for (OntType type : entity.getTypes().keySet()) {
      if (relationTypeToOntTypeMap.get(relationType).equals(type.getType()))
        return true;
    }
    return false;
  }
}
