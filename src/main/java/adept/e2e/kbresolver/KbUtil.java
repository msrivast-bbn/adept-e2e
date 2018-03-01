package adept.e2e.kbresolver;

import adept.kbapi.*;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import adept.common.OntType;
import adept.common.OntTypeFactory;

/**
 * Created by bmin on 9/24/16.
 */
public class KbUtil {
    private static Logger log = LoggerFactory.getLogger(KbUtil.class);

    public static void printKbSummaryStats(KBParameters kbParameters) throws IOException {
        log.info("========== KB Summary Stats ==========");
        try {
            KB kb = new KB(kbParameters);
            // generate list of entities to split
            for (String stringEntityType : KbUtil.entityTypes) {

                Optional<OntType> ontEntityTypeOptional =
                        KbUtil.getEntityOntTypeFromString(stringEntityType);
                if (!ontEntityTypeOptional.isPresent()) {
                    continue;
                }

                List<KBEntity> kbEntities = kb.getEntitiesByType(ontEntityTypeOptional.get());

                for (KBEntity kbEntity : kbEntities) {
                    Set<KBProvenance> kbProvenances = kbEntity.getProvenances();

                    log.info("===== Entity:");
                    log.info("Entity:\t{}\t{}\t{}\t{}\t{}", stringEntityType,
                             kbEntity.getCanonicalString(),
                             kbEntity.getKBID().getKBNamespace(),
                             kbEntity.getKBID().getObjectID(), kbProvenances.size());
                }
            }

            for (String reln : KbUtil.reln2focus2role2maxCardinality.keySet()) {
                OntType relnOntType = (OntType) OntTypeFactory.getInstance().getType(
                        "adept-kb", reln);

                for (KBRelation kbRelation : kb.getRelationsByType(relnOntType)) {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (KBRelationArgument kbRelationArgument : kbRelation.getArguments()) {
                        stringBuilder.append(kbRelationArgument.getRole().getType() + ":" + kbRelationArgument.getKBID().getObjectID() + " | " + kbRelationArgument.getTarget().getKBID().getObjectID() + "\t");
                    }
                    log.info("\t - Reln: {}\t{}\t{}", kbRelation.getType().getType(), kbRelation.getKBID().getObjectID(),
                             stringBuilder.toString().trim());
                }
            }


            log.info("=====================================");
        } catch (KBQueryException e) {
            e.printStackTrace();
        } catch (KBConfigurationException e) {
            log.error("Cannot load kb configuration parameters");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //////////////////////////////////////////////
    // Borrow from com.bbn.akbc.kb.common.SlotFactory
    // There is not relations mapped from
    // - per:alternate_names
    // There is not roles specified for
    // - Origin
    // - Religion
    public static ImmutableMultimap<String, ImmutableMultimap<String, ImmutableMultimap<String, Integer>>> reln2focus2role2maxCardinality
            = ImmutableMultimap.<String, ImmutableMultimap<String, ImmutableMultimap<String, Integer>>>builder()
            .put("OrgHeadquarter", ImmutableMultimap.of(
                    "organization", ImmutableMultimap.of("location", 1),
                    "location", ImmutableMultimap.of("organization", 1)))
            .put("ParentChildRelationship", ImmutableMultimap.of(
                    "parent", ImmutableMultimap.of("child", 1),
                    "child", ImmutableMultimap.of("parent", 1)))
            .put("InvestorShareholder",
                    ImmutableMultimap.of("investorShareholder", ImmutableMultimap.of("organization", 1)))
            .put("ChargeIndict", ImmutableMultimap.of("person", ImmutableMultimap.of("crime", 1)))
            .put("SpousalRelationship",
                    ImmutableMultimap.of("person", ImmutableMultimap.of("person", 1)))
            .put("SiblingRelationship",
                    ImmutableMultimap.of("person", ImmutableMultimap.of("person", 1)))
            .put("FamilyRelationship", ImmutableMultimap.of("person", ImmutableMultimap
                    .of("person", 1))) // there is a type on the Ont Map as "per:otherfamily"
            .put("MemberOriginReligionEthnicity", ImmutableMultimap.of("organization",
                    ImmutableMultimap
                            .of("affiliatedEntity", 1))) // reln_card(org:political_religious_affiliation) = 3
            .put("Leadership", ImmutableMultimap.of(
                    "organization", ImmutableMultimap.of("leader", 1),
                    "leader", ImmutableMultimap.of("organization", 1)
            ))
            .put("Founder", ImmutableMultimap.of("organization", ImmutableMultimap.of("founder", 1)))
            .put("Die", ImmutableMultimap.of(
                    "person", ImmutableMultimap.of("location", 1),
                    "person", ImmutableMultimap.of("time", 1)
            )) // what is the "agent" role?
            .put("PersonAge", ImmutableMultimap.of("person", ImmutableMultimap.of("age", 1)))
            .put("NumberOfEmployeesMembers",
                    ImmutableMultimap.of("organization", ImmutableMultimap.of("number", 1)))
            .put("StartOrganization",
                    ImmutableMultimap.of("organization", ImmutableMultimap.of("time", 1)))
            .put("EndOrganization",
                    ImmutableMultimap.of("organization", ImmutableMultimap.of("time", 1)))
            .put("OrganizationWebsite",
                    ImmutableMultimap.of("organization", ImmutableMultimap.of("url", 1)))
            .put("StudentAlum",
                    ImmutableMultimap.of("person", ImmutableMultimap.of("organization", 1)))
            .put("Membership",
                    ImmutableMultimap.of(
                            "member", ImmutableMultimap.of("organization", 1),
                            "organization", ImmutableMultimap.of("member", 1)
                    ))
            .put("Subsidiary", ImmutableMultimap.of(
                    "parent", ImmutableMultimap.of("subOrganization", 1),
                    "subOrganization", ImmutableMultimap.of("parent", 1)
                    // there is an error (duplicated roles) for this relation in the ontology/Ont Map
            ))
            .put("EmploymentMembership", ImmutableMultimap.of(
                    "person", ImmutableMultimap.of("organization", 1),
                    "organization", ImmutableMultimap.of("person", 1)
            ))
            .put("Resident", ImmutableMultimap.of(
                    "person", ImmutableMultimap.of("location", 1),
                    "location", ImmutableMultimap.of("person", 1)
            )) // higher cardinality for countries than cities
            .put("Title", ImmutableMultimap.of("person", ImmutableMultimap.of("role", 1)))
            .build();
    //////////////////////////////////////////////

    public static ImmutableSet<String> relationTypesAdept
            = ImmutableSet.<String>builder()
            .add("OrgHeadquarter")
            .add("ParentChildRelationship")
            .add("InvestorShareholder")
            .add("ChargeIndict")
            .add("SpousalRelationship")
            .add("SiblingRelationship")
            .add("FamilyRelationship")
            .add("MemberOriginReligionEthnicity")
            .add("Leadership")
            .add("Founder")
            .add("Die")
            .add("PersonAge")
            .add("NumberOfEmployeesMembers")
            .add("StartOrganization")
            .add("EndOrganization")
            .add("OrganizationWebsite")
            .add("StudentAlum")
            .add("Membership")
            .add("Subsidiary")
            .add("EmploymentMembership")
            .add("Resident")
            .add("Title")
            .build();

    public static ImmutableSet<String> relationTypesTAC
            = ImmutableSet.<String>builder()
            .add("per:alternate_names")
            .add("per:date_of_birth")
            .add("per:age")
            .add("per:country_of_birth")
            .add("per:stateorprovince_of_birth")
            .add("per:city_of_birth")
            .add("per:origin")
            .add("per:date_of_death")
            .add("per:country_of_death")
            .add("per:stateorprovince_of_death")
            .add("per:city_of_death")
            .add("per:cause_of_death")
            .add("per:countries_of_residence")
            .add("per:statesorprovinces_of_residence")
            .add("per:cities_of_residence")
            .add("per:schools_attended")
            .add("per:title")
            .add("per:member_of")
            .add("per:employee_of")
            .add("per:religion")
            .add("per:spouse")
            .add("per:children")
            .add("per:parents")
            .add("per:siblings")
            .add("per:other_family")
            .add("per:charges")
            .add("org:alternate_names")
            .add("org:political_religious_affiliation")
            .add("org:top_members_employees")
            .add("org:number_of_employees_members")
            .add("org:members")
            .add("org:member_of")
            .add("org:subsidiaries")
            .add("org:parents")
            .add("org:founded_by")
            .add("org:date_founded")
            .add("org:date_dissolved")
            .add("org:country_of_headquarters")
            .add("org:stateorprovince_of_headquarters")
            .add("org:city_of_headquarters")
            .add("org:shareholders")
            .add("org:website")
            .add("REVERB")
            .add("OLLIE")
            .add("UIUCEvent")
            .add("CAUSE_ENABLE/PART_OF/RELATED")
            .add("NOMSRL")
            .add("VERBSRL")
            .add("Positive")
            .add("Negative")
            .add("event:domaineventtype")
            .add("event:subevent_of")
            .add("event:member_of")
            .add("event:epistemic_status")
            .add("event:participant1")
            .add("event:participant2")
            .add("event:location")
            .add("event:time")
            .add("Interaction")
            .add("Cognition")
            .add("per:up_hierarchy")
            .add("per:down_hierarchy")
            .add("per:same_hierarchy")
            .build();

    public static Set<String> entityTypes = ImmutableSet.of("per", "org", "gpe");

    public static Optional<OntType> getEntityOntTypeFromString(String entityStringType) throws Exception {
        KBOntologyMap ontMap = KBOntologyMap.getTACOntologyMap();
        OntType ontTypeRaw = (OntType) OntTypeFactory.getInstance().getType("TACKBP", entityStringType);
        Optional<OntType> ontTypeOptional = ontMap.getKBTypeForType(ontTypeRaw);

        return ontTypeOptional;
    }

 
    public static String getTextProvenance(KBTextProvenance kbTextProvenance) {
        StringBuilder sb = new StringBuilder();

        sb.append("value=" + kbTextProvenance.getValue() + ", ");
        sb.append("doc=" + kbTextProvenance.getDocumentID() + ", ");
        sb.append("beg=" + kbTextProvenance.getBeginOffset() + ", ");
        sb.append("end=" + kbTextProvenance.getEndOffset() + ", ");
        sb.append("conf=" + kbTextProvenance.getConfidence());

        return sb.toString().trim();
    }

    public static String getStringInfo(KBEntity kbEntity) {
        StringBuilder sb = new StringBuilder();

        try {
            sb.append("c_str=" + kbEntity.getCanonicalString() + ", ");
            sb.append("kbid=" + kbEntity.getKBID() + ", ");
            sb.append("conf=" + kbEntity.getConfidence() + ", ");
            sb.append("c_ment_conf=" + kbEntity.getCanonicalMentionConfidence() + ", ");

            sb.append("type=[ ");
            for (OntType ontType : kbEntity.getTypes().keySet()) {
                sb.append("(" + kbEntity.getTypes().get(ontType) + ") ");
            }
            sb.append("] ");

            sb.append("n_prov=" + kbEntity.getProvenances().size());
        } catch(adept.kbapi.KBQueryException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    public static String getStringInfoShort(KBEntity kbEntity) {
        StringBuilder sb = new StringBuilder();

        try {
            sb.append(kbEntity.getKBID() + ":"
                    + kbEntity.getCanonicalString() + "(" + kbEntity.getProvenances().size() + ")");
        } catch(adept.kbapi.KBQueryException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    public static String getStringInfoShort(KBRelation kbRelation) {
        StringBuilder sb = new StringBuilder();

        try {
            sb.append(kbRelation.getType().getType() + "<");
            for (KBRelationArgument kbRelationArgument : kbRelation.getArguments()) {
                KBPredicateArgument kbPredicateArgument = kbRelationArgument.getTarget();
                sb.append(kbRelationArgument.getRole().getType() + ":");
                if(kbPredicateArgument instanceof KBDate) {
                    KBDate kbDate = (KBDate) kbPredicateArgument;
                    sb.append(kbDate.getTimexString());
                } else if(kbPredicateArgument instanceof KBNumber) {
                    KBNumber kbNumber = (KBNumber) kbPredicateArgument;
                    sb.append(kbNumber.getNumber().doubleValue());
                } else if(kbPredicateArgument instanceof KBEntity) {
                    KBEntity kbArgEntity = (KBEntity) kbPredicateArgument;
                    sb.append(kbArgEntity.getCanonicalString());
                } else if(kbPredicateArgument instanceof KBGenericThing) {
                    KBGenericThing kbGenericThing = (KBGenericThing) kbPredicateArgument;
                    sb.append(kbGenericThing.getCanonicalString());
                }

                sb.append(", ");
            }
            sb.append(">");

            sb.append("[");
            for(KBProvenance kbProvenance : kbRelation.getProvenances()) {
                if(kbProvenance instanceof KBTextProvenance) {
                    KBTextProvenance kbTextProvenance = (KBTextProvenance) kbProvenance;
                    String value = kbTextProvenance.getValue();

                    sb.append(kbTextProvenance.getDocumentID() + ":" + kbTextProvenance.getBeginOffset() + "-" + kbTextProvenance.getEndOffset()
                            + "::" + StringUtil.getNormalizedString(value) + ", ");
                }
            }
            sb.append("]");
      /*
      sb.append(kbRelation.getKBID() + ":"
          + kbRelation.getType().getType() + "<");
      for (KBRelationArgument kbRelationArgument : kbRelation.getArguments()) {
        KBPredicateArgument kbPredicateArgument = kbRelationArgument.getTarget();
        sb.append(kbRelationArgument.getRole().getURI() + ":" + kbPredicateArgument.getKBID() + ":");
        if(kbPredicateArgument instanceof KBDate) {
          KBDate kbDate = (KBDate) kbPredicateArgument;
          sb.append(kbDate.getTimexString());
        } else if(kbPredicateArgument instanceof KBNumber) {
          KBNumber kbNumber = (KBNumber) kbPredicateArgument;
          sb.append(kbNumber.getNumber().doubleValue());
        } else if(kbPredicateArgument instanceof KBEntity) {
          KBEntity kbArgEntity = (KBEntity) kbPredicateArgument;
          sb.append(kbArgEntity.getCanonicalString());
        } else if(kbPredicateArgument instanceof KBGenericThing) {
          KBGenericThing kbGenericThing = (KBGenericThing) kbPredicateArgument;
          sb.append(kbGenericThing.getCanonicalString());
        }

        sb.append(", ");
      }
      sb.append(">");

      sb.append("[");
      for(KBProvenance kbProvenance : kbRelation.getProvenances()) {
        if(kbProvenance instanceof KBTextProvenance) {
          KBTextProvenance kbTextProvenance = (KBTextProvenance) kbProvenance;
          String value = kbTextProvenance.getValue();

          sb.append(kbTextProvenance.getDocumentID() + ":" + kbTextProvenance.getBeginOffset() + "-" + kbTextProvenance.getEndOffset()
              + "::" + StringUtil.getNormalizedString(value) + ", ");
        }
      }
      sb.append("]");
      */
        } catch(Exception e) {
            e.printStackTrace();
        }

        return sb.toString();
    }
}
