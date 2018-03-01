package adept.e2e.artifactextraction.artifactkeys;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import adept.common.DocumentRelation;
import adept.common.DocumentRelationArgument;
import adept.common.Entity;
import adept.common.GenericThing;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Item;
import adept.common.NumericValue;
import adept.common.OntType;
import adept.common.TemporalValue;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 9/28/16.
 */
public final class RelationKey implements Serializable {

  private static final long serialVersionUID = -6997140125558413947L;
  OntType relationKBType;
  ImmutableSet<ArgumentKey> argumentKeys;

  private RelationKey(OntType relationKBType, ImmutableSet<ArgumentKey> argumentKeys) {
    this.relationKBType = relationKBType;
    this.argumentKeys = argumentKeys;
  }

  public static Optional<RelationKey> getKey(DocumentRelation docRelation,
      HltContentContainer hltCC, KBOntologyMap kbOntologyMap, KBOntologyMap entityOntologyMap)
      throws Exception{
    Optional<OntType> relationKBType = getKBTypeForRelationType(docRelation.getRelationType(),
        kbOntologyMap);
    if (!relationKBType.isPresent() || docRelation.getArguments() == null ||
        docRelation.getArguments().size() == 0) {
      return Optional.absent();
    }
    Set<ArgumentKey> argumentKeys = new HashSet<>();
    for (DocumentRelationArgument docArgument : docRelation.getArguments()) {
      Optional<OntType> kbRole = getKBRoleForType(docRelation.getRelationType(),docArgument
          .getRole(), kbOntologyMap);
      if (!kbRole.isPresent()) {
        return Optional.absent();
      }
      Optional<Item> item = docArgument.getFiller().asItem();
      if (item.isPresent()) {
        if (item.get() instanceof Entity) {
          EntityKey key = EntityKey.getKey((Entity) item.get(), hltCC, entityOntologyMap).orNull();
          if (key == null) {
            return Optional.absent();
          }
          argumentKeys.add(ArgumentKey.getKey(kbRole.get(), key, getAdditionalTypeForEntityArg(docRelation
              .getRelationType(),docArgument.getRole(),kbOntologyMap)));
        } else if (item.get() instanceof NumericValue) {
          NumericValueKey key = NumericValueKey.getKey((NumericValue) item.get());
          argumentKeys.add(ArgumentKey.getKey(kbRole.get(), key, Optional.absent()));
        } else if (item.get() instanceof TemporalValue) {
          TemporalValueKey key = TemporalValueKey.getKey((TemporalValue) item.get());
          argumentKeys.add(ArgumentKey.getKey(kbRole.get(), key, Optional.absent()));
        } else if (item.get() instanceof GenericThing) {
          GenericThingKey key = GenericThingKey.getKey((GenericThing) item.get()).orNull();
          if (key == null) {
            return Optional.absent();
          }
          argumentKeys.add(ArgumentKey.getKey(kbRole.get(), key, Optional.absent()));
        }
      } else {
        return Optional.absent();
      }
    }
    return Optional.of(new RelationKey(relationKBType.get(), ImmutableSet.copyOf(argumentKeys)));
  }

  public static Optional<RelationKey> getKey(IType relationIType, ImmutableSet<ArgumentKey>
      argumentKeys, KBOntologyMap kbOntologyMap) throws Exception{
    checkNotNull(relationIType);
    checkNotNull(argumentKeys);
    Optional<OntType> relationKBType = getKBTypeForRelationType(relationIType,kbOntologyMap);
    if(!relationKBType.isPresent()){
      return Optional.absent();
    }
    //Commenting out the below check for arg-roles, since the reverse-ontology mapping does not
    // work as expected
//    boolean kbTypeOrRoleNotFound = false;
//
//    for (ArgumentKey argKey : argumentKeys) {
//      //Make sure that arg-roles are compatible with this relation-kbType
//      //and have a mapping into KB-roles
//      Optional<? extends IType> argRole = getTACRoleForKBRole(relationKBType.get(),argKey.kbRole
//          ());
//      if (!argRole.isPresent() || !getKBRoleForType(relationIType,argRole.get()).isPresent()) {
//        kbTypeOrRoleNotFound = true;
//        break;
//      }
//    }
//    if (kbTypeOrRoleNotFound) {
//      return Optional.absent();
//    }
    return Optional.fromNullable(new RelationKey(relationKBType.get(), argumentKeys));
  }

  public OntType getRelationKBType() {
    return relationKBType;
  }

  public Set<ArgumentKey> getArgumentKeys() {
    return argumentKeys;
  }

  @Override
  public boolean equals(Object that) {
    return that != null && that instanceof RelationKey &&
        this.relationKBType.equals(((RelationKey) that).relationKBType) &&
        this.argumentKeys.equals(((RelationKey) that).argumentKeys) &&
        ((RelationKey) that).argumentKeys.equals(this.argumentKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.relationKBType, this.argumentKeys);
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder("{RelationKey: relationKBType=");
    string.append(relationKBType.getType());
    string.append(", argumentKeys=");
    for (ArgumentKey argumentKey : argumentKeys) {
      string.append(argumentKey.toString()).append(", ");
    }
    string.setLength(string.length()-2);
    string.append('}');
    return string.toString();
  }

  public static Optional<OntType> getKBTypeForRelationType(IType relationType, KBOntologyMap
      kbOntologyMap) throws Exception{
    checkNotNull(relationType);
    return kbOntologyMap.getKBTypeForType(relationType);
  }

  public static Optional<OntType> getKBRoleForType(IType relationType, IType argumentRole,
      KBOntologyMap kbOntologyMap)
      throws Exception {
    checkNotNull(relationType);
    checkNotNull(argumentRole);
    return kbOntologyMap.getKBRoleForType(relationType,
          argumentRole);
  }

//  private static Optional<? extends IType> getTACRoleForKBRole(OntType relationType, OntType
//      argRole) throws Exception{
//    checkNotNull(relationType);
//    checkNotNull(argRole);
//    OntType argRoleType = new OntType(KBOntologyModel.ONTOLOGY_CORE_PREFIX,relationType.getType
//        ()+"."+argRole.getType());
//    return KBOntologyMap.getTACOntologyMap().getTypeForKBType(argRoleType);
//  }

  public static Optional<OntType> getAdditionalTypeForEntityArg(IType relationType, IType
      argType, KBOntologyMap kbOntologyMap) throws Exception{
    return kbOntologyMap.getAdditionalTypeForRoleTarget(
        relationType, argType);

  }
}
