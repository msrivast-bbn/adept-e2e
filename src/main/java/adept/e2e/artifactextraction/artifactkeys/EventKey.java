package adept.e2e.artifactextraction.artifactkeys;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import adept.common.DocumentEvent;
import adept.common.DocumentEventArgument;
import adept.common.Entity;
import adept.common.GenericThing;
import adept.common.HltContentContainer;
import adept.common.IType;
import adept.common.Item;
import adept.common.OntType;
import adept.common.OntTypeFactory;
import adept.common.TemporalValue;
import adept.kbapi.KBOntologyMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by msrivast on 9/28/16.
 */
public final class EventKey implements Serializable {

  private static final long serialVersionUID = 168301640087908413L;
  OntType eventKBType;
  Optional<OntType> realisKBType;
  ImmutableSet<ArgumentKey> argumentKeys;

  private EventKey(OntType eventKBType, Optional<OntType> realisKBType,
      ImmutableSet<ArgumentKey> argumentKeys) {
    this.eventKBType = eventKBType;
    this.realisKBType = realisKBType;
    this.argumentKeys = argumentKeys;
  }

  public static Optional<EventKey> getKey(DocumentEvent docEvent,
      HltContentContainer hltCC, KBOntologyMap kbOntologyMap, KBOntologyMap entityOntologyMap)
      throws Exception {
    Optional<OntType> eventKBType = getKBTypeForEventType(docEvent.getEventType(),kbOntologyMap);
    if (!eventKBType.isPresent() || docEvent.getArguments() == null ||
        docEvent.getArguments().size() == 0) {
      return Optional.absent();
    }
    Optional<OntType> realisKBType = getRealisKBType(docEvent,kbOntologyMap);
    Set<ArgumentKey> argumentKeys = new HashSet<>();
    for (DocumentEventArgument docArgument : docEvent.getArguments()) {
      Optional<OntType> kbRole = getKBRoleForType(docEvent.getEventType(),docArgument.getRole(),
          kbOntologyMap);
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
          argumentKeys.add(ArgumentKey.getKey(kbRole.get(), key, getAdditionalTypeForEntityArg
              (docEvent.getEventType(),docArgument.getRole(),kbOntologyMap)));
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
    return Optional.of(new EventKey(eventKBType.get(), realisKBType, ImmutableSet.copyOf
        (argumentKeys)));
  }

  public static Optional<EventKey> getKey(IType eventIType,
      Optional<OntType> realisKBType, ImmutableSet<ArgumentKey> argumentKeys, KBOntologyMap
      kbOntologyMap) throws Exception {
    checkNotNull(eventIType);
    checkNotNull(argumentKeys);
    Optional<OntType> eventKBType = getKBTypeForEventType(eventIType,kbOntologyMap);
    if(!eventKBType.isPresent()){
      return Optional.absent();
    }
    //Commenting out the below check for arg-roles, since the reverse-ontology mapping does not
    // work as expected
//    boolean kbTypeOrRoleNotFound = false;
//    for (ArgumentKey argKey : argumentKeys) {
//      //Make sure that arg-roles are compatible with this event-type
//      //and have a mapping into KB-roles
//      Optional<? extends IType> argRole = getRichERERoleForKBRole(eventKBType.get(),argKey.kbRole
//          ());
//      if (!argRole.isPresent() || !getKBRoleForType(eventIType,argRole.get()).isPresent()) {
//        kbTypeOrRoleNotFound = true;
//        break;
//      }
//    }
//    if (kbTypeOrRoleNotFound) {
//      return Optional.absent();
//    }
    return Optional.fromNullable(new EventKey(eventKBType.get(), realisKBType, argumentKeys));
  }

  public OntType getEventKBType() {
    return eventKBType;
  }

  public Set<ArgumentKey> getArgumentKeys() {
    return argumentKeys;
  }

  public Optional<OntType> getRealisKBType() {
    return realisKBType;
  }

  @Override
  public boolean equals(Object that) {
    boolean match = that != null && that instanceof EventKey;
    if (!match) {
      return match;
    }

    if (this.realisKBType.isPresent() && ((EventKey) that).realisKBType.isPresent()) {
      match = this.realisKBType.get().equals(((EventKey) that).realisKBType.get());
    } else if (!this.realisKBType.isPresent() && !((EventKey) that).realisKBType.isPresent()) {
      //NO OP
    } else {
      return false;
    }
    return match && this.eventKBType.equals(((EventKey) that).eventKBType) &&
        this.argumentKeys.equals(((EventKey) that).argumentKeys) &&
        ((EventKey) that).argumentKeys.equals(this.argumentKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.eventKBType, this.realisKBType, this.argumentKeys);
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder("{EventKey: eventKBType=");
    string.append(eventKBType.getType());
    if (realisKBType.isPresent()) {
      string.append(", realisKBType=");
      string.append(realisKBType.get().getType());
    }
    string.append(", argumentKeys=");
    for (ArgumentKey argumentKey : argumentKeys) {
      string.append(argumentKey.toString()).append(", ");
    }
    string.setLength(string.length()-2);
    string.append('}');
    return string.toString();
  }

  public static Optional<OntType> getKBTypeForEventType(IType eventType, KBOntologyMap
      kbOntologyMap) throws Exception{
    checkNotNull(eventType);
    return kbOntologyMap.getKBTypeForType(
          eventType);
  }

  private static IType getEreEventType(IType eventType){
    IType ereEventType = eventType;
    if (eventType.getType().contains(".") ||
        eventType.getType().contains("-")) {
      ereEventType = (OntType) OntTypeFactory.getInstance().getType("EVENT_ERE",
          eventType.getType().substring(eventType.getType().indexOf(".") + 1)
              .replace("-", ""));
    }
    return ereEventType;
  }

  public static Optional<OntType> getRealisKBType(DocumentEvent docEvent, KBOntologyMap
      kbOntologyMap) throws Exception{
    checkNotNull(docEvent);
    Optional<OntType> realisKBType = Optional.absent();
    if(docEvent.getScoredUnaryAttributes()!=null&&!docEvent.getScoredUnaryAttributes().isEmpty()){
      IType attribute = docEvent.getScoredUnaryAttributes().keySet().iterator().next();
      realisKBType =  kbOntologyMap.getKBTypeForType(attribute);
    }
    return realisKBType;
  }

  public static Optional<OntType> getKBRoleForType(IType eventType, IType argumentRole,
      KBOntologyMap kbOntologyMap) throws
    Exception{
    checkNotNull(eventType);
    checkNotNull(argumentRole);
    return kbOntologyMap.getKBRoleForType(eventType,
          argumentRole);
  }

//  private static Optional<? extends IType> getRichERERoleForKBRole(OntType relationType, OntType
//      argRole) throws Exception{
//    checkNotNull(relationType);
//    checkNotNull(argRole);
//    OntType argRoleType = new OntType(KBOntologyModel.ONTOLOGY_CORE_PREFIX,relationType.getType
//        ()+"."+argRole.getType());
//    return KBOntologyMap.getRichEREOntologyMap().getTypeForKBType(argRoleType);
//  }

  public static Optional<OntType> getAdditionalTypeForEntityArg(IType relationType, IType
      argType, KBOntologyMap kbOntologyMap) throws Exception{
    return kbOntologyMap.getAdditionalTypeForRoleTarget(
        relationType, argType);

  }
}
