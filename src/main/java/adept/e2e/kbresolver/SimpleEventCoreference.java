package adept.e2e.kbresolver;

import com.bbn.bue.common.StringUtils;
import com.bbn.bue.common.TextGroupImmutable;
import com.bbn.bue.common.files.FileUtils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Resources;

import org.apache.commons.io.Charsets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.immutables.func.Functional;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import adept.common.KBID;
import adept.common.OntType;
import adept.kbapi.KB;
import adept.kbapi.KBDate;
import adept.kbapi.KBEvent;
import adept.kbapi.KBOntologyModel;
import adept.kbapi.KBParameters;
import adept.kbapi.KBPredicateArgument;
import adept.kbapi.KBProvenance;
import adept.kbapi.KBQueryException;
import adept.kbapi.KBRelationArgument;
import adept.kbapi.KBThing;
import adept.kbapi.KBUpdateException;
import scala.Tuple2;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.transform;

/**
 * This class runs a handful of high precision joining rules on the output of the KB ontology. They
 * were designed ad-hoc and closely resemble the rules from the BBN 2016 TAC KBP EAL system. See
 * workshop slides and conference publications for more details.
 *
 * The overall function of this algorithm can be thought of as a process of filling in the third
 * partition of a tripartite graph. This graph consists of partition (A), the event arguments;
 * partition (b), the events themselves; and partition (c) the cross document events, where an edge
 * between (A) and (B) indicates a membership relation, and an edge between (B) and (C) represents
 * an equivalence relation.
 *
 * The edges between (B) and (C) are initially populated by the {@link EventCoreffer} (as are the
 * members of (C)). As the algorithm is designed to run efficiently on Spark, the
 * {@link EventCoreffer} is limited in the constraints it can impose on both its input and its
 * output. As an event in (B) has multiple arguments in (A), which are used (1) to partition all the
 * events in the KB into different Spark partitions, and (2) as a strong indicator that the events
 * are the same. A limitation of this design is that the edges initially output by the
 * {@link EventCoreffer} are not equivalence relations because each event will appear in a different
 * Spark partition and be processed separately, thus the initial links from (B) to (C) are not proper
 * equivalence relations. To make them equivalence relations, we {@link #reify(Map)} the output so
 * for any triplet of events (x), (y) in (C_1) and (y), (z) in (C_2), (C_1) and (C_2) become the same
 * partition.
 *
 * The {@link RealisFilter} step is to filter events of undesirable realis or weak realis confidences.
 */
public final class SimpleEventCoreference extends KBResolverAbstractModule {

  private static Logger log = LoggerFactory.getLogger(SimpleEventCoreference.class);

  private KBParameters kbParameters;
  private JavaSparkContext sc;
  private Properties properties;


  @Override
  public void run() {
    try {
      final RealisFilter realisFilter =
          RealisFilter
              .create(Float.parseFloat(properties.getProperty("event_xdoc_realis_threshold")));
      final int minSharedArgs = Integer.parseInt(properties.getProperty("event_xdoc_min_shared_args"));
      final double maxFillersPerRole = Double.parseDouble(properties.getProperty("event_xdoc_max_fillers_per_role"));
      final int maxFillers = Integer.parseInt(properties.getProperty("event_xdoc_max_fillers"));
      final EventArgMapper eventArgMapper = EventArgMapper.create();
      final ImmutableMultimap<String, String> eventToRoles = FileUtils.loadStringMultimap(
          Resources.asCharSource(
              Resources.getResource("adept/e2e/kb/event-coref/event_ontology.txt"),
              Charsets.UTF_8));
      final ImmutableMultimap<String, String> eventToNonConflictingRoles =
          FileUtils.loadStringMultimap(
              Resources.asCharSource(
                  Resources.getResource(
                      "adept/e2e/kb/event-coref/identifying_event_role_heuristics.txt"),
                  Charsets.UTF_8));
      final ImmutableSet<OntType> eventOntTypes = extractEventOntTypes(eventToRoles.keySet());
      final KB kb = new KB(kbParameters);

      // we iterate over all the event types present in the database and do the xdoc event coref
      // operations to each of them.
      // note: this algorithm does not handle the allowed overlapping event types
      // (e.g. Transaction.Transaction, Transaction.TransferMoney), and ignores:
      // Transaction.Transaction, Contact.Contact, and perhaps Contact.Correspondence depending on DTRA event types.
      log.info("Have {} event types", eventOntTypes.size());
      for (final OntType eventType : eventOntTypes) {
        final EventCoreffer eventCoreffer = EventCoreffer
            .create(eventToNonConflictingRoles.get(eventType.getType()), minSharedArgs,
                maxFillersPerRole, maxFillers);
        corefEventType(realisFilter, eventArgMapper, eventCoreffer, kb, eventType);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Performs the actual coreference operation, and updates the {@code KB} in place with the new
   * events and removes old ones in place.
   */
  private void corefEventType(final RealisFilter realisFilter, final EventArgMapper eventArgMapper,
      final EventCoreffer eventCoreffer, final KB kb, final OntType eventType)
      throws KBQueryException, KBUpdateException {
    final ImmutableSet.Builder<KBID> eventIdsToDelete = ImmutableSet.builder();
    log.info("Merging events for OntType {}", eventType.getType());
    int totalEventsInput = 0;
    int totalEventsOutput = 0;
    Multiset<Integer> inputEventSizes = HashMultiset.create();
    Multiset<Integer> outputEventSizes = HashMultiset.create();

    final List<KBEvent> inputEvents = kb.getEventsByType(eventType);
    if (inputEvents.size() == 0) {
      log.info("Skipping empty event type {}", eventType.getType());
      return;
    }
    ensureSingleOntType(inputEvents);

    final List<KBEventProxy> inputKBEventProxy =
        ImmutableList.copyOf(transform(inputEvents, KBEventProxy.toKBEventProxyFunction()));
    totalEventsInput += inputKBEventProxy.size();
    for (final KBEventProxy kbEvent : inputKBEventProxy) {
      inputEventSizes.add(kbEvent.arguments().size());
    }
    // in practice, the KBRelationArgument is one of KBEntity, KBDate, KBNumber, KBGenericThing
    // we group each KBEvent that shares an argument/role/filler together
    // this is a heuristic so whatever joining we do only considers actual possible joins.
    final JavaPairRDD<KBEventArgumentProxy, Iterable<KBEventProxy>> sharedEventArgs =
        sc.parallelize(inputKBEventProxy)
            .filter(realisFilter)
            .flatMapToPair(eventArgMapper)
            .groupByKey();

    final JavaPairRDD<KBEventArgumentProxy, Iterable<? extends Iterable<KBEventProxy>>>
        corefferedEvents = sharedEventArgs.mapValues(eventCoreffer);
    final Map<KBEventArgumentProxy, Iterable<? extends Iterable<KBEventProxy>>> corefMap =
        corefferedEvents.collectAsMap();
    final Iterable<? extends Iterable<KBEventProxy>> reifiedEvents = reify(corefMap);
    for (final Iterable<KBEventProxy> eventProxyCluster : reifiedEvents) {
      int currentClusterSize = 0;
      final Iterable<KBEvent> eventCluster =
          transform(ImmutableSet.copyOf(eventProxyCluster), KBEventProxy.toKBEventConverter(kb));
      final int size = size(eventCluster);
      checkState(size > 0, "Expected to coref events but have none!");
      if (size == 1) {
        final KBEventProxy singleton = getOnlyElement(eventProxyCluster);
        currentClusterSize += singleton.arguments().size();
        outputEventSizes.add(currentClusterSize);
        totalEventsOutput++;
        log.info("Event of id {} and size {} is not merged", singleton.guid(),
            singleton.arguments().size());
        continue;
      }
      log.info("Merging events of ids {} into a single event",
          Iterables.transform(eventProxyCluster, KBEventProxyFunctions.guid()));
      log.info("Merging events of sizes {} into a single event",
          Iterables.transform(eventProxyCluster, KBEventProxy.argumentSizeFunction()));
      log.info("Merging events of arguments {} into a single event",
          getEventProxyClusterforLogging(eventCluster));
      insertNewEventIntoKB(kb, eventIdsToDelete, outputEventSizes, currentClusterSize,
          eventCluster);

      totalEventsOutput++;
    }

    log.info("For event type {}, total number of input events is {} and output is {}",
        eventType.getType(), totalEventsInput, totalEventsOutput);
    log.info("For event type {}, input KB Event Sizes are {}", eventType.getType(),
        inputEventSizes);
    log.info("For event type {}, output modified KB Event Sizes are {}", eventType.getType(),
        outputEventSizes);

    for (final KBID toDelete : eventIdsToDelete.build()) {
      kb.deleteKBObject(toDelete);
    }
  }

  private void insertNewEventIntoKB(final KB kb, final ImmutableSet.Builder<KBID> eventIdsToDelete,
      final Multiset<Integer> outputEventSizes, int currentClusterSize,
      final Iterable<KBEvent> eventCluster) throws KBQueryException, KBUpdateException {
    // we want there to be exactly one event type in this cluster.
    final OntType singleEventType = ensureSingleOntType(eventCluster);
    // this is an approximation.
    final float confidence = getConfidence(eventCluster);

    final Table<String, KBID, Set<KBRelationArgument>> fillerTable =
        HashBasedTable.create();
    for (final KBEvent e : eventCluster) {
      for (final KBRelationArgument arg : e.getArguments()) {
        final String role = arg.getRole().getType();
        final KBID target = arg.getTarget().getKBID();
        final Set<KBRelationArgument> existingArgs;
        if (fillerTable.contains(role, target)) {
          existingArgs = fillerTable.get(role, target);
        } else {
          existingArgs = Sets.newHashSet();
          fillerTable.put(role, target, existingArgs);
        }
        existingArgs.add(arg);
      }
    }

    final KBEvent.InsertionBuilder insertionBuilder =
        KBEvent.eventInsertionBuilder(singleEventType, confidence);
    for (final Table.Cell<String, KBID, Set<KBRelationArgument>> c : fillerTable.cellSet()) {
      final float argConfidence = getConfidence(c.getValue());
      final ImmutableSet.Builder<KBProvenance> provenances = ImmutableSet.builder();
      final ImmutableSet.Builder<OntType> roleType = ImmutableSet.builder();
      final ImmutableSet.Builder<KBPredicateArgument> targets = ImmutableSet.builder();

      for (final KBRelationArgument arg : c.getValue()) {
        provenances.addAll(arg.getProvenances());
        roleType.add(arg.getRole());
        targets.add(arg.getTarget());
      }

      final OntType role = Iterables.getOnlyElement(roleType.build());
      final KBPredicateArgument target = Iterables.getOnlyElement(targets.build());
      final KBRelationArgument.InsertionBuilder argBuilder =
          KBRelationArgument.insertionBuilder(role, target, argConfidence);
      for (final KBProvenance provenance : provenances.build()) {
        argBuilder.addProvenance(provenance.modifiedCopyInsertionBuilder());
      }
      insertionBuilder.addArgument(argBuilder);
    }

    for (final KBEvent e : eventCluster) {
      currentClusterSize += e.getArguments().size();
      for (final KBProvenance provenance : e.getProvenances()) {
        insertionBuilder.addProvenance(provenance.modifiedCopyInsertionBuilder());
      }
    }
    outputEventSizes.add(currentClusterSize);
    if (size(eventCluster) > 1) {
      extractRealisDistribution(eventCluster, insertionBuilder);
      for (final KBEvent e : eventCluster) {
        eventIdsToDelete.add(e.getKBID());
      }
      insertionBuilder.insert(kb);
    }
  }

  // this performs a BFS across each event so that the coreference relation between events is an actual equivalence relation.
  private Iterable<? extends Iterable<KBEventProxy>> reify(
      final Map<KBEventArgumentProxy, Iterable<? extends Iterable<KBEventProxy>>> corefMap) {
    final LinkedHashMultimap<KBEventProxy, KBEventProxy> initialMap = LinkedHashMultimap.create();
    for (final Map.Entry<KBEventArgumentProxy, Iterable<? extends Iterable<KBEventProxy>>> e : corefMap
        .entrySet()) {
      for (final Iterable<KBEventProxy> cluster : e.getValue()) {
        for (final KBEventProxy member : cluster) {
          initialMap.putAll(member, cluster);
        }
      }
    }

    // a more efficient implementation would memoize these results
    final ImmutableSet<KBEventProxy> allEvents =
        ImmutableSet.copyOf(concat(initialMap.keySet(), initialMap.values()));
    final ImmutableSet.Builder<ImmutableSet<KBEventProxy>> ret = ImmutableSet.builder();
    for (final KBEventProxy node : allEvents) {
      ret.add(reachable(initialMap, node));
    }

    return ret.build();
  }


  private <T> ImmutableSet<T> reachable(final Multimap<T, T> nodeMap, T start) {
    final Set<T> visited = Sets.newHashSet();
    final Queue<T> toVisit = new ArrayDeque<>();
    toVisit.add(start);
    while (!toVisit.isEmpty()) {
      final T next = checkNotNull(toVisit.poll());
      visited.add(next);
      // we do the extra computation here to attempt to avoid a little GC overhead..
      toVisit.addAll(Sets.difference(ImmutableSet.copyOf(nodeMap.get(next)), visited));
    }
    return ImmutableSet.copyOf(visited);
  }


  private static float getConfidence(final Iterable<? extends KBPredicateArgument> args)
      throws KBQueryException {
    float weightedConfidence = 0.0f;
    int accumulatedWeight = 0;
    for (final KBPredicateArgument arg : args) {
      final int weight = arg.getProvenances().size();
      if (arg instanceof KBEvent) {
        weightedConfidence += ((KBEvent) arg).getConfidence() * weight;
      } else if (arg instanceof KBRelationArgument) {
        weightedConfidence += ((KBRelationArgument) arg).getConfidence() * weight;
      } else {
        throw new RuntimeException("Wrong type for getting confidence " + arg.getClass());
      }
      accumulatedWeight += weight;
    }
    return weightedConfidence / accumulatedWeight;
  }

  private static OntType ensureSingleOntType(final Iterable<KBEvent> eventCluster) {
    final ImmutableSet.Builder<OntType> ontTypesB = ImmutableSet.builder();
    for (final KBEvent e : eventCluster) {
      ontTypesB.add(e.getType());
    }
    final ImmutableSet<OntType> ontTypes = ontTypesB.build();
    checkArgument(ontTypes.size() == 1,
        "Expected exactly one OntType but have " + getOntTypesForLoging(ontTypes) + " for events "
            + getIdSForLogging(eventCluster));
    return getOnlyElement(ontTypes);
  }

  private static String getOntTypesForLoging(final Iterable<OntType> types) {
    final ImmutableSet<OntType> minimalTypes = ImmutableSet.copyOf(types);
    final ImmutableList.Builder<String> typeStrings = ImmutableList.builder();
    for (final OntType ontType : minimalTypes) {
      typeStrings.add(ontType.getType());
    }
    return StringUtils.commaJoiner().join(typeStrings.build());
  }

  private static ImmutableSet<String> getIdSForLogging(final Iterable<KBEvent> events) {
    final ImmutableSet.Builder<String> kbIDs = ImmutableSet.builder();
    for (final KBEvent kbEvent : events) {
      kbIDs.add(kbEvent.getKBID().toString());
    }
    return kbIDs.build();
  }

  private ImmutableSet<ImmutableSet<String>> getEventProxyClusterforLogging(
      final Iterable<KBEvent> eventProxyCluster) throws KBQueryException {
    final ImmutableSet.Builder<ImmutableSet<String>> ret = ImmutableSet.builder();
    for (final KBEvent event : eventProxyCluster) {
      final ImmutableSet.Builder<String> collectedArgs = ImmutableSet.builder();
      for (final KBRelationArgument arg : event.getArguments()) {
        final String repr;
        final KBPredicateArgument argTarget = arg.getTarget();
        if (argTarget instanceof KBDate) {
          repr = ((KBDate) argTarget).getTimexString();
        } else if (argTarget instanceof KBThing) {
          repr = ((KBThing) argTarget).getCanonicalString();
        } else {
          log.warn("Unhandled KBArgTarget type {} for {}", argTarget.getClass(), argTarget);
          repr = "??";
        }
        collectedArgs.add(String
            .format("%s/%s/%s/%s", event.getType().getType(), arg.getRole().getType(),
                argTarget.getKBID(), repr));
      }
      ret.add(collectedArgs.build());
    }
    return ret.build();
  }

  private static void extractRealisDistribution(final Iterable<KBEvent> eventCluster,
      final KBEvent.InsertionBuilder insertionBuilder) throws KBQueryException {
    int aggregatedWeight = 0;
    final LinkedHashMap<OntType, Float> weightedRealisDistribution = Maps.newLinkedHashMap();
    for (final KBEvent e : eventCluster) {
      final int weight = e.getProvenances().size();
      aggregatedWeight += weight;
      for (final Map.Entry<OntType, Float> realis : e.getRealisTypes().entrySet()) {
        final float realisContribution = realis.getValue() * weight;
        final float oldRealisTotal = weightedRealisDistribution.getOrDefault(realis.getKey(), 0.0f);
        weightedRealisDistribution.put(realis.getKey(), oldRealisTotal + realisContribution);
      }
    }
    for (final OntType realis : weightedRealisDistribution.keySet()) {
      insertionBuilder
          .addRealisType(realis, weightedRealisDistribution.get(realis) / aggregatedWeight);
    }
  }

  private static ImmutableSet<OntType> extractEventOntTypes(final ImmutableSet<String> types) {
    final ImmutableSet.Builder<OntType> ret = ImmutableSet.builder();
    for (final String et : types) {
      ret.add(new OntType(KBOntologyModel.ONTOLOGY_CORE_PREFIX, et));
    }
    return ret.build();
  }

  @Override
  public void initialize(final JavaSparkContext sc, final Properties properties,
      final KBParameters kbParameters)
      throws InvalidPropertiesFormatException {
    this.sc = sc;
    this.kbParameters = kbParameters;
    this.properties = properties;
  }

  /**
   * A well-formed cross-document event coreference algorithm does not coreference events of
   * non-Actual realis, as it's not particularly meaningful to coreference Generic events, and this
   * is an under-defined operation for events of realis type Other.
   *
   * When this algorithm was developed, it seemed as though most events in the KB didn't come with
   * an associated realis. General tuning guidance in that case is "just 0.0 and move on".
   */
  private static final class RealisFilter implements Function<KBEventProxy, Boolean> {

    private final float realisThreshold;
    private final static OntType ACTUAL =
        new OntType(KBOntologyModel.ONTOLOGY_CORE_PREFIX, "Actual");

    private RealisFilter(final float realisThreshold) {

      this.realisThreshold = realisThreshold;
    }

    public static RealisFilter create(final float realisThreshold) {
      return new RealisFilter(realisThreshold);
    }

    @Override
    public Boolean call(final KBEventProxy kbEvent) throws Exception {
      if (!kbEvent.realisTypes().containsKey(ACTUAL)) {
        log.info("No distribution information for {} in event {}, have {}", ACTUAL, kbEvent.guid(),
            kbEvent.realisTypes());
      }
      return kbEvent.realisTypes().getOrDefault(ACTUAL, 0.0f) >= realisThreshold;
    }
  }

  private static final class EventArgMapper
      implements PairFlatMapFunction<KBEventProxy, KBEventArgumentProxy, KBEventProxy> {

    public static EventArgMapper create() {
      return new EventArgMapper();
    }

    @Override
    public Iterator<Tuple2<KBEventArgumentProxy, KBEventProxy>> call(final KBEventProxy event)
        throws Exception {
      final ImmutableList.Builder<Tuple2<KBEventArgumentProxy, KBEventProxy>> ret =
          ImmutableList.builder();
      for (KBEventArgumentProxy arg : event.arguments()) {
        ret.add(Tuple2.apply(arg, event));
      }
      return ret.build().iterator();
    }
  }

  /**
   * A very simple event coreference class; it groups an {@link Iterable} of potentially compatible
   * events into an {@link Iterable} of {@link Iterable}s of events that it deems to be
   * substantially similar.
   *
   * This implementation does not return singleton events; any events that are not touched in its
   * operation get dropped from its output (they are already KBEvents so we don't need to modify
   * them). It is not perfect or the most well-formed thing one might imagine doing.
   *
   * General guidance on <ul>
   *   <li>setting the minSharedArgs: whatever value seems sane to you. In this implementation's details, you'll find at least one before coreffering them. 2 seemed a reasonable number based on cursory examination of the output.</li>
   *   <li>setting the maxFillersPerRole: this is a hack to keep an event from becoming "too noisy"; 4.0 worked for visual inspection purposes and may or may not work for future uses</li>
   *   <li>setting the maxFillers: this is also a hack to keep an event from becoming "too big"; 8 worked for visual inspection purposes and may or may not work for future uses.</li>
   * </ul>
   */
  private static final class EventCoreffer
      implements Function<Iterable<KBEventProxy>, Iterable<? extends Iterable<KBEventProxy>>> {

    private static final Logger log = LoggerFactory.getLogger(EventCoreffer.class);

    // the roles which *must* be compatible for this event
    private final ImmutableSet<String> eventRoles;
    // arbitrary constants to prevent too much overlap
    private final int minSharedArgs;
    private final double maxFillersPerRole;
    private final int maxFillers;

    private EventCoreffer(final ImmutableSet<String> eventRoles, final int minSharedArgs,
        final double maxFillersPerRole, final int maxFillers) {
      this.eventRoles = checkNotNull(eventRoles);
      checkArgument(eventRoles.size() > 0, "Must provide a set of event roles that are considered for overlap.");
      this.minSharedArgs = minSharedArgs;
      checkArgument(minSharedArgs >= 0, "Must provide a minimum number of shared arguments greater than or equal to 0.");
      this.maxFillersPerRole = maxFillersPerRole;
      checkArgument(maxFillersPerRole >= 0.0, "Must provide a positive or 0 number of maximum fillers allowed per role. Set to something really high to allow any events get merged, 0 to let none get merged.");
      this.maxFillers = maxFillers;
      checkArgument(maxFillers >= 0, "Must provide a maximum number of fillers allowed in the given event");
    }

    public static EventCoreffer create(
        final Iterable<String> eventRoles,
        final int minSharedArgs,
        final double maxFillersPerRole,
        final int maxFillers) {
      return new EventCoreffer(ImmutableSet.copyOf(eventRoles), minSharedArgs, maxFillersPerRole,
          maxFillers);
    }

    @Override
    public Iterable<? extends Iterable<KBEventProxy>> call(final Iterable<KBEventProxy> events)
        throws Exception {
      final ImmutableSet.Builder<ImmutableSet<KBEventProxy>> ret = ImmutableSet.builder();
      // collapse events as we iterate and remove them from consideration.
      // for each event in our list, we attach it to as many other events as possible (up to the input ordering), and do our best to retain the input ordering.
      // we remove all the events that get coreferenced.
      final List<KBEventProxy> activeEventsSet = Lists.newArrayList(events);
      // -1 just skips an inner no-op loop
      for (int i = 0; i < activeEventsSet.size() - 1; i++) {
        // create a "prototype" event from the current one
        // LinkedHash* for determinism
        final LinkedHashSet<KBEventProxy> joinedEventsInPrototype = Sets.newLinkedHashSet();
        // role string -> fillers
        final LinkedHashMultimap<String, KBID> rolesInPrototype = LinkedHashMultimap.create();
        rolesInPrototype.putAll(eventRolesToFiller(activeEventsSet.get(i)));
        for (int j = i + 1; j < activeEventsSet.size(); j++) {
          final ImmutableMultimap<String, KBID> candidateRoleFillers =
              eventRolesToFiller(activeEventsSet.get(j));
          // this is very permissive since we require *any* overlap.
          boolean comptabile = true;
          // we also need to ensure that every event actually overlaps, otherwise events with complementary sets of roles overlap won't get coreffed.
          boolean anyJoined = false;
          final Iterable<String> relevantSharedRoles = Sets.intersection(eventRoles,
              Sets.intersection(rolesInPrototype.keySet(), candidateRoleFillers.keySet()));
          int totalShared = 0;
          for (final String sharedRole : relevantSharedRoles) {
            final ImmutableSet<KBID> sharedFillers =
                Sets.intersection(rolesInPrototype.get(sharedRole),
                    ImmutableSet.copyOf(candidateRoleFillers.get(sharedRole))).immutableCopy();
            totalShared += sharedFillers.size();
            if (sharedFillers.size() == 0) {
              comptabile = false;
              break;
            } else {
              anyJoined = true;
            }
          }
          if (totalShared < minSharedArgs) {
            comptabile = false;
            anyJoined = false;
          }
          if (comptabile && anyJoined) {
            final Multimap<String, KBID> rolesInResultingPrototype =
                LinkedHashMultimap.create(rolesInPrototype);
            rolesInResultingPrototype.putAll(candidateRoleFillers);
            final double averageFillersPerRoleInResultingPrototype =
                ImmutableSet.copyOf(rolesInResultingPrototype.values()).size()
                    / ((double) rolesInPrototype.keySet().size());
            if (averageFillersPerRoleInResultingPrototype <= maxFillersPerRole
                && rolesInResultingPrototype.values().size() < maxFillers) {
              joinedEventsInPrototype.add(activeEventsSet.get(i));
              joinedEventsInPrototype.add(activeEventsSet.get(j));
              rolesInPrototype.putAll(candidateRoleFillers);
              activeEventsSet.remove(j);
              j--;
            } else {
              log.info("Blocked joining {} and {} due to too many resulting fillers per role: {}",
                  joinedEventsInPrototype, activeEventsSet.get(j),
                  averageFillersPerRoleInResultingPrototype);
            }
          }
        }
        if(joinedEventsInPrototype.size() > 0) {
          ret.add(ImmutableSet.copyOf(joinedEventsInPrototype));
        }
      }
      return ret.build();
    }

    private ImmutableSetMultimap<String, KBID> eventRolesToFiller(final KBEventProxy event) {
      // this is more cleanly done with functional elements in guava style but it doesn't look like the ADEPT API provides those.
      // ImmutableSetMultimap for deterministic ordering of keys and values.
      final ImmutableSetMultimap.Builder<String, KBID> ret = ImmutableSetMultimap.builder();
      for (final KBEventArgumentProxy arg : event.arguments()) {
        ret.put(arg.role().getType(), arg.targetId());
      }
      return ret.build();
    }

  }

  /**
   * A subset of the information necessary to represent an Event. This class exists to run on Spark.
   */
  @TextGroupImmutable
  @Functional
  @Value.Immutable
  abstract static class KBEventProxy implements Serializable {

    abstract KBID guid();

    abstract OntType eventType();

    abstract ImmutableMap<OntType, Float> realisTypes();

    abstract ImmutableSet<KBEventArgumentProxy> arguments();

    private enum ArgumentSizeFunction implements com.google.common.base.Function<KBEventProxy, Integer> {
      INSTANCE;

      @Override
      public Integer apply(final KBEventProxy kbEventProxy) {
        checkNotNull(kbEventProxy);
        return kbEventProxy.arguments().size();
      }
    }

    static com.google.common.base.Function<KBEventProxy, Integer> argumentSizeFunction() {
      return ArgumentSizeFunction.INSTANCE;
    }

    public static KBEventProxy fromKBEvent(final KBEvent kbEvent) {
      final Builder ret = new Builder();
      ret.guid(kbEvent.getKBID());
      ret.eventType(kbEvent.getType());
      ret.realisTypes(kbEvent.getRealisTypes());
      for(final KBRelationArgument arg: kbEvent.getArguments()) {
        ret.addArguments(KBEventArgumentProxy.fromKBRelationArgument(arg));
      }
      return ret.build();
    }

    private enum ToKBEventProxy implements com.google.common.base.Function<KBEvent, KBEventProxy> {
      INSTANCE {
        @Override
        public KBEventProxy apply(final KBEvent v1) {
          return fromKBEvent(v1);
        }
      };
    }

    static com.google.common.base.Function<KBEvent, KBEventProxy> toKBEventProxyFunction() {
      return ToKBEventProxy.INSTANCE;
    }

    KBEvent toKBEvent(final KB kb) throws KBQueryException {
      return kb.getEventById(guid());
    }

    static com.google.common.base.Function<KBEventProxy, KBEvent> toKBEventConverter(final KB kb) {
      return new com.google.common.base.Function<KBEventProxy, KBEvent>() {
        @Override
        public KBEvent apply(final KBEventProxy v1) {
          try {
            return v1.toKBEvent(kb);
          } catch (KBQueryException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }


    public static final class Builder extends ImmutableKBEventProxy.Builder {

    }
  }

  /**
   * A subset of the information to represent an event argument. The most salient points are the role and the target. This is a class that exists for running on Spark.
   */
  @TextGroupImmutable
  @Functional
  @Value.Immutable
  abstract static class KBEventArgumentProxy implements Serializable {

    abstract OntType role();

    abstract KBID targetId();

    public static KBEventArgumentProxy fromKBRelationArgument(final KBRelationArgument arg) {
      final Builder ret = new Builder();
      ret.role(arg.getRole());
      ret.targetId(arg.getTarget().getKBID());
      return ret.build();
    }

    public static final class Builder extends ImmutableKBEventArgumentProxy.Builder {

    }
  }

}
