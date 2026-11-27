/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.glossary.TermPublication.State;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper.Head;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper.Version;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.service.DomainRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.TargetType;
import org.apache.gravitino.storage.relational.service.GlossaryTermDomainService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService.ScopedTerm;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.service.GlossaryTermVersions;
import org.apache.gravitino.storage.relational.service.GlossaryTermVersions.Snapshot;
import org.apache.gravitino.storage.relational.service.LiveEndpointService;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Manages live glossary definitions. Callers supply the metalake scope for every operation. */
public final class GlossaryManager {
  private static final GlossaryTermDomainService DOMAINS = GlossaryTermDomainService.getInstance();
  private static final GlossaryTermBindingService BINDINGS =
      GlossaryTermBindingService.getInstance();
  private static final int TARGET_ID_BATCH_SIZE = 500;
  private static final Predicate<GlossaryTermEntity> ALL_TERMS = Predicates.alwaysTrue();
  static final int MAX_HIERARCHY_DEPTH = 20;
  private final GlossaryTermRelService edges = GlossaryTermRelService.getInstance();
  private final Predicate<GlossaryTermEntity> readable;
  private final int depthLimit;
  private final GlossaryTermMetaService terms = GlossaryTermMetaService.getInstance();
  private final IdGenerator ids;
  private final boolean authorizationEnabled;
  private final TermChangeHandler handler;
  private final TermPublicationPolicy policy;

  /** Creates a manager using the server-owned policy for all definition writes. */
  public GlossaryManager(
      IdGenerator ids, boolean authorizationEnabled, TermPublicationPolicy policy) {
    this(
        ids,
        authorizationEnabled,
        (before, after, persist) -> persist.run(),
        ALL_TERMS,
        MAX_HIERARCHY_DEPTH,
        policy);
  }

  /** Creates a manager that applies effective changes immediately. */
  public GlossaryManager(IdGenerator ids, boolean authorizationEnabled) {
    this(ids, authorizationEnabled, (before, after, persist) -> persist.run());
  }

  GlossaryManager(IdGenerator ids, boolean authorizationEnabled, TermChangeHandler handler) {
    this(ids, authorizationEnabled, handler, ALL_TERMS, MAX_HIERARCHY_DEPTH);
  }

  GlossaryManager(
      IdGenerator ids,
      boolean authorizationEnabled,
      TermChangeHandler handler,
      Predicate<GlossaryTermEntity> readable,
      int depthLimit) {
    this(ids, authorizationEnabled, handler, readable, depthLimit, lake -> false);
  }

  GlossaryManager(
      IdGenerator ids,
      boolean authorizationEnabled,
      TermChangeHandler handler,
      Predicate<GlossaryTermEntity> readable,
      int depthLimit,
      TermPublicationPolicy policy) {
    this.policy = Objects.requireNonNull(policy);
    this.readable = readable;
    this.depthLimit = depthLimit;
    this.ids = ids;
    this.authorizationEnabled = authorizationEnabled;
    this.handler = handler;
  }

  /**
   * Applies an additional visibility filter to every term this manager reads or writes, including
   * the term an edit targets and the terms a write references. A term the filter rejects is
   * reported as missing. Filters compose: a term must pass every filter added to this manager.
   */
  public GlossaryManager withTermVisibility(Predicate<GlossaryTermEntity> visibility) {
    return new GlossaryManager(
        ids, authorizationEnabled, handler, readable.and(visibility), depthLimit, policy);
  }

  record SnapshotTerms(long metalakeId, GlossaryAccess access, List<GlossaryTermEntity> terms) {}

  boolean isTermVisible(
      GlossaryTermEntity term, GlossaryAccess access, @Nullable GlossaryTerm.Status status) {
    return (status == null || status == term.status())
        && access.canRead(term)
        && readable.test(term);
  }

  boolean hasTermVisibilityFilter() {
    return readable != ALL_TERMS;
  }

  GlossaryBindingAccess bindingAccess(String metalake, long lake) {
    return GlossaryBindingAccess.forBindingList(metalake, authorizationEnabled, lake);
  }

  GlossaryAccess accessForMetalake(String metalake) {
    return GlossaryAccess.forRequest(metalake, authorizationEnabled);
  }

  /** Selects the direction of a hierarchy walk or the symmetric direct related terms. */
  public enum Direction {
    /** Follow child-to-parent edges. */
    BROADER,
    /** Follow parent-to-child edges. */
    NARROWER,
    /** Return direct related terms without transitive expansion. */
    RELATED
  }

  /**
   * Lists visible neighbors; broader and narrower walks may optionally expand transitively. Both
   * visibility and the optional status filter apply before expanding each frontier.
   *
   * @throws NoSuchTermException if the root is missing, hidden, or excluded by the status filter
   * @throws IllegalArgumentException if a transitive RELATED walk is requested
   * @throws IllegalStateException if the calling thread already owns a SQL session
   */
  public List<GlossaryTermEntity> listRelations(
      String metalake,
      String name,
      Direction direction,
      boolean transitive,
      @Nullable GlossaryTerm.Status status) {
    Preconditions.checkNotNull(direction, "direction");
    Preconditions.checkArgument(
        direction != Direction.RELATED || !transitive, "RELATED is not transitive");
    return SqlSessions.readSnapshot(
        () -> listRelationsInSnapshot(metalake, name, direction, transitive, status).terms());
  }

  SnapshotTerms listRelationsInSnapshot(
      String metalake,
      String name,
      Direction direction,
      boolean transitive,
      @Nullable GlossaryTerm.Status status) {
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, name);
    long lake = termReadMetalakeId(ident);
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    GlossaryTermEntity root = loadRootForView(ident, lake, access);
    Predicate<GlossaryTermEntity> visible = term -> isTermVisible(term, access, status);
    if (!visible.test(root)) {
      throw missingRelationRoot(ident);
    }
    List<GlossaryTermEntity> result = new ArrayList<>();
    Set<Long> seen = new HashSet<>(Set.of(root.id()));
    Set<Long> frontier = Set.of(root.id());
    for (; ; ) {
      Set<Long> next = new LinkedHashSet<>();
      List<GlossaryTermRelPO> links =
          switch (direction) {
            case BROADER -> edges.listBroaderEdges(lake, frontier);
            case NARROWER -> edges.listNarrowerEdges(lake, frontier);
            case RELATED -> edges.listRelatedEdges(lake, frontier);
          };
      for (GlossaryTermRelPO link : links) {
        long id =
            direction == Direction.NARROWER
                    || (direction == Direction.RELATED && link.getToTermId().equals(root.id()))
                ? link.getFromTermId()
                : link.getToTermId();
        if (seen.add(id)) {
          next.add(id);
        }
      }
      frontier = new LinkedHashSet<>();
      for (GlossaryTermEntity term : terms.listTermsByIds(root.namespace(), lake, next)) {
        if (visible.test(term)) {
          result.add(term);
          frontier.add(term.id());
        }
      }
      if (!transitive || frontier.isEmpty()) {
        break;
      }
    }
    return new SnapshotTerms(lake, access, visibleDomains(result, access));
  }

  /**
   * Lists the visible term definitions in the given metalake.
   *
   * @throws NoSuchMetalakeException if the metalake does not exist
   * @throws IllegalStateException if authorization is enabled and the caller already has a SQL
   *     session
   */
  public List<GlossaryTermEntity> listTerms(String metalake) {
    return listTerms(metalake, null);
  }

  /**
   * Lists the visible term definitions in the given metalake. With a domain, lists only the terms
   * of that domain and of its subdomains that the caller can see. A caller without glossary view
   * access receives an empty list before the domain filter is evaluated.
   *
   * @throws NoSuchMetalakeException if the metalake does not exist
   * @throws NoSuchDomainException if the domain does not exist or is a draft hidden from the caller
   * @throws ForbiddenException if the domain is published but the caller cannot use it
   * @throws IllegalStateException if authorization is enabled or a domain is given, and the caller
   *     already has a SQL session
   */
  public List<GlossaryTermEntity> listTerms(String metalake, @Nullable String domain) {
    if (!authorizationEnabled && domain == null) {
      List<GlossaryTermEntity> all = loadTerms(metalake);
      all.removeIf(term -> !readable.test(term));
      return all;
    }
    return SqlSessions.readSnapshot(
        () ->
            new GlossaryTermQuery(this, metalake, domain, null, null, Map.of()).definitions(true));
  }

  /**
   * Loads a definition by normalized name.
   *
   * @throws NoSuchTermException if the term is hidden or the term or its metalake does not exist
   * @throws IllegalStateException if authorization is enabled and the caller already has a SQL
   *     session
   */
  public GlossaryTermEntity getTerm(String metalake, String name) {
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, name);
    if (!authorizationEnabled) {
      return requireVisible(ident, relationRoot(ident).term());
    }
    return SqlSessions.readSnapshot(() -> getTermInSnapshot(metalake, name, null).terms().get(0));
  }

  SnapshotTerms getTermInSnapshot(
      String metalake, String name, @Nullable GlossaryTerm.Status status) {
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, name);
    long lake = termReadMetalakeId(ident);
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    GlossaryTermEntity term = loadRootForView(ident, lake, access);
    if (!isTermVisible(term, access, status)) {
      throw missingRelationRoot(ident);
    }
    return new SnapshotTerms(lake, access, List.of(visibleDomains(term, access)));
  }

  /**
   * Creates a published definition or an unpublished candidate under the server policy. When
   * authorization is enabled the creator must be a registered user in the metalake. The definition
   * and creator ownership share a transaction. Callers that open an enclosing transaction must roll
   * it back if creation fails.
   *
   * @throws NoSuchMetalakeException if the metalake does not exist
   * @throws ForbiddenException if the caller lacks effective create access
   * @throws NoSuchTermException if a referenced term is missing or hidden
   * @throws IllegalArgumentException if the proposed hierarchy or related terms are invalid
   * @throws NoSuchEntityException if the required creator is missing
   * @throws TermAlreadyExistsException if the term name or source reference is already in use
   */
  public GlossaryTermEntity createTerm(String metalake, TermSpec spec) {
    Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
    return terms.withHierarchyLock(
        ns,
        lake -> {
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          access.requireCreate();
          GlossaryTermEntity term = TermEdits.create(ids.nextId(), ns, spec);
          State state =
              resolvePublicationState(
                  policy.requiresApproval(metalake), spec.status().orElse(null));
          Preconditions.checkArgument(
              state == State.PUBLISHED || term.status() != GlossaryTerm.Status.DEPRECATED,
              "Omit status to submit a new term or specify DRAFT to save it.");
          List<TermRelations.Edit> edits = new ArrayList<>();
          for (String name : spec.broader()) {
            edits.add(new TermRelations.Edit(RelationType.BROADER, name, true));
          }
          for (String name : spec.related()) {
            edits.add(new TermRelations.Edit(RelationType.RELATED, name, true));
          }
          List<TermDomains.Edit> domainEdits = new ArrayList<>();
          for (String domain : spec.domains()) {
            domainEdits.add(new TermDomains.Edit(domain, true));
          }
          return withRelationPlan(
              lake,
              term,
              edits,
              access,
              true,
              ignored -> {},
              (fresh, plan) -> {
                TermDomains domains = planDomains(lake, term, domainEdits, access);
                GlossaryTermEntity created =
                    domains.changed() ? TermEdits.withDomains(term, domains.names()) : term;
                terms.validateTermNameSynonymsAndSourceRef(created);
                checkTermNameReservation(lake, created);
                if (state == State.PUBLISHED) {
                  handler.handle(
                      null, created, () -> persistNewTerm(metalake, created, plan, domains));
                }
                Snapshot snapshot =
                    GlossaryTermVersions.captureSnapshot(
                        lake,
                        created,
                        domains.ids(),
                        plan == null ? Set.of() : plan.broader(),
                        plan == null ? Set.of() : plan.related());
                if (state != State.PUBLISHED) {
                  Long creatorId =
                      authorizationEnabled
                          ? UserMetaService.getInstance()
                              .getUserIdByMetalakeIdAndName(lake, created.auditInfo().creator())
                          : null;
                  if (authorizationEnabled)
                    lockCandidateCreator(lake, created.auditInfo().creator(), creatorId);
                  snapshot =
                      snapshot.withCandidateReferences(created, Map.of(), Set.of(), creatorId);
                }
                TermPublication result =
                    GlossaryTermVersions.recordTermVersion(
                        lake,
                        snapshot,
                        0,
                        state,
                        state == State.PUBLISHED ? null : GlossaryTermNames.key(created.name()));
                return visibleDomains(
                    TermEdits.toPublicationView(created, created.domains(), result), access);
              });
        });
  }

  /**
   * Applies all changes atomically to the current definition.
   *
   * @throws ForbiddenException if the term is visible but the caller cannot edit it
   * @throws NoSuchTermException if the metalake is missing, or the term being edited or a
   *     referenced relation target is missing or hidden
   * @throws TermAlreadyExistsException if the new name or source reference is already in use
   * @throws IllegalArgumentException if the requested edits are invalid
   */
  public GlossaryTermEntity alterTerm(String metalake, String name, TermChange... changes) {
    return alter(metalake, name, false, null, changes);
  }

  /**
   * Applies all changes only if the locked identity and revision match the supplied token.
   *
   * @throws ForbiddenException if the term is visible but the caller cannot edit it
   * @throws NoSuchTermException if the metalake is missing, or the term being edited or a
   *     referenced relation target is missing or hidden
   * @throws TermVersionConflictException if a valid token does not match the current term
   * @throws TermAlreadyExistsException if the new name or source reference is already in use
   * @throws IllegalArgumentException if the supplied token or requested edits are invalid
   */
  public GlossaryTermEntity alterTerm(
      String metalake, String name, String expectedRevision, TermChange... changes) {
    return alter(metalake, name, true, expectedRevision, changes);
  }

  /**
   * Drops a visible term of any status. Missing and hidden terms both return false.
   *
   * @throws ForbiddenException if the term is visible but the caller lacks management access, or if
   *     deletion is disabled while publication requires approval
   */
  public boolean dropTerm(String metalake, String name) {
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, name);
    return terms.withHierarchyLock(
        ident.namespace(),
        lake -> {
          GlossaryTermEntity root;
          try {
            root = terms.get(ident);
          } catch (NoSuchEntityException e) {
            return false;
          }
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          if (!access.canRead(root) || !readable.test(root)) {
            return false;
          }
          if (!access.manage()) {
            throw new ForbiddenException("Deleting a glossary term requires MANAGE_DEFINITIONS");
          }
          if (policy.requiresApproval(metalake)) {
            throw new ForbiddenException(
                "Deleting terms is disabled while publication requires approval");
          }
          Set<Long> children = edges.listNarrower(lake, root.id());
          Set<Long> related = edges.listRelated(lake, root.id());
          Set<Long> affected = new LinkedHashSet<>(children);
          affected.addAll(related);
          Set<Long> locks = new HashSet<>(affected);
          locks.add(root.id());
          return edges.withHierarchyLock(
              lake,
              locks,
              () -> {
                GlossaryTermEntity locked = terms.get(ident);
                // Content-only edits can change visibility while the hierarchy mutex is held.
                if (!readable.test(locked)) {
                  return false;
                }
                handler.handle(
                    locked,
                    null,
                    () -> {
                      for (GlossaryTermEntity survivor :
                          terms.listTermsByIds(ident.namespace(), lake, affected)) {
                        GlossaryTermEntity after =
                            TermEdits.apply(survivor, new TermChange[0], true);
                        handler.handleRelationLoss(
                            survivor,
                            after,
                            locked,
                            children.contains(survivor.id())
                                ? RelationType.BROADER
                                : RelationType.RELATED,
                            () -> {
                              loadOrCreateVersionHead(lake, survivor);
                              TermRelations remaining =
                                  new TermRelations(lake, survivor.id(), false);
                              remaining.edit(
                                  new TermRelations.Edit(
                                      children.contains(survivor.id())
                                          ? RelationType.BROADER
                                          : RelationType.RELATED,
                                      locked.name(),
                                      false),
                                  locked.id());
                              persistDefinition(survivor.nameIdentifier(), after);
                              GlossaryTermVersions.recordTermVersion(
                                  lake,
                                  captureTermWithRelationships(lake, after, remaining, null),
                                  survivor.version(),
                                  State.PUBLISHED,
                                  null);
                            });
                      }
                      Preconditions.checkState(terms.delete(ident), "Locked term disappeared");
                    });
                return true;
              });
        },
        missing -> false);
  }

  /**
   * Binds and unbinds glossary terms on an Implemented-by metadata object atomically.
   *
   * <p>Locks the live endpoint chain first, then all requested term rows in ascending ID order.
   * Each term is authorized independently using the term-editor path (any status) or the
   * object-admin path (APPROVED only). Any failure rejects the entire request.
   *
   * @throws IllegalStateException if another SQL session is already active on this thread
   * @throws IllegalArgumentException if the target type is unsupported, a term name is blank, or an
   *     object-admin-only caller binds/unbinds a visible non-APPROVED term
   * @throws NoSuchMetadataObjectException if the target object does not exist
   * @throws NoSuchTermException if a requested term is missing or hidden from the caller
   * @throws ForbiddenException if the caller satisfies neither authorization path for a term or a
   *     binding would change while publication requires approval
   */
  public String[] associateTerms(
      String metalake,
      MetadataObject object,
      @Nullable String[] termsToAdd,
      @Nullable String[] termsToRemove) {
    Preconditions.checkArgument(object != null, "Bound object cannot be null");
    TargetType targetType = TargetType.fromMetadataObjectType(object.type());
    return associateTargetTerms(metalake, object, targetType, null, termsToAdd, termsToRemove);
  }

  /**
   * Binds and unbinds glossary terms on a semantic-model metric atomically.
   *
   * <p>Resolves the metric name within the locked semantic model to its stable metric ID. Locks the
   * semantic-model endpoint chain first, then all requested term rows in ascending ID order.
   *
   * @throws IllegalStateException if another SQL session is already active on this thread
   * @throws IllegalArgumentException if the object is not a semantic model, the metric or a term
   *     name is blank, or an object-admin-only caller binds/unbinds a visible non-APPROVED term
   * @throws NoSuchMetadataObjectException if the semantic model or metric does not exist
   * @throws NoSuchTermException if a requested term is missing or hidden from the caller
   * @throws ForbiddenException if the caller satisfies neither authorization path for a term or a
   *     binding would change while publication requires approval
   */
  public String[] associateMetricTerms(
      String metalake,
      MetadataObject semanticModel,
      String metricName,
      @Nullable String[] termsToAdd,
      @Nullable String[] termsToRemove) {
    Preconditions.checkArgument(semanticModel != null, "Semantic model cannot be null");
    Preconditions.checkArgument(
        semanticModel.type() == MetadataObject.Type.SEMANTIC_MODEL,
        "A term can be used only by a metric of a semantic model, not by an object of type %s",
        semanticModel.type());
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "Metric name cannot be blank");
    return associateTargetTerms(
        metalake, semanticModel, TargetType.METRIC, metricName, termsToAdd, termsToRemove);
  }

  /**
   * Lists the visible terms bound to an Implemented-by metadata object after authorizing target
   * read access.
   *
   * @throws IllegalArgumentException if the target object is null or unsupported
   * @throws ForbiddenException if the caller lacks read access to the target object
   * @throws NoSuchMetadataObjectException if the target object does not exist
   * @throws IllegalStateException if an SQL session is already active on the calling thread
   */
  public List<GlossaryTermEntity> listObjectTerms(
      String metalake, MetadataObject object, @Nullable GlossaryTerm.Status status) {
    Preconditions.checkArgument(object != null, "Bound object cannot be null");
    TargetType.fromMetadataObjectType(object.type());
    return readTargetTerms(metalake, object, null, false, true, status, BoundTerms::direct);
  }

  /**
   * Lists the visible terms bound to a semantic-model metric after authorizing semantic-model read
   * access and verifying metric existence.
   *
   * @throws IllegalArgumentException if the semantic model is null/wrong type or metric name is
   *     blank
   * @throws ForbiddenException if the caller lacks read access to the semantic model
   * @throws NoSuchMetadataObjectException if the semantic model or metric does not exist
   * @throws IllegalStateException if an SQL session is already active on the calling thread
   */
  public List<GlossaryTermEntity> listMetricTerms(
      String metalake,
      MetadataObject semanticModel,
      String metricName,
      @Nullable GlossaryTerm.Status status) {
    Preconditions.checkArgument(semanticModel != null, "Semantic model cannot be null");
    Preconditions.checkArgument(
        semanticModel.type() == MetadataObject.Type.SEMANTIC_MODEL,
        "Expected a SEMANTIC_MODEL object, got %s",
        semanticModel.type());
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "Metric name cannot be blank");
    return readTargetTerms(
        metalake, semanticModel, metricName, false, true, status, BoundTerms::direct);
  }

  /**
   * Lists all matching bindings of a visible term in stable target order. The root status and
   * target permissions are evaluated in the same read snapshot.
   *
   * @throws IllegalArgumentException if a requested target type is unsupported
   * @throws ForbiddenException if the configured authorizer cannot evaluate resolved target IDs
   * @throws NoSuchTermException if the term is missing, hidden, or excluded by the status filter
   * @throws IllegalStateException if an SQL session is already active on the calling thread
   */
  public List<TermBinding> listTermBindings(
      String metalake,
      String termName,
      @Nullable TermBinding.Kind kind,
      @Nullable Set<MetadataObject.Type> types,
      @Nullable GlossaryTerm.Status status) {
    Set<TargetType> targetTypes = resolveBindingFilterTypes(kind, types);
    return SqlSessions.readSnapshot(
        () -> {
          NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, termName);
          long lake = termReadMetalakeId(ident);
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          GlossaryTermEntity root = loadRootForView(ident, lake, access);
          if (!isTermVisible(root, access, status)) {
            throw missingRelationRoot(ident);
          }
          GlossaryBindingAccess objectAccess = bindingAccess(metalake, lake);
          List<GlossaryTermBindingPO> rows = BINDINGS.listByTerm(lake, root.id(), targetTypes);
          List<TermBinding> visible = new ArrayList<>();
          for (List<GlossaryTermBindingPO> batch : Lists.partition(rows, TARGET_ID_BATCH_SIZE)) {
            for (ResolvedBinding resolved : resolveBindings(batch, objectAccess)) {
              TermBinding binding = resolved.binding();
              if (objectAccess.canReadTarget(binding.object())) {
                visible.add(binding);
              }
            }
          }
          return List.copyOf(visible);
        });
  }

  private String[] associateTargetTerms(
      String metalake,
      MetadataObject targetObject,
      TargetType targetType,
      @Nullable String metricName,
      @Nullable String[] termsToAdd,
      @Nullable String[] termsToRemove) {
    Preconditions.checkState(
        !SqlSessions.hasActiveSession(), "Binding writes must own their transaction");
    List<String> addNames = normalizeTermNames(termsToAdd);
    List<String> removeNames = normalizeTermNames(termsToRemove);
    GlossaryBindingAccess preflight =
        GlossaryBindingAccess.forRequest(metalake, authorizationEnabled);
    if (!preflight.canReadTarget(targetObject) && !preflight.canAdminTarget(targetObject)) {
      throw new ForbiddenException(
          "Caller lacks permission to modify bindings on %s", targetObject.fullName());
    }
    BindingEndpoint endpoint = prepareEndpoint(metalake, targetObject);
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermBindingMapper.class,
        ignoredMapper -> {
          try {
            LiveEndpointService.lockLiveEndpoint(endpoint.ident(), endpoint.type(), endpoint.ids());
          } catch (NoSuchEntityException e) {
            throw missingTarget(targetObject, e);
          }
          MetadataObject canonical =
              NameIdentifierUtil.toMetadataObject(endpoint.ident(), endpoint.type());
          GlossaryBindingAccess objectAccess =
              GlossaryBindingAccess.forResolvedTarget(
                  metalake, authorizationEnabled, canonical, endpoint.ids());
          boolean canReadObject = objectAccess.canReadTarget(canonical);
          boolean canAdminObject = objectAccess.canAdminTarget(canonical);
          if (!canReadObject && !canAdminObject) {
            throw new ForbiddenException(
                "Caller lacks permission to modify bindings on %s", targetObject.fullName());
          }
          long metalakeId = endpoint.ids().namespaceIds()[0];
          long targetId =
              resolveTargetId(endpoint.ids().entityId(), targetObject, targetType, metricName);
          GlossaryAccess termAccess = GlossaryAccess.forRequest(metalake, authorizationEnabled);

          Map<String, String> requestedOrder = new LinkedHashMap<>();
          Set<String> addKeys = new LinkedHashSet<>();
          for (String name : addNames) {
            String key = GlossaryTermNames.key(name);
            requestedOrder.putIfAbsent(key, name);
            addKeys.add(key);
          }
          Set<String> removeKeys = new LinkedHashSet<>();
          for (String name : removeNames) {
            String key = GlossaryTermNames.key(name);
            requestedOrder.putIfAbsent(key, name);
            removeKeys.add(key);
          }

          Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
          if (!requestedOrder.isEmpty()) {
            Map<String, Long> idsByKey = terms.getIdsByKeys(metalakeId, requestedOrder.keySet());
            Map<Long, GlossaryTermEntity> lockedById =
                terms.lockByIds(ns, metalakeId, Set.copyOf(idsByKey.values()));
            Map<String, GlossaryTermEntity> lockedByKey = new HashMap<>(lockedById.size());
            for (GlossaryTermEntity entity : lockedById.values()) {
              lockedByKey.put(GlossaryTermNames.key(entity.name()), entity);
            }

            // Authorize every requested term before writing anything.
            for (Map.Entry<String, String> entry : requestedOrder.entrySet()) {
              GlossaryTermEntity term = lockedByKey.get(entry.getKey());
              NameIdentifier termIdent =
                  NameIdentifierUtil.ofGlossaryTerm(metalake, entry.getValue());
              if (term == null || !termAccess.canRead(term) || !readable.test(term)) {
                throw missingRelationRoot(termIdent);
              }
              // Visibility of a draft already required an edit check on this locked term.
              if (canReadObject
                  && (term.status() == GlossaryTerm.Status.DRAFT || termAccess.canEdit(term))) {
                continue;
              }
              if (canAdminObject) {
                if (term.status() != GlossaryTerm.Status.APPROVED) {
                  throw new IllegalArgumentException(
                      "Object administrators can bind or unbind only APPROVED glossary terms");
                }
                continue;
              }
              throw new ForbiddenException(
                  "Caller lacks permission to bind or unbind glossary term %s", term.name());
            }

            // Cancel out terms appearing in both add and remove lists.
            Set<String> common = new HashSet<>(addKeys);
            common.retainAll(removeKeys);
            addKeys.removeAll(common);
            removeKeys.removeAll(common);

            Set<Long> currentlyBound = BINDINGS.listTermIds(metalakeId, targetType, targetId);
            Set<Long> toAddIds = new TreeSet<>();
            for (String key : addKeys) {
              long id = lockedByKey.get(key).id();
              if (!currentlyBound.contains(id)) {
                toAddIds.add(id);
              }
            }
            Set<Long> toRemoveIds = new TreeSet<>();
            for (String key : removeKeys) {
              long id = lockedByKey.get(key).id();
              if (currentlyBound.contains(id)) {
                toRemoveIds.add(id);
              }
            }

            Set<Long> changedIds = new TreeSet<>(toAddIds);
            changedIds.addAll(toRemoveIds);
            // Bindings follow the stable term ID. A definition candidate cannot stage a binding.
            if (!changedIds.isEmpty() && policy.requiresApproval(metalake)) {
              throw new ForbiddenException(
                  "Changing bindings is disabled while publication requires approval");
            }
            for (long termId : changedIds) {
              GlossaryTermEntity before = lockedById.get(termId);
              GlossaryTermEntity after = TermEdits.apply(before, new TermChange[0], true);
              boolean isAdd = toAddIds.contains(termId);
              handler.handle(
                  before,
                  after,
                  () -> {
                    if (isAdd) {
                      BINDINGS.add(metalakeId, termId, targetType, targetId);
                    } else {
                      Preconditions.checkState(
                          BINDINGS.remove(termId, targetType, targetId),
                          "Failed to remove binding for locked glossary term %s",
                          termId);
                    }
                    terms.advanceBindingRevision(
                        metalakeId, termId, before.version(), after.auditInfo());
                  });
            }
          }

          Set<Long> finalBoundIds = BINDINGS.listTermIds(metalakeId, targetType, targetId);
          if (finalBoundIds.isEmpty() || !termAccess.canView()) {
            return new String[0];
          }
          return terms.listTermsByIds(ns, metalakeId, finalBoundIds).stream()
              .filter(term -> isTermVisible(term, termAccess, null))
              .map(GlossaryTermEntity::name)
              .sorted()
              .toArray(String[]::new);
        });
  }

  record BoundTerms(
      long lake,
      GlossaryAccess access,
      List<GlossaryTermEntity> direct,
      Map<String, List<GlossaryTermEntity>> children) {}

  <R> R readTargetTerms(
      String metalake,
      MetadataObject targetObject,
      @Nullable String metricName,
      boolean children,
      boolean includeDomains,
      @Nullable GlossaryTerm.Status status,
      Function<BoundTerms, R> project) {
    boolean model = targetObject.type() == MetadataObject.Type.SEMANTIC_MODEL;
    Preconditions.checkArgument(
        metricName == null || (model && StringUtils.isNotBlank(metricName)),
        "Expected a semantic model and a nonblank metric name");
    Preconditions.checkArgument(
        !children
            || (metricName == null && (model || targetObject.type() == MetadataObject.Type.TABLE)),
        "Only tables and semantic models support children");
    TargetType targetType =
        model ? TargetType.METRIC : TargetType.fromMetadataObjectType(targetObject.type());
    Preconditions.checkState(
        !SqlSessions.hasActiveSession(),
        "A snapshot read must start outside an active SQL session");
    if (!GlossaryBindingAccess.forRequest(metalake, authorizationEnabled)
        .canReadTarget(targetObject)) {
      throw new ForbiddenException(
          "Caller lacks read access to target %s", targetObject.fullName());
    }
    BindingEndpoint endpoint = prepareEndpoint(metalake, targetObject);
    return SqlSessions.readSnapshot(
        () -> {
          NamespacedEntityId ids;
          try {
            ids =
                new RelationalEntityStoreIdResolver()
                    .getEntityIds(endpoint.ident(), endpoint.type());
          } catch (NoSuchEntityException e) {
            throw missingTarget(targetObject, e);
          }
          MetadataObject canonical =
              NameIdentifierUtil.toMetadataObject(endpoint.ident(), endpoint.type());
          if (!GlossaryBindingAccess.forResolvedTarget(
                  metalake, authorizationEnabled, canonical, ids)
              .canReadTarget(canonical)) {
            throw new ForbiddenException(
                "Caller lacks read access to target %s", targetObject.fullName());
          }
          long metalakeId = ids.namespaceIds()[0];
          long targetId =
              metricName == null
                  ? ids.entityId()
                  : resolveTargetId(ids.entityId(), targetObject, targetType, metricName);
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          if (!access.canView() || (model && metricName == null && !children)) {
            return project.apply(new BoundTerms(metalakeId, access, List.of(), Map.of()));
          }
          BoundTerms bound =
              visibleTargetTerms(
                  metalake,
                  metalakeId,
                  targetType,
                  targetId,
                  children,
                  includeDomains,
                  status,
                  access);
          return project.apply(bound);
        });
  }

  private BoundTerms visibleTargetTerms(
      String metalake,
      long metalakeId,
      TargetType targetType,
      long targetId,
      boolean children,
      boolean includeDomains,
      @Nullable GlossaryTerm.Status status,
      GlossaryAccess access) {
    Set<Long> direct = new HashSet<>();
    Map<Long, Set<Long>> byChild = new HashMap<>();
    if (children) {
      List<GlossaryTermBindingPO> rows =
          SessionUtils.getWithoutCommit(
              GlossaryTermBindingMapper.class,
              mapper ->
                  targetType == TargetType.METRIC
                      ? mapper.listBySemanticModel(metalakeId, targetId)
                      : mapper.listByTable(metalakeId, targetId));
      for (GlossaryTermBindingPO row : rows) {
        if (TargetType.TABLE.name().equals(row.getTargetType())) {
          direct.add(row.getTermId());
        } else {
          byChild
              .computeIfAbsent(row.getTargetId(), ignored -> new HashSet<>())
              .add(row.getTermId());
        }
      }
    } else {
      direct.addAll(BINDINGS.listTermIds(metalakeId, targetType, targetId));
    }
    Set<Long> termIds = new HashSet<>(direct);
    byChild.values().forEach(termIds::addAll);
    Map<Long, GlossaryTermEntity> visible = new HashMap<>();
    for (GlossaryTermEntity term :
        terms.listTermsByIds(NamespaceUtil.ofGlossaryTerm(metalake), metalakeId, termIds)) {
      if (isTermVisible(term, access, status)) {
        visible.put(term.id(), includeDomains ? visibleDomains(term, access) : term);
      }
    }
    return new BoundTerms(
        metalakeId,
        access,
        sortedVisibleTerms(direct, visible),
        visibleChildTerms(targetType, byChild, visible));
  }

  private static Map<String, List<GlossaryTermEntity>> visibleChildTerms(
      TargetType targetType, Map<Long, Set<Long>> byChild, Map<Long, GlossaryTermEntity> visible) {
    Map<String, List<GlossaryTermEntity>> childTerms = new TreeMap<>();
    Map<Long, List<GlossaryTermEntity>> visibleChildren = new HashMap<>();
    byChild.forEach(
        (id, bound) -> {
          List<GlossaryTermEntity> matches = sortedVisibleTerms(bound, visible);
          if (!matches.isEmpty()) {
            visibleChildren.put(id, matches);
          }
        });
    for (List<Long> batch :
        Lists.partition(List.copyOf(visibleChildren.keySet()), TARGET_ID_BATCH_SIZE)) {
      if (targetType == TargetType.METRIC) {
        SessionUtils.getWithoutCommit(
                SemanticModelMetricMapper.class,
                mapper -> mapper.listSemanticModelMetricsByMetricIds(batch))
            .forEach(
                metric ->
                    childTerms.put(metric.metricName(), visibleChildren.get(metric.metricId())));
      } else {
        SessionUtils.getWithoutCommit(
                TableColumnMapper.class, mapper -> mapper.listColumnPOsByColumnIds(batch))
            .forEach(
                column ->
                    childTerms.put(
                        column.getColumnName(), visibleChildren.get(column.getColumnId())));
      }
    }
    return childTerms;
  }

  private static List<GlossaryTermEntity> sortedVisibleTerms(
      Set<Long> ids, Map<Long, GlossaryTermEntity> visible) {
    return ids.stream()
        .map(visible::get)
        .filter(Objects::nonNull)
        .sorted(Comparator.comparing(GlossaryTermEntity::name))
        .toList();
  }

  @Nullable
  private static Set<TargetType> resolveBindingFilterTypes(
      @Nullable TermBinding.Kind kind, @Nullable Set<MetadataObject.Type> types) {
    if (kind == null && (types == null || types.isEmpty())) {
      return null;
    }
    Set<TargetType> allowed = EnumSet.noneOf(TargetType.class);
    if (kind == null || kind == TermBinding.Kind.IMPLEMENTED_BY) {
      for (MetadataObject.Type type : TermBinding.IMPLEMENTED_BY_TYPES) {
        allowed.add(TargetType.fromMetadataObjectType(type));
      }
    }
    if (kind == null || kind == TermBinding.Kind.USED_BY) {
      allowed.add(TargetType.METRIC);
    }
    if (types != null && !types.isEmpty()) {
      Set<TargetType> requested = EnumSet.noneOf(TargetType.class);
      for (MetadataObject.Type type : types) {
        if (type == MetadataObject.Type.SEMANTIC_MODEL) {
          requested.add(TargetType.METRIC);
        } else {
          requested.add(TargetType.fromMetadataObjectType(type));
        }
      }
      allowed.retainAll(requested);
    }
    return allowed;
  }

  record ResolvedBinding(long termId, TermBinding binding) {}

  static List<ResolvedBinding> resolveBindings(
      List<GlossaryTermBindingPO> rows, GlossaryBindingAccess objectAccess) {
    Map<TargetType, List<Long>> idsByType = new EnumMap<>(TargetType.class);
    for (GlossaryTermBindingPO row : rows) {
      idsByType
          .computeIfAbsent(TargetType.valueOf(row.getTargetType()), ignored -> new ArrayList<>())
          .add(row.getTargetId());
    }

    Map<TargetType, Map<Long, MetadataObject>> objectsByType = new EnumMap<>(TargetType.class);
    Map<Long, SemanticModelMetricPO> metricsById = new HashMap<>();
    Map<Long, MetadataObject> semanticModelsById = new HashMap<>();

    for (Map.Entry<TargetType, List<Long>> entry : idsByType.entrySet()) {
      TargetType type = entry.getKey();
      if (type == TargetType.METRIC) {
        Set<Long> modelIds = new LinkedHashSet<>();
        List<SemanticModelMetricPO> metrics =
            SessionUtils.getWithoutCommit(
                SemanticModelMetricMapper.class,
                mapper -> mapper.listSemanticModelMetricsByMetricIds(entry.getValue()));
        for (SemanticModelMetricPO metric : metrics) {
          metricsById.put(metric.metricId(), metric);
          modelIds.add(metric.semanticModelId());
        }
        if (!modelIds.isEmpty()) {
          semanticModelsById.putAll(
              MetadataObjectService.resolveMetadataObjects(
                  MetadataObject.Type.SEMANTIC_MODEL, List.copyOf(modelIds)));
        }
      } else {
        objectsByType.put(
            type,
            MetadataObjectService.resolveMetadataObjects(
                type.toMetadataObjectType(), entry.getValue()));
      }
    }

    List<ResolvedBinding> result = new ArrayList<>(rows.size());
    for (GlossaryTermBindingPO row : rows) {
      TargetType type = TargetType.valueOf(row.getTargetType());
      if (type == TargetType.METRIC) {
        SemanticModelMetricPO metricPO = metricsById.get(row.getTargetId());
        if (metricPO == null) {
          continue;
        }
        MetadataObject modelObj = semanticModelsById.get(metricPO.semanticModelId());
        if (modelObj != null) {
          objectAccess.registerTarget(modelObj, metricPO.semanticModelId());
          MetadataObject schemaObj = MetadataObjects.parent(modelObj);
          if (schemaObj != null) {
            objectAccess.registerTarget(schemaObj, metricPO.schemaId());
            MetadataObject catalogObj = MetadataObjects.parent(schemaObj);
            if (catalogObj != null) {
              objectAccess.registerTarget(catalogObj, metricPO.catalogId());
            }
          }
          result.add(
              new ResolvedBinding(
                  row.getTermId(), TermBinding.usedBy(modelObj, metricPO.metricName())));
        }
      } else {
        MetadataObject obj = objectsByType.getOrDefault(type, Map.of()).get(row.getTargetId());
        if (obj != null) {
          if (type != TargetType.COLUMN) {
            objectAccess.registerTarget(obj, row.getTargetId());
          }
          result.add(new ResolvedBinding(row.getTermId(), TermBinding.implementedBy(obj)));
        }
      }
    }
    return result;
  }

  @SuppressWarnings("UnusedVariable") // Error Prone treats private record components as unused.
  private record BindingEndpoint(NameIdentifier ident, EntityType type, NamespacedEntityId ids) {}

  private static NoSuchMetadataObjectException missingTarget(
      MetadataObject object, NotFoundException missing) {
    return new NoSuchMetadataObjectException(
        missing, "Metadata object %s does not exist", object.fullName());
  }

  private static NameIdentifier canonicalTarget(String metalake, MetadataObject object) {
    NameIdentifier raw = MetadataObjectUtil.toEntityIdent(metalake, object);
    if (object.type() == MetadataObject.Type.CATALOG) {
      return raw;
    }
    GravitinoEnv env = GravitinoEnv.getInstance();
    return CapabilityHelpers.normalizeMetadataIdentifier(
        raw,
        Capability.Scope.valueOf(object.type().name()),
        env.catalogManager(),
        env::entityStore);
  }

  // A cold registration may import catalog metadata and take tree locks. Finish it before
  // opening a write transaction or read snapshot. Only the fenced identity can be written.
  private static BindingEndpoint prepareEndpoint(String metalake, MetadataObject object) {
    RelationalEntityStoreIdResolver resolver = new RelationalEntityStoreIdResolver();
    EntityType type = MetadataObjectUtil.toEntityType(object);
    try {
      NameIdentifier ident = canonicalTarget(metalake, object);
      try {
        return new BindingEndpoint(ident, type, resolver.getEntityIds(ident, type));
      } catch (NoSuchEntityException missing) {
        if (type == EntityType.CATALOG || type == EntityType.SEMANTIC_MODEL) {
          throw missing;
        }
        MetadataObjectUtil.checkMetadataObject(metalake, object);
        ident = canonicalTarget(metalake, object);
        return new BindingEndpoint(ident, type, resolver.getEntityIds(ident, type));
      }
    } catch (NotFoundException missing) {
      throw missingTarget(object, missing);
    }
  }

  private static long resolveTargetId(
      long entityId,
      MetadataObject targetObject,
      TargetType targetType,
      @Nullable String metricName) {
    if (targetType == TargetType.METRIC) {
      SemanticModelMetricPO metric =
          SessionUtils.getWithoutCommit(
              SemanticModelMetricMapper.class,
              mapper -> mapper.selectSemanticModelMetricByModelIdAndName(entityId, metricName));
      if (metric == null) {
        throw new NoSuchMetadataObjectException(
            "Metric %s does not exist in semantic model %s", metricName, targetObject.fullName());
      }
      return metric.metricId();
    }
    return entityId;
  }

  private static List<String> normalizeTermNames(@Nullable String[] names) {
    if (names == null || names.length == 0) {
      return List.of();
    }
    List<String> result = new ArrayList<>(names.length);
    for (String name : names) {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "Term name cannot be blank");
      result.add(name);
    }
    return result;
  }

  /** Definition and visible direct relationship identities from one exact business version. */
  public record TermVersion(GlossaryTermEntity term, Set<Long> broader, Set<Long> related) {}

  /** Lists accessible business versions newest first for a stable term identity. */
  public List<Long> listTermVersions(String metalake, long termId) {
    return listTermVersions(metalake, termId, null, Integer.MAX_VALUE);
  }

  /**
   * Returns at most limit visible version IDs in descending order. Pass null for the first page and
   * its last returned ID as the exclusive cursor for the next page.
   */
  public List<Long> listTermVersions(
      String metalake, long termId, @Nullable Long beforeVersionId, int limit) {
    Preconditions.checkArgument(limit > 0, "limit must be positive");
    Preconditions.checkArgument(
        beforeVersionId == null || beforeVersionId > 0, "beforeVersionId must be positive");
    return readHistory(
        metalake,
        termId,
        (mapper, head) -> {
          long upper =
              beforeVersionId == null
                  ? head.getLastVersion()
                  : Math.min(head.getLastVersion(), beforeVersionId - 1);
          if (!authorizationEnabled && readable == ALL_TERMS)
            return mapper.listVersionIdsByTermId(termId, upper, limit);
          List<Long> result = new ArrayList<>();
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          Map<Long, Long> currentUserIds = new HashMap<>();
          while (upper > 0 && result.size() < limit) {
            int batchSize = Math.min(100, Math.max(limit - result.size(), 16));
            List<Version> rows = mapper.listVersionsByTermId(termId, upper, batchSize);
            for (Version row : rows) {
              if (canReadTermVersion(metalake, head, row, access, currentUserIds)) {
                result.add(row.getVersionId());
                if (result.size() == limit) break;
              }
            }
            if (rows.size() < batchSize) break;
            upper = rows.get(rows.size() - 1).getVersionId() - 1;
          }
          return result;
        });
  }

  /**
   * Reads the definition, domain names and direct edges frozen for the exact version. When
   * authorization or visibility filtering is active, deleted or hidden domains and relation targets
   * are omitted.
   */
  public TermVersion getTermVersion(String metalake, long termId, long versionId) {
    return readHistory(
        metalake,
        termId,
        (mapper, head) -> {
          Version row = mapper.getVersionByTermIdAndVersionId(termId, versionId);
          if (row == null) throw new NoSuchTermException("Glossary version does not exist");
          Snapshot snapshot = GlossaryTermVersions.decodeSnapshot(row);
          GlossaryTermEntity term = snapshot.term(NamespaceUtil.ofGlossaryTerm(metalake));
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          if (!canReadTermVersion(head, row, access, snapshot, term))
            throw new NoSuchTermException("Glossary version does not exist");
          TermPublication publication = GlossaryTermVersions.toPublicationResult(head, row);
          if (!access.canEdit(term) && head.getPublishedVersion() != null) {
            publication =
                new TermPublication(
                    termId, versionId, publication.state(), head.getPublishedVersion(), null);
          }
          String[] domains =
              authorizationEnabled
                  ? snapshot.visibleDomainNames(access::canUseDomain)
                  : term.domains();
          GlossaryTermEntity result = TermEdits.toPublicationView(term, domains, publication);
          if (!authorizationEnabled && readable == ALL_TERMS) {
            return new TermVersion(
                result, Set.copyOf(snapshot.broader()), Set.copyOf(snapshot.related()));
          }
          return new TermVersion(
              result,
              visibleEndpoints(
                  snapshot.definition().getMetalakeId(), term, snapshot.broader(), access),
              visibleEndpoints(
                  snapshot.definition().getMetalakeId(), term, snapshot.related(), access));
        });
  }

  /**
   * Submits an exact saved draft for review. Only the creator or an authorized editor may submit
   * it.
   */
  public TermPublication submitDraft(String metalake, long termId, long draftVersionId) {
    return withLockedHistory(
        NamespaceUtil.ofGlossaryTerm(metalake),
        termId,
        true,
        (mapper, head) -> {
          if (!Objects.equals(head.getCandidateVersion(), draftVersionId))
            throw staleCandidateConflict();
          Version row = mapper.getVersionByTermIdAndVersionId(termId, draftVersionId);
          if (!State.DRAFT.name().equals(row.getLifecycle())) throw staleCandidateConflict();
          if (!canReadTermVersion(metalake, head, row))
            throw new NoSuchTermException("Glossary draft does not exist");
          Snapshot draft = GlossaryTermVersions.decodeSnapshot(row);
          GlossaryTermEntity base = null;
          if (head.getPublishedVersion() != null) {
            long lake = draft.definition().getMetalakeId();
            List<GlossaryTermEntity> live =
                terms.listTermsByIds(NamespaceUtil.ofGlossaryTerm(metalake), lake, Set.of(termId));
            if (live.isEmpty()
                || live.get(0).version() != row.getBaseRevision()
                || !edges.listRelated(lake, termId).equals(draft.baseRelated())) {
              throw staleCandidateConflict();
            }
            base = live.get(0);
          }
          restoreDraftDefinitionStatus(draft, base);
          return GlossaryTermVersions.recordTermVersion(
              draft.definition().getMetalakeId(),
              draft,
              row.getBaseRevision(),
              State.SUBMITTED,
              head.getReservedName());
        });
  }

  /** Publishes exactly the submitted candidate, atomically applying its definition and edges. */
  public TermPublication publishCandidate(String metalake, long termId, long candidateVersionId) {
    return recordCandidateDecision(metalake, termId, candidateVersionId, State.PUBLISHED);
  }

  /** Rejects exactly the submitted candidate and leaves the published snapshot in place. */
  public TermPublication rejectCandidate(String metalake, long termId, long candidateVersionId) {
    return recordCandidateDecision(metalake, termId, candidateVersionId, State.REJECTED);
  }

  static long resolveMetalakeId(String metalake) {
    try {
      return MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
    }
  }

  private GlossaryTermEntity alter(
      String metalake,
      String name,
      boolean conditional,
      @Nullable String token,
      TermChange[] changes) {
    Preconditions.checkArgument(
        changes != null && changes.length > 0, "At least one term change is required");
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, name);
    if (policy.requiresApproval(metalake)) {
      return terms.withHierarchyLock(
          ident.namespace(),
          lake -> {
            Long id =
                withVersionMapper(
                    m -> m.findTermIdByReservedName(lake, GlossaryTermNames.key(name)));
            if (id != null) {
              return withVersionMapper(
                  mapper -> {
                    Head head = mapper.getVersionHeadByMetalakeAndTermId(lake, id);
                    Version row =
                        mapper.getVersionByTermIdAndVersionId(id, head.getCandidateVersion());
                    if (!canReadTermVersion(metalake, head, row)) throw missingRelationRoot(ident);
                    return editTermCandidate(
                        ident, lake, head, row, null, conditional, token, changes);
                  });
            }
            if (conditional) {
              return editWithPublicationToken(metalake, ident, lake, token, changes);
            }
            return editPublishedTerm(metalake, ident, false, null, changes, true);
          },
          missing -> {
            throw missingRelationRoot(ident);
          });
    }
    return editPublishedTerm(metalake, ident, conditional, token, changes, false);
  }

  private GlossaryTermEntity editWithPublicationToken(
      String metalake,
      NameIdentifier ident,
      long lake,
      @Nullable String token,
      TermChange[] changes) {
    return terms.withLockedTerm(
        ident,
        false,
        locked -> {
          GlossaryTermEntity live = locked.orElseThrow(() -> missingRelationRoot(ident));
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          requireEdit(access, ident, live);
          validateRevisionToken(token);
          if (live.revisionToken().equals(token)) {
            return editPublishedTerm(metalake, ident, true, token, changes, true);
          }
          return withVersionMapper(
              mapper -> {
                Head head = mapper.getVersionHeadByMetalakeAndTermId(lake, live.id());
                if (head == null || head.getCandidateVersion() == null)
                  throw staleCandidateConflict();
                Version row =
                    mapper.getVersionByTermIdAndVersionId(live.id(), head.getCandidateVersion());
                if (!canReadTermVersion(metalake, head, row, access))
                  throw missingRelationRoot(ident);
                if (row.getBaseRevision() != live.version()
                    || !edges
                        .listRelated(lake, live.id())
                        .equals(GlossaryTermVersions.decodeSnapshot(row).baseRelated()))
                  throw staleCandidateConflict();
                return editTermCandidate(ident, lake, head, row, live, true, token, changes);
              });
        });
  }

  private GlossaryTermEntity editTermCandidate(
      NameIdentifier ident,
      long lake,
      Head head,
      Version row,
      @Nullable GlossaryTermEntity live,
      boolean conditional,
      @Nullable String token,
      TermChange[] changes) {
    String metalake = ident.namespace().level(0);
    Snapshot previous = GlossaryTermVersions.decodeSnapshot(row);
    GlossaryTermEntity term = previous.term(ident.namespace());
    TermPublication publication = GlossaryTermVersions.toPublicationResult(head, row);
    checkRevision(
        ident, TermEdits.toPublicationView(term, term.domains(), publication), conditional, token);
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    List<TermChange> content = new ArrayList<>();
    TermRelations relations =
        new TermRelations(lake, term.id(), head.getPublishedVersion() == null);
    relations.replace(previous.broader(), previous.related());
    List<TermDomains.Edit> domainEdits = new ArrayList<>();
    Map<RelationType, Map<String, Long>> resolvedReferences = new EnumMap<>(RelationType.class);
    State state = State.SUBMITTED;
    for (TermChange change : changes) {
      TermRelations.Edit edge = TermRelations.Edit.from(change);
      TermDomains.Edit domain = TermDomains.Edit.from(change);
      if (edge != null) {
        Map<String, Long> resolved =
            resolvedReferences.computeIfAbsent(edge.type(), ignored -> new HashMap<>());
        String targetKey = GlossaryTermNames.key(edge.target());
        Long removedId = resolved.get(targetKey);
        if (!edge.add() && removedId == null) {
          removedId =
              findSnapshotReference(
                  previous.referenceNames(),
                  edge.type() == RelationType.BROADER ? relations.broader() : relations.related(),
                  edge.target());
        }
        if (!edge.add() && removedId != null) {
          for (GlossaryTermEntity target :
              terms.listTermsByIds(term.namespace(), lake, Set.of(removedId))) {
            if (!access.canRead(target) || !readable.test(target))
              throw new NoSuchTermException("Referenced term does not exist");
          }
          resolved.put(targetKey, removedId);
          relations.edit(edge, removedId);
        } else if (head.getPublishedVersion() == null
            && GlossaryTermNames.key(term.name()).equals(targetKey)) {
          relations.edit(edge, term.id());
        } else {
          GlossaryTermEntity endpoint =
              relationRoot(NameIdentifierUtil.ofGlossaryTerm(metalake, edge.target())).term();
          if (!access.canRead(endpoint) || !readable.test(endpoint))
            throw new NoSuchTermException("Referenced term does not exist");
          relations.edit(edge, endpoint.id());
          resolved.put(targetKey, endpoint.id());
        }
      } else if (domain != null) {
        domainEdits.add(domain);
      } else {
        if (change instanceof TermChange.SetStatus status)
          state = resolvePublicationState(true, status.getNewStatus());
        content.add(change);
      }
    }
    relations.validate(depthLimit);
    TermDomains domains =
        planCandidateDomains(lake, term.id(), previous.domains(), domainEdits, access);
    boolean domainsChanged = domains.changed() || !previous.domainIds().equals(domains.ids());
    GlossaryTermEntity edited =
        TermEdits.apply(
            term,
            content.toArray(new TermChange[0]),
            !previous.related().equals(relations.related())
                || !previous.broader().equals(relations.broader()),
            domainsChanged ? domains.names() : null,
            true);
    Preconditions.checkArgument(
        head.getPublishedVersion() != null || edited.status() != GlossaryTerm.Status.DEPRECATED,
        "Omit status to submit a new term or specify DRAFT to save it.");
    terms.validateTermNameSynonymsAndSourceRef(edited);
    checkTermNameReservation(lake, edited);
    String key = GlossaryTermNames.key(edited.name());
    if (edited == term && state == publication.state())
      return visibleDomains(
          TermEdits.toPublicationView(term, domains.names(), publication), access);
    Snapshot next =
        GlossaryTermVersions.captureSnapshot(
            lake, edited, domains.ids(), relations.broader(), relations.related());
    if (state == State.SUBMITTED) {
      restoreDraftDefinitionStatus(next, live);
    }
    next =
        next.withCandidateReferences(
            edited, previous.referenceNames(), previous.baseRelated(), previous.creatorUserId());
    next.definition().setCurrentVersion(Math.incrementExact(row.getBaseRevision()));
    TermPublication result =
        GlossaryTermVersions.recordTermVersion(
            lake,
            next,
            row.getBaseRevision(),
            state,
            head.getPublishedVersion() == null ? key : null);
    return visibleDomains(
        TermEdits.toPublicationView(next.term(ident.namespace()), domains.names(), result), access);
  }

  private GlossaryTermEntity editPublishedTerm(
      String metalake,
      NameIdentifier ident,
      boolean conditional,
      @Nullable String token,
      TermChange[] changes,
      boolean approval) {
    TermChange[] request = changes.clone();
    List<TermRelations.Edit> edits = new ArrayList<>();
    List<TermDomains.Edit> domainEdits = new ArrayList<>();
    List<TermChange> content = new ArrayList<>();
    for (TermChange change : request) {
      TermRelations.Edit edit = TermRelations.Edit.from(change);
      TermDomains.Edit domainEdit = TermDomains.Edit.from(change);
      if (edit != null) {
        edits.add(edit);
      } else if (domainEdit != null) {
        domainEdits.add(domainEdit);
      } else {
        content.add(change);
      }
    }
    if (!edits.isEmpty()) {
      return terms.withHierarchyLock(
          ident.namespace(),
          lake -> {
            GlossaryTermEntity root = relationRoot(ident).term();
            GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
            return withRelationPlan(
                lake,
                root,
                edits,
                access,
                false,
                locked -> {
                  requireEdit(access, ident, locked);
                  checkRevision(ident, locked, conditional, token);
                },
                (locked, plan) ->
                    visibleDomains(
                        applyChanges(
                            approval,
                            ident,
                            locked,
                            content.toArray(new TermChange[0]),
                            plan,
                            planDomains(lake, locked, domainEdits, access)),
                        access));
          },
          missing -> {
            throw missingRelationRoot(ident);
          });
    }
    return terms.withLockedTerm(
        ident,
        TermEdits.needsHierarchyLock(request),
        locked -> {
          GlossaryTermEntity before =
              locked.orElseThrow(() -> new NoSuchTermException("Term %s does not exist", ident));
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          requireEdit(access, ident, before);
          checkRevision(ident, before, conditional, token);
          TermDomains domains =
              domainEdits.isEmpty() && before.domains().length == 0
                  ? null
                  : planDomains(lakeOf(ident), before, domainEdits, access);
          return visibleDomains(
              applyChanges(
                  approval, ident, before, content.toArray(new TermChange[0]), null, domains),
              access);
        });
  }

  private void requireEdit(GlossaryAccess access, NameIdentifier ident, GlossaryTermEntity term) {
    if (!access.canRead(term)) {
      throw missingRelationRoot(ident);
    }
    requireVisible(ident, term);
    // Draft visibility already required edit access, including any live owner lookup.
    if (term.status() != GlossaryTerm.Status.DRAFT && !access.canEdit(term)) {
      throw new ForbiddenException(
          "Editing a glossary term requires MANAGE_DEFINITIONS or ownership with CREATE_DEFINITION");
    }
  }

  private void checkRevision(
      NameIdentifier ident,
      GlossaryTermEntity before,
      boolean conditional,
      @Nullable String token) {
    if (conditional) {
      validateRevisionToken(token);
      if (!before.revisionToken().equals(token)) {
        throw new TermVersionConflictException("Term %s has changed", ident);
      }
    }
  }

  private static void validateRevisionToken(@Nullable String token) {
    Preconditions.checkArgument(
        token != null && token.length() == 36 && UUID.fromString(token).toString().equals(token),
        "Malformed glossary revision token");
  }

  private GlossaryTermEntity applyChanges(
      boolean approval,
      NameIdentifier ident,
      GlossaryTermEntity before,
      TermChange[] request,
      @Nullable TermRelations plan,
      @Nullable TermDomains domains) {
    boolean membership = domains != null && domains.changed();
    if (domains != null) {
      // A domain may have been renamed between the initial term read and its domain locks.
      before = TermEdits.withDomains(before, domains.originalNames());
    }
    GlossaryTermEntity after =
        TermEdits.apply(
            before,
            request,
            plan != null && plan.changed(),
            membership ? domains.names() : null,
            approval);

    State state = approval ? State.SUBMITTED : State.PUBLISHED;
    for (TermChange change : request) {
      if (change instanceof TermChange.SetStatus status) {
        state = resolvePublicationState(approval, status.getNewStatus());
      }
    }
    long lake = lakeOf(ident);
    if (before == after && state != State.DRAFT) {
      Head head = loadOrCreateVersionHead(lake, before);
      Version row =
          withVersionMapper(
              m -> m.getVersionByTermIdAndVersionId(after.id(), head.getPublishedVersion()));
      return TermEdits.toPublicationView(
          after, after.domains(), GlossaryTermVersions.toPublicationResult(head, row));
    }
    if (approval || TermEdits.needsHierarchyLock(request)) {
      terms.validateTermNameSynonymsAndSourceRef(after);
      checkTermNameReservation(lake, after);
    }
    loadOrCreateVersionHead(lake, before);
    Snapshot frozen = captureTermWithRelationships(lake, after, plan, domains);
    if (state == State.SUBMITTED) {
      restoreDraftDefinitionStatus(frozen, before);
    }
    if (before == after && state == State.DRAFT) {
      frozen.definition().setCurrentVersion(Math.incrementExact(before.version()));
    }
    Snapshot snapshot =
        state == State.PUBLISHED
            ? frozen
            : frozen.withCandidateReferences(
                after, Map.of(), edges.listRelated(lake, after.id()), null);
    if (state == State.PUBLISHED)
      handler.handle(
          before,
          after,
          () -> {
            persistDefinition(ident, after);
            if (plan != null) {
              plan.persist();
            }
            if (membership) {
              domains.persist();
            }
          });
    TermPublication result =
        GlossaryTermVersions.recordTermVersion(lake, snapshot, before.version(), state, null);
    return TermEdits.toPublicationView(snapshot.term(ident.namespace()), after.domains(), result);
  }

  private static void restoreDraftDefinitionStatus(
      Snapshot snapshot, @Nullable GlossaryTermEntity live) {
    // New terms and older saved drafts can carry DRAFT in the definition itself.
    if (GlossaryTerm.Status.DRAFT.name().equals(snapshot.definition().getStatus())) {
      GlossaryTerm.Status status =
          live == null || live.status() == GlossaryTerm.Status.DRAFT
              ? GlossaryTerm.Status.APPROVED
              : live.status();
      snapshot.definition().setStatus(status.name());
    }
  }

  private static State resolvePublicationState(
      boolean requiresApproval, @Nullable GlossaryTerm.Status explicit) {
    if (!requiresApproval) return State.PUBLISHED;
    Preconditions.checkArgument(
        explicit != GlossaryTerm.Status.APPROVED,
        "Omit APPROVED to submit a candidate for publication, or specify DRAFT to save it.");
    return explicit == GlossaryTerm.Status.DRAFT ? State.DRAFT : State.SUBMITTED;
  }

  private static <T> T withVersionMapper(Function<GlossaryTermVersionMapper, T> operation) {
    return SessionUtils.getWithoutCommit(GlossaryTermVersionMapper.class, operation);
  }

  private void checkTermNameReservation(long lake, GlossaryTermEntity term) {
    Long reserved =
        withVersionMapper(
            m -> m.findTermIdByReservedName(lake, GlossaryTermNames.key(term.name())));
    if (reserved != null && !reserved.equals(term.id()))
      throw new TermAlreadyExistsException("Term name already exists");
  }

  private Snapshot captureTermWithRelationships(
      long lake,
      GlossaryTermEntity term,
      @Nullable TermRelations plan,
      @Nullable TermDomains domains) {
    return GlossaryTermVersions.captureSnapshot(
        lake,
        term,
        domains == null ? DOMAINS.domainIds(lake, term.id()) : domains.ids(),
        plan == null ? edges.listBroader(lake, term.id()) : plan.broader(),
        plan == null ? edges.listRelated(lake, term.id()) : plan.related());
  }

  private Head loadOrCreateVersionHead(long lake, GlossaryTermEntity term) {
    Head head = withVersionMapper(m -> m.getVersionHeadByMetalakeAndTermId(lake, term.id()));
    if (head == null) {
      GlossaryTermVersions.recordTermVersion(
          lake,
          captureTermWithRelationships(lake, term, null, null),
          term.version(),
          State.PUBLISHED,
          null);
      head = withVersionMapper(m -> m.getVersionHeadByMetalakeAndTermId(lake, term.id()));
    }
    return head;
  }

  private Set<Long> visibleEndpoints(
      long lake, GlossaryTermEntity term, Set<Long> ids, GlossaryAccess access) {
    Set<Long> result = new HashSet<>();
    for (GlossaryTermEntity endpoint : terms.listTermsByIds(term.namespace(), lake, ids)) {
      if (access.canRead(endpoint) && readable.test(endpoint)) result.add(endpoint.id());
    }
    return Set.copyOf(result);
  }

  private <T> T readHistory(
      String metalake, long id, BiFunction<GlossaryTermVersionMapper, Head, T> read) {
    Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
    T result =
        SqlSessions.readSnapshot(
            () -> {
              long lake;
              try {
                lake = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
              } catch (NoSuchEntityException missing) {
                throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
              }
              return withVersionMapper(
                  mapper -> readScopedVersion(metalake, lake, id, mapper, read, false, false));
            });
    // Only a legacy live term without pointers needs a write transaction for its baseline.
    return result != null ? result : withLockedHistory(ns, id, false, read);
  }

  private <T> T withLockedHistory(
      Namespace ns,
      long id,
      boolean requireEditor,
      BiFunction<GlossaryTermVersionMapper, Head, T> read) {
    return terms.withHierarchyLock(
        ns,
        lake -> {
          Set<Long> locks =
              terms.listTermsByIds(ns, lake, Set.of(id)).isEmpty() ? Set.of() : Set.of(id);
          return edges.withHierarchyLock(
              lake,
              locks,
              () ->
                  withVersionMapper(
                      mapper ->
                          readScopedVersion(
                              ns.level(0), lake, id, mapper, read, true, requireEditor)));
        });
  }

  @Nullable
  private <T> T readScopedVersion(
      String metalake,
      long lake,
      long id,
      GlossaryTermVersionMapper mapper,
      BiFunction<GlossaryTermVersionMapper, Head, T> read,
      boolean mayInitialize,
      boolean requireEditor) {
    List<GlossaryTermEntity> live =
        terms.listTermsByIds(NamespaceUtil.ofGlossaryTerm(metalake), lake, Set.of(id));
    Head head = mapper.getVersionHeadByMetalakeAndTermId(lake, id);
    if (live.isEmpty()) {
      if (head == null
          || head.getPublishedVersion() != null
          || !canReadTermVersion(
              metalake, head, mapper.getVersionByTermIdAndVersionId(id, head.getLastVersion())))
        throw new NoSuchTermException("Glossary term does not exist");
    } else {
      GlossaryTermEntity current = live.get(0);
      GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
      if (!readable.test(current)
          || !access.canRead(current)
          || (requireEditor && !access.canEdit(current)))
        throw new NoSuchTermException("Glossary term does not exist");
      if (head == null) {
        if (!mayInitialize) return null;
        head = loadOrCreateVersionHead(lake, current);
      }
    }
    return read.apply(mapper, head);
  }

  private boolean canReadTermVersion(String metalake, Head head, Version row) {
    return canReadTermVersion(
        metalake, head, row, GlossaryAccess.forRequest(metalake, authorizationEnabled));
  }

  private boolean canReadTermVersion(
      String metalake, Head head, Version row, GlossaryAccess access) {
    return canReadTermVersion(metalake, head, row, access, null);
  }

  private boolean canReadTermVersion(
      String metalake,
      Head head,
      Version row,
      GlossaryAccess access,
      @Nullable Map<Long, Long> currentUserIds) {
    if (!State.PUBLISHED.name().equals(row.getLifecycle())
        && head.getPublishedVersion() != null
        && !access.canEdit(head.getTermId())) {
      return false;
    }
    Snapshot snapshot = GlossaryTermVersions.decodeSnapshot(row);
    return canReadTermVersion(
        head,
        row,
        access,
        snapshot,
        snapshot.term(NamespaceUtil.ofGlossaryTerm(metalake)),
        currentUserIds);
  }

  private boolean canReadTermVersion(
      Head head, Version row, GlossaryAccess access, Snapshot snapshot, GlossaryTermEntity term) {
    return canReadTermVersion(head, row, access, snapshot, term, null);
  }

  private boolean canReadTermVersion(
      Head head,
      Version row,
      GlossaryAccess access,
      Snapshot snapshot,
      GlossaryTermEntity term,
      @Nullable Map<Long, Long> currentUserIds) {
    if (!readable.test(term)) return false;
    if (State.PUBLISHED.name().equals(row.getLifecycle())) return access.canRead(term);
    // A candidate's stored definition and its private DRAFT view must both pass the filter.
    if (readable != ALL_TERMS
        && !readable.test(
            TermEdits.toPublicationView(
                term, term.domains(), GlossaryTermVersions.toPublicationResult(head, row)))) {
      return false;
    }
    if (head.getPublishedVersion() != null) return access.canEdit(term);
    if (access.manage()) return true;
    String currentUser = PrincipalUtils.getCurrentUserName();
    if (!term.auditInfo().creator().equals(currentUser)) return false;
    if (!access.canCreate()) return false;
    long metalakeId = snapshot.definition().getMetalakeId();
    Long currentUserId;
    if (currentUserIds != null && currentUserIds.containsKey(metalakeId)) {
      currentUserId = currentUserIds.get(metalakeId);
    } else {
      currentUserId =
          SessionUtils.getWithoutCommit(
              UserMetaMapper.class,
              m -> m.selectUserIdByMetalakeIdAndName(metalakeId, currentUser));
      if (currentUserIds != null) {
        currentUserIds.put(metalakeId, currentUserId);
      }
    }
    return currentUserId != null && currentUserId.equals(snapshot.creatorUserId());
  }

  private TermPublication recordCandidateDecision(
      String metalake, long termId, long candidateVersionId, State outcome) {
    if (!policy.canPublishOrRejectCandidate(metalake, termId, candidateVersionId))
      throw new ForbiddenException("Publication decision is not authorized");
    Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
    return terms.withHierarchyLock(
        ns,
        lake ->
            withVersionMapper(
                mapper -> {
                  Head head = mapper.getVersionHeadByMetalakeAndTermId(lake, termId);
                  if (head == null
                      || !Objects.equals(head.getCandidateVersion(), candidateVersionId))
                    throw staleCandidateConflict();
                  Version row = mapper.getVersionByTermIdAndVersionId(termId, candidateVersionId);
                  if (!State.SUBMITTED.name().equals(row.getLifecycle()))
                    throw staleCandidateConflict();
                  Snapshot snapshot = GlossaryTermVersions.decodeSnapshot(row);
                  Set<Long> locks = new HashSet<>();
                  if (outcome == State.PUBLISHED) {
                    locks.addAll(snapshot.related());
                    locks.addAll(snapshot.broader());
                  }
                  if (head.getPublishedVersion() != null) {
                    if (terms.listTermsByIds(ns, lake, Set.of(termId)).isEmpty()) {
                      throw new NoSuchTermException("Glossary term %s does not exist", termId);
                    }
                    locks.add(termId);
                    if (outcome == State.PUBLISHED) {
                      locks.addAll(edges.listBroader(lake, termId));
                      locks.addAll(edges.listRelated(lake, termId));
                    }
                  }
                  boolean[] lockedEntered = {false};
                  try {
                    return edges.withHierarchyLock(
                        lake,
                        locks,
                        () -> {
                          lockedEntered[0] = true;
                          SessionUtils.clearSessionCache();
                          Head locked = mapper.getVersionHeadByMetalakeAndTermId(lake, termId);
                          if (!Objects.equals(locked.getCandidateVersion(), candidateVersionId))
                            throw staleCandidateConflict();
                          GlossaryTermEntity after = snapshot.term(ns);
                          if (!readable.test(after))
                            throw new NoSuchTermException("Glossary term does not exist");
                          if (outcome == State.PUBLISHED) {
                            persistCandidateAsLiveTerm(
                                metalake, lake, locked, row, snapshot, after);
                            locked.setPublishedVersion(candidateVersionId);
                          }
                          locked.setCandidateVersion(null);
                          locked.setReservedName(null);
                          row.setLifecycle(outcome.name());
                          mapper.updateVersionLifecycle(termId, candidateVersionId, outcome.name());
                          mapper.updateVersionHead(locked);
                          return GlossaryTermVersions.toPublicationResult(locked, row);
                        });
                  } catch (NoSuchEntityException missing) {
                    if (!lockedEntered[0]) {
                      throw new NoSuchTermException(
                          missing, "Referenced term is no longer visible");
                    }
                    throw missing;
                  }
                }));
  }

  private void persistCandidateAsLiveTerm(
      String metalake,
      long lake,
      Head head,
      Version row,
      Snapshot snapshot,
      GlossaryTermEntity after) {
    long id = head.getTermId();
    Namespace ns = after.namespace();
    GlossaryTermEntity before =
        head.getPublishedVersion() == null
            ? null
            : terms.listTermsByIds(ns, lake, Set.of(id)).get(0);
    if (before != null && !readable.test(before))
      throw new NoSuchTermException("Glossary term does not exist");
    if (before != null
        && (before.version() != row.getBaseRevision()
            || !edges.listRelated(lake, id).equals(snapshot.baseRelated())))
      throw staleCandidateConflict();
    TermRelations relations = new TermRelations(lake, id, before == null);
    Set<Long> addedBroader = new HashSet<>(snapshot.broader());
    addedBroader.removeAll(relations.broader());
    Set<Long> addedRelated = new HashSet<>(snapshot.related());
    addedRelated.removeAll(relations.related());
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    if (!visibleEndpoints(lake, after, addedBroader, access).equals(addedBroader)
        || !visibleEndpoints(lake, after, addedRelated, access).equals(addedRelated))
      throw new NoSuchTermException("Referenced term is no longer visible");
    relations.replace(snapshot.broader(), snapshot.related());
    relations.validate(depthLimit);
    Set<Long> current = before == null ? Set.of() : DOMAINS.domainIds(lake, id);
    Map<Long, DomainPO> domainRows =
        DOMAINS.lockDomains(lake, current, snapshot.domainIds(), Set.of(), ignored -> {});
    Map<Long, String> oldNames = new HashMap<>(), newNames = new HashMap<>();
    for (long domain : current) oldNames.put(domain, domainRows.get(domain).domainName());
    for (long domain : snapshot.domainIds()) {
      DomainPO target = domainRows.get(domain);
      String frozenName = snapshot.domains().get(domain);
      if (target == null) {
        throw new NoSuchDomainException("Domain %s does not exist", frozenName);
      }
      if (!current.contains(domain)) {
        requireAccessToResolvedDomain(frozenName, target, access);
      }
      newNames.put(domain, target.domainName());
    }
    TermDomains domains = new TermDomains(lake, id, oldNames);
    domains.replace(newNames);
    if (before != null) {
      before = TermEdits.withDomains(before, domains.originalNames());
    }
    if (before == null && authorizationEnabled) {
      lockCandidateCreator(lake, after.auditInfo().creator(), snapshot.creatorUserId());
    }
    GlossaryTermEntity resolvedAfter = TermEdits.withDomains(after, domains.names());
    checkTermNameReservation(lake, resolvedAfter);
    terms.validateTermNameSynonymsAndSourceRef(resolvedAfter);
    GlossaryTermEntity finalBefore = before;
    handler.handle(
        finalBefore,
        resolvedAfter,
        () -> {
          if (finalBefore == null) persistNewTerm(metalake, resolvedAfter, relations, domains);
          else {
            persistDefinition(finalBefore.nameIdentifier(), resolvedAfter);
            relations.persist();
            domains.persist();
          }
        });
  }

  private static TermVersionConflictException staleCandidateConflict() {
    return new TermVersionConflictException("The exact submitted candidate is no longer current");
  }

  private static void lockCandidateCreator(long lake, String name, @Nullable Long expectedId) {
    UserPO creator =
        expectedId == null
            ? null
            : SessionUtils.getWithoutCommit(
                UserMetaMapper.class, m -> m.selectUserMetaByIdForShare(expectedId));
    if (creator == null || creator.getMetalakeId() != lake || !name.equals(creator.getUserName())) {
      throw new NoSuchUserException("Original candidate creator %s no longer exists", name);
    }
  }

  @Nullable
  private static Long findSnapshotReference(
      Map<Long, String> names, Set<Long> members, String name) {
    String key = GlossaryTermNames.key(name);
    for (long id : members) {
      String previous = names.get(id);
      if (previous != null && key.equals(GlossaryTermNames.key(previous))) return id;
    }
    return null;
  }

  private void persistNewTerm(
      String metalake, GlossaryTermEntity term, @Nullable TermRelations plan, TermDomains domains) {
    try {
      terms.insert(term, false);
    } catch (EntityAlreadyExistsException e) {
      throw new TermAlreadyExistsException(e, "Term name or source reference already exists");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (plan != null) {
      plan.persist();
    }
    domains.persist();
    if (authorizationEnabled) {
      OwnerMetaService.getInstance()
          .setOwner(
              term.nameIdentifier(),
              EntityType.GLOSSARY_TERM,
              NameIdentifierUtil.ofUser(metalake, term.auditInfo().creator()),
              EntityType.USER);
    }
  }

  private void persistDefinition(NameIdentifier ident, GlossaryTermEntity after) {
    try {
      terms.update(ident, (GlossaryTermEntity ignored) -> after);
    } catch (EntityAlreadyExistsException e) {
      throw new TermAlreadyExistsException(e, "Term name or source reference already exists");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<GlossaryTermEntity> loadTerms(String metalake) {
    try {
      return terms.list(NamespaceUtil.ofGlossaryTerm(metalake));
    } catch (NoSuchEntityException e) {
      throw new NoSuchMetalakeException(e, "Metalake %s does not exist", metalake);
    }
  }

  private static long termReadMetalakeId(NameIdentifier ident) {
    try {
      return MetalakeMetaService.getInstance().getMetalakeIdByName(ident.namespace().level(0));
    } catch (NoSuchEntityException missing) {
      throw missingRelationRoot(ident);
    }
  }

  private GlossaryTermEntity requireVisible(NameIdentifier ident, GlossaryTermEntity term) {
    if (!readable.test(term)) {
      throw missingRelationRoot(ident);
    }
    return term;
  }

  private GlossaryTermEntity loadRootForView(
      NameIdentifier ident, long metalakeId, GlossaryAccess access) {
    if (!access.canView()) {
      throw missingRelationRoot(ident);
    }
    try {
      return terms.get(ident, metalakeId);
    } catch (NoSuchEntityException missing) {
      throw missingRelationRoot(ident);
    }
  }

  private ScopedTerm relationRoot(NameIdentifier requested) {
    try {
      return terms.getWithMetalakeId(requested);
    } catch (NoSuchEntityException missing) {
      throw missingRelationRoot(requested);
    }
  }

  // A relation caller must receive the same error for missing and unreadable roots.
  private static NoSuchTermException missingRelationRoot(NameIdentifier requested) {
    return new NoSuchTermException("Term %s does not exist", requested);
  }

  /**
   * Resolves, authorizes and locks every requested domain after all term rows of the request are
   * locked. A missing domain and a hidden draft domain report the same error.
   */
  private TermDomains planDomains(
      long lake, GlossaryTermEntity term, List<TermDomains.Edit> edits, GlossaryAccess access) {
    Set<Long> members = term.domains().length == 0 ? Set.of() : DOMAINS.domainIds(lake, term.id());
    return planDomains(lake, term.id(), members, edits, access);
  }

  private TermDomains planDomains(
      long lake,
      long termId,
      Set<Long> members,
      List<TermDomains.Edit> edits,
      GlossaryAccess access) {
    Set<String> requested = new HashSet<>();
    for (TermDomains.Edit edit : edits) {
      requested.add(edit.domain());
    }
    if (members.isEmpty() && requested.isEmpty()) {
      return new TermDomains(lake, termId, Map.of());
    }
    Map<String, DomainPO> targets = new HashMap<>();
    Map<Long, DomainPO> locked =
        DOMAINS.lockDomains(
            lake,
            members,
            requested,
            domain -> {
              String name = domain.domainName();
              requireVisibleDomain(name, domain, access);
              targets.put(name, domain);
            });
    Map<Long, String> current = new HashMap<>();
    for (long id : members) {
      current.put(id, locked.get(id).domainName());
    }
    TermDomains plan = new TermDomains(lake, termId, current);
    for (TermDomains.Edit edit : edits) {
      DomainPO target = targets.get(edit.domain());
      plan.edit(edit, target.domainId(), target.domainName());
    }
    return plan;
  }

  private TermDomains planCandidateDomains(
      long lake,
      long termId,
      Map<Long, String> previousDomains,
      List<TermDomains.Edit> edits,
      GlossaryAccess access) {
    Set<String> requested = new HashSet<>();
    Map<String, Long> snapshotIdsByName = new HashMap<>();
    previousDomains.forEach((id, name) -> snapshotIdsByName.put(name, id));
    for (TermDomains.Edit edit : edits) {
      if (edit.add() || !snapshotIdsByName.containsKey(edit.domain())) requested.add(edit.domain());
    }
    if (previousDomains.isEmpty() && requested.isEmpty()) {
      return new TermDomains(lake, termId, Map.of());
    }
    Map<String, DomainPO> targets = new HashMap<>();
    Map<Long, DomainPO> locked =
        DOMAINS.lockDomains(
            lake,
            Set.of(),
            previousDomains.keySet(),
            requested,
            domain -> {
              String name = domain.domainName();
              requireVisibleDomain(name, domain, access);
              targets.put(name, domain);
            });
    Map<Long, String> current = new HashMap<>(previousDomains);
    for (long id : previousDomains.keySet()) {
      DomainPO row = locked.get(id);
      if (row != null) {
        current.put(id, row.domainName());
      }
    }
    TermDomains plan = new TermDomains(lake, termId, current);
    for (TermDomains.Edit edit : edits) {
      // After a name is added in this request, later removals refer to that live identity.
      if (edit.add()) snapshotIdsByName.remove(edit.domain());
      Long originalId = snapshotIdsByName.get(edit.domain());
      DomainPO target = originalId == null ? targets.get(edit.domain()) : locked.get(originalId);
      if (target != null) {
        requireAccessToResolvedDomain(edit.domain(), target, access);
        plan.edit(edit, target.domainId(), target.domainName());
      } else if (!edit.add() && originalId != null) {
        plan.edit(edit, originalId, edit.domain());
      }
    }
    for (long id : plan.ids()) {
      if (!locked.containsKey(id)) {
        throw new NoSuchDomainException("Domain %s does not exist", current.get(id));
      }
    }
    return plan;
  }

  /**
   * Resolves the domain scope of a list filter: the domain and its visible direct subdomains. A
   * missing domain and a hidden draft domain report the same error.
   */
  Set<Long> domainScope(
      String metalake,
      long metalakeId,
      String name,
      GlossaryAccess access,
      @Nullable TermLookupBudget budget) {
    DomainPO domain = DOMAINS.findDomain(metalakeId, name);
    requireVisibleDomain(name, domain, access);
    Set<Long> scope = new HashSet<>();
    scope.add(domain.domainId());
    if (budget != null) {
      budget.consume(1);
      budget.consume(
          SessionUtils.getWithoutCommit(
              DomainRelMapper.class,
              mapper ->
                  mapper.countLiveChildren(metalakeId, domain.domainId(), budget.remaining() + 1)));
    }
    for (DomainEntity child :
        DomainRelService.getInstance()
            .listSubDomains(NameIdentifierUtil.ofDomain(metalake, domain.domainName()))) {
      if (access.canUseDomain(child)) {
        scope.add(child.id());
      }
    }
    return scope;
  }

  private static void requireVisibleDomain(
      String name, @Nullable DomainPO domain, GlossaryAccess access) {
    if (domain == null || !name.equals(domain.domainName())) {
      throw new NoSuchDomainException("Domain %s does not exist", name);
    }
    requireAccessToResolvedDomain(name, domain, access);
  }

  private static void requireAccessToResolvedDomain(
      String requestedName, DomainPO domain, GlossaryAccess access) {
    if (!access.canUseDomain(domain.domainName())) {
      if (Domain.Status.DRAFT.name().equals(domain.status())) {
        throw new NoSuchDomainException("Domain %s does not exist", requestedName);
      }
      throw new ForbiddenException("Cannot use domain %s", requestedName);
    }
  }

  /** Returns the term with only the domains the caller can see. Stored membership is unchanged. */
  static GlossaryTermEntity visibleDomains(GlossaryTermEntity term, GlossaryAccess access) {
    String[] domains = term.domains();
    String[] visible = Arrays.stream(domains).filter(access::canUseDomain).toArray(String[]::new);
    return visible.length == domains.length ? term : TermEdits.withDomains(term, visible);
  }

  private static List<GlossaryTermEntity> visibleDomains(
      List<GlossaryTermEntity> terms, GlossaryAccess access) {
    List<GlossaryTermEntity> result = new ArrayList<>(terms.size());
    for (GlossaryTermEntity term : terms) {
      result.add(visibleDomains(term, access));
    }
    return result;
  }

  private static long lakeOf(NameIdentifier ident) {
    return MetalakeMetaService.getInstance().getMetalakeIdByName(ident.namespace().level(0));
  }

  private <T> T withRelationPlan(
      long lake,
      GlossaryTermEntity root,
      List<TermRelations.Edit> edits,
      GlossaryAccess access,
      boolean creating,
      Consumer<GlossaryTermEntity> checkRoot,
      BiFunction<GlossaryTermEntity, TermRelations, T> work) {
    if (edits.isEmpty()) {
      return work.apply(root, null);
    }
    String rootKey = GlossaryTermNames.key(root.name());
    Set<String> keys = new LinkedHashSet<>();
    for (TermRelations.Edit edit : edits) {
      keys.add(GlossaryTermNames.key(edit.target()));
    }
    keys.remove(rootKey);
    Map<String, Long> targets = terms.getIdsByKeys(lake, keys);
    Set<Long> endpoints = Set.copyOf(targets.values());
    Set<Long> locks = new HashSet<>(endpoints);
    if (!creating) {
      locks.add(root.id());
    }
    return edges.withHierarchyLock(
        lake,
        locks,
        () -> {
          GlossaryTermEntity locked = creating ? root : terms.get(root.nameIdentifier());
          checkRoot.accept(locked);
          Set<Long> visibleIds = new HashSet<>();
          for (GlossaryTermEntity endpoint :
              terms.listTermsByIds(root.namespace(), lake, endpoints)) {
            if (access.canRead(endpoint) && readable.test(endpoint)) {
              visibleIds.add(endpoint.id());
            }
          }
          TermRelations plan = new TermRelations(lake, root.id(), creating);
          for (TermRelations.Edit edit : edits) {
            String key = GlossaryTermNames.key(edit.target());
            Long id = key.equals(rootKey) ? root.id() : targets.get(key);
            if (id == null || (!id.equals(root.id()) && !visibleIds.contains(id))) {
              throw new NoSuchTermException("Term %s does not exist", edit.target());
            }
            plan.edit(edit, id);
          }
          plan.validate(depthLimit);
          return work.apply(locked, plan);
        });
  }
}
