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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.gravitino.storage.relational.service.DomainRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.TargetType;
import org.apache.gravitino.storage.relational.service.GlossaryTermDomainService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService.ScopedTerm;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.service.LiveEndpointService;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** Manages live glossary definitions. Callers supply the metalake scope for every operation. */
public final class GlossaryManager {
  private static final GlossaryTermDomainService DOMAINS = GlossaryTermDomainService.getInstance();
  private static final GlossaryTermBindingService BINDINGS =
      GlossaryTermBindingService.getInstance();
  private static final int RESOLVE_BATCH_SIZE = 500;
  static final int MAX_HIERARCHY_DEPTH = 20;
  private final GlossaryTermRelService edges = GlossaryTermRelService.getInstance();
  private final Predicate<GlossaryTermEntity> readable;
  private final int depthLimit;
  private final GlossaryTermMetaService terms = GlossaryTermMetaService.getInstance();
  private final IdGenerator ids;
  private final boolean authorizationEnabled;
  private final TermChangeHandler handler;

  /** Creates a manager that applies effective changes immediately. */
  public GlossaryManager(IdGenerator ids, boolean authorizationEnabled) {
    this(ids, authorizationEnabled, (before, after, persist) -> persist.run());
  }

  GlossaryManager(IdGenerator ids, boolean authorizationEnabled, TermChangeHandler handler) {
    this(ids, authorizationEnabled, handler, term -> true, MAX_HIERARCHY_DEPTH);
  }

  GlossaryManager(
      IdGenerator ids,
      boolean authorizationEnabled,
      TermChangeHandler handler,
      Predicate<GlossaryTermEntity> readable,
      int depthLimit) {
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
        ids, authorizationEnabled, handler, readable.and(visibility), depthLimit);
  }

  record SnapshotTerms(long metalakeId, GlossaryAccess access, List<GlossaryTermEntity> terms) {}

  boolean isTermVisible(
      GlossaryTermEntity term, GlossaryAccess access, @Nullable GlossaryTerm.Status status) {
    return (status == null || status == term.status())
        && access.canRead(term)
        && readable.test(term);
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
        () -> listTermsInSnapshot(metalake, domain, ignored -> true, null).terms());
  }

  SnapshotTerms listTermsInSnapshot(
      String metalake,
      @Nullable String domain,
      Predicate<GlossaryTermEntity> matchesName,
      @Nullable GlossaryTerm.Status status) {
    long metalakeId = resolveMetalakeId(metalake);
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    return new SnapshotTerms(
        metalakeId,
        access,
        listVisibleTermsInSnapshot(
            metalake,
            metalakeId,
            access,
            domain,
            matchesName,
            status,
            GlossaryManager::visibleDomains));
  }

  String[] listTermNamesInSnapshot(
      String metalake,
      @Nullable String domain,
      Predicate<GlossaryTermEntity> matchesName,
      @Nullable GlossaryTerm.Status status) {
    long metalakeId = resolveMetalakeId(metalake);
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    return listVisibleTermsInSnapshot(
            metalake,
            metalakeId,
            access,
            domain,
            matchesName,
            status,
            (term, ignored) -> term.name())
        .toArray(String[]::new);
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
   * Creates a live definition. When authorization is enabled the creator must be a registered user
   * in the metalake. The definition and creator ownership share a transaction. Callers that open an
   * enclosing transaction must roll it back if creation fails.
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
                terms.checkTermNames(created);
                // The hierarchy lock fences concurrent creates, renames and deletes. Reject a
                // duplicate without waiting on its term row while holding requested domain locks.
                if (!terms
                    .getIdsByKeys(lake, Set.of(GlossaryTermNames.key(created.name())))
                    .isEmpty()) {
                  throw new TermAlreadyExistsException(
                      "Term name or source reference already exists");
                }
                handler.handle(
                    null, created, () -> persistNewTerm(metalake, created, plan, domains));
                return visibleDomains(created, access);
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
   * @throws ForbiddenException if the term is visible but the caller lacks management access
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
                            () -> persistDefinition(survivor.nameIdentifier(), after));
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
   * @throws IllegalArgumentException if the target type is unsupported, a term name is blank, or an
   *     object-admin-only caller binds/unbinds a visible non-APPROVED term
   * @throws NoSuchMetadataObjectException if the target object does not exist
   * @throws NoSuchTermException if a requested term is missing or hidden from the caller
   * @throws ForbiddenException if the caller satisfies neither authorization path for a term
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
   * @throws IllegalArgumentException if the object is not a semantic model, the metric or a term
   *     name is blank, or an object-admin-only caller binds/unbinds a visible non-APPROVED term
   * @throws NoSuchMetadataObjectException if the semantic model or metric does not exist
   * @throws NoSuchTermException if a requested term is missing or hidden from the caller
   * @throws ForbiddenException if the caller satisfies neither authorization path for a term
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
    TargetType targetType = TargetType.fromMetadataObjectType(object.type());
    return SqlSessions.readSnapshot(
        () -> listTargetTermsInSnapshot(metalake, object, targetType, null, status));
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
    return SqlSessions.readSnapshot(
        () ->
            listTargetTermsInSnapshot(
                metalake, semanticModel, TargetType.METRIC, metricName, status));
  }

  /**
   * Lists the authorized bindings of a visible glossary term in a single read snapshot.
   *
   * @throws IllegalArgumentException if {@code types} contains a null or unsupported type
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
        () -> listTermBindingsInSnapshot(metalake, termName, targetTypes, status));
  }

  private String[] associateTargetTerms(
      String metalake,
      MetadataObject targetObject,
      TargetType targetType,
      @Nullable String metricName,
      @Nullable String[] termsToAdd,
      @Nullable String[] termsToRemove) {
    List<String> addNames = normalizeTermNames(termsToAdd);
    List<String> removeNames = normalizeTermNames(termsToRemove);
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermBindingMapper.class,
        ignoredMapper -> {
          GlossaryBindingAccess objectAccess =
              GlossaryBindingAccess.forRequest(metalake, authorizationEnabled);
          boolean canReadObject = objectAccess.canReadTarget(targetObject);
          boolean canAdminObject = objectAccess.canAdminTarget(targetObject);
          if (!canReadObject && !canAdminObject) {
            throw new ForbiddenException(
                "Caller lacks permission to modify bindings on %s", targetObject.fullName());
          }
          ResolvedTarget target =
              resolveAndLockTarget(metalake, targetObject, targetType, metricName);
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
            Map<String, Long> idsByKey =
                terms.getIdsByKeys(target.metalakeId(), requestedOrder.keySet());
            Map<Long, GlossaryTermEntity> lockedById =
                terms.lockByIds(ns, target.metalakeId(), Set.copyOf(idsByKey.values()));
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
              boolean termEditorPath = termAccess.canEdit(term) && canReadObject;
              boolean objectAdminPath = canAdminObject && termAccess.canRead(term);
              if (termEditorPath) {
                continue;
              }
              if (objectAdminPath) {
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

            Set<Long> currentlyBound =
                BINDINGS.listTermIds(target.metalakeId(), targetType, target.targetId());
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
            for (long termId : changedIds) {
              GlossaryTermEntity before = lockedById.get(termId);
              GlossaryTermEntity after = TermEdits.apply(before, new TermChange[0], true);
              boolean isAdd = toAddIds.contains(termId);
              handler.handle(
                  before,
                  after,
                  () -> {
                    if (isAdd) {
                      BINDINGS.add(target.metalakeId(), termId, targetType, target.targetId());
                    } else {
                      Preconditions.checkState(
                          BINDINGS.remove(termId, targetType, target.targetId()),
                          "Failed to remove binding for locked glossary term %s",
                          termId);
                    }
                    persistDefinition(before.nameIdentifier(), after);
                  });
            }
          }

          Set<Long> finalBoundIds =
              BINDINGS.listTermIds(target.metalakeId(), targetType, target.targetId());
          if (finalBoundIds.isEmpty() || !termAccess.canView()) {
            return new String[0];
          }
          return terms.listTermsByIds(ns, target.metalakeId(), finalBoundIds).stream()
              .filter(term -> isTermVisible(term, termAccess, null))
              .map(GlossaryTermEntity::name)
              .sorted()
              .toArray(String[]::new);
        });
  }

  private List<GlossaryTermEntity> listTargetTermsInSnapshot(
      String metalake,
      MetadataObject targetObject,
      TargetType targetType,
      @Nullable String metricName,
      @Nullable GlossaryTerm.Status status) {
    GlossaryBindingAccess objectAccess =
        GlossaryBindingAccess.forRequest(metalake, authorizationEnabled);
    if (!objectAccess.canReadTarget(targetObject)) {
      throw new ForbiddenException(
          "Caller lacks read access to target %s", targetObject.fullName());
    }
    ResolvedTarget target = resolveTargetReadOnly(metalake, targetObject, targetType, metricName);
    GlossaryAccess termAccess = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    if (!termAccess.canView()) {
      return List.of();
    }
    Set<Long> boundIds = BINDINGS.listTermIds(target.metalakeId(), targetType, target.targetId());
    if (boundIds.isEmpty()) {
      return List.of();
    }
    Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
    List<GlossaryTermEntity> visible = new ArrayList<>();
    for (GlossaryTermEntity term : terms.listTermsByIds(ns, target.metalakeId(), boundIds)) {
      if (isTermVisible(term, termAccess, status)) {
        visible.add(visibleDomains(term, termAccess));
      }
    }
    visible.sort(Comparator.comparing(GlossaryTermEntity::name));
    return visible;
  }

  private List<TermBinding> listTermBindingsInSnapshot(
      String metalake,
      String termName,
      @Nullable Set<TargetType> targetTypes,
      @Nullable GlossaryTerm.Status status) {
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, termName);
    long lake = termReadMetalakeId(ident);
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    GlossaryTermEntity root = loadRootForView(ident, lake, access);
    if (!isTermVisible(root, access, status)) {
      throw missingRelationRoot(ident);
    }
    if (targetTypes != null && targetTypes.isEmpty()) {
      return List.of();
    }
    List<GlossaryTermBindingPO> rows = BINDINGS.listByTerm(lake, root.id(), targetTypes);
    if (rows.isEmpty()) {
      return List.of();
    }
    GlossaryBindingAccess objectAccess =
        GlossaryBindingAccess.forRequest(metalake, authorizationEnabled);
    return resolveAndFilterBindings(rows, objectAccess);
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

  private List<TermBinding> resolveAndFilterBindings(
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
        for (List<Long> batch : Lists.partition(entry.getValue(), RESOLVE_BATCH_SIZE)) {
          List<SemanticModelMetricPO> metricPOs =
              SessionUtils.getWithoutCommit(
                  SemanticModelMetricMapper.class,
                  mapper -> mapper.listSemanticModelMetricsByMetricIds(batch));
          for (SemanticModelMetricPO metricPO : metricPOs) {
            metricsById.put(metricPO.metricId(), metricPO);
            modelIds.add(metricPO.semanticModelId());
          }
        }
        for (List<Long> batch : Lists.partition(List.copyOf(modelIds), RESOLVE_BATCH_SIZE)) {
          semanticModelsById.putAll(
              MetadataObjectService.resolveMetadataObjects(
                  MetadataObject.Type.SEMANTIC_MODEL, batch));
        }
      } else {
        Map<Long, MetadataObject> resolved = new HashMap<>(entry.getValue().size());
        for (List<Long> batch : Lists.partition(entry.getValue(), RESOLVE_BATCH_SIZE)) {
          resolved.putAll(
              MetadataObjectService.resolveMetadataObjects(type.toMetadataObjectType(), batch));
        }
        objectsByType.put(type, resolved);
      }
    }

    List<TermBinding> result = new ArrayList<>();
    for (GlossaryTermBindingPO row : rows) {
      TargetType type = TargetType.valueOf(row.getTargetType());
      if (type == TargetType.METRIC) {
        SemanticModelMetricPO metricPO = metricsById.get(row.getTargetId());
        if (metricPO == null) {
          continue;
        }
        MetadataObject modelObj = semanticModelsById.get(metricPO.semanticModelId());
        if (modelObj != null && objectAccess.canReadTarget(modelObj)) {
          result.add(TermBinding.usedBy(modelObj, metricPO.metricName()));
        }
      } else {
        MetadataObject obj = objectsByType.getOrDefault(type, Map.of()).get(row.getTargetId());
        if (obj != null && objectAccess.canReadTarget(obj)) {
          result.add(TermBinding.implementedBy(obj));
        }
      }
    }
    return result;
  }

  private static final class ResolvedTarget {
    private final long metalakeId;
    private final long targetId;

    private ResolvedTarget(long metalakeId, long targetId) {
      this.metalakeId = metalakeId;
      this.targetId = targetId;
    }

    private long metalakeId() {
      return metalakeId;
    }

    private long targetId() {
      return targetId;
    }
  }

  private static ResolvedTarget resolveAndLockTarget(
      String metalake,
      MetadataObject targetObject,
      TargetType targetType,
      @Nullable String metricName) {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, targetObject);
    EntityType entityType = MetadataObjectUtil.toEntityType(targetObject);
    RelationalEntityStoreIdResolver resolver = new RelationalEntityStoreIdResolver();
    NamespacedEntityId observed;
    try {
      observed = resolver.getEntityIds(entityIdent, entityType);
      LiveEndpointService.lockLiveEndpoint(entityIdent, entityType, observed);
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetadataObjectException(
          missing, "Metadata object %s does not exist", targetObject.fullName());
    }
    long metalakeId = observed.namespaceIds()[0];
    if (targetType == TargetType.METRIC) {
      SemanticModelMetricPO metric =
          SessionUtils.getWithoutCommit(
              SemanticModelMetricMapper.class,
              mapper ->
                  mapper.selectSemanticModelMetricByModelIdAndName(
                      observed.entityId(), metricName));
      if (metric == null) {
        throw new NoSuchMetadataObjectException(
            "Metric %s does not exist in semantic model %s", metricName, targetObject.fullName());
      }
      return new ResolvedTarget(metalakeId, metric.metricId());
    }
    return new ResolvedTarget(metalakeId, observed.entityId());
  }

  private static ResolvedTarget resolveTargetReadOnly(
      String metalake,
      MetadataObject targetObject,
      TargetType targetType,
      @Nullable String metricName) {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, targetObject);
    EntityType entityType = MetadataObjectUtil.toEntityType(targetObject);
    RelationalEntityStoreIdResolver resolver = new RelationalEntityStoreIdResolver();
    NamespacedEntityId observed;
    try {
      observed = resolver.getEntityIds(entityIdent, entityType);
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetadataObjectException(
          missing, "Metadata object %s does not exist", targetObject.fullName());
    }
    long metalakeId = observed.namespaceIds()[0];
    if (targetType == TargetType.METRIC) {
      SemanticModelMetricPO metric =
          SessionUtils.getWithoutCommit(
              SemanticModelMetricMapper.class,
              mapper ->
                  mapper.selectSemanticModelMetricByModelIdAndName(
                      observed.entityId(), metricName));
      if (metric == null) {
        throw new NoSuchMetadataObjectException(
            "Metric %s does not exist in semantic model %s", metricName, targetObject.fullName());
      }
      return new ResolvedTarget(metalakeId, metric.metricId());
    }
    return new ResolvedTarget(metalakeId, observed.entityId());
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

  private static long resolveMetalakeId(String metalake) {
    try {
      return MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
    }
  }

  private <T> List<T> listVisibleTermsInSnapshot(
      String metalake,
      long metalakeId,
      GlossaryAccess access,
      @Nullable String domain,
      Predicate<GlossaryTermEntity> matchesName,
      @Nullable GlossaryTerm.Status status,
      BiFunction<GlossaryTermEntity, GlossaryAccess, T> project) {
    if (!access.canView()) {
      return new ArrayList<>();
    }
    Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
    List<GlossaryTermEntity> result;
    if (domain == null) {
      result = terms.list(ns, metalakeId);
    } else {
      Set<Long> scope = domainScope(metalake, metalakeId, domain, access);
      result = terms.listTermsByIds(ns, metalakeId, DOMAINS.termIds(metalakeId, scope));
    }
    List<T> visible = new ArrayList<>();
    for (GlossaryTermEntity term : result) {
      if (matchesName.test(term) && isTermVisible(term, access, status)) {
        visible.add(project.apply(term, access));
      }
    }
    return visible;
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
              applyChanges(ident, before, content.toArray(new TermChange[0]), null, domains),
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
      Preconditions.checkArgument(
          token != null && token.length() == 36 && UUID.fromString(token).toString().equals(token),
          "Malformed glossary revision token");
      if (!before.revisionToken().equals(token)) {
        throw new TermVersionConflictException("Term %s has changed", ident);
      }
    }
  }

  private GlossaryTermEntity applyChanges(
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
            before, request, plan != null && plan.changed(), membership ? domains.names() : null);
    if (before == after) {
      return before;
    }
    if (TermEdits.needsHierarchyLock(request)) {
      terms.checkTermNames(after);
    }
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
    return after;
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
    Set<String> requested = new HashSet<>();
    for (TermDomains.Edit edit : edits) {
      requested.add(edit.domain());
    }
    if (members.isEmpty() && requested.isEmpty()) {
      return new TermDomains(lake, term.id(), Map.of());
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
    TermDomains plan = new TermDomains(lake, term.id(), current);
    for (TermDomains.Edit edit : edits) {
      DomainPO target = targets.get(edit.domain());
      plan.edit(edit, target.domainId(), target.domainName());
    }
    return plan;
  }

  /**
   * Resolves the domain scope of a list filter: the domain and its visible direct subdomains. A
   * missing domain and a hidden draft domain report the same error.
   */
  private Set<Long> domainScope(
      String metalake, long metalakeId, String name, GlossaryAccess access) {
    DomainPO domain = DOMAINS.findDomain(metalakeId, name);
    requireVisibleDomain(name, domain, access);
    Set<Long> scope = new HashSet<>();
    scope.add(domain.domainId());
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
    if (!access.canUseDomain(name)) {
      if (Domain.Status.DRAFT.name().equals(domain.status())) {
        throw new NoSuchDomainException("Domain %s does not exist", name);
      }
      throw new ForbiddenException("Cannot use domain %s", name);
    }
  }

  /** Returns the term with only the domains the caller can see. Stored membership is unchanged. */
  private static GlossaryTermEntity visibleDomains(GlossaryTermEntity term, GlossaryAccess access) {
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
