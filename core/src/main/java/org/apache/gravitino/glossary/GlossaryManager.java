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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.glossary.TermPublication.State;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper.Head;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermVersionMapper.Version;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.DomainRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermDomainService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService.ScopedTerm;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.service.GlossaryTermVersions;
import org.apache.gravitino.storage.relational.service.GlossaryTermVersions.Snapshot;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Manages live glossary definitions. Callers supply the metalake scope for every operation. */
public final class GlossaryManager {
  private static final GlossaryTermDomainService DOMAINS = GlossaryTermDomainService.getInstance();
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
        () -> {
          NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, name);
          long lake = termReadMetalakeId(ident);
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          GlossaryTermEntity root = loadRootForView(ident, lake, access);
          Predicate<GlossaryTermEntity> visible =
              term ->
                  (status == null || status == term.status())
                      && access.canRead(term)
                      && readable.test(term);
          if (!visible.test(root)) {
            throw missingRelationRoot(ident);
          }
          List<GlossaryTermEntity> result = new ArrayList<>();
          Set<Long> seen = new HashSet<>(Set.of(root.id()));
          Set<Long> frontier = Set.of(root.id());
          do {
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
                          || (direction == Direction.RELATED
                              && link.getToTermId().equals(root.id()))
                      ? link.getFromTermId()
                      : link.getToTermId();
              if (seen.add(id)) {
                next.add(id);
              }
            }
            frontier = new LinkedHashSet<>();
            for (GlossaryTermEntity term : terms.getByIds(root.namespace(), lake, next)) {
              if (visible.test(term)) {
                result.add(term);
                frontier.add(term.id());
              }
            }
          } while (transitive && !frontier.isEmpty());
          return visibleDomains(result, access);
        });
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
        () -> {
          long metalakeId;
          try {
            metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
          } catch (NoSuchEntityException missing) {
            throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
          }
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          if (!access.canView()) {
            return new ArrayList<>();
          }
          Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
          List<GlossaryTermEntity> result;
          if (domain == null) {
            result = terms.list(ns, metalakeId);
          } else {
            Set<Long> scope = domainScope(metalake, metalakeId, domain, access);
            result = terms.getByIds(ns, metalakeId, DOMAINS.termIds(metalakeId, scope));
          }
          result.removeIf(term -> !access.canRead(term) || !readable.test(term));
          return visibleDomains(result, access);
        });
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
    return SqlSessions.readSnapshot(
        () -> {
          long lake = termReadMetalakeId(ident);
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, true);
          GlossaryTermEntity term = loadRootForView(ident, lake, access);
          if (!access.canRead(term)) {
            throw missingRelationRoot(ident);
          }
          return visibleDomains(requireVisible(ident, term), access);
        });
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
                checkTermNameReservation(lake, created);
                State state =
                    resolvePublicationState(
                        policy.requiresApproval(metalake), spec.status().orElse(null));
                Preconditions.checkArgument(
                    state == State.PUBLISHED || created.status() != GlossaryTerm.Status.DEPRECATED,
                    "Omit status to submit a new term or specify DRAFT to save it.");
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
                          terms.getByIds(ident.namespace(), lake, affected)) {
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

  /** Definition and visible direct relationship identities from one exact business version. */
  public record TermVersion(GlossaryTermEntity term, Set<Long> broader, Set<Long> related) {}

  /** Lists accessible business versions newest first for a stable term identity. */
  public List<Long> listTermVersions(String metalake, long termId) {
    return readHistory(
        metalake,
        termId,
        (mapper, head) -> {
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          List<Long> result = new ArrayList<>();
          for (Version row : mapper.listVersionsByTermId(termId)) {
            if (canReadTermVersion(metalake, head, row, access)) result.add(row.getVersionId());
          }
          return result;
        });
  }

  /** Reads the definition, domain names and direct edges frozen for the exact version. */
  public TermVersion getTermVersion(String metalake, long termId, long versionId) {
    return readHistory(
        metalake,
        termId,
        (mapper, head) -> {
          Version row = mapper.getVersionByTermIdAndVersionId(termId, versionId);
          if (row == null || !canReadTermVersion(metalake, head, row))
            throw new NoSuchTermException("Glossary version does not exist");
          Snapshot snapshot = GlossaryTermVersions.decodeSnapshot(row);
          GlossaryTermEntity term = snapshot.term(NamespaceUtil.ofGlossaryTerm(metalake));
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
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
        (mapper, head) -> {
          if (!Objects.equals(head.getCandidateVersion(), draftVersionId))
            throw staleCandidateConflict();
          Version row = mapper.getVersionByTermIdAndVersionId(termId, draftVersionId);
          if (!State.DRAFT.name().equals(row.getLifecycle())) throw staleCandidateConflict();
          if (!canReadTermVersion(metalake, head, row))
            throw new NoSuchTermException("Glossary draft does not exist");
          Snapshot draft = GlossaryTermVersions.decodeSnapshot(row);
          draft.definition().setStatus(GlossaryTerm.Status.APPROVED.name());
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
                    return editUnpublishedCandidate(
                        ident, lake, head, row, conditional, token, changes);
                  });
            }
            return editPublishedTerm(metalake, ident, conditional, token, changes, true);
          },
          missing -> {
            throw missingRelationRoot(ident);
          });
    }
    return editPublishedTerm(metalake, ident, conditional, token, changes, false);
  }

  private GlossaryTermEntity editUnpublishedCandidate(
      NameIdentifier ident,
      long lake,
      Head head,
      Version row,
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
    TermRelations relations = new TermRelations(lake, term.id(), true);
    relations.replace(previous.broader(), previous.related());
    List<TermDomains.Edit> domainEdits = new ArrayList<>();
    State state = State.SUBMITTED;
    for (TermChange change : changes) {
      TermRelations.Edit edge = TermRelations.Edit.from(change);
      TermDomains.Edit domain = TermDomains.Edit.from(change);
      if (edge != null) {
        GlossaryTermEntity endpoint =
            relationRoot(NameIdentifierUtil.ofGlossaryTerm(metalake, edge.target())).term();
        if (!access.canRead(endpoint) || !readable.test(endpoint))
          throw new NoSuchTermException("Referenced term does not exist");
        relations.edit(edge, endpoint.id());
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
            domainsChanged ? domains.names() : null);
    if (state == State.SUBMITTED && edited.status() == GlossaryTerm.Status.DRAFT) {
      edited =
          TermEdits.copyWithStatus(edited, domains.names(), null, GlossaryTerm.Status.APPROVED);
    }
    terms.checkTermNames(edited);
    checkTermNameReservation(lake, edited);
    String key = GlossaryTermNames.key(edited.name());
    if (!terms.getIdsByKeys(lake, Set.of(key)).isEmpty())
      throw new TermAlreadyExistsException("Term name already exists");
    if (edited == term && state == publication.state())
      return visibleDomains(
          TermEdits.toPublicationView(term, domains.names(), publication), access);
    Snapshot next =
        GlossaryTermVersions.captureSnapshot(
            lake, edited, domains.ids(), relations.broader(), relations.related());
    next.definition().setCurrentVersion(1L);
    TermPublication result = GlossaryTermVersions.recordTermVersion(lake, next, 0, state, key);
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
      Preconditions.checkArgument(
          token != null && token.length() == 36 && UUID.fromString(token).toString().equals(token),
          "Malformed glossary revision token");
      if (!before.revisionToken().equals(token)) {
        throw new TermVersionConflictException("Term %s has changed", ident);
      }
    }
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
            before, request, plan != null && plan.changed(), membership ? domains.names() : null);

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
    if (TermEdits.needsHierarchyLock(request)) {
      terms.checkTermNames(after);
      checkTermNameReservation(lake, after);
    }
    loadOrCreateVersionHead(lake, before);
    Snapshot frozen = captureTermWithRelationships(lake, after, plan, domains);
    if (before == after && state == State.DRAFT) {
      frozen.definition().setCurrentVersion(Math.incrementExact(before.version()));
    }
    Snapshot snapshot =
        state == State.PUBLISHED
            ? frozen
            : frozen.withBaseRelated(edges.listRelated(lake, after.id()));
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
    return TermEdits.toPublicationView(after, after.domains(), result);
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
    for (GlossaryTermEntity endpoint : terms.getByIds(term.namespace(), lake, ids)) {
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
                  mapper -> readScopedVersion(metalake, lake, id, mapper, read, false));
            });
    // Only a legacy live term without pointers needs a write transaction for its baseline.
    return result != null ? result : withLockedHistory(ns, id, read);
  }

  private <T> T withLockedHistory(
      Namespace ns, long id, BiFunction<GlossaryTermVersionMapper, Head, T> read) {
    return terms.withHierarchyLock(
        ns,
        lake -> {
          Set<Long> locks = terms.getByIds(ns, lake, Set.of(id)).isEmpty() ? Set.of() : Set.of(id);
          return edges.withHierarchyLock(
              lake,
              locks,
              () ->
                  withVersionMapper(
                      mapper -> readScopedVersion(ns.level(0), lake, id, mapper, read, true)));
        });
  }

  @Nullable
  private <T> T readScopedVersion(
      String metalake,
      long lake,
      long id,
      GlossaryTermVersionMapper mapper,
      BiFunction<GlossaryTermVersionMapper, Head, T> read,
      boolean mayInitialize) {
    List<GlossaryTermEntity> live =
        terms.getByIds(NamespaceUtil.ofGlossaryTerm(metalake), lake, Set.of(id));
    Head head = mapper.getVersionHeadByMetalakeAndTermId(lake, id);
    if (live.isEmpty()) {
      if (head == null
          || head.getPublishedVersion() != null
          || !canReadTermVersion(
              metalake, head, mapper.getVersionByTermIdAndVersionId(id, head.getLastVersion())))
        throw new NoSuchTermException("Glossary term does not exist");
    } else {
      GlossaryTermEntity current = live.get(0);
      if (!readable.test(current)
          || !GlossaryAccess.forRequest(metalake, authorizationEnabled).canRead(current))
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
    GlossaryTermEntity term =
        GlossaryTermVersions.decodeSnapshot(row).term(NamespaceUtil.ofGlossaryTerm(metalake));
    if (!readable.test(term)) return false;
    if (State.PUBLISHED.name().equals(row.getLifecycle())) return access.canRead(term);
    if (head.getPublishedVersion() != null) return access.canEdit(term);
    if (access.manage()) return true;
    if (!term.auditInfo().creator().equals(PrincipalUtils.getCurrentUserName())) return false;
    return access.canCreate();
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
                  if (head.getPublishedVersion() != null) locks.add(termId);
                  try {
                    return edges.withHierarchyLock(
                        lake,
                        locks,
                        () -> {
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
                    throw new NoSuchTermException(missing, "Referenced term is no longer visible");
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
        head.getPublishedVersion() == null ? null : terms.getByIds(ns, lake, Set.of(id)).get(0);
    if (before != null && !readable.test(before))
      throw new NoSuchTermException("Glossary term does not exist");
    if (before != null
        && (before.version() != row.getBaseRevision()
            || !edges.listRelated(lake, id).equals(snapshot.baseRelated())))
      throw staleCandidateConflict();
    GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
    if (!visibleEndpoints(lake, after, snapshot.broader(), access).equals(snapshot.broader())
        || !visibleEndpoints(lake, after, snapshot.related(), access).equals(snapshot.related()))
      throw new NoSuchTermException("Referenced term is no longer visible");
    TermRelations relations = new TermRelations(lake, id, before == null);
    relations.replace(snapshot.broader(), snapshot.related());
    relations.validate(depthLimit);
    Set<Long> current = before == null ? Set.of() : DOMAINS.domainIds(lake, id);
    Map<Long, DomainPO> domainRows =
        DOMAINS.lockDomains(lake, current, snapshot.domainIds(), Set.of(), ignored -> {});
    Map<Long, String> oldNames = new HashMap<>(), newNames = new HashMap<>();
    for (long domain : current) oldNames.put(domain, domainRows.get(domain).domainName());
    for (long domain : snapshot.domainIds()) {
      DomainPO target = domainRows.get(domain);
      String domainName = target == null ? snapshot.domains().get(domain) : target.domainName();
      requireVisibleDomain(domainName, target, access);
      newNames.put(domain, target.domainName());
    }
    TermDomains domains = new TermDomains(lake, id, oldNames);
    domains.replace(newNames);
    if (before != null) {
      before = TermEdits.withDomains(before, domains.originalNames());
    }
    GlossaryTermEntity resolvedAfter = TermEdits.withDomains(after, domains.names());
    checkTermNameReservation(lake, resolvedAfter);
    terms.checkTermNames(resolvedAfter);
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
    List<TermDomains.Edit> liveEdits = new ArrayList<>();
    for (TermDomains.Edit edit : edits) {
      if (!edit.add()
          && previousDomains.containsValue(edit.domain())
          && DOMAINS.findDomain(lake, edit.domain()) == null) {
        continue;
      }
      liveEdits.add(edit);
      requested.add(edit.domain());
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
    Map<Long, String> current = new HashMap<>();
    for (long id : previousDomains.keySet()) {
      DomainPO row = locked.get(id);
      if (row != null) {
        current.put(id, row.domainName());
      }
    }
    TermDomains plan = new TermDomains(lake, termId, current);
    for (TermDomains.Edit edit : liveEdits) {
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
          for (GlossaryTermEntity endpoint : terms.getByIds(root.namespace(), lake, endpoints)) {
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
