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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.DefinitionMemberPage;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.DomainRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermDomainService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService.ScopedTerm;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** Manages live glossary definitions. Callers supply the metalake scope for every operation. */
public final class GlossaryManager {
  private static final GlossaryTermDomainService DOMAINS = GlossaryTermDomainService.getInstance();
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
                TermDomains domains =
                    planDomains(lake, term, domainEdits, row -> requireManagement(metalake, row));
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
                            () -> persistDefinition(survivor.nameIdentifier(), after));
                      }
                      Preconditions.checkState(terms.delete(ident), "Locked term disappeared");
                    });
                return true;
              });
        },
        missing -> false);
  }

  /** Lists the direct, live definitions administered through a Domain. */
  public DefinitionMemberPage listDefinitionMembers(
      String metalake, String domain, int limit, int offset) {
    Preconditions.checkArgument(
        limit >= 1 && limit <= 1000 && offset >= 0, "Invalid membership page");
    return SqlSessions.readSnapshot(
        () -> {
          DomainPO row = managedDomain(metalake, domain);
          List<DomainMemberMapper.NamedMember> members =
              SessionUtils.getWithoutCommit(
                  DomainMemberMapper.class,
                  mapper ->
                      mapper.namedMembers(row.metalakeId(), row.domainId(), limit + 1, offset));
          return new DefinitionMemberPage(
              new ArrayList<>(members.subList(0, Math.min(limit, members.size()))),
              members.size() > limit);
        });
  }

  /** Detaches the named Term, including a hidden draft, with Domain management authority. */
  public void detachTermDomain(String metalake, String term, String domain) {
    DomainPO observed = SqlSessions.readSnapshot(() -> managedDomain(metalake, domain));
    NameIdentifier ident = NameIdentifierUtil.ofGlossaryTerm(metalake, term);
    Consumer<DomainPO> check =
        row -> {
          if (!row.domainId().equals(observed.domainId())) {
            throw new NoSuchDomainException("Domain %s does not exist", domain);
          }
          requireManagement(metalake, row);
        };
    terms.withLockedTerm(
        ident,
        false,
        locked -> {
          if (locked.isEmpty()) {
            DOMAINS.lockDomains(observed.metalakeId(), Set.of(), Set.of(domain), check);
          } else {
            GlossaryTermEntity before = locked.get();
            TermDomains plan =
                planDomains(
                    observed.metalakeId(),
                    before,
                    List.of(new TermDomains.Edit(domain, false)),
                    check);
            applyChanges(ident, before, new TermChange[0], null, plan);
          }
          return null;
        });
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
                            planDomains(
                                lake,
                                locked,
                                domainEdits,
                                row -> requireManagement(metalake, row))),
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
                  : planDomains(
                      lakeOf(ident), before, domainEdits, row -> requireManagement(metalake, row));
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

  private DomainPO managedDomain(String metalake, String name) {
    long lake;
    try {
      lake = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetalakeException(missing, "Metalake %s does not exist", metalake);
    }
    DomainPO row = DOMAINS.findDomain(lake, name);
    if (row == null || !name.equals(row.domainName())) {
      throw new NoSuchDomainException("Domain %s does not exist", name);
    }
    requireManagement(metalake, row);
    return row;
  }

  private void requireManagement(String metalake, DomainPO row) {
    if (authorizationEnabled) {
      DomainEntity domain =
          DomainRelService.getInstance().toEntityWithParent(row, NamespaceUtil.ofDomain(metalake));
      GlossaryAccess.requireDomainManagement(metalake, domain);
    }
  }

  /**
   * Resolves, authorizes and locks every requested domain after all term rows of the request are
   * locked. A missing domain and a hidden draft domain report the same error.
   */
  private TermDomains planDomains(
      long lake, GlossaryTermEntity term, List<TermDomains.Edit> edits, Consumer<DomainPO> check) {
    Set<Long> members = term.domains().length == 0 ? Set.of() : DOMAINS.domainIds(term.id());
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
              check.accept(domain);
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
