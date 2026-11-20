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
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService.ScopedTerm;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** Manages live glossary definitions. Callers supply the metalake scope for every operation. */
public final class GlossaryManager {
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

  /** Applies an additional visibility filter to hierarchy reads and referenced terms in writes. */
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
          NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
          ScopedTerm snapshot = relationRoot(ident);
          GlossaryTermEntity root = snapshot.term();
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, authorizationEnabled);
          Predicate<GlossaryTermEntity> visible =
              term ->
                  (status == null || status == term.status())
                      && access.canRead(term)
                      && readable.test(term);
          if (!visible.test(root)) {
            throw missingRelationRoot(ident);
          }
          long lake = snapshot.metalakeId();
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
          return result;
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
    if (!authorizationEnabled) {
      return loadTerms(metalake);
    }
    return SqlSessions.readSnapshot(
        () -> {
          List<GlossaryTermEntity> result = loadTerms(metalake);
          GlossaryAccess access = GlossaryAccess.forRequest(metalake, true);
          result.removeIf(term -> !access.canRead(term));
          return result;
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
    NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
    if (!authorizationEnabled) {
      return relationRoot(ident).term();
    }
    return SqlSessions.readSnapshot(
        () -> {
          GlossaryTermEntity term = relationRoot(ident).term();
          if (!GlossaryAccess.forRequest(metalake, true).canRead(term)) {
            throw missingRelationRoot(ident);
          }
          return term;
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
          return withRelationPlan(
              lake,
              term,
              edits,
              access,
              true,
              ignored -> {},
              (fresh, plan) -> {
                terms.checkTermNames(term);
                handler.handle(null, term, () -> persistNewTerm(metalake, term, plan));
                return term;
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
    NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
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
          if (!access.canRead(root)) {
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

  private GlossaryTermEntity alter(
      String metalake,
      String name,
      boolean conditional,
      @Nullable String token,
      TermChange[] changes) {
    NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
    TermChange[] request = changes == null ? new TermChange[0] : changes.clone();
    List<TermRelations.Edit> edits = new ArrayList<>();
    List<TermChange> content = new ArrayList<>();
    for (TermChange change : request) {
      TermRelations.Edit edit = TermRelations.Edit.from(change);
      if (edit == null) {
        content.add(change);
      } else {
        edits.add(edit);
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
                  requireEdit(access, ident, locked, readable);
                  checkRevision(ident, locked, conditional, token);
                },
                (locked, plan) ->
                    applyChanges(ident, locked, content.toArray(new TermChange[0]), plan));
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
          requireEdit(
              GlossaryAccess.forRequest(metalake, authorizationEnabled), ident, before, null);
          Preconditions.checkArgument(request.length > 0, "At least one term change is required");
          checkRevision(ident, before, conditional, token);
          return applyChanges(ident, before, request, null);
        });
  }

  private static void requireEdit(
      GlossaryAccess access,
      NameIdentifier ident,
      GlossaryTermEntity term,
      @Nullable Predicate<GlossaryTermEntity> relationFilter) {
    if (!access.canRead(term) || (relationFilter != null && !relationFilter.test(term))) {
      throw missingRelationRoot(ident);
    }
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
      @Nullable TermRelations plan) {
    GlossaryTermEntity after = TermEdits.apply(before, request, plan != null && plan.changed());
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
        });
    return after;
  }

  private void persistNewTerm(
      String metalake, GlossaryTermEntity term, @Nullable TermRelations plan) {
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
