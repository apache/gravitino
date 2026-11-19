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
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** Manages live glossary definitions. Callers supply the metalake scope for every operation. */
public final class GlossaryManager {
  private final GlossaryTermMetaService terms = GlossaryTermMetaService.getInstance();
  private final IdGenerator ids;
  private final boolean authorizationEnabled;
  private final TermChangeHandler handler;

  /** Creates a manager that applies effective changes immediately. */
  public GlossaryManager(IdGenerator ids, boolean authorizationEnabled) {
    this(ids, authorizationEnabled, (before, after, persist) -> persist.run());
  }

  GlossaryManager(IdGenerator ids, boolean authorizationEnabled, TermChangeHandler handler) {
    this.ids = ids;
    this.authorizationEnabled = authorizationEnabled;
    this.handler = handler;
  }

  /**
   * Lists term definitions in the given metalake.
   *
   * @throws NoSuchMetalakeException if the metalake does not exist
   */
  public List<GlossaryTermEntity> listTerms(String metalake) {
    try {
      return terms.list(NamespaceUtil.ofGlossaryTerm(metalake));
    } catch (NoSuchEntityException e) {
      throw new NoSuchMetalakeException(e, "Metalake %s does not exist", metalake);
    }
  }

  /** Loads a definition by normalized name. */
  public GlossaryTermEntity getTerm(String metalake, String name) {
    NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
    try {
      return terms.get(ident);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTermException(e, "Term %s does not exist", ident);
    }
  }

  /**
   * Creates a live definition. When authorization is enabled the creator must be a registered user
   * in the metalake. The definition and creator ownership commit or roll back together.
   *
   * @throws NoSuchMetalakeException if the metalake does not exist
   * @throws NoSuchEntityException if the required creator is missing
   */
  public GlossaryTermEntity createTerm(String metalake, TermSpec spec) {
    Namespace ns = NamespaceUtil.ofGlossaryTerm(metalake);
    GlossaryTermEntity term = TermEdits.create(ids.nextId(), ns, spec);
    return terms.withHierarchyLock(
        ns,
        () -> {
          terms.checkTermNames(term);
          handler.handle(
              null,
              term,
              () -> {
                try {
                  terms.insert(term, false);
                } catch (EntityAlreadyExistsException e) {
                  throw new TermAlreadyExistsException(
                      e, "Term name or source reference already exists");
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
                if (authorizationEnabled) {
                  OwnerMetaService.getInstance()
                      .setOwner(
                          term.nameIdentifier(),
                          EntityType.GLOSSARY_TERM,
                          NameIdentifierUtil.ofUser(metalake, term.auditInfo().creator()),
                          EntityType.USER);
                }
              });
          return term;
        });
  }

  /** Applies all changes atomically to the current definition. */
  public GlossaryTermEntity alterTerm(String metalake, String name, TermChange... changes) {
    return alter(metalake, name, false, null, changes);
  }

  /** Applies all changes only if the locked identity and revision match the supplied token. */
  public GlossaryTermEntity alterTerm(
      String metalake, String name, String expectedRevision, TermChange... changes) {
    return alter(metalake, name, true, expectedRevision, changes);
  }

  /** Drops a live term of any status and returns whether it existed. */
  public boolean dropTerm(String metalake, String name) {
    NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
    return terms.withLockedTerm(
        ident,
        true,
        locked -> {
          if (locked.isEmpty()) {
            return false;
          }
          GlossaryTermEntity term = locked.get();
          handler.handle(
              term,
              null,
              () -> Preconditions.checkState(terms.delete(ident), "Locked term disappeared"));
          return true;
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
    NameIdentifier ident = NameIdentifier.of(NamespaceUtil.ofGlossaryTerm(metalake), name);
    TermChange[] request = changes.clone();
    boolean hierarchy = TermEdits.needsHierarchyLock(request);
    return terms.withLockedTerm(
        ident,
        hierarchy,
        locked -> {
          GlossaryTermEntity before =
              locked.orElseThrow(() -> new NoSuchTermException("Term %s does not exist", ident));
          if (conditional) {
            Preconditions.checkArgument(
                token != null
                    && token.length() == 36
                    && UUID.fromString(token).toString().equals(token),
                "Malformed glossary revision token");
            if (!before.revisionToken().equals(token)) {
              throw new TermVersionConflictException("Term %s has changed", ident);
            }
          }
          GlossaryTermEntity after = TermEdits.apply(before, request);
          if (before == after) {
            return before;
          }
          if (hierarchy) {
            terms.checkTermNames(after);
          }
          handler.handle(
              before,
              after,
              () -> {
                try {
                  terms.update(ident, (GlossaryTermEntity ignored) -> after);
                } catch (EntityAlreadyExistsException e) {
                  throw new TermAlreadyExistsException(
                      e, "Term name or source reference already exists");
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
          return after;
        });
  }
}
