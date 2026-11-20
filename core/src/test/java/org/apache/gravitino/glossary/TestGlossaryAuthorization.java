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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.authorization.OwnerNormalizeDispatcher;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizationLookups;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinChangeListener;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

/** Runs glossary operations against real grant, ownership and term rows. */
class TestGlossaryAuthorization extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private final AtomicLong ids = new AtomicLong(100);
  private final GlossaryManager manager = new GlossaryManager(ids::incrementAndGet, true);
  private final GravitinoEnv env = GravitinoEnv.getInstance();
  private GravitinoAuthorizer previous;
  private EntityStore previousStore;
  private CatalogManager previousCatalogs;
  private final CatalogManager catalogs = Mockito.mock(CatalogManager.class);
  private JcasbinAuthorizer nodeA;
  private OwnerManager owners;

  @BeforeEach
  void setUpAuthorization() throws Exception {
    previous = env.gravitinoAuthorizer();
    previousStore = (EntityStore) FieldUtils.readField(env, "entityStore", true);
    previousCatalogs = (CatalogManager) FieldUtils.readField(env, "catalogManager", true);
    FieldUtils.writeField(env, "catalogManager", catalogs, true);
    Config config = env.config();
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_CACHE_EXPIRATION_SECS))
        .thenReturn(3600L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_ROLE_CACHE_SIZE)).thenReturn(1000L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_OWNER_CACHE_SIZE)).thenReturn(1000L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_METADATA_ID_CACHE_SIZE))
        .thenReturn(1000L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS))
        .thenReturn(3600L);
    RelationalEntityStore store = RelationalEntityStore.forOfflineMigration(backend, config);
    // The fixture owns the database lifecycle. Expose only EntityStore to avoid registering an
    // entity-change listener on the offline store, which has no background workers.
    FieldUtils.writeField(
        env,
        "entityStore",
        Mockito.mock(EntityStore.class, AdditionalAnswers.delegatesTo(store)),
        true);
    owners = new OwnerManager(store);
    createAndInsertMakeLake(LAKE);
    role("view", Privileges.allow("VIEW_DEFINITION"));
    role("create", Privileges.allow("CREATE_DEFINITION"));
    role("manage", Privileges.allow("MANAGE_DEFINITIONS"));
    role("denyView", Privileges.deny("VIEW_DEFINITION"));
    role("denyCreate", Privileges.deny("CREATE_DEFINITION"));
    role("denyManage", Privileges.deny("MANAGE_DEFINITIONS"));
    user("viewer", "view");
    user("writer", "create");
    user("writer2", "create");
    user("admin", "manage");
    user("denied", "manage", "denyView");
    user("noCreate", "manage", "denyCreate");
    user("noManage", "manage", "denyManage");
    user("outsider");
    user("lakeOwner");
    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE),
            Entity.EntityType.METALAKE,
            NameIdentifierUtil.ofUser(LAKE, "lakeOwner"),
            Entity.EntityType.USER);
    nodeA = new JcasbinAuthorizer();
    nodeA.initialize();
    env.setGravitinoAuthorizer(nodeA);
  }

  @AfterEach
  void tearDownAuthorization() throws Exception {
    try {
      if (nodeA != null) nodeA.close();
    } finally {
      env.setGravitinoAuthorizer(previous);
      FieldUtils.writeField(env, "entityStore", previousStore, true);
      FieldUtils.writeField(env, "catalogManager", previousCatalogs, true);
      Mockito.verifyNoInteractions(catalogs);
    }
  }

  // Expected capabilities are independent of the production grant expansion.
  record Actor(String name, boolean published, boolean ownEdits, boolean manage, boolean create) {}
  /** Covers T6.1–T6.3 and T6.8–T6.11, including the precedence of visibility over edit errors. */
  @TestTemplate
  void testOperationMatrix() throws Exception {
    for (Actor actor :
        List.of(
            new Actor("viewer", true, false, false, false),
            new Actor("writer", true, true, false, true),
            new Actor("writer2", true, true, false, true),
            new Actor("admin", true, true, true, true),
            new Actor("denied", false, false, false, false),
            new Actor("lakeOwner", true, true, true, true),
            new Actor("outsider", false, false, false, false),
            new Actor("noCreate", true, false, true, false),
            new Actor("noManage", true, true, false, true))) {
      for (GlossaryTerm.Status status : GlossaryTerm.Status.values()) {
        TermSpec spec =
            TermSpec.builder("created-" + actor.name() + "-" + status).withStatus(status).build();
        if (actor.create()) {
          assertEquals(status, as(actor.name(), () -> manager.createTerm(LAKE, spec)).status());
          assertTrue(as("admin", () -> manager.dropTerm(LAKE, spec.name())));
        } else {
          assertThrows(
              ForbiddenException.class,
              () -> as(actor.name(), () -> manager.createTerm(LAKE, spec)));
        }
        for (boolean own : List.of(false, true)) {
          String name = actor.name() + "-" + status + "-" + own;
          GlossaryTermEntity term =
              as(
                  "admin",
                  () ->
                      manager.createTerm(LAKE, TermSpec.builder(name).withStatus(status).build()));
          if (own) {
            as("lakeOwner", () -> transfer(term, actor.name(), Owner.Type.USER));
          }
          boolean edit = actor.manage() || own && actor.ownEdits();
          boolean visible = actor.published() && (status != GlossaryTerm.Status.DRAFT || edit);
          as(
              actor.name(),
              () -> {
                assertEquals(
                    visible,
                    manager.listTerms(LAKE).stream().anyMatch(t -> t.id().equals(term.id())),
                    name);
                if (visible) {
                  assertEquals(term.id(), manager.getTerm(LAKE, name).id());
                  if (!actor.manage()) {
                    assertThrows(ForbiddenException.class, () -> manager.dropTerm(LAKE, name));
                  }
                } else {
                  assertThrows(NoSuchTermException.class, () -> manager.getTerm(LAKE, name));
                }
                if (edit) {
                  manager.alterTerm(LAKE, name, TermChange.updateDefinition("updated"));
                  manager.alterTerm(LAKE, name, TermChange.setStatus(GlossaryTerm.Status.DRAFT));
                } else {
                  Class<? extends RuntimeException> error =
                      visible ? ForbiddenException.class : NoSuchTermException.class;
                  assertThrows(
                      error,
                      () ->
                          manager.alterTerm(
                              LAKE, name, "invalid", TermChange.updateDefinition("bad")));
                  assertThrows(
                      error,
                      () ->
                          manager.alterTerm(
                              LAKE, name, TermChange.setStatus(GlossaryTerm.Status.DRAFT)));
                }
                if (actor.manage()) {
                  assertTrue(manager.dropTerm(LAKE, name));
                } else if (visible) {
                  assertThrows(ForbiddenException.class, () -> manager.dropTerm(LAKE, name));
                } else {
                  assertFalse(manager.dropTerm(LAKE, name));
                }
                return null;
              });
          if (!actor.manage()) {
            assertEquals(
                edit ? "updated" : term.definition(),
                as("admin", () -> manager.getTerm(LAKE, name)).definition());
            assertTrue(as("admin", () -> manager.dropTerm(LAKE, name)));
          }
        }
      }
    }
  }

  /** Covers T6.6 and T6.7. */
  @TestTemplate
  void testHiddenParentAndDuplicateName() throws Exception {
    GlossaryTermEntity hidden =
        as(
            "writer",
            () ->
                manager.createTerm(
                    LAKE,
                    TermSpec.builder("Hidden")
                        .withStatus(GlossaryTerm.Status.DRAFT)
                        .withDefinition("private definition")
                        .build()));
    as("admin", () -> manager.createTerm(LAKE, TermSpec.builder("Visible").build()));
    as(
        "admin",
        () ->
            manager.createTerm(
                LAKE, TermSpec.builder("Child").withBroader("Hidden", "Visible").build()));
    assertEquals(
        List.of("Visible"),
        as(
                "viewer",
                () ->
                    manager.listRelations(
                        LAKE, "Child", GlossaryManager.Direction.BROADER, true, null))
            .stream()
            .map(GlossaryTermEntity::name)
            .toList());
    assertEquals(
        2,
        as(
                "admin",
                () ->
                    manager.listRelations(
                        LAKE, "Child", GlossaryManager.Direction.BROADER, true, null))
            .size());
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "writer2",
                () ->
                    manager.createTerm(
                        LAKE, TermSpec.builder("rejected").withRelated("Hidden").build())));
    assertThrows(
        NoSuchTermException.class, () -> as("writer2", () -> manager.getTerm(LAKE, "Hidden")));
    assertFalse(
        as("writer2", () -> manager.listTerms(LAKE)).stream()
            .anyMatch(t -> t.id().equals(hidden.id())));
    TermAlreadyExistsException duplicate =
        assertThrows(
            TermAlreadyExistsException.class,
            () ->
                as(
                    "writer2",
                    () -> manager.createTerm(LAKE, TermSpec.builder(" hidden ").build())));
    assertEquals("Term name or source reference already exists", duplicate.getMessage());
    GlossaryTermEntity after = as("writer", () -> manager.getTerm(LAKE, "Hidden"));
    assertEquals(hidden.id(), after.id());
    assertEquals(hidden.revisionToken(), after.revisionToken());
    assertEquals(hidden.name(), after.name());
    assertEquals(hidden.definition(), after.definition());
    assertEquals(
        "writer", as("writer", () -> owners.getOwner(LAKE, object(hidden))).orElseThrow().name());
    assertEquals(3, as("admin", () -> manager.listTerms(LAKE)).size());
    assertEquals(
        GlossaryTerm.Status.APPROVED,
        as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("published").build()))
            .status());
    as(
        "writer",
        () -> manager.alterTerm(LAKE, "published", TermChange.updateDefinition("new definition")));
    assertEquals(
        "new definition", as("viewer", () -> manager.getTerm(LAKE, "published")).definition());
  }

  @TestTemplate
  void testListChecksWritePrivilegesOnlyWhenDraftsNeedThem() throws Exception {
    as("admin", () -> manager.createTerm(LAKE, TermSpec.builder("Published").build()));
    GravitinoAuthorizer observed =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(nodeA));
    env.setGravitinoAuthorizer(observed);
    assertEquals(1, as("viewer", () -> manager.listTerms(LAKE)).size());
    for (Privilege.Name privilege :
        List.of(Privilege.Name.CREATE_DEFINITION, Privilege.Name.MANAGE_DEFINITIONS)) {
      Mockito.verify(observed, Mockito.never())
          .authorize(
              Mockito.any(), Mockito.eq(LAKE), Mockito.any(), Mockito.eq(privilege), Mockito.any());
    }

    for (String name : List.of("Draft1", "Draft2")) {
      as(
          "writer",
          () ->
              manager.createTerm(
                  LAKE, TermSpec.builder(name).withStatus(GlossaryTerm.Status.DRAFT).build()));
    }
    Mockito.clearInvocations(observed);
    assertEquals(3, as("writer", () -> manager.listTerms(LAKE)).size());
    for (Privilege.Name privilege :
        List.of(Privilege.Name.CREATE_DEFINITION, Privilege.Name.MANAGE_DEFINITIONS)) {
      Mockito.verify(observed)
          .authorize(
              Mockito.any(), Mockito.eq(LAKE), Mockito.any(), Mockito.eq(privilege), Mockito.any());
    }
    Mockito.verify(observed, Mockito.times(2))
        .isGlossaryTermOwner(Mockito.any(), Mockito.eq(LAKE), Mockito.anyLong(), Mockito.any());
  }

  @TestTemplate
  void testMissingWriteScopeIsReportedBeforePrivilegeLookup() throws Exception {
    GravitinoAuthorizer observed = Mockito.mock(GravitinoAuthorizer.class);
    env.setGravitinoAuthorizer(observed);
    String absent = "absent";
    assertThrows(
        NoSuchMetalakeException.class,
        () -> as("writer", () -> manager.createTerm(absent, TermSpec.builder("term").build())));
    for (TermChange change :
        List.of(TermChange.updateDefinition("updated"), TermChange.addRelated("peer"))) {
      assertThrows(
          NoSuchTermException.class,
          () -> as("writer", () -> manager.alterTerm(absent, "term", change)));
    }
    Mockito.verifyNoInteractions(observed);
  }

  @TestTemplate
  void testListPreservesAuthorizationLookupFailure() throws Exception {
    GravitinoAuthorizer authorizer = Mockito.mock(GravitinoAuthorizer.class);
    NoSuchEntityException failure = new NoSuchEntityException("Owner lookup failed");
    Mockito.when(authorizer.isOwner(Mockito.any(), Mockito.eq(LAKE), Mockito.any(), Mockito.any()))
        .thenThrow(failure);
    env.setGravitinoAuthorizer(authorizer);
    assertSame(
        failure,
        assertThrows(
            NoSuchEntityException.class, () -> as("viewer", () -> manager.listTerms(LAKE))));
  }

  @TestTemplate
  void testRelationFilterHidesRootBeforeCheckingEditPermission() throws Exception {
    as("admin", () -> manager.createTerm(LAKE, TermSpec.builder("Root").build()));
    as("admin", () -> manager.createTerm(LAKE, TermSpec.builder("Peer").build()));
    GlossaryManager filtered = manager.withTermVisibility(term -> !term.name().equals("Root"));
    assertThrows(
        ForbiddenException.class,
        () -> as("viewer", () -> manager.alterTerm(LAKE, "Root", TermChange.addRelated("Peer"))));
    assertThrows(
        NoSuchTermException.class,
        () -> as("viewer", () -> filtered.alterTerm(LAKE, "Root", TermChange.addRelated("Peer"))));
    GlossaryManager scoped = filtered.withTermVisibility(term -> !term.name().equals("Peer"));
    for (String name : List.of("Root", "Peer")) {
      assertThrows(
          NoSuchTermException.class,
          () ->
              as(
                  "admin",
                  () ->
                      scoped.listRelations(
                          LAKE, name, GlossaryManager.Direction.RELATED, false, null)));
    }
    assertThrows(
        NoSuchTermException.class,
        () -> as("admin", () -> scoped.alterTerm(LAKE, "Root", TermChange.addRelated("Peer"))));
    assertEquals(1, as("admin", () -> manager.getTerm(LAKE, "Root")).version());
  }

  /** Covers T6.4, T6.12 and T6.13 with independent authorizers over one metadata store. */
  @TestTemplate
  void testOwnershipAcrossNodesAndNameReuse() throws Exception {
    for (String group : List.of("authors", "editors")) {
      backend.insert(
          createGroupEntity(
              ids.incrementAndGet(),
              AuthorizationUtils.ofGroupNamespace(LAKE),
              group,
              AUDIT_INFO,
              List.of(),
              List.of()),
          false);
    }
    try (JcasbinAuthorizer nodeB = new JcasbinAuthorizer()) {
      nodeB.initialize();
      // Stop peer invalidations before warming either cache so staleness is deterministic.
      ((JcasbinChangeListener) FieldUtils.readField(nodeB, "changePoller", true)).close();
      JcasbinAuthorizationLookups cached =
          (JcasbinAuthorizationLookups) FieldUtils.readField(nodeB, "lookups", true);
      for (boolean groups : List.of(false, true)) {
        env.setGravitinoAuthorizer(nodeA);
        UserPrincipal oldOwner = principal("writer", groups ? "authors" : null);
        UserPrincipal newOwner = principal("writer2", groups ? "editors" : null);
        String name = "draft.with.dot-" + groups;
        GlossaryTermEntity original =
            as(
                "writer",
                () ->
                    manager.createTerm(
                        LAKE,
                        TermSpec.builder(name).withStatus(GlossaryTerm.Status.DRAFT).build()));
        if (groups) as("writer", () -> transfer(original, "authors", Owner.Type.GROUP));
        MetadataObject object = object(original);
        assertEquals(
            original.id(),
            cached
                .resolveMetadataId(object, LAKE, new AuthorizationRequestContext())
                .orElseThrow());
        OwnerInfo before =
            cached
                .resolveOwnerId(original.id(), object.type(), new AuthorizationRequestContext())
                .orElseThrow();
        assertOwnerAccess(nodeB, oldOwner, name, true);
        assertOwnerAccess(nodeB, newOwner, name, false);
        env.setGravitinoAuthorizer(nodeA);
        PrincipalUtils.doAs(
            oldOwner,
            () -> {
              assertTrue(canTransfer(nodeA, name));
              transfer(
                  original,
                  groups ? "editors" : "writer2",
                  groups ? Owner.Type.GROUP : Owner.Type.USER);
              return null;
            });
        assertEquals(
            before.getOwnerId(),
            cached
                .resolveOwnerId(original.id(), object.type(), new AuthorizationRequestContext())
                .orElseThrow()
                .getOwnerId());
        for (JcasbinAuthorizer node : List.of(nodeA, nodeB)) {
          assertOwnerAccess(node, oldOwner, name, false);
          assertOwnerAccess(node, newOwner, name, true);
          if (groups) assertOwnerAccess(node, new UserPrincipal("writer2"), name, false);
        }
        env.setGravitinoAuthorizer(nodeA);
        as("admin", () -> manager.alterTerm(LAKE, name, TermChange.rename(name + "-renamed")));
        GlossaryTermEntity replacement =
            as(
                "admin",
                () ->
                    manager.createTerm(
                        LAKE,
                        TermSpec.builder(name).withStatus(GlossaryTerm.Status.DRAFT).build()));
        assertNotEquals(original.id(), replacement.id());
        assertEquals(
            original.id(),
            cached
                .resolveMetadataId(object, LAKE, new AuthorizationRequestContext())
                .orElseThrow());
        assertOwnerAccess(nodeB, newOwner, name, false);
        assertOwnerAccess(nodeB, oldOwner, name, false);
        assertOwnerAccess(nodeB, newOwner, name + "-renamed", true);
        SessionUtils.beginTransaction();
        try {
          as("admin", () -> manager.dropTerm(LAKE, name + "-renamed"));
          as("admin", () -> manager.createTerm(LAKE, TermSpec.builder(name + "-renamed").build()));
        } finally {
          SessionUtils.rollbackTransaction();
        }
        assertOwnerAccess(nodeB, newOwner, name + "-renamed", true);
        env.setGravitinoAuthorizer(nodeA);
        assertTrue(as("admin", () -> manager.dropTerm(LAKE, name + "-renamed")));
        as(
            "writer",
            () ->
                manager.createTerm(
                    LAKE,
                    TermSpec.builder(name + "-renamed")
                        .withStatus(GlossaryTerm.Status.DRAFT)
                        .build()));
        for (JcasbinAuthorizer node : List.of(nodeA, nodeB)) {
          assertOwnerAccess(node, newOwner, name + "-renamed", false);
          assertOwnerAccess(node, oldOwner, name + "-renamed", true);
        }
      }
    }
  }

  @TestTemplate
  void testGlossaryOwnerOperationsUseCurrentTerm() throws Exception {
    GlossaryTermEntity term =
        as(
            "writer",
            () ->
                manager.createTerm(
                    LAKE,
                    TermSpec.builder("draft.with.dot")
                        .withStatus(GlossaryTerm.Status.DRAFT)
                        .build()));
    OwnerNormalizeDispatcher dispatcher =
        new OwnerNormalizeDispatcher(owners, catalogs, env.entityStore());
    // Transfer on another connection after the visibility check and before the owner lookup.
    GravitinoAuthorizer observed =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(nodeA));
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      Mockito.doAnswer(
              invocation -> {
                boolean visible =
                    nodeA.canAccessGlossaryTerm(
                        invocation.getArgument(0), LAKE, object(term), invocation.getArgument(3));
                writer
                    .submit(() -> as("writer", () -> transfer(term, "writer2", Owner.Type.USER)))
                    .get(30, TimeUnit.SECONDS);
                return visible;
              })
          .when(observed)
          .canAccessGlossaryTerm(Mockito.any(), Mockito.eq(LAKE), Mockito.any(), Mockito.any());
      env.setGravitinoAuthorizer(observed);
      assertEquals(
          "writer",
          as("writer", () -> dispatcher.getOwner(LAKE, object(term))).orElseThrow().name());
    } finally {
      env.setGravitinoAuthorizer(nodeA);
      writer.shutdownNow();
    }
    assertThrows(
        ForbiddenException.class,
        () -> as("writer", () -> dispatcher.getOwner(LAKE, object(term))));
    assertThrows(
        ForbiddenException.class,
        () -> as("writer", () -> transfer(term, "writer", Owner.Type.USER)));
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "writer",
                () -> {
                  dispatcher.setOwners(LAKE, List.of(object(term)), "writer", Owner.Type.USER);
                  return null;
                }));
    assertEquals(
        "writer2",
        as("writer2", () -> dispatcher.getOwner(LAKE, object(term))).orElseThrow().name());
  }

  private static UserPrincipal principal(String user, @Nullable String group) {
    return group == null
        ? new UserPrincipal(user)
        : new UserPrincipal(user, List.of(new UserGroup(Optional.empty(), group)));
  }

  private boolean canTransfer(JcasbinAuthorizer node, String name) {
    return node.hasSetOwnerPermission(
        LAKE, "glossary_term", name, new AuthorizationRequestContext());
  }

  private void assertOwnerAccess(
      JcasbinAuthorizer node, UserPrincipal user, String name, boolean allowed) throws Exception {
    env.setGravitinoAuthorizer(node);
    PrincipalUtils.doAs(
        user,
        () -> {
          assertEquals(allowed, canTransfer(node, name));
          MetadataObject object = MetadataObjects.of(null, name, MetadataObject.Type.GLOSSARY_TERM);
          Map<Entity.EntityType, NameIdentifier> names =
              NameIdentifierUtil.splitNameIdentifier(
                  LAKE,
                  Entity.EntityType.GLOSSARY_TERM,
                  MetadataObjectUtil.toEntityIdent(LAKE, object));
          AuthorizationExpressionEvaluator readOwner =
              new AuthorizationExpressionEvaluator(
                  AuthorizationExpressionConstants.CAN_ACCESS_METADATA, node);
          assertEquals(
              allowed,
              readOwner.evaluate(
                  names, new AuthorizationRequestContext(), user, Optional.of("GLOSSARY_TERM")));
          if (allowed) {
            manager.getTerm(LAKE, name);
            manager.alterTerm(LAKE, name, TermChange.updateDefinition(user.getName()));
            assertThrows(ForbiddenException.class, () -> manager.dropTerm(LAKE, name));
          } else {
            assertThrows(NoSuchTermException.class, () -> manager.getTerm(LAKE, name));
            assertThrows(
                NoSuchTermException.class,
                () -> manager.alterTerm(LAKE, name, TermChange.updateDefinition("denied")));
            assertFalse(manager.dropTerm(LAKE, name));
          }
          return null;
        });
  }

  private void role(String name, Privilege privilege) throws Exception {
    backend.insert(
        createRoleEntity(
            ids.incrementAndGet(),
            AuthorizationUtils.ofRoleNamespace(LAKE),
            name,
            AUDIT_INFO,
            List.of(SecurableObjects.ofMetalake(LAKE, List.of(privilege))),
            null),
        false);
  }

  private void user(String name, String... roles) throws Exception {
    List<Long> roleIds = new ArrayList<>();
    for (String role : roles)
      roleIds.add(backend.get(NameIdentifierUtil.ofRole(LAKE, role), Entity.EntityType.ROLE).id());
    backend.insert(
        createUserEntity(
            ids.incrementAndGet(),
            AuthorizationUtils.ofUserNamespace(LAKE),
            name,
            AUDIT_INFO,
            List.of(roles),
            roleIds),
        false);
  }

  private static MetadataObject object(GlossaryTermEntity term) {
    return MetadataObjects.of(null, term.name(), MetadataObject.Type.GLOSSARY_TERM);
  }

  private Void transfer(GlossaryTermEntity term, String owner, Owner.Type type) {
    owners.setOwner(LAKE, object(term), owner, type);
    return null;
  }

  private static <T> T as(String user, PrivilegedExceptionAction<T> work) throws Exception {
    return PrincipalUtils.doAs(new UserPrincipal(user), work);
  }
}
