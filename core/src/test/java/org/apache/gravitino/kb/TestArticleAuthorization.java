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

package org.apache.gravitino.kb;

import static org.apache.gravitino.storage.relational.service.GlossaryDomainFixtures.insertDomain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermChange;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.policy.PolicyManager;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.DomainMetaService;
import org.apache.gravitino.storage.relational.service.DomainRelService;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.service.OrphanedMetadataObjectRelationService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

/** Exercises Article operations against real role, principal and owner rows. */
class TestArticleAuthorization extends TestJDBCBackend {
  private static final String LAKE = "articles";
  private final AtomicLong ids = new AtomicLong(100);
  private final ArticleManager manager = new ArticleManager(ids::incrementAndGet, 1024, true);
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
      if (nodeA != null) {
        nodeA.close();
      }
    } finally {
      env.setGravitinoAuthorizer(previous);
      FieldUtils.writeField(env, "entityStore", previousStore, true);
      FieldUtils.writeField(env, "catalogManager", previousCatalogs, true);
      Mockito.verifyNoInteractions(catalogs);
    }
  }

  @TestTemplate
  void testReadEditAndDelete() throws Exception {
    var draft =
        as("writer", () -> manager.createArticle(LAKE, article("draft", ArticleStatus.DRAFT)));
    as("writer", () -> manager.createArticle(LAKE, article("published", ArticleStatus.PUBLISHED)));
    as(
        "writer",
        () -> manager.createArticle(LAKE, article("deprecated", ArticleStatus.DEPRECATED)));
    for (String viewer : List.of("viewer", "writer2", "noManage", "denied", "outsider")) {
      as(
          viewer,
          () -> {
            assertThrows(
                NoSuchKnowledgeBaseArticleException.class,
                () -> manager.loadArticle(LAKE, "draft"));
            assertThrows(
                NoSuchKnowledgeBaseArticleException.class,
                () -> manager.getArticleContent(LAKE, "draft"));
            assertThrows(
                NoSuchKnowledgeBaseArticleException.class,
                () -> manager.alterArticle(LAKE, "draft", "malformed"));
            assertFalse(manager.deleteArticle(LAKE, "draft"));
            return null;
          });
    }
    assertEquals(
        List.of("published", "deprecated"),
        as(
            "viewer",
            () ->
                manager.listArticles(LAKE).stream()
                    .map(KnowledgeBaseArticleEntity::name)
                    .toList()));
    assertTrue(as("denied", () -> manager.listArticles(LAKE)).isEmpty());
    assertEquals(
        "body", as("viewer", () -> manager.getArticleContent(LAKE, "deprecated")).body().content());
    for (String actor : List.of("viewer", "writer2", "noManage")) {
      as(
          actor,
          () -> {
            assertThrows(
                ForbiddenException.class,
                () -> manager.alterArticle(LAKE, "published", "malformed"));
            assertThrows(ForbiddenException.class, () -> manager.deleteArticle(LAKE, "published"));
            return null;
          });
    }
    for (String actor : List.of("writer", "admin", "noCreate")) {
      assertEquals(draft, as(actor, () -> manager.loadArticle(LAKE, "draft")));
    }
    for (String actor : List.of("viewer", "denied", "noCreate")) {
      assertThrows(
          ForbiddenException.class,
          () ->
              as(
                  actor,
                  () -> manager.createArticle(LAKE, article(actor, ArticleStatus.PUBLISHED))));
    }
    assertEquals(
        2,
        as(
                "admin",
                () ->
                    manager.alterArticle(
                        LAKE,
                        "draft",
                        draft.revisionToken(),
                        KnowledgeBaseArticleChange.updateComment("ok")))
            .article()
            .revision());
    assertTrue(as("writer", () -> manager.deleteArticle(LAKE, "draft")));
    assertTrue(as("admin", () -> manager.deleteArticle(LAKE, "published")));
  }

  @TestTemplate
  void metadataProjectsTheCurrentOwnerOnCreateLoadAndEdit() throws Exception {
    var created =
        as("writer", () -> manager.createArticle(LAKE, article("guide", ArticleStatus.PUBLISHED)));
    assertEquals("writer", created.article().owner().name());
    assertEquals(Owner.Type.USER, created.article().owner().type());
    backend.insert(
        createGroupEntity(
            ids.incrementAndGet(),
            AuthorizationUtils.ofGroupNamespace(LAKE),
            "editors",
            AUDIT_INFO,
            List.of(),
            List.of()),
        false);
    as(
        "writer",
        () -> {
          owners.setOwner(
              LAKE,
              MetadataObjects.of(null, "guide", MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE),
              "editors",
              Owner.Type.GROUP);
          return null;
        });
    var loaded = as("viewer", () -> manager.loadArticle(LAKE, "guide"));
    assertEquals("editors", loaded.article().owner().name());
    assertEquals(Owner.Type.GROUP, loaded.article().owner().type());
    assertEquals(created.revisionToken(), loaded.revisionToken());
    var edited =
        as(
            "admin",
            () ->
                manager.alterArticle(
                    LAKE,
                    "guide",
                    loaded.revisionToken(),
                    KnowledgeBaseArticleChange.updateTitle("Updated guide")));
    assertEquals("editors", edited.article().owner().name());
    assertEquals(Owner.Type.GROUP, edited.article().owner().type());
  }

  @TestTemplate
  void transferUsesCurrentUserOrGroupAndPreservesRevision() throws Exception {
    var draft =
        as("writer", () -> manager.createArticle(LAKE, article("draft", ArticleStatus.DRAFT)));
    MetadataObject object =
        MetadataObjects.of(null, "draft", MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE);
    backend.insert(
        createGroupEntity(
            ids.incrementAndGet(),
            AuthorizationUtils.ofGroupNamespace(LAKE),
            "authors",
            AUDIT_INFO,
            List.of(),
            List.of()),
        false);
    as(
        "writer",
        () -> {
          assertEquals("writer", owners.getOwner(LAKE, object).orElseThrow().name());
          assertTrue(
              nodeA.hasSetOwnerPermission(
                  LAKE, object.type().name(), object.name(), new AuthorizationRequestContext()));
          owners.setOwners(LAKE, List.of(object, object), "authors", Owner.Type.GROUP);
          return null;
        });
    as(
        "admin",
        () -> {
          assertFalse(
              nodeA.hasSetOwnerPermission(
                  LAKE, object.type().name(), object.name(), new AuthorizationRequestContext()));
          assertThrows(
              ForbiddenException.class,
              () -> owners.setOwner(LAKE, object, "writer", Owner.Type.USER));
          return null;
        });
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () -> as("writer", () -> manager.loadArticle(LAKE, "draft")));
    assertThrows(ForbiddenException.class, () -> as("writer", () -> owners.getOwner(LAKE, object)));
    UserPrincipal member =
        new UserPrincipal("writer2", List.of(new UserGroup(Optional.empty(), "authors")));
    PrincipalUtils.doAs(
        member,
        () -> {
          assertEquals(
              draft.revisionToken(), manager.getArticleContent(LAKE, "draft").revisionToken());
          assertEquals("authors", owners.getOwner(LAKE, object).orElseThrow().name());
          var evaluator =
              new AuthorizationExpressionEvaluator(
                  AuthorizationExpressionConstants.CAN_ACCESS_METADATA, nodeA);
          var names =
              NameIdentifierUtil.splitNameIdentifier(
                  LAKE,
                  Entity.EntityType.KNOWLEDGE_BASE_ARTICLE,
                  MetadataObjectUtil.toEntityIdent(LAKE, object));
          assertTrue(
              evaluator.evaluate(
                  names,
                  new AuthorizationRequestContext(),
                  member,
                  Optional.of(object.type().name())));
          assertTrue(
              nodeA.hasSetOwnerPermission(
                  LAKE, object.type().name(), object.name(), new AuthorizationRequestContext()));
          owners.setOwner(LAKE, object, "viewer", Owner.Type.USER);
          return null;
        });
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () -> as("viewer", () -> manager.loadArticle(LAKE, "draft")));
    as(
        "lakeOwner",
        () -> {
          owners.setOwner(LAKE, object, "writer", Owner.Type.USER);
          return null;
        });
    assertEquals(
        draft.revisionToken(),
        as("writer", () -> manager.getArticleContent(LAKE, "draft")).revisionToken());
  }

  @TestTemplate
  void explicitViewDenyAlsoAppliesToTheMetalakeOwner() throws Exception {
    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE),
            Entity.EntityType.METALAKE,
            NameIdentifierUtil.ofUser(LAKE, "denied"),
            Entity.EntityType.USER);
    as("writer", () -> manager.createArticle(LAKE, article("public", ArticleStatus.PUBLISHED)));
    assertTrue(
        as(
            "denied",
            () ->
                nodeA.isOwner(
                    PrincipalUtils.getCurrentPrincipal(),
                    LAKE,
                    MetadataObjects.of(null, LAKE, MetadataObject.Type.METALAKE),
                    new AuthorizationRequestContext())));
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () -> as("denied", () -> manager.loadArticle(LAKE, "public")));
    assertFalse(as("denied", () -> manager.deleteArticle(LAKE, "public")));
  }

  @TestTemplate
  void disabledAuthorizationAllowsAnUnregisteredCreatorAndSkipsOwnership() throws Exception {
    ArticleManager unrestricted = new ArticleManager(ids::incrementAndGet, 1024, false);
    var draft =
        as(
            "unregistered",
            () -> unrestricted.createArticle(LAKE, article("draft", ArticleStatus.DRAFT)));
    assertEquals(draft, as("outsider", () -> unrestricted.loadArticle(LAKE, "draft")));
    assertTrue(
        OwnerMetaService.getInstance()
            .getOwner(draft.article().nameIdentifier(), Entity.EntityType.KNOWLEDGE_BASE_ARTICLE)
            .isEmpty());
  }

  @TestTemplate
  void contentVisibilityUsesTheSameSnapshotAsTheBody() throws Exception {
    var draft =
        as("writer", () -> manager.createArticle(LAKE, article("draft", ArticleStatus.DRAFT)));
    MetadataObject object =
        MetadataObjects.of(null, "draft", MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE);
    GravitinoAuthorizer observed =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(nodeA));
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      // Redact the body and transfer the draft between the content read and its owner check.
      Mockito.doAnswer(
              invocation -> {
                worker
                    .submit(
                        () ->
                            as(
                                "writer",
                                () -> {
                                  manager.alterArticle(
                                      LAKE,
                                      "draft",
                                      draft.revisionToken(),
                                      KnowledgeBaseArticleChange.updateBody(
                                          ArticleBody.of(ArticleBody.Format.MARKDOWN, "revised")));
                                  owners.setOwner(LAKE, object, "writer2", Owner.Type.USER);
                                  return null;
                                }))
                    .get(30, TimeUnit.SECONDS);
                return nodeA.isArticleOwner(
                    invocation.getArgument(0),
                    LAKE,
                    draft.article().id(),
                    invocation.getArgument(3));
              })
          .when(observed)
          .isArticleOwner(
              Mockito.argThat(principal -> principal.getName().equals("writer2")),
              Mockito.eq(LAKE),
              Mockito.eq(draft.article().id()),
              Mockito.any());
      env.setGravitinoAuthorizer(observed);
      assertThrows(
          NoSuchKnowledgeBaseArticleException.class,
          () -> as("writer2", () -> manager.getArticleContent(LAKE, "draft")));
    } finally {
      env.setGravitinoAuthorizer(nodeA);
      worker.shutdownNow();
    }
    assertEquals(
        "revised", as("writer2", () -> manager.getArticleContent(LAKE, "draft")).body().content());
  }

  @TestTemplate
  void genericGrantsAndAssociationsRejectArticleTargets() {
    MetadataObject.Type type = MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE;
    MetadataObject object = MetadataObjects.of(null, "draft", type);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SecurableObjects.parse(
                object.name(), type, List.of(Privileges.allow("VIEW_DEFINITION"))));
    SecurableObject supplied = Mockito.mock(SecurableObject.class);
    Mockito.when(supplied.type()).thenReturn(type);
    assertThrows(
        IllegalArgumentException.class,
        () -> RoleEntity.builder().withSecurableObjects(List.of(supplied)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TagManager(ids::incrementAndGet, owners.getStore())
                .associateTagsForMetadataObject(LAKE, object, new String[] {"tag"}, new String[0]));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PolicyManager(ids::incrementAndGet, owners.getStore())
                .addPolicyToObject(LAKE, "policy", object, AUDIT_INFO));
  }

  @TestTemplate
  void createChecksTheMetalakeAndReauthorizesNameRetries() throws Exception {
    as("writer", () -> manager.createArticle(LAKE, article("guide", ArticleStatus.PUBLISHED)));
    GravitinoAuthorizer grants = Mockito.mock(GravitinoAuthorizer.class);
    env.setGravitinoAuthorizer(grants);
    assertThrows(
        NoSuchMetalakeException.class,
        () ->
            as(
                "writer",
                () -> manager.createArticle("absent", article("guide", ArticleStatus.DRAFT))));
    assertThrows(
        NoSuchMetalakeException.class, () -> as("writer", () -> manager.listArticles("absent")));
    Mockito.verifyNoInteractions(grants);

    Mockito.when(
            grants.authorize(
                Mockito.any(),
                Mockito.eq(LAKE),
                Mockito.any(),
                Mockito.eq(Privilege.Name.VIEW_DEFINITION),
                Mockito.any()))
        .thenReturn(true);
    AtomicLong checks = new AtomicLong();
    Mockito.when(
            grants.authorize(
                Mockito.any(),
                Mockito.eq(LAKE),
                Mockito.any(),
                Mockito.eq(Privilege.Name.CREATE_DEFINITION),
                Mockito.any()))
        .thenAnswer(
            invocation -> {
              assertTrue(SqlSessions.hasActiveSession());
              return checks.incrementAndGet() == 1;
            });
    ArticleCreate generated =
        ArticleCreate.builder()
            .withTitle("guide")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
            .build();
    assertThrows(
        ForbiddenException.class, () -> as("writer", () -> manager.createArticle(LAKE, generated)));
    assertEquals(2, checks.get());
    env.setGravitinoAuthorizer(nodeA);
    assertEquals(
        List.of("guide"),
        as("writer", () -> manager.listArticles(LAKE)).stream()
            .map(KnowledgeBaseArticleEntity::name)
            .toList());
  }

  @TestTemplate
  void readChecksViewBeforeFetchingAndPreservesAuthorizationFailures() throws Exception {
    var storage = Mockito.mock(KnowledgeBaseArticleMetaService.class);
    ArticleManager reader;
    try (var services = Mockito.mockStatic(KnowledgeBaseArticleMetaService.class)) {
      services.when(KnowledgeBaseArticleMetaService::getInstance).thenReturn(storage);
      reader = new ArticleManager(ids::incrementAndGet, 1024, true);
    }
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () -> as("outsider", () -> reader.loadArticle(LAKE, "guide")));
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () -> as("outsider", () -> reader.getArticleContent(LAKE, "guide")));
    assertTrue(as("outsider", () -> reader.listArticles(LAKE)).isEmpty());
    Mockito.verifyNoInteractions(storage);

    NoSuchEntityException failure = new NoSuchEntityException("Owner lookup failed");
    var unavailable = Mockito.mock(GravitinoAuthorizer.class);
    Mockito.when(unavailable.isOwner(Mockito.any(), Mockito.eq(LAKE), Mockito.any(), Mockito.any()))
        .thenThrow(failure);
    env.setGravitinoAuthorizer(unavailable);
    assertSame(failure, assertThrows(NoSuchEntityException.class, () -> reader.listArticles(LAKE)));
    assertSame(
        failure,
        assertThrows(NoSuchEntityException.class, () -> reader.loadArticle(LAKE, "guide")));
    assertSame(
        failure,
        assertThrows(NoSuchEntityException.class, () -> reader.getArticleContent(LAKE, "guide")));
    Mockito.verifyNoInteractions(storage);
  }

  @TestTemplate
  void ownerCanTransferAfterCreateIsRevoked() throws Exception {
    var draft =
        as("writer", () -> manager.createArticle(LAKE, article("draft", ArticleStatus.DRAFT)));
    List<String> remainingRoles = List.of("view");
    List<Long> remainingIds = roleIds(remainingRoles);
    backend.update(
        NameIdentifierUtil.ofUser(LAKE, "writer"),
        Entity.EntityType.USER,
        (UserEntity old) ->
            createUserEntity(
                old.id(),
                old.namespace(),
                old.name(),
                old.auditInfo(),
                remainingRoles,
                remainingIds));
    nodeA.handleUserRoleRelChange(LAKE, "writer");

    MetadataObject object =
        MetadataObjects.of(null, "draft", MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE);
    as(
        "writer",
        () -> {
          assertThrows(
              NoSuchKnowledgeBaseArticleException.class, () -> manager.loadArticle(LAKE, "draft"));
          assertTrue(
              nodeA.hasSetOwnerPermission(
                  LAKE, object.type().name(), object.name(), new AuthorizationRequestContext()));
          owners.setOwner(LAKE, object, "writer2", Owner.Type.USER);
          return null;
        });
    assertEquals(draft, as("writer2", () -> manager.loadArticle(LAKE, "draft")));
    assertEquals(
        "writer2", as("writer2", () -> owners.getOwner(LAKE, object)).orElseThrow().name());
  }

  @TestTemplate
  void creationRollsBackIfOwnerAssignmentFails() throws Exception {
    // A grant through a group can allow creation before the user has been registered locally.
    GravitinoAuthorizer allowed = Mockito.mock(GravitinoAuthorizer.class);
    Mockito.when(
            allowed.authorize(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);
    env.setGravitinoAuthorizer(allowed);
    assertThrows(
        NoSuchEntityException.class,
        () ->
            as(
                "unregistered",
                () -> manager.createArticle(LAKE, article("retry", ArticleStatus.DRAFT))));
    env.setGravitinoAuthorizer(nodeA);
    assertThrows(
        NoSuchKnowledgeBaseArticleException.class,
        () -> as("admin", () -> manager.loadArticle(LAKE, "retry")));
    assertEquals(
        "retry",
        as("writer", () -> manager.createArticle(LAKE, article("retry", ArticleStatus.DRAFT)))
            .article()
            .name());
  }

  @TestTemplate
  void hiddenTermBindingsAreFilteredBeforePagingAndStatusConflicts() throws Exception {
    var glossary = new GlossaryManager(ids::incrementAndGet, true);
    as("writer2", () -> glossary.createTerm(LAKE, TermSpec.builder("First").build()));
    as("writer2", () -> glossary.createTerm(LAKE, TermSpec.builder("Second").build()));
    as(
        "writer",
        () ->
            manager.createArticle(
                LAKE,
                ArticleCreate.builder()
                    .withTitle("Guide")
                    .withName("guide")
                    .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
                    .addTerms("First", "Second")
                    .build()));
    var token = as("viewer", () -> manager.loadArticle(LAKE, "guide")).revisionToken();
    as(
        "writer2",
        () -> glossary.alterTerm(LAKE, "First", TermChange.setStatus(GlossaryTerm.Status.DRAFT)));
    as(
        "writer",
        () -> {
          var page = manager.listTermBindings(LAKE, "guide", 1, 0);
          assertEquals("Second", page.terms().get(0).name());
          assertFalse(page.hasMore());
          var loaded = manager.loadArticle(LAKE, "guide", 1, 0);
          assertEquals(1, loaded.article().bindings().size());
          assertFalse(loaded.article().termsHasMore());
          assertTrue(manager.listTermBindings(LAKE, "guide", 1, 1).terms().isEmpty());
          assertThrows(
              NoSuchTermException.class,
              () ->
                  manager.alterArticle(
                      LAKE, "guide", token, KnowledgeBaseArticleChange.addTerms("First")));
          assertThrows(
              NoSuchTermException.class,
              () ->
                  manager.alterArticle(
                      LAKE, "guide", "malformed", KnowledgeBaseArticleChange.addTerms("First")));
          assertThrows(
              NoSuchTermException.class,
              () -> manager.listArticlesForTerm(LAKE, "First", true, 1, 0));
          var summary = manager.listArticlesForTerm(LAKE, "Second", false, 10, 0).articles().get(0);
          assertEquals("writer", summary.owner().name());
          assertEquals("Second", summary.terms()[0].name());
          assertEquals(1, summary.terms().length);
          return null;
        });
    as(
        "writer2",
        () -> {
          assertEquals(
              ArticleTermBinding.Status.DRAFT,
              manager.listTermBindings(LAKE, "guide", 1, 0).terms().get(0).status());
          assertThrows(
              UnapprovedTermException.class,
              () ->
                  manager.createArticle(
                      LAKE,
                      ArticleCreate.builder()
                          .withTitle("Other")
                          .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
                          .addTerms("First")
                          .build()));
          return null;
        });
    as(
        "outsider",
        () -> {
          assertThrows(
              NoSuchKnowledgeBaseArticleException.class,
              () -> manager.listTermBindings(LAKE, "guide", 1, 0));
          assertThrows(
              NoSuchTermException.class,
              () -> manager.listArticlesForTerm(LAKE, "Second", false, 1, 0));
          return null;
        });
  }

  @TestTemplate
  void domainVisibilityAndOrderedReplacements() throws Exception {
    insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    long retail = insertDomain(LAKE, "Retail", Domain.Status.PUBLISHED, "Sales", AUDIT_INFO);
    long hidden = insertDomain(LAKE, "Secret", Domain.Status.DRAFT, "Sales", AUDIT_INFO);
    role(
        "salesReader",
        SecurableObjects.parse(
            "Sales", MetadataObject.Type.DOMAIN, List.of(Privileges.allow("USE_DOMAIN"))));
    user("editor", "manage", "salesReader");
    var initial =
        as(
            "lakeOwner",
            () ->
                manager.createArticle(
                    LAKE, article("guide", ArticleStatus.PUBLISHED, "Secret", "Retail")));
    as(
        "lakeOwner",
        () ->
            manager.createArticle(LAKE, article("secretOnly", ArticleStatus.PUBLISHED, "Secret")));
    assertEquals(List.of("Retail", "Secret"), initial.article().domains());
    as(
        "editor",
        () -> {
          var guide = manager.loadArticle(LAKE, "guide");
          assertEquals(Set.of(retail), guide.article().domainIds());
          assertEquals(List.of("Retail"), guide.article().domains());
          assertEquals(List.of(guide.article()), manager.listArticles(LAKE, "Sales"));
          assertEquals(List.of(guide.article()), manager.listArticles(LAKE, "Retail"));
          assertThrows(NoSuchDomainException.class, () -> manager.listArticles(LAKE, "Secret"));
          assertThrows(NoSuchDomainException.class, () -> manager.listArticles(LAKE, "sales"));
          var roundtrip =
              manager.alterArticle(LAKE, "guide", guide.revisionToken(), domains("Retail"));
          assertEquals(guide.revisionToken(), roundtrip.revisionToken());
          assertEquals(List.of("Retail"), roundtrip.article().domains());
          assertThrows(
              ForbiddenException.class, () -> manager.alterArticle(LAKE, "guide", null, domains()));
          assertThrows(
              ForbiddenException.class,
              () ->
                  manager.alterArticle(
                      LAKE, "guide", null, domains("Sales", "Retail"), domains("Retail")));
          assertThrows(
              ForbiddenException.class,
              () -> manager.createArticle(LAKE, article("denied", ArticleStatus.DRAFT, "Retail")));
          return null;
        });
    assertTrue(as("outsider", () -> manager.listArticles(LAKE, "missing")).isEmpty());
    as(
        "lakeOwner",
        () -> {
          assertEquals(
              Set.of(retail, hidden), manager.loadArticle(LAKE, "guide").article().domainIds());
          assertEquals(
              initial.revisionToken(),
              manager
                  .alterArticle(LAKE, "guide", null, domains(), domains("Secret", "Retail"))
                  .revisionToken());
          assertThrows(
              NoSuchDomainException.class,
              () -> manager.alterArticle(LAKE, "guide", null, domains("missing"), domains()));
          assertThrows(
              NoSuchDomainException.class,
              () -> manager.createArticle(LAKE, article("bad", ArticleStatus.DRAFT, "missing")));
          assertThrows(
              NoSuchKnowledgeBaseArticleException.class, () -> manager.loadArticle(LAKE, "bad"));
          var changed =
              manager.alterArticle(
                  LAKE,
                  "guide",
                  initial.revisionToken(),
                  KnowledgeBaseArticleChange.updateComment("updated"),
                  domains("Retail"));
          assertEquals(2L, changed.article().revision());
          assertEquals(List.of("Retail"), changed.article().domains());
          assertThrows(
              ArticleRevisionConflictException.class,
              () -> manager.alterArticle(LAKE, "guide", initial.revisionToken(), domains()));
          DomainMetaService.getInstance()
              .updateDomain(
                  NameIdentifierUtil.ofDomain(LAKE, "Retail"),
                  (DomainEntity current) -> current.toBuilder().withName("Stores").build());
          insertDomain(LAKE, "Retail", Domain.Status.PUBLISHED, null, AUDIT_INFO);
          assertTrue(manager.listArticles(LAKE, "Retail").isEmpty());
          var renamed = manager.loadArticle(LAKE, "guide");
          assertEquals(changed.revisionToken(), renamed.revisionToken());
          assertEquals(List.of("Stores"), renamed.article().domains());
          assertEquals(Set.of(retail), renamed.article().domainIds());
          return null;
        });
  }

  @TestTemplate
  void domainAdminDetachesHiddenDraftWithoutArticlePrivileges() throws Exception {
    insertDomain(LAKE, "Sales", Domain.Status.DRAFT, null, AUDIT_INFO);
    role("domainAdmin", Privileges.allow("MANAGE_DOMAIN"));
    user("steward", "domainAdmin");
    var draft =
        as(
            "lakeOwner",
            () -> manager.createArticle(LAKE, article("draft", ArticleStatus.DRAFT, "Sales")));
    as(
        "outsider",
        () -> {
          assertThrows(
              NoSuchDomainException.class, () -> manager.detachDomain(LAKE, "missing", "Sales"));
          return null;
        });
    as(
        "steward",
        () -> {
          assertThrows(
              NoSuchKnowledgeBaseArticleException.class, () -> manager.loadArticle(LAKE, "draft"));
          manager.detachDomain(LAKE, "missing", "Sales");
          manager.detachDomain(LAKE, "draft", "Sales");
          manager.detachDomain(LAKE, "draft", "Sales");
          return null;
        });
    var after = as("lakeOwner", () -> manager.loadArticle(LAKE, "draft"));
    assertEquals(draft.article().revision() + 1, after.article().revision());
    assertEquals("steward", after.article().auditInfo().lastModifier());
    assertTrue(after.article().domainIds().isEmpty());
    assertTrue(
        DomainMetaService.getInstance().deleteDomain(NameIdentifierUtil.ofDomain(LAKE, "Sales")));
  }

  @TestTemplate
  void reparentRevokesInheritedAuthorityBeforeAlterAndDetach() throws Exception {
    insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    insertDomain(LAKE, "Finance", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    role(
        "salesAdmin",
        SecurableObjects.parse(
            "Sales", MetadataObject.Type.DOMAIN, List.of(Privileges.allow("MANAGE_DOMAIN"))));
    user("salesWriter", "manage", "salesAdmin");
    for (boolean detach : List.of(false, true)) {
      String name = detach ? "detach" : "alter";
      insertDomain(LAKE, name, Domain.Status.PUBLISHED, "Sales", AUDIT_INFO);
      var before =
          as(
              "lakeOwner",
              () ->
                  manager.createArticle(
                      LAKE,
                      article(
                          name,
                          ArticleStatus.PUBLISHED,
                          detach ? new String[] {name} : new String[0])));
      var observed = Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(nodeA));
      var moved = new AtomicBoolean();
      var worker = Executors.newSingleThreadExecutor();
      try {
        // Commit the move after the initial inherited grant, before the write takes endpoint locks.
        Mockito.doAnswer(
                call -> {
                  boolean granted =
                      nodeA.authorize(
                          call.getArgument(0),
                          LAKE,
                          call.getArgument(2),
                          Privilege.Name.MANAGE_DOMAIN,
                          call.getArgument(4));
                  if (granted && moved.compareAndSet(false, true)) {
                    worker
                        .submit(
                            () ->
                                DomainRelService.getInstance()
                                    .setDomainParentAndGet(
                                        NameIdentifierUtil.ofDomain(LAKE, name),
                                        NameIdentifierUtil.ofDomain(LAKE, "Finance"),
                                        AUDIT_INFO,
                                        ignored -> {}))
                        .get(30, TimeUnit.SECONDS);
                  }
                  return granted;
                })
            .when(observed)
            .authorize(
                Mockito.any(),
                Mockito.eq(LAKE),
                Mockito.eq(MetadataObjects.of(null, "Sales", MetadataObject.Type.DOMAIN)),
                Mockito.eq(Privilege.Name.MANAGE_DOMAIN),
                Mockito.any());
        env.setGravitinoAuthorizer(observed);
        as(
            "salesWriter",
            () -> {
              assertThrows(
                  ForbiddenException.class,
                  () -> {
                    if (detach) {
                      manager.detachDomain(LAKE, name, name);
                    } else {
                      manager.alterArticle(
                          LAKE,
                          name,
                          before.revisionToken(),
                          KnowledgeBaseArticleChange.updateBody(
                              ArticleBody.of(ArticleBody.Format.MARKDOWN, "changed")),
                          domains(name));
                    }
                  });
              return null;
            });
        assertTrue(moved.get());
      } finally {
        env.setGravitinoAuthorizer(nodeA);
        worker.shutdownNow();
      }
      var after = as("lakeOwner", () -> manager.loadArticle(LAKE, name));
      assertEquals(before.revisionToken(), after.revisionToken());
      assertEquals(before.article().domainIds(), after.article().domainIds());
      assertEquals(
          "body", as("lakeOwner", () -> manager.getArticleContent(LAKE, name)).body().content());
    }
  }

  @TestTemplate
  void assetPrivilegesFilterBindingsBeforeOffsetAndRecheckNoOps() throws Exception {
    installAssetCatalog();
    createAndInsertCatalog(LAKE, "warehouse");
    createAndInsertSchema(LAKE, "warehouse", "sales");
    var ns = Namespace.of(LAKE, "warehouse", "sales");
    var names = List.of("first", "hidden", "last");
    List<ArticleObjectIdentifier> targets = new ArrayList<>();
    for (String name : names) {
      backend.insert(createTableEntity(ids.incrementAndGet(), ns, name, AUDIT_INFO), false);
      targets.add(
          ArticleObjectIdentifier.of(
              MetadataObject.Type.TABLE, List.of("warehouse", "sales"), name));
    }
    role("useCatalog", Privileges.allow("USE_CATALOG"));
    role("useSchema", Privileges.allow("USE_SCHEMA"));
    role("allTables", Privileges.allow("SELECT_TABLE"));
    role(
        "hideTable",
        SecurableObjects.parse(
            "warehouse.sales.hidden",
            MetadataObject.Type.TABLE,
            List.of(Privileges.deny("SELECT_TABLE"))));
    user("assetEditor", "manage", "useCatalog", "useSchema", "allTables", "hideTable");
    var created =
        as(
            "lakeOwner",
            () ->
                manager.createArticle(
                    LAKE,
                    ArticleCreate.builder()
                        .withTitle("Guide")
                        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
                        .addObjects(targets.toArray(ArticleObjectIdentifier[]::new))
                        .build()));
    as(
        "assetEditor",
        () -> {
          var first = manager.listAssetBindings(LAKE, "guide", 1, 0);
          assertEquals(List.of(targets.get(0)), first.objects());
          assertTrue(first.hasMore());
          var last = manager.loadArticle(LAKE, "guide", 1, 1).article();
          assertEquals(List.of(targets.get(2)), last.objects());
          assertFalse(last.objectsHasMore());
          assertThrows(
              NoSuchMetadataObjectException.class,
              () -> manager.listArticlesForObject(LAKE, targets.get(1), true, 1, 0));
          assertThrows(
              NoSuchMetadataObjectException.class,
              () ->
                  manager.alterArticle(
                      LAKE,
                      "guide",
                      null,
                      KnowledgeBaseArticleChange.updateTitle("Changed"),
                      KnowledgeBaseArticleChange.addObjects(targets.get(1))));
          assertThrows(
              NoSuchMetadataObjectException.class,
              () ->
                  manager.alterArticle(
                      LAKE,
                      "guide",
                      null,
                      KnowledgeBaseArticleChange.removeObjects(targets.get(1))));
          assertEquals(created.revisionToken(), manager.loadArticle(LAKE, "guide").revisionToken());
          return null;
        });
    assertTrue(
        as("viewer", () -> manager.listAssetBindings(LAKE, "guide", 1, 0)).objects().isEmpty());
  }

  @TestTemplate
  void columnUsesTableReadPermissionAndChecksTheCurrentTableId() throws Exception {
    installAssetCatalog();
    createAndInsertCatalog(LAKE, "warehouse");
    createAndInsertSchema(LAKE, "warehouse", "sales");
    var ns = Namespace.of(LAKE, "warehouse", "sales");
    var table =
        TableEntity.builder()
            .withId(ids.incrementAndGet())
            .withNamespace(ns)
            .withName("orders")
            .withColumns(
                List.of(
                    ColumnEntity.builder()
                        .withId(ids.incrementAndGet())
                        .withName("total.amount")
                        .withDataType(Types.IntegerType.get())
                        .withPosition(0)
                        .withNullable(true)
                        .withAutoIncrement(false)
                        .withAuditInfo(AUDIT_INFO)
                        .build()))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(table, false);
    role("useCatalog", Privileges.allow("USE_CATALOG"));
    role("useSchema", Privileges.allow("USE_SCHEMA"));
    role(
        "ordersReader",
        SecurableObjects.parse(
            "warehouse.sales.orders",
            MetadataObject.Type.TABLE,
            List.of(Privileges.allow("SELECT_TABLE"))));
    user("ordersReaderUser", "view", "useCatalog", "useSchema", "ordersReader");
    var column =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN, List.of("warehouse", "sales", "orders"), "total.amount");
    as(
        "lakeOwner",
        () ->
            manager.createArticle(
                LAKE,
                ArticleCreate.builder()
                    .withTitle("Guide")
                    .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
                    .addObjects(column)
                    .build()));
    assertEquals(
        List.of(column),
        as("ordersReaderUser", () -> manager.loadArticle(LAKE, "guide")).article().objects());
    assertEquals(
        "guide",
        as("ordersReaderUser", () -> manager.listArticlesForObject(LAKE, column, false, 1, 0))
            .articles()
            .get(0)
            .name());
    backend.delete(table.nameIdentifier(), Entity.EntityType.TABLE, false);
    createAndInsertTableEntity(ns, "orders");
    var replacement =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.TABLE, List.of("warehouse", "sales"), "orders");
    as(
        "lakeOwner",
        () ->
            manager.alterArticle(
                LAKE, "guide", null, KnowledgeBaseArticleChange.addObjects(replacement)));
    as(
        "ordersReaderUser",
        () -> {
          assertTrue(manager.loadArticle(LAKE, "guide").article().objects().isEmpty());
          assertThrows(
              NoSuchMetadataObjectException.class,
              () -> manager.listArticlesForObject(LAKE, replacement, false, 1, 0));
          return null;
        });
  }

  @TestTemplate
  void sweepPreservesLiveBindingHiddenFromCaller() throws Exception {
    installAssetCatalog();
    createAndInsertCatalog(LAKE, "warehouse");
    createAndInsertSchema(LAKE, "warehouse", "sales");
    var ns = Namespace.of(LAKE, "warehouse", "sales");
    backend.insert(createTableEntity(ids.incrementAndGet(), ns, "orders", AUDIT_INFO), false);
    var orders =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.TABLE, List.of("warehouse", "sales"), "orders");
    var created =
        as(
            "lakeOwner",
            () ->
                manager.createArticle(
                    LAKE,
                    ArticleCreate.builder()
                        .withTitle("Guide")
                        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
                        .addObjects(orders)
                        .build()));
    as(
        "viewer",
        () -> {
          assertTrue(manager.listAssetBindings(LAKE, "guide", 1, 0).objects().isEmpty());
          assertEquals(
              0,
              OrphanedMetadataObjectRelationService.getInstance()
                  .softDeleteOrphanedRelations(MetadataObject.Type.TABLE, 10));
          return null;
        });
    var loaded = as("lakeOwner", () -> manager.loadArticle(LAKE, "guide"));
    assertEquals(List.of(orders), loaded.article().objects());
    assertEquals(created.revisionToken(), loaded.revisionToken());
  }

  private void installAssetCatalog() throws Exception {
    // Role loading resolves the catalog's case and table-name rules for its securable objects.
    var manager = Mockito.mock(CatalogManager.class);
    var catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(new Capability() {});
    Mockito.doAnswer(
            call -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = call.getArgument(1);
              return operation.apply(catalog);
            })
        .when(manager)
        .doWithCatalog(Mockito.any(), Mockito.any());
    FieldUtils.writeField(env, "catalogManager", manager, true);
  }

  private static KnowledgeBaseArticleChange domains(String... names) {
    return KnowledgeBaseArticleChange.updateDomains(names);
  }

  private static ArticleCreate article(String name, ArticleStatus status, String... domains) {
    return ArticleCreate.builder()
        .withName(name)
        .withTitle(name)
        .addDomains(domains)
        .withStatus(status)
        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
        .build();
  }

  private void role(String name, Privilege privilege) throws Exception {
    role(name, SecurableObjects.ofMetalake(LAKE, List.of(privilege)));
  }

  private void role(String name, SecurableObject object) throws Exception {
    backend.insert(
        createRoleEntity(
            ids.incrementAndGet(),
            AuthorizationUtils.ofRoleNamespace(LAKE),
            name,
            AUDIT_INFO,
            List.of(object),
            null),
        false);
  }

  private void user(String name, String... roles) throws Exception {
    List<String> roleNames = List.of(roles);
    backend.insert(
        createUserEntity(
            ids.incrementAndGet(),
            AuthorizationUtils.ofUserNamespace(LAKE),
            name,
            AUDIT_INFO,
            roleNames,
            roleIds(roleNames)),
        false);
  }

  private List<Long> roleIds(List<String> roles) throws Exception {
    List<Long> ids = new ArrayList<>();
    for (String role : roles) {
      ids.add(backend.get(NameIdentifierUtil.ofRole(LAKE, role), Entity.EntityType.ROLE).id());
    }
    return ids;
  }

  private static <T> T as(String user, PrivilegedExceptionAction<T> work) throws Exception {
    return PrincipalUtils.doAs(new UserPrincipal(user), work);
  }
}
