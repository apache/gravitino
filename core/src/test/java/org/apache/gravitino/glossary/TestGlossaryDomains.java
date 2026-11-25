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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainManager;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.server.authorization.DomainAuthzHelper;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermRelMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.service.GlossaryDomainFixtures;
import org.apache.gravitino.storage.relational.service.GlossaryInvariants;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/** Runs glossary domain membership against real grant, domain and term rows (T7). */
class TestGlossaryDomains extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private final AtomicLong ids = new AtomicLong(100);
  private final GlossaryManager manager = new GlossaryManager(ids::incrementAndGet, true);
  private final GravitinoEnv env = GravitinoEnv.getInstance();
  private GravitinoAuthorizer previous;
  private EntityStore previousStore;
  private DomainDispatcher previousDomains;
  private CatalogManager previousCatalogs;
  private RelationalEntityStore store;
  private JcasbinAuthorizer authorizer;
  private OwnerManager owners;

  @BeforeEach
  void setUpDomains() throws Exception {
    previous = env.gravitinoAuthorizer();
    previousStore = (EntityStore) FieldUtils.readField(env, "entityStore", true);
    previousDomains =
        (DomainDispatcher) FieldUtils.readField(env, "internalDomainDispatcher", true);
    previousCatalogs = (CatalogManager) FieldUtils.readField(env, "catalogManager", true);
    FieldUtils.writeField(env, "catalogManager", Mockito.mock(CatalogManager.class), true);
    Config config = env.config();
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_CACHE_EXPIRATION_SECS))
        .thenReturn(3600L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_ROLE_CACHE_SIZE)).thenReturn(1000L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_OWNER_CACHE_SIZE)).thenReturn(1000L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_METADATA_ID_CACHE_SIZE))
        .thenReturn(1000L);
    Mockito.when(config.get(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS))
        .thenReturn(3600L);
    store = RelationalEntityStore.forOfflineMigration(backend, config);
    FieldUtils.writeField(
        env,
        "entityStore",
        Mockito.mock(EntityStore.class, AdditionalAnswers.delegatesTo(store)),
        true);
    FieldUtils.writeField(
        env, "internalDomainDispatcher", new DomainManager(ids::incrementAndGet, store), true);
    owners = new OwnerManager(store);
    createAndInsertMakeLake(LAKE);
    GlossaryDomainFixtures.insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    GlossaryDomainFixtures.insertDomain(
        LAKE, "Retail", Domain.Status.PUBLISHED, "Sales", AUDIT_INFO);
    GlossaryDomainFixtures.insertDomain(
        LAKE, "HiddenSub", Domain.Status.DRAFT, "Sales", AUDIT_INFO);
    GlossaryDomainFixtures.insertDomain(LAKE, "Finance", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    GlossaryDomainFixtures.insertDomain(LAKE, "DraftX", Domain.Status.DRAFT, null, AUDIT_INFO);
    metalakeRole("view", Privileges.allow("VIEW_DEFINITION"));
    metalakeRole("create", Privileges.allow("CREATE_DEFINITION"));
    metalakeRole("manage", Privileges.allow("MANAGE_DEFINITIONS"));
    metalakeRole("domainAdmin", Privileges.allow("MANAGE_DOMAIN"));
    domainRole("useSales", "Sales");
    domainRole("useFinance", "Finance");
    user("viewer", "view", "useSales");
    user("writer", "create", "useSales");
    user("writer2", "create", "useSales");
    user("admin", "manage", "useSales", "useFinance");
    user("domainAdmin", "domainAdmin");
    user("lakeOwner");
    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE),
            Entity.EntityType.METALAKE,
            NameIdentifierUtil.ofUser(LAKE, "lakeOwner"),
            Entity.EntityType.USER);
    authorizer = new JcasbinAuthorizer();
    authorizer.initialize();
    env.setGravitinoAuthorizer(authorizer);
  }

  @AfterEach
  void tearDownDomains() throws Exception {
    try {
      if (authorizer != null) authorizer.close();
      GlossaryInvariants.assertDefinitions();
      GlossaryInvariants.assertRelations();
      GlossaryInvariants.assertDomainMembership();
    } finally {
      env.setGravitinoAuthorizer(previous);
      FieldUtils.writeField(env, "entityStore", previousStore, true);
      FieldUtils.writeField(env, "internalDomainDispatcher", previousDomains, true);
      FieldUtils.writeField(env, "catalogManager", previousCatalogs, true);
    }
  }

  /** T7.1. */
  @TestTemplate
  void testCreateWithVisibleDomains() throws Exception {
    as(
        "writer",
        () ->
            manager.createTerm(
                LAKE, TermSpec.builder("Churn").withDomains("Sales", "Retail").build()));
    GlossaryTermEntity term = as("lakeOwner", () -> manager.getTerm(LAKE, "Churn"));
    assertArrayEquals(new String[] {"Retail", "Sales"}, term.domains());
    assertEquals(1, term.version());
  }

  @TestTemplate
  void testNameListSkipsMemberDomainProjection() throws Exception {
    GlossaryTermViews views = new GlossaryTermViews(manager);
    create("Needle", "Sales");
    create("Other", "Finance");
    as("viewer", () -> views.listTermNames(LAKE, null, null, null));
    List<String> lookups = new ArrayList<>();
    try (MockedStatic<SqlSessions> ignored =
        observeMapper(
            DomainMetaMapper.class,
            call -> {
              if (call.getMethod().getName().equals("selectDomainMetaByMetalakeAndName")) {
                lookups.add(call.getArgument(1));
              }
            })) {
      assertArrayEquals(
          new String[] {"Needle"},
          as("viewer", () -> views.listTermNames(LAKE, null, "EEd", null)));
      assertEquals(List.of(), lookups);
      TermDTO[] details = as("viewer", () -> views.listTerms(LAKE, null, "EEd", null));
      assertEquals(1, details.length);
      assertArrayEquals(new String[] {"Sales"}, details[0].getDomains());
      assertEquals(List.of("Sales"), lookups);
    }
  }

  @TestTemplate
  void testStatusFilterSkipsOwnershipChecksForExcludedDrafts() throws Exception {
    GlossaryTermViews views = new GlossaryTermViews(manager);
    GlossaryTerm.Status approved = GlossaryTerm.Status.APPROVED;
    as("writer", () -> views.createTerm(LAKE, TermSpec.builder("Root").build()));
    as(
        "writer",
        () ->
            views.createTerm(
                LAKE,
                TermSpec.builder("Draft")
                    .withStatus(GlossaryTerm.Status.DRAFT)
                    .withBroader("Root")
                    .build()));
    GravitinoAuthorizer observed =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(authorizer));
    env.setGravitinoAuthorizer(observed);
    as(
        "writer",
        () -> {
          assertArrayEquals(new String[] {"Root"}, views.listTermNames(LAKE, null, null, approved));
          assertEquals("Root", views.listTerms(LAKE, null, null, approved)[0].getName());
          assertThrows(NoSuchTermException.class, () -> views.getTerm(LAKE, "Draft", approved));
          assertEquals(0, views.getTerm(LAKE, "Root", approved).getNarrower().length);
          assertEquals(
              0,
              views.getTermHierarchy(LAKE, "Root", GlossaryManager.Direction.NARROWER, approved)
                  .length);
          return null;
        });
    Mockito.verify(observed, Mockito.never())
        .isGlossaryTermOwner(Mockito.any(), Mockito.eq(LAKE), Mockito.anyLong(), Mockito.any());
    assertEquals("Draft", as("writer", () -> views.getTerm(LAKE, "Draft", null)).getName());
    Mockito.verify(observed)
        .isGlossaryTermOwner(Mockito.any(), Mockito.eq(LAKE), Mockito.anyLong(), Mockito.any());
  }

  @TestTemplate
  void testDetailedListReusesVisibleRelationEndpoints() throws Exception {
    GlossaryTermViews views = new GlossaryTermViews(manager);
    GlossaryTermEntity parent = create("Parent");
    as(
        "lakeOwner",
        () -> views.createTerm(LAKE, TermSpec.builder("Child").withBroader("Parent").build()));
    List<Long> loaded = new ArrayList<>();
    try (MockedStatic<SqlSessions> ignored =
        observeMapper(
            GlossaryTermMetaMapper.class,
            call -> {
              if (call.getMethod().getName().equals("selectTermSnapshotsByIds")) {
                loaded.addAll(call.<List<Long>>getArgument(2));
              }
            })) {
      TermDTO[] all = as("lakeOwner", () -> views.listTerms(LAKE, null, null, null));
      TermDTO child =
          Arrays.stream(all).filter(t -> t.getName().equals("Child")).findFirst().orElseThrow();
      assertArrayEquals(new String[] {"Parent"}, child.getBroader());
      assertEquals(List.of(), loaded);
      TermDTO[] onlyChild = as("lakeOwner", () -> views.listTerms(LAKE, null, "Child", null));
      assertArrayEquals(new String[] {"Parent"}, onlyChild[0].getBroader());
      assertEquals(List.of(parent.id()), loaded);
    }
  }

  @TestTemplate
  void testProjectionFailureRollsBackCreate() throws Exception {
    GlossaryTermViews views = new GlossaryTermViews(manager);
    RuntimeException failure = new IllegalStateException("Owner projection failed");
    try (MockedStatic<SqlSessions> ignored =
        observeMapper(
            OwnerMetaMapper.class,
            call -> {
              if (call.getMethod()
                  .getName()
                  .equals("batchSelectUserOwnerMetaByMetadataObjectIdAndType")) {
                throw failure;
              }
            })) {
      assertEquals(
          failure,
          as(
              "writer",
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () ->
                          views.createTerm(
                              LAKE, TermSpec.builder("Retry").withDomains("Sales").build()))));
    }
    as(
        "lakeOwner",
        () -> assertThrows(NoSuchTermException.class, () -> views.getTerm(LAKE, "Retry", null)));
    assertTrue(as("lakeOwner", () -> manager.listTerms(LAKE, "Sales")).isEmpty());
    assertEquals(
        1L,
        as("writer", () -> views.createTerm(LAKE, TermSpec.builder("Retry").build())).getVersion());
  }

  @TestTemplate
  void testTermResponseSnapshot() throws Exception {
    GlossaryTermViews views = new GlossaryTermViews(manager);
    GlossaryTerm.Status approved = GlossaryTerm.Status.APPROVED;
    GlossaryTermEntity leaf = create("Leaf", "Sales", "Finance");
    as("lakeOwner", () -> transfer(leaf, "writer"));
    as(
        "lakeOwner",
        () -> {
          views.createTerm(LAKE, TermSpec.builder("Root").build());
          views.createTerm(
              LAKE, TermSpec.builder("Peer").withStatus(GlossaryTerm.Status.DEPRECATED).build());
          views.createTerm(
              LAKE,
              TermSpec.builder("Middle")
                  .withStatus(GlossaryTerm.Status.DRAFT)
                  .withBroader("Root")
                  .build());
          TermDTO result =
              views.alterTerm(
                  LAKE,
                  "Leaf",
                  leaf.revisionToken(),
                  new TermChange[] {
                    TermChange.addBroader("Middle"), TermChange.addRelated("Peer")
                  });
          assertArrayEquals(new String[] {"Middle"}, result.getBroader());
          assertEquals("writer", result.getOwner().name());
          assertThrows(
              TermVersionConflictException.class,
              () ->
                  views.alterTerm(
                      LAKE,
                      "Leaf",
                      leaf.revisionToken(),
                      new TermChange[] {TermChange.rename("Gone")}));
          assertEquals(
              2,
              views.getTermHierarchy(LAKE, "Root", GlossaryManager.Direction.NARROWER, null)
                  .length);
          assertEquals(
              0,
              views.getTermHierarchy(LAKE, "Root", GlossaryManager.Direction.NARROWER, approved)
                  .length);
          assertThrows(NoSuchTermException.class, () -> views.getTerm(LAKE, "Middle", approved));
          assertThrows(
              NoSuchTermException.class,
              () ->
                  views.getTermHierarchy(
                      LAKE, "Middle", GlossaryManager.Direction.BROADER, approved));
          assertEquals(0, views.getTerm(LAKE, "Leaf", approved).getBroader().length);
          assertArrayEquals(new String[] {"Peer"}, result.getRelated());
          assertEquals(0, views.getTerm(LAKE, "Leaf", approved).getRelated().length);
          assertEquals(0, views.getTerm(LAKE, "Root", approved).getNarrower().length);
          assertFalse(
              Arrays.asList(views.listTermNames(LAKE, null, null, approved)).contains("Middle"));
          return null;
        });
    as(
        "writer",
        () -> {
          String[] visible = {"Sales"};
          assertArrayEquals(
              visible,
              views
                  .createTerm(
                      LAKE,
                      TermSpec.builder("Child").withDomains("Sales").withBroader("Leaf").build())
                  .getDomains());
          assertArrayEquals(visible, views.getTerm(LAKE, "Leaf", null).getDomains());
          assertArrayEquals(visible, views.listTerms(LAKE, "Sales", "eAF", null)[0].getDomains());
          assertArrayEquals(
              visible,
              views.getTermHierarchy(LAKE, "Child", GlossaryManager.Direction.BROADER, null)[0]
                  .getDomains());
          assertArrayEquals(
              visible,
              views
                  .alterTerm(
                      LAKE, "Leaf", null, new TermChange[] {TermChange.updateDefinition("changed")})
                  .getDomains());
          return null;
        });
    assertArrayEquals(
        new String[] {"Finance", "Sales"},
        as("lakeOwner", () -> views.getTerm(LAKE, "Leaf", null)).getDomains());

    GlossaryTermViews unauthzViews =
        new GlossaryTermViews(new GlossaryManager(ids::incrementAndGet, false));
    TermDTO unowned = unauthzViews.createTerm(LAKE, TermSpec.builder("Unowned").build());
    assertEquals(null, unowned.getOwner());
  }

  @TestTemplate
  void testMembershipRecordsTheAttachingUser() throws Exception {
    GlossaryTermEntity term =
        as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    Instant start = Instant.ofEpochMilli(System.currentTimeMillis());
    GlossaryTermEntity updated =
        as("admin", () -> manager.alterTerm(LAKE, term.name(), TermChange.addDomain("Sales")));
    Instant end = Instant.now();
    assertEquals("writer", updated.auditInfo().creator());
    assertEquals(term.auditInfo().createTime(), updated.auditInfo().createTime());
    assertEquals("admin", updated.auditInfo().lastModifier());

    // Repeating the add as the Term creator must preserve the original attachment.
    as("writer", () -> manager.alterTerm(LAKE, term.name(), TermChange.addDomain("Sales")));
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement query =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT audit_info FROM domain_member_rel WHERE member_type='GLOSSARY_TERM' AND member_id=? AND deleted_at=0")) {
      query.setLong(1, term.id());
      try (ResultSet rows = query.executeQuery()) {
        assertTrue(rows.next());
        AuditInfo membership =
            JsonUtils.anyFieldMapper().readValue(rows.getString(1), AuditInfo.class);
        assertEquals("admin", membership.creator());
        assertFalse(membership.createTime().isBefore(start));
        assertFalse(membership.createTime().isAfter(end));
        assertFalse(rows.next());
      }
    }
  }

  /** T7.2. */
  @TestTemplate
  void testCreateInHiddenDomainCreatesNothing() throws Exception {
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "writer",
                () ->
                    manager.createTerm(
                        LAKE, TermSpec.builder("Churn").withDomains("Sales", "Finance").build())));
    assertTrue(as("lakeOwner", () -> manager.listTerms(LAKE)).isEmpty());
    assertThrows(
        NoSuchDomainException.class,
        () ->
            as(
                "lakeOwner",
                () ->
                    manager.createTerm(
                        LAKE, TermSpec.builder("Churn").withDomains("Sales", "NoSuch").build())));
    assertTrue(as("lakeOwner", () -> manager.listTerms(LAKE)).isEmpty());
  }

  @TestTemplate
  void testAddRemoveBumpsRevisionOnce() throws Exception {
    as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    GlossaryTermEntity added =
        as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("Sales")));
    assertEquals(2, added.version());
    assertArrayEquals(new String[] {"Sales"}, added.domains());
    GlossaryTermEntity again =
        as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("Sales")));
    assertEquals(added.revisionToken(), again.revisionToken());
    GlossaryTermEntity removed =
        as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.removeDomain("Sales")));
    assertEquals(3, removed.version());
    assertArrayEquals(new String[0], removed.domains());
    GlossaryTermEntity noop =
        as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.removeDomain("Sales")));
    assertEquals(3, noop.version());
  }

  /** Review focus: net domain edits within one request. */
  @TestTemplate
  void testNetDomainEditsInOneRequest() throws Exception {
    as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    GlossaryTermEntity first =
        as(
            "writer",
            () ->
                manager.alterTerm(
                    LAKE,
                    "Churn",
                    TermChange.addDomain("Sales"),
                    TermChange.removeDomain("Sales")));
    assertEquals(1, first.version());
    assertArrayEquals(new String[0], first.domains());
    as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("Sales")));
    GlossaryTermEntity second =
        as(
            "writer",
            () ->
                manager.alterTerm(
                    LAKE,
                    "Churn",
                    TermChange.removeDomain("Sales"),
                    TermChange.addDomain("Sales")));
    assertEquals(2, second.version());
    assertArrayEquals(new String[] {"Sales"}, second.domains());
  }

  /** Review focus: a hidden draft domain looks exactly like a missing one. */
  @TestTemplate
  void testHiddenDraftDomainLooksMissing() throws Exception {
    as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    for (boolean add : List.of(true, false)) {
      NoSuchDomainException draft =
          assertThrows(
              NoSuchDomainException.class,
              () -> as("writer", () -> manager.alterTerm(LAKE, "Churn", change(add, "DraftX"))));
      NoSuchDomainException missing =
          assertThrows(
              NoSuchDomainException.class,
              () -> as("writer", () -> manager.alterTerm(LAKE, "Churn", change(add, "NoSuch"))));
      assertEquals(missing.getMessage(), draft.getMessage().replace("DraftX", "NoSuch"));
    }
    assertThrows(
        ForbiddenException.class,
        () ->
            as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("Finance"))));
  }

  @TestTemplate
  void testBatchReportsHiddenAndMissingDomainInTheSameOrder() throws Exception {
    as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    for (String name : List.of("DraftX", "Absent")) {
      for (String other : List.of("NoSuch", "Finance")) {
        NoSuchDomainException createFailure =
            assertThrows(
                NoSuchDomainException.class,
                () ->
                    as(
                        "writer",
                        () ->
                            manager.createTerm(
                                LAKE,
                                TermSpec.builder("Denied").withDomains(other, name).build())));
        NoSuchDomainException alterFailure =
            assertThrows(
                NoSuchDomainException.class,
                () ->
                    as(
                        "writer",
                        () ->
                            manager.alterTerm(
                                LAKE,
                                "Churn",
                                TermChange.addDomain(other),
                                TermChange.removeDomain(name))));
        assertEquals("Domain " + name + " does not exist", createFailure.getMessage());
        assertEquals(createFailure.getMessage(), alterFailure.getMessage());
      }
    }
    assertEquals(1, as("writer", () -> manager.getTerm(LAKE, "Churn")).version());
    assertThrows(
        NoSuchTermException.class, () -> as("lakeOwner", () -> manager.getTerm(LAKE, "Denied")));
  }

  /** Review focus: a content edit by a caller who cannot see a domain keeps that domain. */
  @TestTemplate
  void testContentEditKeepsHiddenDomains() throws Exception {
    GlossaryTermEntity term =
        as(
            "lakeOwner",
            () ->
                manager.createTerm(
                    LAKE, TermSpec.builder("Churn").withDomains("Sales", "Finance").build()));
    as("lakeOwner", () -> transfer(term, "writer"));
    as("writer", () -> manager.alterTerm(LAKE, "Churn", TermChange.updateDefinition("edited")));
    GlossaryTermEntity read = as("admin", () -> manager.getTerm(LAKE, "Churn"));
    assertEquals("edited", read.definition());
    assertArrayEquals(new String[] {"Finance", "Sales"}, read.domains());
  }

  /** A caller without edit access cannot probe which domains exist. */
  @TestTemplate
  void testViewerCannotProbeDomains() throws Exception {
    as("admin", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    assertThrows(
        ForbiddenException.class,
        () -> as("viewer", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("NoSuch"))));
  }

  /** T7.7: domain rows are locked after the term row, and a failed request changes nothing. */
  @TestTemplate
  void testMixedAlterLocksDomainsLast() throws Exception {
    GlossaryTermEntity parent =
        as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Parent").build()));
    GlossaryTermEntity before =
        as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Mixed").build()));
    assertThrows(
        NoSuchDomainException.class,
        () ->
            as(
                "lakeOwner",
                () ->
                    manager.alterTerm(
                        LAKE,
                        "Mixed",
                        TermChange.addBroader("Parent"),
                        TermChange.addDomain("NoSuch"),
                        TermChange.updateDefinition("lost"))));
    GlossaryTermEntity unchanged = as("lakeOwner", () -> manager.getTerm(LAKE, "Mixed"));
    assertEquals(before.revisionToken(), unchanged.revisionToken());
    assertEquals(before.definition(), unchanged.definition());
    assertArrayEquals(new String[0], unchanged.domains());
    assertTrue(
        as(
                "lakeOwner",
                () ->
                    manager.listRelations(
                        LAKE, "Mixed", GlossaryManager.Direction.BROADER, false, null))
            .isEmpty());

    long sales = domainId("Sales");
    List<Long> locks = new ArrayList<>();
    try (MockedStatic<SqlSessions> observed =
        Mockito.mockStatic(
            SqlSessions.class,
            call -> {
              Object mapper = call.callRealMethod();
              if (call.getMethod().getName().equals("getMapper")) {
                if (call.getArgument(0) == GlossaryTermMetaMapper.class) {
                  return Mockito.mock(
                      GlossaryTermMetaMapper.class,
                      lookup -> {
                        Object row = AdditionalAnswers.delegatesTo(mapper).answer(lookup);
                        if (lookup.getMethod().getName().equals("getForUpdate") && row != null) {
                          locks.add(((GlossaryTermPO) row).getTermId());
                        }
                        return row;
                      });
                }
                if (call.getArgument(0) == GlossaryTermRelMapper.class) {
                  return observeLock(GlossaryTermRelMapper.class, mapper, "lockTerm", 1, locks);
                }
                if (call.getArgument(0) == DomainMetaMapper.class) {
                  return observeLock(
                      DomainMetaMapper.class, mapper, "selectDomainMetaByIdForShare", 0, locks);
                }
              }
              return mapper;
            })) {
      as(
          "lakeOwner",
          () ->
              manager.alterTerm(
                  LAKE,
                  "Mixed",
                  TermChange.addBroader("Parent"),
                  TermChange.addDomain("Sales"),
                  TermChange.updateDefinition("mixed")));
      assertEquals(List.of(parent.id(), before.id(), sales), locks.stream().distinct().toList());
      locks.clear();
      GlossaryTermEntity created =
          as(
              "lakeOwner",
              () ->
                  manager.createTerm(
                      LAKE,
                      TermSpec.builder("New")
                          .withRelated("Mixed", "Parent")
                          .withDomains("Sales")
                          .build()));
      // The inserted term row already belongs to this transaction when edge persistence
      // locks the endpoints again. Check the first acquisition of each pre-existing row.
      assertEquals(
          List.of(parent.id(), before.id(), sales),
          locks.stream().filter(id -> !id.equals(created.id())).distinct().toList());
      locks.clear();
      assertThrows(
          TermAlreadyExistsException.class,
          () ->
              as(
                  "lakeOwner",
                  () ->
                      manager.createTerm(
                          LAKE,
                          TermSpec.builder("Mixed")
                              .withRelated("Parent")
                              .withDomains("Sales")
                              .build())));
      assertEquals(List.of(parent.id(), sales), locks.stream().distinct().toList());
      assertThrows(
          NoSuchDomainException.class,
          () ->
              as(
                  "lakeOwner",
                  () ->
                      manager.createTerm(
                          LAKE, TermSpec.builder("Mixed").withDomains("NoSuch").build())));
    }
    GlossaryTermEntity after = as("lakeOwner", () -> manager.getTerm(LAKE, "Mixed"));
    assertEquals("mixed", after.definition());
    assertEquals(2, after.version());
    assertArrayEquals(new String[] {"Sales"}, after.domains());
  }

  /** T7.3: the domain filter covers the domain and the subdomains the caller can see. */
  @TestTemplate
  void testDomainFilterIncludesVisibleSubdomains() throws Exception {
    create("A", "Sales");
    create("B", "Retail");
    create("C", "HiddenSub");
    create("D", "Finance");
    assertEquals(List.of("A", "B"), names(as("writer", () -> manager.listTerms(LAKE, "Sales"))));
    assertEquals(
        List.of("A", "B", "C"), names(as("lakeOwner", () -> manager.listTerms(LAKE, "Sales"))));
    assertEquals(List.of("B"), names(as("writer", () -> manager.listTerms(LAKE, "Retail"))));
    GlossaryManager filtered = manager.withTermVisibility(term -> !term.name().equals("B"));
    assertEquals(List.of("A"), names(as("writer", () -> filtered.listTerms(LAKE, "Sales"))));
  }

  /** T7.8: hidden domains never appear in returned terms, and reads change nothing. */
  @TestTemplate
  void testHiddenDomainsAreFilteredFromResults() throws Exception {
    GlossaryTermEntity both = create("Both", "Sales", "Finance");
    create("FinanceOnly", "Finance");
    as("lakeOwner", () -> manager.alterTerm(LAKE, "FinanceOnly", TermChange.addRelated("Both")));
    String[] sales = {"Sales"};
    as(
        "writer",
        () -> {
          assertArrayEquals(sales, manager.getTerm(LAKE, "Both").domains());
          for (GlossaryTermEntity term : manager.listTerms(LAKE)) {
            assertFalse(String.join(",", term.domains()).contains("Finance"), term.name());
          }
          assertArrayEquals(new String[0], manager.getTerm(LAKE, "FinanceOnly").domains());
          for (GlossaryTermEntity term :
              manager.listRelations(
                  LAKE, "FinanceOnly", GlossaryManager.Direction.RELATED, false, null)) {
            assertArrayEquals(sales, term.domains());
          }
          return null;
        });
    as("lakeOwner", () -> transfer(both, "writer"));
    GlossaryTermEntity altered =
        as("writer", () -> manager.alterTerm(LAKE, "Both", TermChange.updateDefinition("new")));
    assertArrayEquals(sales, altered.domains());
    GlossaryTermEntity stored = as("admin", () -> manager.getTerm(LAKE, "Both"));
    assertArrayEquals(new String[] {"Finance", "Sales"}, stored.domains());
    assertEquals(
        as("admin", () -> manager.getTerm(LAKE, "FinanceOnly")).revisionToken(),
        as("lakeOwner", () -> manager.getTerm(LAKE, "FinanceOnly")).revisionToken());
  }

  /** Review focus: membership follows the domain ID across a rename. */
  @TestTemplate
  void testDomainRenameFollowsId() throws Exception {
    GlossaryTermEntity created = create("Churn", "Sales");
    env.internalDomainDispatcher().alterDomain(LAKE, "Sales", DomainChange.rename("Revenue"));
    assertArrayEquals(
        new String[] {"Sales"},
        as("lakeOwner", () -> manager.getTermVersion(LAKE, created.id(), 1)).term().domains());
    assertArrayEquals(
        new String[] {"Revenue"}, as("lakeOwner", () -> manager.getTerm(LAKE, "Churn")).domains());
    assertThrows(
        NoSuchDomainException.class,
        () ->
            as("lakeOwner", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("Sales"))));
  }

  @TestTemplate
  void testViewerCannotSubmitPrivateDraftOrDistinguishVersionNumbers() throws Exception {
    GlossaryTermEntity live = create("published");
    GlossaryManager gated = new GlossaryManager(ids::incrementAndGet, true, lake -> true);
    GlossaryTermEntity draft =
        as(
            "lakeOwner",
            () ->
                gated.alterTerm(
                    LAKE, "published", TermChange.setStatus(GlossaryTerm.Status.DRAFT)));
    assertEquals(List.of(1L), as("viewer", () -> gated.listTermVersions(LAKE, live.id(), null, 1)));
    assertEquals(
        List.of(draft.publication().versionId()),
        as("lakeOwner", () -> gated.listTermVersions(LAKE, live.id(), null, 1)));
    assertEquals(
        List.of(1L),
        as(
            "lakeOwner",
            () -> gated.listTermVersions(LAKE, live.id(), draft.publication().versionId(), 1)));
    for (long version : List.of(1L, draft.publication().versionId(), 3L)) {
      assertThrows(
          NoSuchTermException.class,
          () -> as("viewer", () -> gated.submitDraft(LAKE, live.id(), version)));
    }
    assertEquals(
        TermPublication.State.DRAFT,
        as(
                "lakeOwner",
                () -> gated.getTermVersion(LAKE, live.id(), draft.publication().versionId()))
            .term()
            .publication()
            .state());
  }

  @TestTemplate
  void testRecreatedUserCannotInheritCandidate() throws Exception {
    TermPublicationPolicy policy =
        new TermPublicationPolicy() {
          public boolean requiresApproval(String lake) {
            return true;
          }

          public boolean canPublishOrRejectCandidate(String lake, long id, long version) {
            return true;
          }
        };
    GlossaryManager gated = new GlossaryManager(ids::incrementAndGet, true, policy);
    GlossaryTermEntity candidate =
        as("writer", () -> gated.createTerm(LAKE, TermSpec.builder("candidate").build()));
    backend.delete(NameIdentifierUtil.ofUser(LAKE, "writer"), Entity.EntityType.USER, false);
    user("writer", "create", "useSales");
    assertThrows(
        NoSuchTermException.class,
        () -> as("writer", () -> gated.getTermVersion(LAKE, candidate.id(), 1)));
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "writer",
                () ->
                    gated.alterTerm(
                        LAKE, "candidate", TermChange.updateDefinition("replacement"))));
    assertThrows(
        NoSuchUserException.class,
        () -> as("lakeOwner", () -> gated.publishCandidate(LAKE, candidate.id(), 1)));
    assertThrows(
        NoSuchTermException.class, () -> as("lakeOwner", () -> manager.getTerm(LAKE, "candidate")));
    as("lakeOwner", () -> gated.rejectCandidate(LAKE, candidate.id(), 1));
  }

  @TestTemplate
  void testCandidatePublicationHandlesRenamedAndDeletedDomains() throws Exception {
    TermPublicationPolicy policy =
        new TermPublicationPolicy() {
          @Override
          public boolean requiresApproval(String lake) {
            return true;
          }

          @Override
          public boolean canPublishOrRejectCandidate(String lake, long id, long version) {
            return true;
          }
        };
    List<String[]> observedAfterDomains = new ArrayList<>();
    GlossaryManager gated =
        new GlossaryManager(
            ids::incrementAndGet,
            true,
            (before, after, persist) -> {
              if (after != null) {
                observedAfterDomains.add(after.domains());
              }
              persist.run();
            },
            term -> true,
            GlossaryManager.MAX_HIERARCHY_DEPTH,
            policy);

    GlossaryTermEntity renamedCandidate =
        as(
            "lakeOwner",
            () ->
                gated.createTerm(
                    LAKE, TermSpec.builder("RenamedDomTerm").withDomains("Sales").build()));
    GlossaryTermEntity toRemove =
        as(
            "lakeOwner",
            () ->
                gated.createTerm(
                    LAKE, TermSpec.builder("removeDomain").withDomains("Sales").build()));
    env.internalDomainDispatcher().alterDomain(LAKE, "Sales", DomainChange.rename("Revenue"));
    as("lakeOwner", () -> gated.publishCandidate(LAKE, renamedCandidate.id(), 1));
    assertEquals(1, observedAfterDomains.size());
    assertArrayEquals(new String[] {"Revenue"}, observedAfterDomains.get(0));
    assertArrayEquals(
        new String[] {"Revenue"},
        as("lakeOwner", () -> manager.getTerm(LAKE, "RenamedDomTerm")).domains());

    GlossaryDomainFixtures.insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    GlossaryTermEntity removed =
        as(
            "lakeOwner",
            () -> gated.alterTerm(LAKE, "removeDomain", TermChange.removeDomain("Sales")));
    assertArrayEquals(new String[0], removed.domains());
    as(
        "lakeOwner",
        () -> gated.publishCandidate(LAKE, toRemove.id(), removed.publication().versionId()));
    assertArrayEquals(
        new String[0], as("lakeOwner", () -> manager.getTerm(LAKE, "removeDomain")).domains());
    GlossaryTermEntity deletedDomainCandidate =
        as(
            "lakeOwner",
            () ->
                gated.createTerm(
                    LAKE, TermSpec.builder("DeletedDomTerm").withDomains("Finance").build()));
    GlossaryTermEntity deletedDomainDraft =
        as(
            "lakeOwner",
            () ->
                gated.createTerm(
                    LAKE,
                    TermSpec.builder("DeletedDomDraft")
                        .withStatus(GlossaryTerm.Status.DRAFT)
                        .withDomains("Finance")
                        .build()));
    env.internalDomainDispatcher().deleteDomain(LAKE, "Finance");
    assertThrows(
        NoSuchDomainException.class,
        () -> as("lakeOwner", () -> gated.publishCandidate(LAKE, deletedDomainCandidate.id(), 1)));
    assertThrows(
        NoSuchDomainException.class,
        () ->
            as(
                "lakeOwner",
                () ->
                    gated.alterTerm(LAKE, "DeletedDomDraft", TermChange.updateDefinition("new"))));
    GlossaryTermEntity cleaned =
        as(
            "lakeOwner",
            () ->
                gated.alterTerm(
                    LAKE,
                    "DeletedDomDraft",
                    TermChange.removeDomain("Finance"),
                    TermChange.setStatus(GlossaryTerm.Status.DRAFT)));
    assertArrayEquals(new String[0], cleaned.domains());
    as(
        "lakeOwner",
        () -> gated.submitDraft(LAKE, deletedDomainDraft.id(), cleaned.publication().versionId()));
    as(
        "lakeOwner",
        () ->
            gated.publishCandidate(
                LAKE, deletedDomainDraft.id(), cleaned.publication().versionId() + 1));
    assertArrayEquals(
        new String[0], as("lakeOwner", () -> manager.getTerm(LAKE, "DeletedDomDraft")).domains());
  }

  @TestTemplate
  void testNonOwnerApproverPublishesHiddenDomainAndHidesRenamedDraftDomain() throws Exception {
    TermPublicationPolicy policy =
        new TermPublicationPolicy() {
          @Override
          public boolean requiresApproval(String lake) {
            return true;
          }

          @Override
          public boolean canPublishOrRejectCandidate(String lake, long id, long version) {
            return true;
          }
        };
    user("reviewer", "manage", "useSales");
    GlossaryManager gated = new GlossaryManager(ids::incrementAndGet, true, policy);

    // Content-only edit on a term with an unchanged hidden domain can be published by a
    // non-lakeOwner approver without access to Finance, preserving the Finance membership.
    GlossaryTermEntity term =
        as(
            "admin",
            () ->
                manager.createTerm(
                    LAKE, TermSpec.builder("HiddenDomainTerm").withDomains("Finance").build()));
    GlossaryTermEntity edit =
        as(
            "admin",
            () ->
                gated.alterTerm(LAKE, "HiddenDomainTerm", TermChange.updateDefinition("typo fix")));
    as("reviewer", () -> gated.publishCandidate(LAKE, term.id(), edit.publication().versionId()));
    GlossaryTermEntity published = as("admin", () -> manager.getTerm(LAKE, "HiddenDomainTerm"));
    assertEquals("typo fix", published.definition());
    assertArrayEquals(new String[] {"Finance"}, published.domains());

    // Publishing a candidate whose added domain was renamed to a hidden DRAFT domain must not
    // disclose the hidden new name.
    GlossaryTermEntity draftDomCand =
        as(
            "lakeOwner",
            () ->
                gated.createTerm(
                    LAKE, TermSpec.builder("RenamedDraftDom").withDomains("DraftX").build()));
    env.internalDomainDispatcher().alterDomain(LAKE, "DraftX", DomainChange.rename("SecretDraft"));
    NoSuchDomainException missing =
        assertThrows(
            NoSuchDomainException.class,
            () -> as("reviewer", () -> gated.publishCandidate(LAKE, draftDomCand.id(), 1)));
    assertTrue(missing.getMessage().contains("DraftX"));
    assertFalse(missing.getMessage().contains("SecretDraft"));
  }

  @TestTemplate
  void testOwnershipTransferUpdatesCandidateVisibilityAndSubmission() throws Exception {
    TermPublicationPolicy policy =
        new TermPublicationPolicy() {
          @Override
          public boolean requiresApproval(String lake) {
            return true;
          }

          @Override
          public boolean canPublishOrRejectCandidate(String lake, long id, long version) {
            return true;
          }
        };
    GlossaryManager gated = new GlossaryManager(ids::incrementAndGet, true, policy);

    GlossaryTermEntity owned =
        as("writer", () -> manager.createTerm(LAKE, TermSpec.builder("OwnedTerm").build()));
    GlossaryTermEntity draft =
        as(
            "writer",
            () ->
                gated.alterTerm(
                    LAKE,
                    "OwnedTerm",
                    TermChange.updateDefinition("draft edit"),
                    TermChange.setStatus(GlossaryTerm.Status.DRAFT)));
    as("lakeOwner", () -> transfer(owned, "writer2"));

    // Former owner can still read published history with candidateVersionId masked, but cannot
    // read, continue, or submit the private draft.
    TermPublication formerView =
        as("writer", () -> gated.getTermVersion(LAKE, owned.id(), 1)).term().publication();
    assertNull(formerView.candidateVersionId());
    assertEquals(List.of(1L), as("writer", () -> gated.listTermVersions(LAKE, owned.id())));
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "writer",
                () -> gated.getTermVersion(LAKE, owned.id(), draft.publication().versionId())));
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "writer",
                () -> gated.submitDraft(LAKE, owned.id(), draft.publication().versionId())));

    // New owner inherits draft visibility and can submit it for publication.
    assertEquals(
        List.of(draft.publication().versionId(), 1L),
        as("writer2", () -> gated.listTermVersions(LAKE, owned.id())));
    TermPublication submitted =
        as("writer2", () -> gated.submitDraft(LAKE, owned.id(), draft.publication().versionId()));
    assertEquals(TermPublication.State.SUBMITTED, submitted.state());
  }

  @TestTemplate
  void testAlterResponseFollowsRenamedMemberWhenOldNameIsReused() throws Exception {
    GlossaryTermEntity before =
        as(
            "writer",
            () ->
                manager.createTerm(LAKE, TermSpec.builder("Churn").withDomains("Retail").build()));
    GravitinoAuthorizer observed =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(authorizer));
    env.setGravitinoAuthorizer(observed);
    // Between the initial term read and the domain locks, the member is renamed and its old
    // name is reused by a different domain outside the writer's visible Sales hierarchy.
    Mockito.doAnswer(
            call -> {
              env.internalDomainDispatcher()
                  .alterDomain(LAKE, "Retail", DomainChange.rename("Stores"));
              GlossaryDomainFixtures.insertDomain(
                  LAKE, "Retail", Domain.Status.PUBLISHED, null, AUDIT_INFO);
              return AdditionalAnswers.delegatesTo(authorizer).answer(call);
            })
        .when(observed)
        .isGlossaryTermOwner(
            Mockito.any(), Mockito.eq(LAKE), Mockito.eq(before.id()), Mockito.any());
    GlossaryManager writer =
        new GlossaryManager(
            ids::incrementAndGet,
            true,
            (original, updated, persist) -> {
              assertArrayEquals(new String[] {"Stores"}, original.domains());
              assertArrayEquals(new String[] {"Sales", "Stores"}, updated.domains());
              persist.run();
            });
    GlossaryTermEntity edited =
        as(
            "writer",
            () ->
                writer.alterTerm(
                    LAKE,
                    "Churn",
                    TermChange.updateDefinition("changed"),
                    TermChange.addDomain("Sales")));
    assertEquals(before.version() + 1, edited.version());
    assertArrayEquals(new String[] {"Sales", "Stores"}, edited.domains());
    assertArrayEquals(
        edited.domains(), as("writer", () -> manager.getTerm(LAKE, "Churn")).domains());
  }

  @TestTemplate
  void testFilteredListChecksOwnershipOnlyForMembers() throws Exception {
    create("Included", "Sales");
    GlossaryTermEntity outside =
        as(
            "lakeOwner",
            () ->
                manager.createTerm(
                    LAKE,
                    TermSpec.builder("Outside")
                        .withStatus(GlossaryTerm.Status.DRAFT)
                        .withDomains("Finance")
                        .build()));
    GravitinoAuthorizer observed =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(authorizer));
    env.setGravitinoAuthorizer(observed);
    assertEquals(List.of("Included"), names(as("writer", () -> manager.listTerms(LAKE, "Sales"))));
    Mockito.verify(observed, Mockito.never())
        .isGlossaryTermOwner(
            Mockito.any(), Mockito.eq(LAKE), Mockito.eq(outside.id()), Mockito.any());
  }

  /** T7.6: a matching backing-tag assignment has no effect on explicit term membership. */
  @TestTemplate
  void testBackingTagAssignmentDoesNotChangeTermMembership() throws Exception {
    long tag = createAndInsertTagEntity("classification", "Domain backing tag", LAKE).id();
    env.internalDomainDispatcher()
        .createDomain(LAKE, "Tagged", null, null, TagValue.noValue("classification"), false);
    GlossaryTermEntity term = create("Explicit", "Sales");
    String type = MetadataObject.Type.GLOSSARY_TERM.name();
    as(
        "lakeOwner",
        () -> {
          SessionUtils.doWithCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.batchInsertTagMetadataObjectRels(
                      List.of(
                          POConverters.initializeTagMetadataObjectRelPOWithVersion(
                              tag, term.id(), type))));
          assertTrue(manager.listTerms(LAKE, "Tagged").isEmpty());
          assertArrayEquals(new String[] {"Sales"}, manager.getTerm(LAKE, term.name()).domains());
          manager.alterTerm(LAKE, term.name(), TermChange.addDomain("Tagged"));
          SessionUtils.doWithCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
                      term.id(), type, List.of(tag)));
          assertArrayEquals(
              new String[] {"Sales", "Tagged"}, manager.getTerm(LAKE, term.name()).domains());
          return null;
        });
  }

  /** Domain REST and glossary use the same grant checks even without the generic metadata SPI. */
  @TestTemplate
  void testCustomAuthorizerDomainVisibility() throws Exception {
    Mockito.when(env.config().get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
    GravitinoAuthorizer custom =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(authorizer));
    Mockito.doCallRealMethod()
        .when(custom)
        .canAccessDomain(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any());
    env.setGravitinoAuthorizer(custom);
    as(
        "writer",
        () -> {
          DomainAuthzHelper.requireVisible(env.internalDomainDispatcher(), LAKE, "Retail");
          assertArrayEquals(
              new String[] {"Retail"},
              manager
                  .createTerm(LAKE, TermSpec.builder("Scoped").withDomains("Retail").build())
                  .domains());
          assertThrows(
              ForbiddenException.class,
              () ->
                  DomainAuthzHelper.requireVisible(
                      env.internalDomainDispatcher(), LAKE, "Finance"));
          assertThrows(
              ForbiddenException.class,
              () -> manager.alterTerm(LAKE, "Scoped", TermChange.addDomain("Finance")));
          return null;
        });
  }

  @TestTemplate
  void testDomainFilterOnHiddenDomain() throws Exception {
    for (String hidden : List.of("DraftX", "NoSuch", "Sales ")) {
      assertThrows(
          NoSuchDomainException.class, () -> as("writer", () -> manager.listTerms(LAKE, hidden)));
    }
    assertThrows(
        ForbiddenException.class, () -> as("writer", () -> manager.listTerms(LAKE, "Finance")));
  }

  /** Covers T7.9. */
  @TestTemplate
  void testDomainAdminCannotClearHiddenDraft() throws Exception {
    Mockito.when(env.config().get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
    DomainDispatcher domains = DomainAuthzHelper.managed(env.internalDomainDispatcher(), LAKE);
    GlossaryTermEntity draft =
        as(
            "writer",
            () ->
                manager.createTerm(
                    LAKE,
                    TermSpec.builder("PrivateDraft")
                        .withDefinition("original")
                        .withStatus(GlossaryTerm.Status.DRAFT)
                        .withDomains("Retail")
                        .build()));
    assertThrows(
        ForbiddenException.class, () -> as("viewer", () -> domains.deleteDomain(LAKE, "Retail")));
    NonEmptyDomainException error =
        assertThrows(
            NonEmptyDomainException.class,
            () -> as("domainAdmin", () -> domains.deleteDomain(LAKE, "Retail")));
    assertEquals(
        "Domain Retail under metalake glossary contains definition members. Remove their memberships first."
            + " For glossary terms, ask the term owner with CREATE_DEFINITION or a glossary manager"
            + " with MANAGE_DEFINITIONS to remove the membership. Both roles require access to this domain.",
        error.getMessage());
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "domainAdmin",
                () -> manager.alterTerm(LAKE, draft.name(), TermChange.removeDomain("Retail"))));
    GlossaryTermEntity before = as("writer", () -> manager.getTerm(LAKE, draft.name()));
    assertEquals(draft.revisionToken(), before.revisionToken());
    assertArrayEquals(new String[] {"Retail"}, before.domains());
    as("writer", () -> manager.alterTerm(LAKE, draft.name(), TermChange.removeDomain("Retail")));
    assertTrue(as("domainAdmin", () -> domains.deleteDomain(LAKE, "Retail")));
    GlossaryTermEntity after = as("writer", () -> manager.getTerm(LAKE, draft.name()));
    assertEquals(draft.id(), after.id());
    assertEquals("original", after.definition());
    assertArrayEquals(new String[0], after.domains());
  }

  @TestTemplate
  void testRecreatedTermDoesNotInheritDomains() throws Exception {
    as(
        "admin",
        () -> manager.createTerm(LAKE, TermSpec.builder("Churn").withDomains("Finance").build()));
    assertTrue(as("admin", () -> manager.dropTerm(LAKE, "Churn")));
    as("admin", () -> manager.createTerm(LAKE, TermSpec.builder("Churn").build()));
    assertArrayEquals(new String[0], as("admin", () -> manager.getTerm(LAKE, "Churn")).domains());
    assertTrue(as("lakeOwner", () -> env.internalDomainDispatcher().deleteDomain(LAKE, "Finance")));
  }

  /** Covers T7.4. */
  @TestTemplate
  void testDropDomainWithTermsFails() throws Exception {
    create("Churn", "Finance");
    assertThrows(
        NonEmptyDomainException.class,
        () -> env.internalDomainDispatcher().deleteDomain(LAKE, "Finance"));
    as("lakeOwner", () -> manager.alterTerm(LAKE, "Churn", TermChange.removeDomain("Finance")));
    assertTrue(env.internalDomainDispatcher().deleteDomain(LAKE, "Finance"));
  }

  /** Covers T7.5. */
  @TestTemplate
  void testAddDomainRacesDomainDrop() {
    assertTimeoutPreemptively(
        Duration.ofMinutes(3),
        () -> {
          for (int round = 0; round < 20; round++) {
            runDomainDropRace(round);
          }
        });
  }

  private void runDomainDropRace(int round) throws Exception {
    boolean dropFirst = round % 2 == 0;
    String domain = (dropFirst ? "DropFirst" : "AddFirst") + round;
    long domainId =
        GlossaryDomainFixtures.insertDomain(
            LAKE, domain, Domain.Status.PUBLISHED, null, AUDIT_INFO);
    assertTrue(canLockDomain(domainId));
    GlossaryTermEntity before = create(domain);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    ExecutorService workers = Executors.newFixedThreadPool(2);
    Runnable pause =
        () -> {
          locked.countDown();
          await(release);
        };
    GlossaryManager writer =
        new GlossaryManager(
            ids::incrementAndGet,
            true,
            (oldTerm, newTerm, persist) -> {
              if (!dropFirst) pause.run();
              persist.run();
            });
    DomainDispatcher dropper =
        env.internalDomainDispatcher()
            .withMutationCheck(
                state -> {
                  if (dropFirst) pause.run();
                });
    try {
      Future<?> first =
          workers.submit(
              () ->
                  as(
                      "lakeOwner",
                      () -> {
                        if (dropFirst) assertTrue(dropper.deleteDomain(LAKE, domain));
                        else writer.alterTerm(LAKE, before.name(), TermChange.addDomain(domain));
                        return null;
                      }));
      await(locked);
      Future<?> second =
          workers.submit(
              () ->
                  as(
                      "lakeOwner",
                      () -> {
                        started.countDown();
                        if (dropFirst)
                          assertThrows(
                              NoSuchDomainException.class,
                              () ->
                                  writer.alterTerm(
                                      LAKE, before.name(), TermChange.addDomain(domain)));
                        else
                          assertThrows(
                              NonEmptyDomainException.class,
                              () -> dropper.deleteDomain(LAKE, domain));
                        return null;
                      }));
      await(started);
      // An independent session touches only the domain row. The metalake fence cannot
      // satisfy this assertion, even on H2 where its shared lock is exclusive.
      assertFalse(canLockDomain(domainId));
      release.countDown();
      first.get(20, TimeUnit.SECONDS);
      second.get(20, TimeUnit.SECONDS);
      GlossaryTermEntity after = as("lakeOwner", () -> manager.getTerm(LAKE, before.name()));
      if (dropFirst) {
        assertArrayEquals(new String[0], after.domains());
        assertEquals(before.revisionToken(), after.revisionToken());
      } else {
        assertArrayEquals(new String[] {domain}, after.domains());
        assertEquals(before.version() + 1, after.version());
      }
      GlossaryInvariants.assertDomainMembership();
    } finally {
      release.countDown();
      workers.shutdownNow();
    }
  }

  private boolean canLockDomain(long id) throws SQLException {
    try (SqlSession session =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession()) {
      Connection connection = session.getConnection();
      try {
        if ("mysql".equalsIgnoreCase(backendType)) {
          // MySQL 5.7 does not support SKIP LOCKED. Restore the pooled session's timeout.
          try (Statement settings = connection.createStatement()) {
            int previous;
            try (ResultSet value =
                settings.executeQuery("SELECT @@session.innodb_lock_wait_timeout")) {
              assertTrue(value.next());
              previous = value.getInt(1);
            }
            settings.execute("SET SESSION innodb_lock_wait_timeout=1");
            try {
              return lockDomainRow(connection, id, false);
            } catch (SQLException e) {
              if (e.getErrorCode() == 1205) return false;
              throw e;
            } finally {
              settings.execute("SET SESSION innodb_lock_wait_timeout=" + previous);
            }
          }
        }
        return lockDomainRow(connection, id, true);
      } finally {
        session.rollback(true);
      }
    }
  }

  private static boolean lockDomainRow(Connection connection, long id, boolean skipLocked)
      throws SQLException {
    String sql = "SELECT domain_id FROM domain_meta WHERE domain_id=? FOR UPDATE";
    try (PreparedStatement query =
        connection.prepareStatement(skipLocked ? sql + " SKIP LOCKED" : sql)) {
      query.setLong(1, id);
      try (ResultSet rows = query.executeQuery()) {
        return rows.next();
      }
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(15, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private GlossaryTermEntity create(String name, String... domains) throws Exception {
    return as(
        "lakeOwner",
        () -> manager.createTerm(LAKE, TermSpec.builder(name).withDomains(domains).build()));
  }

  private static List<String> names(List<GlossaryTermEntity> terms) {
    return terms.stream().map(GlossaryTermEntity::name).sorted().toList();
  }

  private static <T> MockedStatic<SqlSessions> observeMapper(
      Class<T> type, Consumer<InvocationOnMock> onCall) {
    return Mockito.mockStatic(
        SqlSessions.class,
        lookup -> {
          Object mapper = lookup.callRealMethod();
          if (lookup.getMethod().getName().equals("getMapper") && lookup.getArgument(0) == type) {
            return Mockito.mock(
                type,
                call -> {
                  onCall.accept(call);
                  return AdditionalAnswers.delegatesTo(mapper).answer(call);
                });
          }
          return mapper;
        });
  }

  private static <T> T observeLock(
      Class<T> type, Object mapper, String method, int idArgument, List<Long> locks) {
    return Mockito.mock(
        type,
        call -> {
          Object result = AdditionalAnswers.delegatesTo(mapper).answer(call);
          if (call.getMethod().getName().equals(method)) {
            locks.add(call.getArgument(idArgument, Long.class));
          }
          return result;
        });
  }

  private static TermChange change(boolean add, String domain) {
    return add ? TermChange.addDomain(domain) : TermChange.removeDomain(domain);
  }

  private long domainId(String name) throws Exception {
    return store
        .get(NameIdentifierUtil.ofDomain(LAKE, name), Entity.EntityType.DOMAIN, DomainEntity.class)
        .id();
  }

  private Void transfer(GlossaryTermEntity term, String owner) {
    owners.setOwner(
        LAKE,
        MetadataObjects.of(null, term.name(), MetadataObject.Type.GLOSSARY_TERM),
        owner,
        Owner.Type.USER);
    return null;
  }

  private void metalakeRole(String name, Privilege privilege) throws Exception {
    role(name, SecurableObjects.ofMetalake(LAKE, List.of(privilege)));
  }

  private void domainRole(String name, String domain) throws Exception {
    role(
        name,
        SecurableObjects.parse(
            domain, MetadataObject.Type.DOMAIN, List.of(Privileges.UseDomain.allow())));
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
    List<Long> roleIds = new ArrayList<>();
    for (String role : roles) {
      roleIds.add(backend.get(NameIdentifierUtil.ofRole(LAKE, role), Entity.EntityType.ROLE).id());
    }
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

  private static <T> T as(String user, PrivilegedExceptionAction<T> work) throws Exception {
    return PrincipalUtils.doAs(new UserPrincipal(user), work);
  }
}
