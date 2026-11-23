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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.GlossaryDomainFixtures;
import org.apache.gravitino.storage.relational.service.GlossaryInvariants;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.RaceTestUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

/** Runs glossary domain membership against real grant, domain and term rows (T7). */
class TestGlossaryDomains extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private static final String NON_EMPTY =
      "Domain %s under metalake %s still has glossary terms. Ask a term owner with"
          + " CREATE_DEFINITION, or a user with MANAGE_DEFINITIONS, who can use the domain to"
          + " remove the terms from it first";
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
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(false);
        Connection holder = session.getConnection()) {
      holder.setAutoCommit(false);
      try (PreparedStatement lock =
          holder.prepareStatement(
              "SELECT domain_id FROM domain_meta WHERE domain_id=? FOR UPDATE")) {
        lock.setLong(1, sales);
        lock.executeQuery().close();
      }
      Future<?> mixed =
          pool.submit(
              () ->
                  as(
                      "lakeOwner",
                      () ->
                          manager.alterTerm(
                              LAKE,
                              "Mixed",
                              TermChange.addBroader("Parent"),
                              TermChange.addDomain("Sales"),
                              TermChange.updateDefinition("mixed"))));
      assertThrows(TimeoutException.class, () -> mixed.get(300, TimeUnit.MILLISECONDS));
      Future<?> content =
          pool.submit(
              () ->
                  as(
                      "lakeOwner",
                      () -> manager.alterTerm(LAKE, "Mixed", TermChange.updateDefinition("last"))));
      // The waiting mixed request already holds the term row, so the content edit queues behind it.
      assertThrows(TimeoutException.class, () -> content.get(300, TimeUnit.MILLISECONDS));
      holder.rollback();
      mixed.get(20, TimeUnit.SECONDS);
      content.get(20, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    GlossaryTermEntity after = as("lakeOwner", () -> manager.getTerm(LAKE, "Mixed"));
    assertEquals("last", after.definition());
    assertEquals(3, after.version());
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
    create("Churn", "Sales");
    env.internalDomainDispatcher().alterDomain(LAKE, "Sales", DomainChange.rename("Revenue"));
    assertArrayEquals(
        new String[] {"Revenue"}, as("lakeOwner", () -> manager.getTerm(LAKE, "Churn")).domains());
    assertThrows(
        NoSuchDomainException.class,
        () ->
            as("lakeOwner", () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain("Sales"))));
  }

  @TestTemplate
  void testDomainFilterOnHiddenDomain() throws Exception {
    for (String hidden : List.of("DraftX", "NoSuch")) {
      assertThrows(
          NoSuchDomainException.class, () -> as("writer", () -> manager.listTerms(LAKE, hidden)));
    }
    assertThrows(
        ForbiddenException.class, () -> as("writer", () -> manager.listTerms(LAKE, "Finance")));
  }

  /** T7.4. */
  @TestTemplate
  void testDropDomainWithTermsFails() throws Exception {
    create("Churn", "Finance");
    NonEmptyDomainException error =
        assertThrows(
            NonEmptyDomainException.class,
            () -> env.internalDomainDispatcher().deleteDomain(LAKE, "Finance"));
    assertEquals(String.format(NON_EMPTY, "Finance", LAKE), error.getMessage());
    as("lakeOwner", () -> manager.alterTerm(LAKE, "Churn", TermChange.removeDomain("Finance")));
    assertTrue(env.internalDomainDispatcher().deleteDomain(LAKE, "Finance"));
  }

  /** T7.5: either the add wins and the drop fails, or the drop wins and the add fails. */
  @TestTemplate
  void testAddDomainRacesDomainDrop() throws Exception {
    create("Churn");
    for (int round = 0; round < 10; round++) {
      String domain = "Race" + round;
      GlossaryDomainFixtures.insertDomain(LAKE, domain, Domain.Status.PUBLISHED, null, AUDIT_INFO);
      List<Object> outcomes =
          RaceTestUtils.runTogether(
              List.of(
                  () -> {
                    try {
                      as(
                          "lakeOwner",
                          () -> manager.alterTerm(LAKE, "Churn", TermChange.addDomain(domain)));
                      return "added";
                    } catch (NoSuchDomainException e) {
                      return "missing";
                    }
                  },
                  () -> {
                    try {
                      return env.internalDomainDispatcher().deleteDomain(LAKE, domain)
                          ? "dropped"
                          : "absent";
                    } catch (NonEmptyDomainException e) {
                      return "nonEmpty";
                    }
                  }));
      assertTrue(
          outcomes.equals(List.of("added", "nonEmpty"))
              || outcomes.equals(List.of("missing", "dropped")),
          domain + " " + outcomes);
      GlossaryInvariants.assertDomainMembership();
      if (outcomes.get(0).equals("added")) {
        as("lakeOwner", () -> manager.alterTerm(LAKE, "Churn", TermChange.removeDomain(domain)));
      }
    }
  }

  /** T7.9: a domain admin without glossary rights cannot clear a hidden draft. */
  @TestTemplate
  void testDomainAdminCannotClearHiddenDraft() throws Exception {
    as(
        "writer",
        () ->
            manager.createTerm(
                LAKE,
                TermSpec.builder("Secret")
                    .withStatus(GlossaryTerm.Status.DRAFT)
                    .withDefinition("kept")
                    .withDomains("Retail")
                    .build()));
    NonEmptyDomainException error =
        assertThrows(
            NonEmptyDomainException.class,
            () ->
                as(
                    "domainAdmin",
                    () -> env.internalDomainDispatcher().deleteDomain(LAKE, "Retail")));
    assertEquals(String.format(NON_EMPTY, "Retail", LAKE), error.getMessage());
    assertFalse(error.getMessage().contains("Secret"));
    assertFalse(error.getMessage().matches(".*\\d.*"));
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "domainAdmin",
                () -> manager.alterTerm(LAKE, "Secret", TermChange.removeDomain("Retail"))));
    as("writer", () -> manager.alterTerm(LAKE, "Secret", TermChange.removeDomain("Retail")));
    assertTrue(
        as("domainAdmin", () -> env.internalDomainDispatcher().deleteDomain(LAKE, "Retail")));
    GlossaryTermEntity secret = as("writer", () -> manager.getTerm(LAKE, "Secret"));
    assertEquals("kept", secret.definition());
    assertEquals(GlossaryTerm.Status.DRAFT, secret.status());
  }

  /** Review focus: a recreated term with the same name starts without domains. */
  @TestTemplate
  void testRecreatedTermDoesNotInheritDomains() throws Exception {
    create("Churn", "Finance");
    assertTrue(as("admin", () -> manager.dropTerm(LAKE, "Churn")));
    assertArrayEquals(new String[0], create("Churn").domains());
    assertTrue(env.internalDomainDispatcher().deleteDomain(LAKE, "Finance"));
  }

  private GlossaryTermEntity create(String name, String... domains) throws Exception {
    return as(
        "lakeOwner",
        () -> manager.createTerm(LAKE, TermSpec.builder(name).withDomains(domains).build()));
  }

  private static List<String> names(List<GlossaryTermEntity> terms) {
    return terms.stream().map(GlossaryTermEntity::name).sorted().toList();
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
