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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.dto.glossary.TermBindingDTO;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.dto.responses.ObjectTermsResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.LookupTooBroadException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermBindingSQLProviderFactory;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryDomainFixtures;
import org.apache.gravitino.storage.relational.service.GlossaryInvariants;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.BindingLossAuditRecord;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.CleanupAuditListener;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.LostBinding;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.TargetType;
import org.apache.gravitino.storage.relational.service.GlossaryTermMetaService;
import org.apache.gravitino.storage.relational.service.MetadataObjectService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.SemanticModelMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.ViewMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.apache.ibatis.session.SqlSession;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Exercises glossary term bindings, authorization, cleanup, and concurrency in core (T11). */
class TestGlossaryBindings extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private static final String CAT = "dw";
  private static final String SCH = "sales";
  private static final String TBL = "customers";
  private static final String VIEW = "active_customers_view";
  private static final String FILESET = "raw_files";
  private static final String TOPIC = "events";
  private static final String MODEL = "churn_model";
  private static final String SMODEL = "sales_model";

  private final AtomicLong ids = new AtomicLong(1000);
  private final GlossaryManager manager = new GlossaryManager(ids::incrementAndGet, true);
  private final GlossaryManager openManager = new GlossaryManager(ids::incrementAndGet, false);
  private final GravitinoEnv env = GravitinoEnv.getInstance();
  private final List<BindingLossAuditRecord> auditRecords = new CopyOnWriteArrayList<>();
  private final CleanupAuditListener auditListener = auditRecords::add;

  private GravitinoAuthorizer previousAuthorizer;
  private EntityStore previousStore;
  private CatalogManager previousCatalogs;
  private TableDispatcher previousTableDispatcher;
  private IdGenerator previousIdGenerator;
  private JcasbinAuthorizer authorizer;
  private Namespace schNs;

  @BeforeEach
  void setUpBindings() throws Exception {
    auditRecords.clear();
    GlossaryTermBindingService.getInstance().registerCleanupAuditListener(auditListener);
    previousAuthorizer = env.gravitinoAuthorizer();
    previousStore = (EntityStore) FieldUtils.readField(env, "entityStore", true);
    previousCatalogs = (CatalogManager) FieldUtils.readField(env, "catalogManager", true);
    previousIdGenerator = env.idGenerator();
    previousTableDispatcher =
        (TableDispatcher) FieldUtils.readField(env, "internalTableDispatcher", true);
    FieldUtils.writeField(
        env, "internalTableDispatcher", Mockito.mock(TableDispatcher.class), true);
    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> mockCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(mockCatalog.capability()).thenReturn(Capability.DEFAULT);
    Mockito.when(mockCatalogManager.doWithCatalog(Mockito.any(), Mockito.any()))
        .thenAnswer(
            inv -> {
              ThrowableFunction<BaseCatalog<?>, Object> fn = inv.getArgument(1);
              return fn.apply(mockCatalog);
            });
    FieldUtils.writeField(env, "catalogManager", mockCatalogManager, true);
    FieldUtils.writeField(env, "idGenerator", (IdGenerator) ids::incrementAndGet, true);
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
    FieldUtils.writeField(
        env,
        "entityStore",
        Mockito.mock(EntityStore.class, AdditionalAnswers.delegatesTo(store)),
        true);

    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, CAT);
    createAndInsertSchema(LAKE, CAT, SCH);
    schNs = Namespace.of(LAKE, CAT, SCH);
    insertTable(TBL, List.of(col("customer_id", 0), col("is_active", 1), col("email", 2)));
    backend.insert(createViewEntity(ids.incrementAndGet(), schNs, VIEW), false);
    backend.insert(createFilesetEntity(ids.incrementAndGet(), schNs, FILESET, AUDIT_INFO), false);
    backend.insert(createTopicEntity(ids.incrementAndGet(), schNs, TOPIC, AUDIT_INFO), false);
    backend.insert(
        createModelEntity(ids.incrementAndGet(), schNs, MODEL, "model", 0, Map.of(), AUDIT_INFO),
        false);
    SemanticModelMetaService.getInstance()
        .insertSemanticModel(
            semanticModel(ids.incrementAndGet(), SMODEL, List.of("net_revenue", "active_users")),
            false);

    // Roles and users for authorization tests.
    role(
        "viewDef", SecurableObjects.ofMetalake(LAKE, List.of(Privileges.allow("VIEW_DEFINITION"))));
    role(
        "createDef",
        SecurableObjects.ofMetalake(LAKE, List.of(Privileges.allow("CREATE_DEFINITION"))));
    role(
        "manageDef",
        SecurableObjects.ofMetalake(LAKE, List.of(Privileges.allow("MANAGE_DEFINITIONS"))));
    role(
        "readTable",
        SecurableObjects.ofMetalake(
            LAKE,
            List.of(
                Privileges.allow("USE_CATALOG"),
                Privileges.allow("USE_SCHEMA"),
                Privileges.allow("SELECT_TABLE"))));
    role(
        "modifyTable",
        SecurableObjects.ofMetalake(
            LAKE,
            List.of(
                Privileges.allow("USE_CATALOG"),
                Privileges.allow("USE_SCHEMA"),
                Privileges.allow("MODIFY_TABLE"))));
    role(
        "readModel",
        SecurableObjects.ofMetalake(
            LAKE,
            List.of(
                Privileges.allow("USE_CATALOG"),
                Privileges.allow("USE_SCHEMA"),
                Privileges.allow("USE_SEMANTIC_MODEL"))));
    role(
        "modifyModel",
        SecurableObjects.ofMetalake(
            LAKE,
            List.of(
                Privileges.allow("USE_CATALOG"),
                Privileges.allow("USE_SCHEMA"),
                Privileges.allow("MODIFY_SEMANTIC_MODEL"))));

    user("admin", "manageDef", "modifyTable", "modifyModel");
    user("glossaryOnlyAdmin", "manageDef");
    user("writer", "createDef", "readTable", "readModel");
    user("writerAndTableAdmin", "createDef", "modifyTable");
    user("tableAdmin", "viewDef", "modifyTable", "modifyModel");
    user("viewerWithTableRead", "viewDef", "readTable");
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
  void tearDownBindings() throws Exception {
    GlossaryTermBindingService.getInstance().unregisterCleanupAuditListener(auditListener);
    try {
      GlossaryInvariants.assertDefinitions();
      GlossaryInvariants.assertBindings();
    } finally {
      if (authorizer != null) {
        authorizer.close();
      }
      env.setGravitinoAuthorizer(previousAuthorizer);
      FieldUtils.writeField(env, "internalTableDispatcher", previousTableDispatcher, true);
      FieldUtils.writeField(env, "entityStore", previousStore, true);
      FieldUtils.writeField(env, "catalogManager", previousCatalogs, true);
      FieldUtils.writeField(env, "idGenerator", previousIdGenerator, true);
    }
  }

  /**
   * Covers T11.1, T11.2, T11.6: bind and unbind on all 8 object types and a metric; reject
   * unsupported types with 400 and missing targets with 404; duplicate bind is a no-op with no
   * extra revision increment.
   */
  @TestTemplate
  void testBindUnbindAllTargetTypesValidationAndIdempotency() throws Exception {
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Customer").build()));

    List<MetadataObject> targets =
        List.of(
            MetadataObjects.of(null, CAT, MetadataObject.Type.CATALOG),
            MetadataObjects.of(CAT, SCH, MetadataObject.Type.SCHEMA),
            MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE),
            MetadataObjects.of(List.of(CAT, SCH, VIEW), MetadataObject.Type.VIEW),
            MetadataObjects.of(List.of(CAT, SCH, TBL, "is_active"), MetadataObject.Type.COLUMN),
            MetadataObjects.of(List.of(CAT, SCH, FILESET), MetadataObject.Type.FILESET),
            MetadataObjects.of(List.of(CAT, SCH, TOPIC), MetadataObject.Type.TOPIC),
            MetadataObjects.of(List.of(CAT, SCH, MODEL), MetadataObject.Type.MODEL));

    long expectedVersion = 1L;
    for (MetadataObject target : targets) {
      String[] bound =
          as(
              "lakeOwner",
              () -> manager.associateTerms(LAKE, target, new String[] {"Customer"}, null));
      assertArrayEquals(new String[] {"Customer"}, bound);
      expectedVersion++;
      assertEquals(
          expectedVersion, as("lakeOwner", () -> manager.getTerm(LAKE, "Customer")).version());

      // T11.6: binding the same term to the same object twice keeps one row and adds no revision.
      String[] again =
          as(
              "lakeOwner",
              () -> manager.associateTerms(LAKE, target, new String[] {"Customer"}, null));
      assertArrayEquals(new String[] {"Customer"}, again);
      assertEquals(
          expectedVersion, as("lakeOwner", () -> manager.getTerm(LAKE, "Customer")).version());
    }

    // Bind to a metric.
    MetadataObject smObj =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    String[] metricBound =
        as(
            "lakeOwner",
            () ->
                manager.associateMetricTerms(
                    LAKE, smObj, "net_revenue", new String[] {"Customer"}, null));
    assertArrayEquals(new String[] {"Customer"}, metricBound);
    expectedVersion++;
    assertEquals(
        expectedVersion, as("lakeOwner", () -> manager.getTerm(LAKE, "Customer")).version());

    List<TermBinding> allBindings =
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "Customer", null, null, null));
    assertEquals(9, allBindings.size());
    assertEquals(
        8,
        as(
                "lakeOwner",
                () ->
                    manager.listTermBindings(
                        LAKE, "Customer", TermBinding.Kind.IMPLEMENTED_BY, null, null))
            .size());
    assertEquals(
        1,
        as(
                "lakeOwner",
                () ->
                    manager.listTermBindings(
                        LAKE, "Customer", TermBinding.Kind.USED_BY, null, null))
            .size());

    // Unbind all targets and the metric.
    for (MetadataObject target : targets) {
      String[] remaining =
          as(
              "lakeOwner",
              () -> manager.associateTerms(LAKE, target, null, new String[] {"Customer"}));
      assertArrayEquals(new String[0], remaining);
      expectedVersion++;
    }
    assertArrayEquals(
        new String[0],
        as(
            "lakeOwner",
            () ->
                manager.associateMetricTerms(
                    LAKE, smObj, "net_revenue", null, new String[] {"Customer"})));
    expectedVersion++;
    assertEquals(
        expectedVersion, as("lakeOwner", () -> manager.getTerm(LAKE, "Customer")).version());
    assertTrue(
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "Customer", null, null, null))
            .isEmpty());

    // T11.2: unsupported object types fail with IllegalArgumentException (400).
    for (MetadataObject unsupported :
        List.of(
            MetadataObjects.of(null, "pii", MetadataObject.Type.TAG),
            MetadataObjects.of(null, "admin", MetadataObject.Type.ROLE),
            MetadataObjects.of(null, "Customer", MetadataObject.Type.GLOSSARY_TERM))) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              as(
                  "lakeOwner",
                  () ->
                      manager.associateTerms(LAKE, unsupported, new String[] {"Customer"}, null)));
    }

    // T11.2: missing object or missing metric fails with NoSuchMetadataObjectException (404).
    MetadataObject missingTable =
        MetadataObjects.of(List.of(CAT, SCH, "no_such_table"), MetadataObject.Type.TABLE);
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            as(
                "lakeOwner",
                () -> manager.associateTerms(LAKE, missingTable, new String[] {"Customer"}, null)));
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            as(
                "lakeOwner",
                () ->
                    manager.associateMetricTerms(
                        LAKE, smObj, "no_such_metric", new String[] {"Customer"}, null)));
    assertEquals(
        expectedVersion, as("lakeOwner", () -> manager.getTerm(LAKE, "Customer")).version());
  }

  @TestTemplate
  void testBindingsFollowTermIdentityAcrossDefinitionVersionsAndRespectApproval() {
    AtomicBoolean approval = new AtomicBoolean();
    GlossaryManager glossary =
        new GlossaryManager(ids::incrementAndGet, false, lake -> approval.get());
    GlossaryTermEntity term = glossary.createTerm(LAKE, TermSpec.builder("Customer").build());
    MetadataObject table = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject model =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    String[] names = {"Customer"};
    glossary.associateTerms(LAKE, table, names, null);
    assertEquals(term.version() + 1, glossary.getTerm(LAKE, "Customer").version());
    assertEquals(List.of(1L), glossary.listTermVersions(LAKE, term.id()));
    glossary.alterTerm(LAKE, "Customer", TermChange.updateDefinition("published"));
    assertEquals(List.of(2L, 1L), glossary.listTermVersions(LAKE, term.id()));
    assertEquals(term.id(), glossary.listObjectTerms(LAKE, table, null).get(0).id());

    approval.set(true);
    GlossaryTermEntity candidate =
        glossary.alterTerm(LAKE, "Customer", TermChange.updateDefinition("proposed"));
    long revision = glossary.getTerm(LAKE, "Customer").version();
    assertThrows(ForbiddenException.class, () -> glossary.associateTerms(LAKE, table, null, names));
    assertThrows(
        ForbiddenException.class,
        () -> glossary.associateMetricTerms(LAKE, model, "net_revenue", names, null));
    assertArrayEquals(names, glossary.associateTerms(LAKE, table, names, null));
    assertTrue(glossary.listMetricTerms(LAKE, model, "net_revenue", null).isEmpty());
    assertEquals(revision, glossary.getTerm(LAKE, "Customer").version());
    assertEquals("published", glossary.getTerm(LAKE, "Customer").definition());
    assertEquals(
        TermPublication.State.SUBMITTED,
        glossary
            .getTermVersion(LAKE, term.id(), candidate.publication().versionId())
            .term()
            .publication()
            .state());
  }

  /**
   * Covers T11.3 (guard), T11.4, T11.5 (guard): two-path authorization, status check order, and
   * all-or-nothing atomicity across multiple terms in one request.
   */
  @TestTemplate
  void testTwoPathAuthorizationAndAtomicity() throws Exception {
    as(
        "writer",
        () ->
            manager.createTerm(
                LAKE,
                TermSpec.builder("ApprovedTerm").withStatus(GlossaryTerm.Status.APPROVED).build()));
    as(
        "writer",
        () ->
            manager.createTerm(
                LAKE, TermSpec.builder("DraftTerm").withStatus(GlossaryTerm.Status.DRAFT).build()));
    as(
        "writer",
        () ->
            manager.createTerm(
                LAKE,
                TermSpec.builder("DeprecatedTerm")
                    .withStatus(GlossaryTerm.Status.DEPRECATED)
                    .build()));
    as(
        "writerAndTableAdmin",
        () ->
            manager.createTerm(
                LAKE, TermSpec.builder("DualDraft").withStatus(GlossaryTerm.Status.DRAFT).build()));
    as(
        "writerAndTableAdmin",
        () ->
            manager.createTerm(
                LAKE,
                TermSpec.builder("DualDeprecated")
                    .withStatus(GlossaryTerm.Status.DEPRECATED)
                    .build()));

    MetadataObject tableObj = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);

    // T11.3: object admin with VIEW (no term edit) can bind APPROVED.
    assertArrayEquals(
        new String[] {"ApprovedTerm"},
        as(
            "tableAdmin",
            () -> manager.associateTerms(LAKE, tableObj, new String[] {"ApprovedTerm"}, null)));
    assertEquals(2L, as("writer", () -> manager.getTerm(LAKE, "ApprovedTerm")).version());

    // T11.3: object admin binding another user's DRAFT gets 404 (NoSuchTermException) before status
    // check.
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "tableAdmin",
                () -> manager.associateTerms(LAKE, tableObj, new String[] {"DraftTerm"}, null)));
    assertEquals(1L, as("writer", () -> manager.getTerm(LAKE, "DraftTerm")).version());

    // T11.3: object admin binding a visible DEPRECATED term gets 400 (IllegalArgumentException).
    assertThrows(
        IllegalArgumentException.class,
        () ->
            as(
                "tableAdmin",
                () ->
                    manager.associateTerms(LAKE, tableObj, new String[] {"DeprecatedTerm"}, null)));
    assertEquals(1L, as("writer", () -> manager.getTerm(LAKE, "DeprecatedTerm")).version());

    // T11.3: caller with neither authorization path gets 403 (ForbiddenException).
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "viewerWithTableRead",
                () ->
                    manager.associateTerms(LAKE, tableObj, new String[] {"DeprecatedTerm"}, null)));
    assertEquals(1L, as("writer", () -> manager.getTerm(LAKE, "DeprecatedTerm")).version());

    // T11.4: term editor with only read access to the object binds a DRAFT term.
    assertArrayEquals(
        new String[] {"ApprovedTerm", "DraftTerm"},
        as(
            "writer",
            () -> manager.associateTerms(LAKE, tableObj, new String[] {"DraftTerm"}, null)));
    assertEquals(2L, as("writer", () -> manager.getTerm(LAKE, "DraftTerm")).version());

    // T11.4: caller who is both object admin and term editor can bind DRAFT and DEPRECATED.
    assertArrayEquals(
        new String[] {"ApprovedTerm", "DualDeprecated", "DualDraft"},
        as(
            "writerAndTableAdmin",
            () ->
                manager.associateTerms(
                    LAKE, tableObj, new String[] {"DualDraft", "DualDeprecated"}, null)));

    // T11.4: MANAGE_DEFINITIONS without object read access fails with 403.
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "glossaryOnlyAdmin",
                () ->
                    manager.associateTerms(LAKE, tableObj, new String[] {"DeprecatedTerm"}, null)));
    assertEquals(1L, as("writer", () -> manager.getTerm(LAKE, "DeprecatedTerm")).version());

    // T11.5: one request binds two terms, caller fails authorization on the second -> 403 and
    // first term is NOT bound either.
    as(
        "admin",
        () ->
            manager.createTerm(
                LAKE,
                TermSpec.builder("AdminApproved")
                    .withStatus(GlossaryTerm.Status.APPROVED)
                    .build()));
    long depVersionBefore = as("writer", () -> manager.getTerm(LAKE, "DeprecatedTerm")).version();
    long adminApprovedBefore = as("admin", () -> manager.getTerm(LAKE, "AdminApproved")).version();
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "writer",
                () ->
                    manager.associateTerms(
                        LAKE, tableObj, new String[] {"DeprecatedTerm", "AdminApproved"}, null)));
    assertEquals(
        depVersionBefore, as("writer", () -> manager.getTerm(LAKE, "DeprecatedTerm")).version());
    assertEquals(
        adminApprovedBefore, as("admin", () -> manager.getTerm(LAKE, "AdminApproved")).version());
    assertFalse(
        as("admin", () -> manager.listObjectTerms(LAKE, tableObj, null)).stream()
            .anyMatch(t -> t.name().equals("DeprecatedTerm") || t.name().equals("AdminApproved")));
  }

  /**
   * Covers T11.16 (guard): view binding works, COLUMN under a view-only parent returns 404 and
   * writes nothing, table-column request resolves table and column IDs.
   */
  @TestTemplate
  void testViewBindingAndRejectViewColumn() throws Exception {
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("ActiveUser").build()));
    MetadataObject viewObj = MetadataObjects.of(List.of(CAT, SCH, VIEW), MetadataObject.Type.VIEW);
    assertArrayEquals(
        new String[] {"ActiveUser"},
        as(
            "lakeOwner",
            () -> manager.associateTerms(LAKE, viewObj, new String[] {"ActiveUser"}, null)));
    assertEquals(2L, as("lakeOwner", () -> manager.getTerm(LAKE, "ActiveUser")).version());

    // COLUMN under a view-only parent name must fail with 404 (NoSuchMetadataObjectException).
    MetadataObject viewCol =
        MetadataObjects.of(List.of(CAT, SCH, VIEW, "id"), MetadataObject.Type.COLUMN);
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            as(
                "lakeOwner",
                () -> manager.associateTerms(LAKE, viewCol, new String[] {"ActiveUser"}, null)));
    assertEquals(2L, as("lakeOwner", () -> manager.getTerm(LAKE, "ActiveUser")).version());
  }

  /**
   * Covers T11.8, T11.9: drop column cleans up only its bindings and increments affected term once;
   * rename table and rename column preserve bindings and resolve to new names.
   */
  @TestTemplate
  void testColumnDropAndSequentialTableColumnRename() throws Exception {
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("CustomerId").build()));
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("ActiveFlag").build()));
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Untouched").build()));

    MetadataObject tableObj = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject colId =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "customer_id"), MetadataObject.Type.COLUMN);
    MetadataObject colActive =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "is_active"), MetadataObject.Type.COLUMN);

    as(
        "lakeOwner",
        () -> manager.associateTerms(LAKE, tableObj, new String[] {"CustomerId"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, colId, new String[] {"CustomerId"}, null));
    as(
        "lakeOwner",
        () -> manager.associateTerms(LAKE, colActive, new String[] {"ActiveFlag"}, null));

    long activeVersionBefore = as("lakeOwner", () -> manager.getTerm(LAKE, "ActiveFlag")).version();
    long customerIdVersionBefore =
        as("lakeOwner", () -> manager.getTerm(LAKE, "CustomerId")).version();
    long untouchedVersionBefore =
        as("lakeOwner", () -> manager.getTerm(LAKE, "Untouched")).version();

    // T11.8: drop column is_active via updateTable.
    NameIdentifier tableIdent = NameIdentifier.of(schNs, TBL);
    TableMetaService.getInstance()
        .updateTable(
            tableIdent,
            (TableEntity old) ->
                TableEntity.builder()
                    .withId(old.id())
                    .withName(old.name())
                    .withNamespace(old.namespace())
                    .withColumns(
                        old.columns().stream().filter(c -> !c.name().equals("is_active")).toList())
                    .withAuditInfo(AUDIT_INFO)
                    .build());

    assertTrue(
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "ActiveFlag", null, null, null))
            .isEmpty());
    assertEquals(
        activeVersionBefore + 1,
        as("lakeOwner", () -> manager.getTerm(LAKE, "ActiveFlag")).version());
    assertEquals(
        customerIdVersionBefore,
        as("lakeOwner", () -> manager.getTerm(LAKE, "CustomerId")).version());
    assertEquals(
        untouchedVersionBefore,
        as("lakeOwner", () -> manager.getTerm(LAKE, "Untouched")).version());
    assertEquals(
        2,
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "CustomerId", null, null, null))
            .size());

    // T11.9: rename table and rename column preserving IDs -> bindings stay and resolve to new
    // names.
    TableMetaService.getInstance()
        .updateTable(
            tableIdent,
            (TableEntity old) ->
                TableEntity.builder()
                    .withId(old.id())
                    .withName("customers_v2")
                    .withNamespace(old.namespace())
                    .withColumns(
                        old.columns().stream()
                            .map(
                                c ->
                                    c.name().equals("customer_id")
                                        ? ColumnEntity.builder()
                                            .withId(c.id())
                                            .withName("cust_pk")
                                            .withPosition(c.position())
                                            .withDataType(c.dataType())
                                            .withNullable(c.nullable())
                                            .withAutoIncrement(c.autoIncrement())
                                            .withAuditInfo(AUDIT_INFO)
                                            .build()
                                        : c)
                            .toList())
                    .withAuditInfo(AUDIT_INFO)
                    .build());

    List<TermBinding> afterRename =
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "CustomerId", null, null, null));
    assertEquals(2, afterRename.size());
    assertTrue(
        afterRename.contains(
            TermBinding.implementedBy(
                MetadataObjects.of(List.of(CAT, SCH, "customers_v2"), MetadataObject.Type.TABLE))));
    assertTrue(
        afterRename.contains(
            TermBinding.implementedBy(
                MetadataObjects.of(
                    List.of(CAT, SCH, "customers_v2", "cust_pk"), MetadataObject.Type.COLUMN))));
  }

  /**
   * Covers T11.10, T11.15, T11.17 (guard): metric removal, metric rename audit details, multiple
   * metrics removed in one alter, long-prefix metric names, semantic-model rename preservation, and
   * reintroducing a removed metric name gets a fresh ID with no old bindings.
   */
  @TestTemplate
  void testMetricCleanupAuditRenameAndLongNames() throws Exception {
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Revenue").build()));
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Growth").build()));
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Unaffected").build()));

    // T11.17: long metric names sharing a long prefix.
    String prefix = "very_long_shared_business_metric_prefix_" + "x".repeat(220);
    String longMetricA = prefix + "_alpha";
    String longMetricB = prefix + "_beta";
    NameIdentifier smIdent = NameIdentifier.of(schNs, SMODEL);
    SemanticModelMetaService.getInstance()
        .updateSemanticModel(
            smIdent,
            (SemanticModelEntity old) ->
                semanticModel(
                    old.id(),
                    old.name(),
                    List.of("net_revenue", "active_users", longMetricA, longMetricB)));

    MetadataObject smObj =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    as(
        "lakeOwner",
        () ->
            manager.associateMetricTerms(LAKE, smObj, longMetricA, new String[] {"Revenue"}, null));
    as(
        "lakeOwner",
        () ->
            manager.associateMetricTerms(
                LAKE, smObj, longMetricB, new String[] {"Revenue", "Growth"}, null));
    as(
        "lakeOwner",
        () ->
            manager.associateMetricTerms(
                LAKE, smObj, "net_revenue", new String[] {"Revenue", "Growth"}, null));

    // Verify long metric names resolve without truncation.
    List<TermBinding> revBindings =
        as(
            "lakeOwner",
            () -> manager.listTermBindings(LAKE, "Revenue", TermBinding.Kind.USED_BY, null, null));
    assertEquals(3, revBindings.size());
    assertTrue(revBindings.contains(TermBinding.usedBy(smObj, longMetricA)));
    assertTrue(revBindings.contains(TermBinding.usedBy(smObj, longMetricB)));

    // T11.17: metric expression/content edits (same metric names) and semantic-model rename
    // preserve bindings without bumping term revision.
    long revVersionBeforeContentEdit =
        as("lakeOwner", () -> manager.getTerm(LAKE, "Revenue")).version();
    SemanticModelMetaService.getInstance()
        .updateSemanticModel(
            smIdent,
            (SemanticModelEntity old) ->
                semanticModelWithExpression(
                    old.id(),
                    old.name(),
                    List.of("net_revenue", "active_users", longMetricA, longMetricB),
                    "SUM(1)"));
    assertEquals(
        revVersionBeforeContentEdit,
        as("lakeOwner", () -> manager.getTerm(LAKE, "Revenue")).version());
    assertEquals(
        3,
        as(
                "lakeOwner",
                () ->
                    manager.listTermBindings(LAKE, "Revenue", TermBinding.Kind.USED_BY, null, null))
            .size());

    // T11.17: semantic-model rename preserves bindings and resolves to the new model name.
    SemanticModelMetaService.getInstance()
        .updateSemanticModel(
            smIdent,
            (SemanticModelEntity old) ->
                semanticModel(
                    old.id(),
                    "sales_model_renamed",
                    List.of("net_revenue", "active_users", longMetricA, longMetricB)));
    MetadataObject renamedSmObj =
        MetadataObjects.of(
            List.of(CAT, SCH, "sales_model_renamed"), MetadataObject.Type.SEMANTIC_MODEL);
    List<TermBinding> afterModelRename =
        as(
            "lakeOwner",
            () -> manager.listTermBindings(LAKE, "Revenue", TermBinding.Kind.USED_BY, null, null));
    assertEquals(3, afterModelRename.size());
    assertTrue(afterModelRename.contains(TermBinding.usedBy(renamedSmObj, longMetricA)));

    // T11.10 & T11.15: remove net_revenue (renamed to gross_revenue) AND longMetricB in one alter.
    long revVersionBefore = as("lakeOwner", () -> manager.getTerm(LAKE, "Revenue")).version();
    long growthVersionBefore = as("lakeOwner", () -> manager.getTerm(LAKE, "Growth")).version();
    long unaffectedVersionBefore =
        as("lakeOwner", () -> manager.getTerm(LAKE, "Unaffected")).version();
    auditRecords.clear();

    NameIdentifier renamedSmIdent = NameIdentifier.of(schNs, "sales_model_renamed");
    SemanticModelEntity currentModel =
        SemanticModelMetaService.getInstance().getSemanticModelByIdentifier(renamedSmIdent);
    long oldNetRevId =
        SessionUtils.getWithoutCommit(
                SemanticModelMetricMapper.class,
                m -> m.selectSemanticModelMetricByModelIdAndName(currentModel.id(), "net_revenue"))
            .metricId();

    SemanticModelMetaService.getInstance()
        .updateSemanticModel(
            renamedSmIdent,
            (SemanticModelEntity old) ->
                semanticModel(
                    old.id(), old.name(), List.of("gross_revenue", "active_users", longMetricA)));

    // Each affected term gets exactly one revision increment and one audit record grouping losses.
    assertEquals(
        revVersionBefore + 1, as("lakeOwner", () -> manager.getTerm(LAKE, "Revenue")).version());
    assertEquals(
        growthVersionBefore + 1, as("lakeOwner", () -> manager.getTerm(LAKE, "Growth")).version());
    assertEquals(
        unaffectedVersionBefore,
        as("lakeOwner", () -> manager.getTerm(LAKE, "Unaffected")).version());
    assertEquals(2, auditRecords.size());

    BindingLossAuditRecord revAudit =
        auditRecords.stream().filter(r -> r.termName().equals("Revenue")).findFirst().orElseThrow();
    assertEquals(revVersionBefore + 1, revAudit.resultingRevision());
    assertEquals("ALTER_SEMANTIC_MODEL", revAudit.triggeringOperation());
    assertEquals(2, revAudit.lostBindings().size());
    LostBinding netRevLoss =
        revAudit.lostBindings().stream()
            .filter(b -> "net_revenue".equals(b.targetName()))
            .findFirst()
            .orElseThrow();
    assertEquals(TargetType.METRIC, netRevLoss.targetType());
    assertEquals(oldNetRevId, netRevLoss.targetId());
    assertEquals(currentModel.id(), netRevLoss.containerId());
    assertEquals("sales_model_renamed", netRevLoss.containerName());

    // T11.17: reintroducing net_revenue gets a new ID with no old bindings; explicit rebind works.
    SemanticModelMetaService.getInstance()
        .updateSemanticModel(
            renamedSmIdent,
            (SemanticModelEntity old) ->
                semanticModel(
                    old.id(),
                    old.name(),
                    List.of("gross_revenue", "active_users", longMetricA, "net_revenue")));
    SemanticModelMetricPO reintroduced =
        SessionUtils.getWithoutCommit(
            SemanticModelMetricMapper.class,
            m -> m.selectSemanticModelMetricByModelIdAndName(currentModel.id(), "net_revenue"));
    assertNotEquals(oldNetRevId, reintroduced.metricId());
    assertTrue(
        as("lakeOwner", () -> manager.listMetricTerms(LAKE, renamedSmObj, "net_revenue", null))
            .isEmpty());
    assertArrayEquals(
        new String[] {"Revenue"},
        as(
            "lakeOwner",
            () ->
                manager.associateMetricTerms(
                    LAKE, renamedSmObj, "net_revenue", new String[] {"Revenue"}, null)));
  }

  /**
   * Covers T11.7, T11.13 (guard): table, schema, catalog, and metalake drop cleanup with single
   * revision increment and audit record per surviving term; rollback on injected cleanup failure;
   * no-op cleanup leaves revisions unchanged.
   */
  @TestTemplate
  void testCascadeDropsAuditRollbackAndNoOpCleanup() throws Exception {
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("CoreTerm").build()));
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("IdleTerm").build()));

    // Bind table + 2 columns to CoreTerm.
    MetadataObject tableObj = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject col1 =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "customer_id"), MetadataObject.Type.COLUMN);
    MetadataObject col2 =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "is_active"), MetadataObject.Type.COLUMN);
    as("lakeOwner", () -> manager.associateTerms(LAKE, tableObj, new String[] {"CoreTerm"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, col1, new String[] {"CoreTerm"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, col2, new String[] {"CoreTerm"}, null));

    // Inject failure during table deletion transaction -> everything rolls back, no audit record.
    long versionBeforeFail = as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version();
    auditRecords.clear();
    NameIdentifier tableIdent = NameIdentifier.of(schNs, TBL);
    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> TableMetaService.getInstance().deleteTable(tableIdent),
                () -> {
                  throw new IllegalStateException("Simulated failure after binding cleanup");
                }));
    assertEquals(
        versionBeforeFail, as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version());
    assertEquals(
        3,
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "CoreTerm", null, null, null)).size());
    assertTrue(auditRecords.isEmpty(), "Rolled-back cleanup must emit no audit record");

    // Actual table drop -> CoreTerm gets +1 revision and 1 audit record listing all 3 lost
    // bindings.
    TableMetaService.getInstance().deleteTable(tableIdent);
    assertEquals(
        versionBeforeFail + 1, as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version());
    assertEquals(1L, as("lakeOwner", () -> manager.getTerm(LAKE, "IdleTerm")).version());
    assertEquals(1, auditRecords.size());
    assertEquals(3, auditRecords.get(0).lostBindings().size());
    assertEquals("DROP_TABLE", auditRecords.get(0).triggeringOperation());

    // No-op cleanup (dropping an unbound view) leaves revisions unchanged and emits no audit.
    auditRecords.clear();
    long versionAfterTableDrop = as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version();
    ViewMetaService.getInstance().deleteView(NameIdentifier.of(schNs, VIEW));
    assertEquals(
        versionAfterTableDrop, as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version());
    assertTrue(auditRecords.isEmpty());

    // Schema cascade drop cleans up schema, fileset, topic, model, and semantic model metric
    // bindings with a single revision increment for CoreTerm.
    MetadataObject schObj = MetadataObjects.of(CAT, SCH, MetadataObject.Type.SCHEMA);
    MetadataObject fsObj =
        MetadataObjects.of(List.of(CAT, SCH, FILESET), MetadataObject.Type.FILESET);
    MetadataObject topicObj =
        MetadataObjects.of(List.of(CAT, SCH, TOPIC), MetadataObject.Type.TOPIC);
    MetadataObject modelObj =
        MetadataObjects.of(List.of(CAT, SCH, MODEL), MetadataObject.Type.MODEL);
    MetadataObject smObj =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    as("lakeOwner", () -> manager.associateTerms(LAKE, schObj, new String[] {"CoreTerm"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, fsObj, new String[] {"CoreTerm"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, topicObj, new String[] {"CoreTerm"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, modelObj, new String[] {"CoreTerm"}, null));
    as(
        "lakeOwner",
        () ->
            manager.associateMetricTerms(
                LAKE, smObj, "net_revenue", new String[] {"CoreTerm"}, null));
    long beforeSchemaDrop = as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version();
    auditRecords.clear();
    SchemaMetaService.getInstance().deleteSchema(NameIdentifier.of(LAKE, CAT, SCH), true);
    assertEquals(
        beforeSchemaDrop + 1, as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version());
    assertEquals(1, auditRecords.size());
    assertEquals(5, auditRecords.get(0).lostBindings().size());
    assertTrue(
        as("lakeOwner", () -> manager.listTermBindings(LAKE, "CoreTerm", null, null, null))
            .isEmpty());

    // Catalog cascade drop cleans up catalog + child schema/table bindings with +1 revision.
    createAndInsertSchema(LAKE, CAT, "sch2");
    Namespace sch2Ns = Namespace.of(LAKE, CAT, "sch2");
    backend.insert(createTableEntity(ids.incrementAndGet(), sch2Ns, "t2", AUDIT_INFO), false);
    MetadataObject catObj = MetadataObjects.of(null, CAT, MetadataObject.Type.CATALOG);
    MetadataObject t2Obj =
        MetadataObjects.of(List.of(CAT, "sch2", "t2"), MetadataObject.Type.TABLE);
    as("lakeOwner", () -> manager.associateTerms(LAKE, catObj, new String[] {"CoreTerm"}, null));
    as("lakeOwner", () -> manager.associateTerms(LAKE, t2Obj, new String[] {"CoreTerm"}, null));
    long beforeCatDrop = as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version();
    auditRecords.clear();
    CatalogMetaService.getInstance().deleteCatalog(NameIdentifier.of(LAKE, CAT), true);
    assertEquals(
        beforeCatDrop + 1, as("lakeOwner", () -> manager.getTerm(LAKE, "CoreTerm")).version());
    assertEquals(1, auditRecords.size());
    assertEquals(2, auditRecords.get(0).lostBindings().size());

    // Metalake cascade drop soft-deletes all glossary data and leaves no surviving terms.
    auditRecords.clear();
    MetalakeMetaService.getInstance().deleteMetalake(NameIdentifier.of(LAKE), true);
    assertTrue(auditRecords.isEmpty());
  }

  /**
   * Covers T11.11: concurrent bind on a table vs dropping the table over 20 rounds leaves no live
   * binding to any dropped table.
   */
  @TestTemplate
  void testConcurrentBindAndDropTableRace() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("RaceTerm").build());
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(60),
          () -> {
            for (int round = 0; round < 20; round++) {
              String tableName = "race_tbl_" + round;
              backend.insert(
                  createTableEntity(ids.incrementAndGet(), schNs, tableName, AUDIT_INFO), false);
              MetadataObject tblObj =
                  MetadataObjects.of(List.of(CAT, SCH, tableName), MetadataObject.Type.TABLE);
              NameIdentifier tblIdent = NameIdentifier.of(schNs, tableName);
              CyclicBarrier barrier = new CyclicBarrier(2);

              Future<?> binder =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          openManager.associateTerms(LAKE, tblObj, new String[] {"RaceTerm"}, null);
                        } catch (NoSuchMetadataObjectException expectedWhenDropWins) {
                          // Expected when table drop wins the fence.
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> dropper =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          TableMetaService.getInstance().deleteTable(tblIdent);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              binder.get();
              dropper.get();
              GlossaryInvariants.assertBindings();
            }
          });
    } finally {
      pool.shutdownNow();
    }
    assertTrue(openManager.listTermBindings(LAKE, "RaceTerm", null, null, null).isEmpty());
  }

  /**
   * Covers T11.14: race column removal against concurrent binding writes and ordinary term edits
   * over 20 rounds with no deadlock, dangling binding, or lost revision increment.
   */
  @TestTemplate
  void testConcurrentColumnRemovalAgainstBindingAndTermEdit() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("ContendedTerm").build());
    openManager.createTerm(LAKE, TermSpec.builder("ContendedTerm2").build());
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(60),
          () -> {
            for (int round = 0; round < 20; round++) {
              String tableName = "col_race_" + round;
              TableEntity table =
                  insertTable(tableName, List.of(col("keep_col", 0), col("drop_col", 1)));
              MetadataObject dropCol =
                  MetadataObjects.of(
                      List.of(CAT, SCH, tableName, "drop_col"), MetadataObject.Type.COLUMN);
              NameIdentifier tblIdent = NameIdentifier.of(schNs, tableName);
              openManager.associateTerms(LAKE, dropCol, new String[] {"ContendedTerm"}, null);
              long v1Before = openManager.getTerm(LAKE, "ContendedTerm").version();
              long v2Before = openManager.getTerm(LAKE, "ContendedTerm2").version();
              AtomicBoolean editSucceeded = new AtomicBoolean(false);
              AtomicBoolean bindSucceeded = new AtomicBoolean(false);
              CyclicBarrier barrier = new CyclicBarrier(3);

              Future<?> remover =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          TableMetaService.getInstance()
                              .updateTable(
                                  tblIdent,
                                  (TableEntity old) ->
                                      TableEntity.builder()
                                          .withId(old.id())
                                          .withName(old.name())
                                          .withNamespace(old.namespace())
                                          .withColumns(
                                              old.columns().stream()
                                                  .filter(c -> !c.name().equals("drop_col"))
                                                  .toList())
                                          .withAuditInfo(AUDIT_INFO)
                                          .build());
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> binder =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          openManager.associateTerms(
                              LAKE, dropCol, new String[] {"ContendedTerm2"}, null);
                          bindSucceeded.set(true);
                        } catch (NoSuchMetadataObjectException expectedWhenDropWins) {
                          // Column drop won the endpoint lock first.
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> editor =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          openManager.alterTerm(
                              LAKE,
                              "ContendedTerm",
                              TermChange.updateDefinition("def-" + table.id()));
                          editSucceeded.set(true);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              remover.get();
              binder.get();
              editor.get();
              assertTrue(editSucceeded.get());
              assertEquals(v1Before + 2, openManager.getTerm(LAKE, "ContendedTerm").version());
              // If the concurrent bind landed before column removal, column removal also cleaned
              // it up (+2 total); if column removal won first, ContendedTerm2 stayed untouched.
              assertEquals(
                  bindSucceeded.get() ? v2Before + 2 : v2Before,
                  openManager.getTerm(LAKE, "ContendedTerm2").version());
              assertTrue(
                  openManager.listTermBindings(LAKE, "ContendedTerm", null, null, null).isEmpty());
              assertTrue(
                  openManager.listTermBindings(LAKE, "ContendedTerm2", null, null, null).isEmpty());
              GlossaryInvariants.assertBindings();
            }
          });
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Covers T11.14 for metrics: race semantic-model metric removal against concurrent metric binding
   * writes and ordinary term edits over 20 rounds with no deadlock, dangling binding, or lost
   * revision increment.
   */
  @TestTemplate
  void testConcurrentMetricRemovalAgainstBindingAndTermEdit() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("MetricRaceTerm1").build());
    openManager.createTerm(LAKE, TermSpec.builder("MetricRaceTerm2").build());
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(60),
          () -> {
            for (int round = 0; round < 20; round++) {
              String modelName = "metric_race_model_" + round;
              long modelId = ids.incrementAndGet();
              SemanticModelMetaService.getInstance()
                  .insertSemanticModel(
                      semanticModel(modelId, modelName, List.of("keep_metric", "drop_metric")),
                      false);
              MetadataObject modelObj =
                  MetadataObjects.of(
                      List.of(CAT, SCH, modelName), MetadataObject.Type.SEMANTIC_MODEL);
              NameIdentifier modelIdent = NameIdentifier.of(schNs, modelName);
              openManager.associateMetricTerms(
                  LAKE, modelObj, "drop_metric", new String[] {"MetricRaceTerm1"}, null);
              long v1Before = openManager.getTerm(LAKE, "MetricRaceTerm1").version();
              long v2Before = openManager.getTerm(LAKE, "MetricRaceTerm2").version();
              AtomicBoolean editSucceeded = new AtomicBoolean(false);
              AtomicBoolean bindSucceeded = new AtomicBoolean(false);
              CyclicBarrier barrier = new CyclicBarrier(3);

              Future<?> remover =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          SemanticModelMetaService.getInstance()
                              .updateSemanticModel(
                                  modelIdent,
                                  (SemanticModelEntity old) ->
                                      semanticModel(old.id(), old.name(), List.of("keep_metric")));
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> binder =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          openManager.associateMetricTerms(
                              LAKE,
                              modelObj,
                              "drop_metric",
                              new String[] {"MetricRaceTerm2"},
                              null);
                          bindSucceeded.set(true);
                        } catch (NoSuchMetadataObjectException expectedWhenDropWins) {
                          // Metric removal won the semantic-model lock first.
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> editor =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          openManager.alterTerm(
                              LAKE,
                              "MetricRaceTerm1",
                              TermChange.updateDefinition("mdef-" + modelId));
                          editSucceeded.set(true);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              remover.get();
              binder.get();
              editor.get();
              assertTrue(editSucceeded.get());
              assertEquals(v1Before + 2, openManager.getTerm(LAKE, "MetricRaceTerm1").version());
              assertEquals(
                  bindSucceeded.get() ? v2Before + 2 : v2Before,
                  openManager.getTerm(LAKE, "MetricRaceTerm2").version());
              assertTrue(
                  openManager
                      .listTermBindings(LAKE, "MetricRaceTerm1", null, null, null)
                      .isEmpty());
              assertTrue(
                  openManager
                      .listTermBindings(LAKE, "MetricRaceTerm2", null, null, null)
                      .isEmpty());
              GlossaryInvariants.assertBindings();
            }
          });
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Regression test: racing cascading schema/catalog drops against concurrent leaf table drops
   * sharing a bound term must neither AB-BA deadlock nor double-increment the term revision.
   */
  @TestTemplate
  void testConcurrentCascadeDropAndTableDropNoDeadlock() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("CascadeRaceTerm").build());
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(60),
          () -> {
            for (int round = 0; round < 10; round++) {
              String raceCat = "race_cat_" + round;
              String raceSch = "race_sch_" + round;
              String raceTbl = "race_tbl_" + round;
              createAndInsertCatalog(LAKE, raceCat);
              createAndInsertSchema(LAKE, raceCat, raceSch);
              Namespace ns = Namespace.of(LAKE, raceCat, raceSch);
              TableEntity tbl =
                  TableEntity.builder()
                      .withId(ids.incrementAndGet())
                      .withName(raceTbl)
                      .withNamespace(ns)
                      .withColumns(List.of(col("c1", 0)))
                      .withAuditInfo(AUDIT_INFO)
                      .build();
              backend.insert(tbl, false);
              MetadataObject tblObj =
                  MetadataObjects.of(List.of(raceCat, raceSch, raceTbl), MetadataObject.Type.TABLE);
              openManager.associateTerms(LAKE, tblObj, new String[] {"CascadeRaceTerm"}, null);
              long versionBefore = openManager.getTerm(LAKE, "CascadeRaceTerm").version();
              CyclicBarrier barrier = new CyclicBarrier(2);
              boolean dropCatalog = (round % 2 == 1);

              Future<?> cascadeDrop =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          if (dropCatalog) {
                            CatalogMetaService.getInstance()
                                .deleteCatalog(NameIdentifier.of(LAKE, raceCat), true);
                          } else {
                            SchemaMetaService.getInstance()
                                .deleteSchema(NameIdentifier.of(LAKE, raceCat, raceSch), true);
                          }
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> tableDrop =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          TableMetaService.getInstance()
                              .deleteTable(NameIdentifier.of(LAKE, raceCat, raceSch, raceTbl));
                        } catch (NoSuchEntityException expectedWhenCascadeWins) {
                          // Cascade drop won first.
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              cascadeDrop.get();
              tableDrop.get();
              // Regardless of winner, the single table binding must retire once and bump revision
              // exactly once.
              assertEquals(
                  versionBefore + 1, openManager.getTerm(LAKE, "CascadeRaceTerm").version());
              assertTrue(
                  openManager
                      .listTermBindings(LAKE, "CascadeRaceTerm", null, null, null)
                      .isEmpty());
              GlossaryInvariants.assertBindings();
            }
          });
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Covers T11.18: external DDL completes then local store cleanup fails (local rows/revisions roll
   * back, no cleanup-success audit record emitted); subsequent load/overwrite reconciles removed
   * column and cleans up its binding; failed local table drop leaves stale registration intact.
   */
  @TestTemplate
  void testExternalDdlFailureRollbackAndLoadColumnReconciliation() throws Exception {
    Config config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);
    Object previousLocks = FieldUtils.readField(env, "lockManager", true);
    CatalogManager oldCatalogs = env.catalogManager();
    LoggerContext context =
        (LoggerContext)
            LogManager.getContext(GlossaryTermBindingService.class.getClassLoader(), false);
    var logger = context.getLogger(GlossaryTermBindingService.class.getName());
    Level previousLevel = logger.getLevel();
    StringWriter logs = new StringWriter();
    WriterAppender appender =
        WriterAppender.newBuilder()
            .setName("bindingCleanup")
            .setTarget(logs)
            .setLayout(PatternLayout.newBuilder().withPattern("%m%n%throwable").build())
            .build();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try (SecretManager secrets = new SecretManager(config);
        CatalogManager catalogs =
            new CatalogManager(config, env.entityStore(), ids::incrementAndGet, secrets)) {
      if (previousLocks == null) {
        FieldUtils.writeField(env, "lockManager", new LockManager(config), true);
      }
      FieldUtils.writeField(env, "catalogManager", catalogs, true);
      NameIdentifier catalog = NameIdentifier.of(LAKE, "external");
      catalogs.createCatalog(
          catalog,
          Catalog.Type.RELATIONAL,
          "test",
          "",
          Map.of("key1", "v1", "key2", "v2", "key5-1", "v3"));
      SchemaOperationDispatcher schemas =
          new SchemaOperationDispatcher(catalogs, env.entityStore(), ids::incrementAndGet, secrets);
      TableOperationDispatcher tables =
          new TableOperationDispatcher(
              catalogs, env.entityStore(), ids::incrementAndGet, () -> schemas, secrets);
      schemas.createSchema(
          NameIdentifier.of(LAKE, "external", SCH), "", Map.of("k1", "v1", "k2", "v2"));
      NameIdentifier table = NameIdentifier.of(LAKE, "external", SCH, TBL);
      tables.createTable(
          table,
          new Column[] {
            Column.of("keep", Types.StringType.get()), Column.of("removed", Types.StringType.get())
          },
          "",
          Map.of("k1", "v1", "k2", "v2"));
      TableEntity stored = TableMetaService.getInstance().getTableByIdentifier(table);
      MetadataObject column =
          MetadataObjects.of(List.of("external", SCH, TBL, "removed"), MetadataObject.Type.COLUMN);
      MetadataObject target = MetadataObjects.parent(column);
      openManager.createTerm(LAKE, TermSpec.builder("ReconcileTerm").build());
      openManager.associateTerms(LAKE, column, new String[] {"ReconcileTerm"}, null);
      long before = openManager.getTerm(LAKE, "ReconcileTerm").version();
      GlossaryTermMetaService termService = Mockito.spy(GlossaryTermMetaService.getInstance());
      AtomicBoolean fail = new AtomicBoolean(true);
      Mockito.doAnswer(
              inv -> {
                if (fail.get()) {
                  throw new IllegalStateException("injected revision failure");
                }
                return inv.callRealMethod();
              })
          .when(termService)
          .advanceBindingRevision(
              Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());
      try (MockedStatic<GlossaryTermMetaService> services =
          Mockito.mockStatic(GlossaryTermMetaService.class, Mockito.CALLS_REAL_METHODS)) {
        services.when(GlossaryTermMetaService::getInstance).thenReturn(termService);
        tables.alterTable(table, TableChange.deleteColumn(new String[] {"removed"}, false));
        List<String> externalColumns =
            catalogs.doWithCatalog(
                catalog,
                c ->
                    Arrays.stream(((TableCatalog) c.ops()).loadTable(table).columns())
                        .map(Column::name)
                        .toList());
        assertEquals(List.of("keep"), externalColumns);
        assertEquals(
            2, TableMetaService.getInstance().getTableByIdentifier(table).columns().size());
        assertEquals(before, openManager.getTerm(LAKE, "ReconcileTerm").version());
        assertEquals(1, openManager.listObjectTerms(LAKE, column, null).size());
        assertTrue(auditRecords.isEmpty());
        assertTrue(logs.toString().contains("ALTER_TABLE"));
        assertTrue(logs.toString().contains("TABLE:" + stored.id()));
        assertTrue(logs.toString().contains("injected revision failure"));
        assertFalse(logs.toString().contains("cleanup committed"));

        fail.set(false);
        tables.loadTable(table);
        assertEquals(
            1, TableMetaService.getInstance().getTableByIdentifier(table).columns().size());
        assertEquals(before + 1, openManager.getTerm(LAKE, "ReconcileTerm").version());
        assertTrue(openManager.listTermBindings(LAKE, "ReconcileTerm", null, null, null).isEmpty());
        assertEquals(1, auditRecords.size());
        assertTrue(logs.toString().contains("cleanup committed"));
        assertTrue(logs.toString().contains("removed"));

        openManager.associateTerms(LAKE, target, new String[] {"ReconcileTerm"}, null);
        long beforeDrop = openManager.getTerm(LAKE, "ReconcileTerm").version();
        fail.set(true);
        logs.getBuffer().setLength(0);
        auditRecords.clear();
        assertThrows(RuntimeException.class, () -> tables.dropTable(table));
        boolean stillExists =
            catalogs.doWithCatalog(catalog, c -> ((TableCatalog) c.ops()).tableExists(table));
        assertFalse(stillExists);
        assertFalse(tables.dropTable(table));
        assertEquals(stored.id(), TableMetaService.getInstance().getTableByIdentifier(table).id());
        assertEquals(beforeDrop, openManager.getTerm(LAKE, "ReconcileTerm").version());
        assertEquals(1, openManager.listObjectTerms(LAKE, target, null).size());
        assertTrue(auditRecords.isEmpty());
        assertTrue(logs.toString().contains("DROP_TABLE"));
        assertTrue(logs.toString().contains("TABLE:" + stored.id()));
        assertTrue(logs.toString().contains("injected revision failure"));
        assertFalse(logs.toString().contains("cleanup committed"));
      }
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
      FieldUtils.writeField(env, "catalogManager", oldCatalogs, true);
      FieldUtils.writeField(env, "lockManager", previousLocks, true);
    }
  }

  /**
   * Regression test: an unauthorized caller must receive 403 Forbidden rather than 404 when
   * attempting to bind or unbind on either existing or nonexistent targets/metrics, preventing
   * target existence probing.
   */
  @TestTemplate
  void testUnauthorizedCallerCannotProbeTargetExistenceViaAssociate() throws Exception {
    as(
        "admin",
        () -> {
          manager.createTerm(LAKE, TermSpec.builder("ProbeTerm").build());
          manager.alterTerm(LAKE, "ProbeTerm", TermChange.setStatus(GlossaryTerm.Status.APPROVED));
          return null;
        });

    MetadataObject existingTable =
        MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject missingTable =
        MetadataObjects.of(List.of(CAT, SCH, "no_such_tbl"), MetadataObject.Type.TABLE);
    MetadataObject existingColumn =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "customer_id"), MetadataObject.Type.COLUMN);
    MetadataObject missingColumn =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "no_such_col"), MetadataObject.Type.COLUMN);
    MetadataObject existingModel =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    MetadataObject missingModel =
        MetadataObjects.of(List.of(CAT, SCH, "no_such_model"), MetadataObject.Type.SEMANTIC_MODEL);

    as(
        "glossaryOnlyAdmin",
        () -> {
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, existingTable, new String[] {"ProbeTerm"}, null));
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, missingTable, new String[] {"ProbeTerm"}, null));
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, existingColumn, new String[] {"ProbeTerm"}, null));
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, missingColumn, new String[] {"ProbeTerm"}, null));
          assertThrows(
              ForbiddenException.class,
              () ->
                  manager.associateMetricTerms(
                      LAKE, existingModel, "net_revenue", new String[] {"ProbeTerm"}, null));
          assertThrows(
              ForbiddenException.class,
              () ->
                  manager.associateMetricTerms(
                      LAKE, existingModel, "no_such_metric", new String[] {"ProbeTerm"}, null));
          assertThrows(
              ForbiddenException.class,
              () ->
                  manager.associateMetricTerms(
                      LAKE, missingModel, "net_revenue", new String[] {"ProbeTerm"}, null));
          return null;
        });
  }

  /**
   * Regression test: overwriting an existing table with null columns must clean up column bindings
   * atomically, increment the surviving term's revision once, and preserve binding invariants.
   */
  @TestTemplate
  void testTableOverwriteWithNullColumnsCleansUpColumnBindings() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("OverwriteNullColTerm").build());
    TableEntity table = insertTable("overwrite_null_cols", List.of(col("c1", 0), col("c2", 1)));
    MetadataObject col1 =
        MetadataObjects.of(
            List.of(CAT, SCH, "overwrite_null_cols", "c1"), MetadataObject.Type.COLUMN);
    openManager.associateTerms(LAKE, col1, new String[] {"OverwriteNullColTerm"}, null);
    long revBefore = openManager.getTerm(LAKE, "OverwriteNullColTerm").version();
    auditRecords.clear();

    TableEntity overwritten =
        TableEntity.builder()
            .withId(table.id())
            .withName(table.name())
            .withNamespace(table.namespace())
            .withColumns(null)
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(overwritten, true);

    assertTrue(
        openManager.listTermBindings(LAKE, "OverwriteNullColTerm", null, null, null).isEmpty());
    assertEquals(revBefore + 1, openManager.getTerm(LAKE, "OverwriteNullColTerm").version());
    assertEquals(1, auditRecords.size());
    assertEquals("RECONCILE_TABLE_COLUMNS", auditRecords.get(0).triggeringOperation());
    assertEquals(1, auditRecords.get(0).lostBindings().size());
    assertEquals(TargetType.COLUMN, auditRecords.get(0).lostBindings().get(0).targetType());
  }

  /**
   * Regression test: a failing CleanupAuditListener must not prevent subsequent listeners from
   * receiving the committed binding-loss audit record.
   */
  @TestTemplate
  void testFailingCleanupAuditListenerDoesNotBlockSubsequentListeners() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("ListenerIsolationTerm").build());
    insertTable("listener_tbl", List.of(col("c1", 0)));
    MetadataObject tblObj =
        MetadataObjects.of(List.of(CAT, SCH, "listener_tbl"), MetadataObject.Type.TABLE);
    openManager.associateTerms(LAKE, tblObj, new String[] {"ListenerIsolationTerm"}, null);

    CleanupAuditListener failingListener =
        record -> {
          throw new RuntimeException("simulated listener failure");
        };
    List<BindingLossAuditRecord> secondListenerRecords = new CopyOnWriteArrayList<>();
    CleanupAuditListener secondListener = secondListenerRecords::add;
    GlossaryTermBindingService.getInstance().registerCleanupAuditListener(failingListener);
    GlossaryTermBindingService.getInstance().registerCleanupAuditListener(secondListener);
    try {
      auditRecords.clear();
      assertTrue(
          TableMetaService.getInstance()
              .deleteTable(NameIdentifier.of(LAKE, CAT, SCH, "listener_tbl")));
      assertEquals(1, auditRecords.size());
      assertEquals(1, secondListenerRecords.size());
      assertEquals("ListenerIsolationTerm", secondListenerRecords.get(0).termName());
    } finally {
      GlossaryTermBindingService.getInstance().unregisterCleanupAuditListener(failingListener);
      GlossaryTermBindingService.getInstance().unregisterCleanupAuditListener(secondListener);
    }
  }

  /**
   * Covers GlossaryBindingAccess across all target types, DENY precedence, unbind authorization,
   * and authorized read filtering on listObjectTerms, listMetricTerms, and listTermBindings.
   */
  @TestTemplate
  void testAuthorizationMatrixAcrossAllTargetTypesDenyPrecedenceUnbindAndReads() throws Exception {
    // Prepare terms: one APPROVED, one DEPRECATED, one private DRAFT owned by admin.
    as(
        "admin",
        () -> {
          manager.createTerm(LAKE, TermSpec.builder("AuthApproved").build());
          manager.alterTerm(
              LAKE, "AuthApproved", TermChange.setStatus(GlossaryTerm.Status.APPROVED));
          manager.createTerm(LAKE, TermSpec.builder("AuthDeprecated").build());
          manager.alterTerm(
              LAKE, "AuthDeprecated", TermChange.setStatus(GlossaryTerm.Status.APPROVED));
          manager.alterTerm(
              LAKE, "AuthDeprecated", TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
          manager.createTerm(
              LAKE,
              TermSpec.builder("AuthPrivateDraft").withStatus(GlossaryTerm.Status.DRAFT).build());
          return null;
        });

    MetadataObject catObj = MetadataObjects.of(null, CAT, MetadataObject.Type.CATALOG);
    MetadataObject schObj = MetadataObjects.of(CAT, SCH, MetadataObject.Type.SCHEMA);
    MetadataObject tblObj = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject colObj =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "customer_id"), MetadataObject.Type.COLUMN);
    MetadataObject viewObj = MetadataObjects.of(List.of(CAT, SCH, VIEW), MetadataObject.Type.VIEW);
    MetadataObject fsObj =
        MetadataObjects.of(List.of(CAT, SCH, FILESET), MetadataObject.Type.FILESET);
    MetadataObject topicObj =
        MetadataObjects.of(List.of(CAT, SCH, TOPIC), MetadataObject.Type.TOPIC);
    MetadataObject modelObj =
        MetadataObjects.of(List.of(CAT, SCH, MODEL), MetadataObject.Type.MODEL);
    MetadataObject smObj =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);

    // Roles covering every non-table target type and explicit DENY overrides.
    role("useCatOnly", SecurableObjects.ofMetalake(LAKE, List.of(Privileges.allow("USE_CATALOG"))));
    role("denyUseCat", SecurableObjects.ofCatalog(CAT, List.of(Privileges.deny("USE_CATALOG"))));
    role(
        "denySelectTbl", SecurableObjects.ofCatalog(CAT, List.of(Privileges.deny("SELECT_TABLE"))));
    role(
        "readAllAssets",
        SecurableObjects.ofMetalake(
            LAKE,
            List.of(
                Privileges.allow("USE_CATALOG"),
                Privileges.allow("USE_SCHEMA"),
                Privileges.allow("SELECT_VIEW"),
                Privileges.allow("READ_FILESET"),
                Privileges.allow("CONSUME_TOPIC"),
                Privileges.allow("USE_MODEL"),
                Privileges.allow("USE_SEMANTIC_MODEL"))));

    user("catViewerOnly", "useCatOnly", "viewDef");
    user("catOwnerUser", "viewDef");
    user("schOwnerWithCat", "useCatOnly", "viewDef");
    user("schOwnerDeniedCat", "useCatOnly", "denyUseCat", "viewDef");
    user("assetReader", "readAllAssets", "viewDef");
    user("assetReaderWriter", "readAllAssets", "createDef");
    user("viewOwnerUser", "readAllAssets", "viewDef");
    user("tableReaderNoDefView", "readTable");
    user("tableDeniedReader", "readTable", "denySelectTbl", "viewDef");

    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE, CAT),
            Entity.EntityType.CATALOG,
            NameIdentifierUtil.ofUser(LAKE, "catOwnerUser"),
            Entity.EntityType.USER);
    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE, CAT, SCH),
            Entity.EntityType.SCHEMA,
            NameIdentifierUtil.ofUser(LAKE, "schOwnerWithCat"),
            Entity.EntityType.USER);
    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE, CAT, SCH, VIEW),
            Entity.EntityType.VIEW,
            NameIdentifierUtil.ofUser(LAKE, "viewOwnerUser"),
            Entity.EntityType.USER);

    // 1. Catalog/Schema/View/Fileset/Topic/Model/SemanticModel/Column authorization checks.
    // Catalog: USE_CATALOG reader cannot bind as object admin; CATALOG::OWNER can bind APPROVED.
    as(
        "catViewerOnly",
        () -> {
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, catObj, new String[] {"AuthApproved"}, null));
          return null;
        });
    assertArrayEquals(
        new String[] {"AuthApproved"},
        as(
            "catOwnerUser",
            () -> manager.associateTerms(LAKE, catObj, new String[] {"AuthApproved"}, null)));

    // Schema: SCHEMA::OWNER with USE_CATALOG can bind APPROVED; DENY USE_CATALOG blocks read/admin.
    assertArrayEquals(
        new String[] {"AuthApproved"},
        as(
            "schOwnerWithCat",
            () -> manager.associateTerms(LAKE, schObj, new String[] {"AuthApproved"}, null)));
    OwnerMetaService.getInstance()
        .setOwner(
            NameIdentifier.of(LAKE, CAT, SCH),
            Entity.EntityType.SCHEMA,
            NameIdentifierUtil.ofUser(LAKE, "schOwnerDeniedCat"),
            Entity.EntityType.USER);
    as(
        "schOwnerDeniedCat",
        () -> {
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, schObj, new String[] {"AuthApproved"}, null));
          assertThrows(ForbiddenException.class, () -> manager.listObjectTerms(LAKE, schObj, null));
          return null;
        });

    // View/Fileset/Topic/Model/SemanticModel: reader-only cannot act as object admin;
    // reader + term-creator (assetReaderWriter) CAN bind own draft across all asset types;
    // VIEW::OWNER can bind APPROVED on view.
    as(
        "assetReader",
        () -> {
          for (MetadataObject obj : List.of(viewObj, fsObj, topicObj, modelObj)) {
            assertThrows(
                ForbiddenException.class,
                () -> manager.associateTerms(LAKE, obj, new String[] {"AuthApproved"}, null));
          }
          assertThrows(
              ForbiddenException.class,
              () ->
                  manager.associateMetricTerms(
                      LAKE, smObj, "net_revenue", new String[] {"AuthApproved"}, null));
          return null;
        });
    assertArrayEquals(
        new String[] {"AuthApproved"},
        as(
            "viewOwnerUser",
            () -> manager.associateTerms(LAKE, viewObj, new String[] {"AuthApproved"}, null)));
    as(
        "assetReaderWriter",
        () -> {
          manager.createTerm(
              LAKE, TermSpec.builder("AssetDraft").withStatus(GlossaryTerm.Status.DRAFT).build());
          for (MetadataObject obj : List.of(viewObj, fsObj, topicObj, modelObj)) {
            manager.associateTerms(LAKE, obj, new String[] {"AssetDraft"}, null);
          }
          manager.associateMetricTerms(
              LAKE, smObj, "net_revenue", new String[] {"AssetDraft"}, null);
          return null;
        });

    // Column normalization: tableAdmin (MODIFY_TABLE) can bind APPROVED on COLUMN;
    // DENY SELECT_TABLE overrides metalake ALLOW SELECT_TABLE.
    assertArrayEquals(
        new String[] {"AuthApproved"},
        as(
            "tableAdmin",
            () -> manager.associateTerms(LAKE, colObj, new String[] {"AuthApproved"}, null)));
    as(
        "tableDeniedReader",
        () -> {
          assertThrows(ForbiddenException.class, () -> manager.listObjectTerms(LAKE, tblObj, null));
          assertThrows(ForbiddenException.class, () -> manager.listObjectTerms(LAKE, colObj, null));
          return null;
        });

    // 2. Unbind authorization (T11.3/T11.4/T11.5): bind DEPRECATED and DRAFT as admin, then verify
    // tableAdmin gets 400 on DEPRECATED unbind, 404 on hidden DRAFT unbind, and
    // viewerWithTableRead gets 403 on APPROVED unbind.
    as(
        "admin",
        () -> {
          manager.associateTerms(
              LAKE,
              tblObj,
              new String[] {"AuthApproved", "AuthDeprecated", "AuthPrivateDraft"},
              null);
          manager.associateMetricTerms(
              LAKE, smObj, "net_revenue", new String[] {"AuthApproved"}, null);
          return null;
        });
    long deprecatedRevBefore = as("admin", () -> manager.getTerm(LAKE, "AuthDeprecated")).version();
    long approvedRevBefore = as("admin", () -> manager.getTerm(LAKE, "AuthApproved")).version();
    as(
        "tableAdmin",
        () -> {
          assertThrows(
              IllegalArgumentException.class,
              () -> manager.associateTerms(LAKE, tblObj, null, new String[] {"AuthDeprecated"}));
          assertThrows(
              NoSuchTermException.class,
              () -> manager.associateTerms(LAKE, tblObj, null, new String[] {"AuthPrivateDraft"}));
          return null;
        });
    as(
        "viewerWithTableRead",
        () -> {
          assertThrows(
              ForbiddenException.class,
              () -> manager.associateTerms(LAKE, tblObj, null, new String[] {"AuthApproved"}));
          return null;
        });
    assertEquals(
        deprecatedRevBefore, as("admin", () -> manager.getTerm(LAKE, "AuthDeprecated")).version());
    assertEquals(
        approvedRevBefore, as("admin", () -> manager.getTerm(LAKE, "AuthApproved")).version());
    assertEquals(
        List.of("AuthApproved", "AuthDeprecated", "AuthPrivateDraft"),
        as("admin", () -> manager.listObjectTerms(LAKE, tblObj, null)).stream()
            .map(GlossaryTermEntity::name)
            .toList());

    // 3. Read filtering on listObjectTerms, listMetricTerms, and listTermBindings.
    // Caller with table read but without VIEW_DEFINITION gets empty list on listObjectTerms.
    assertTrue(
        as("tableReaderNoDefView", () -> manager.listObjectTerms(LAKE, tblObj, null)).isEmpty());
    // Unauthorized caller gets 403 on listObjectTerms and listMetricTerms.
    as(
        "glossaryOnlyAdmin",
        () -> {
          assertThrows(ForbiddenException.class, () -> manager.listObjectTerms(LAKE, tblObj, null));
          assertThrows(
              ForbiddenException.class,
              () -> manager.listMetricTerms(LAKE, smObj, "net_revenue", null));
          return null;
        });
    // Authorized caller on listObjectTerms sees APPROVED and DEPRECATED, not hidden DRAFT.
    List<String> visibleOnTbl =
        as("viewerWithTableRead", () -> manager.listObjectTerms(LAKE, tblObj, null)).stream()
            .map(GlossaryTermEntity::name)
            .toList();
    assertEquals(List.of("AuthApproved", "AuthDeprecated"), visibleOnTbl);
    // Authorized caller on listMetricTerms sees AuthApproved, not AssetDraft owned by another user.
    List<String> visibleOnMetric =
        as("assetReader", () -> manager.listMetricTerms(LAKE, smObj, "net_revenue", null)).stream()
            .map(GlossaryTermEntity::name)
            .toList();
    assertEquals(List.of("AuthApproved"), visibleOnMetric);
    // listTermBindings filters out objects the caller cannot read: viewerWithTableRead has table +
    // catalog/schema USE, NOT view/model/semantic_model.
    List<TermBinding> filteredBindings =
        as(
            "viewerWithTableRead",
            () -> manager.listTermBindings(LAKE, "AuthApproved", null, null, null));
    assertTrue(filteredBindings.stream().anyMatch(b -> b.object().equals(tblObj)));
    assertTrue(filteredBindings.stream().anyMatch(b -> b.object().equals(colObj)));
    assertFalse(filteredBindings.stream().anyMatch(b -> b.object().equals(viewObj)));
    assertFalse(filteredBindings.stream().anyMatch(b -> b.kind() == TermBinding.Kind.USED_BY));
    // Hidden root term in listTermBindings returns 404; invalid types fail fast with 400.
    as(
        "viewerWithTableRead",
        () -> {
          assertThrows(
              NoSuchTermException.class,
              () -> manager.listTermBindings(LAKE, "AuthPrivateDraft", null, null, null));
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  manager.listTermBindings(
                      LAKE, "AuthApproved", null, Set.of(MetadataObject.Type.METALAKE), null));
          return null;
        });
  }

  /**
   * Covers leaf drop cleanups (view, fileset, topic, model, semantic model, semantic model
   * overwrite), non-cascade schema/catalog drops, and failed semantic-model metric alter rollback
   * (T11.15).
   */
  @TestTemplate
  void testAllLeafAndNonCascadeDropCleanupsAndFailedMetricAlterRollback() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("LeafCleanupTerm").build());

    // 1. Leaf drops: View, Fileset, Topic, Model.
    String vName = "drop_view";
    String fName = "drop_fileset";
    String tName = "drop_topic";
    String mName = "drop_model";
    backend.insert(createViewEntity(ids.incrementAndGet(), schNs, vName), false);
    backend.insert(createFilesetEntity(ids.incrementAndGet(), schNs, fName, AUDIT_INFO), false);
    backend.insert(createTopicEntity(ids.incrementAndGet(), schNs, tName, AUDIT_INFO), false);
    backend.insert(
        createModelEntity(ids.incrementAndGet(), schNs, mName, "c", 0, Map.of(), AUDIT_INFO),
        false);
    openManager.associateTerms(
        LAKE,
        MetadataObjects.of(List.of(CAT, SCH, vName), MetadataObject.Type.VIEW),
        new String[] {"LeafCleanupTerm"},
        null);
    openManager.associateTerms(
        LAKE,
        MetadataObjects.of(List.of(CAT, SCH, fName), MetadataObject.Type.FILESET),
        new String[] {"LeafCleanupTerm"},
        null);
    openManager.associateTerms(
        LAKE,
        MetadataObjects.of(List.of(CAT, SCH, tName), MetadataObject.Type.TOPIC),
        new String[] {"LeafCleanupTerm"},
        null);
    openManager.associateTerms(
        LAKE,
        MetadataObjects.of(List.of(CAT, SCH, mName), MetadataObject.Type.MODEL),
        new String[] {"LeafCleanupTerm"},
        null);

    long rev = openManager.getTerm(LAKE, "LeafCleanupTerm").version();
    auditRecords.clear();
    assertTrue(backend.delete(NameIdentifier.of(schNs, vName), Entity.EntityType.VIEW, false));
    assertTrue(backend.delete(NameIdentifier.of(schNs, fName), Entity.EntityType.FILESET, false));
    assertTrue(backend.delete(NameIdentifier.of(schNs, tName), Entity.EntityType.TOPIC, false));
    assertTrue(backend.delete(NameIdentifier.of(schNs, mName), Entity.EntityType.MODEL, false));
    assertEquals(rev + 4, openManager.getTerm(LAKE, "LeafCleanupTerm").version());
    assertEquals(4, auditRecords.size());

    // 2. Semantic model overwrite removing a metric, failed metric alter rollback (T11.15), and
    // semantic model drop.
    String smName = "leaf_sm";
    long smId = ids.incrementAndGet();
    SemanticModelMetaService.getInstance()
        .insertSemanticModel(semanticModel(smId, smName, List.of("m_keep", "m_drop")), false);
    MetadataObject smObj =
        MetadataObjects.of(List.of(CAT, SCH, smName), MetadataObject.Type.SEMANTIC_MODEL);
    openManager.associateMetricTerms(LAKE, smObj, "m_keep", new String[] {"LeafCleanupTerm"}, null);
    openManager.associateMetricTerms(LAKE, smObj, "m_drop", new String[] {"LeafCleanupTerm"}, null);

    // Overwrite removing m_drop cleans up only m_drop's binding.
    rev = openManager.getTerm(LAKE, "LeafCleanupTerm").version();
    auditRecords.clear();
    SemanticModelMetaService.getInstance()
        .insertSemanticModel(semanticModel(smId, smName, List.of("m_keep")), true);
    assertEquals(rev + 1, openManager.getTerm(LAKE, "LeafCleanupTerm").version());
    assertEquals(1, auditRecords.size());
    assertEquals("OVERWRITE_SEMANTIC_MODEL", auditRecords.get(0).triggeringOperation());
    assertEquals(1, openManager.listTermBindings(LAKE, "LeafCleanupTerm", null, null, null).size());

    // Rolled-back metric alter leaves m_keep binding intact and emits no audit record.
    rev = openManager.getTerm(LAKE, "LeafCleanupTerm").version();
    auditRecords.clear();
    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> {
                  try {
                    SemanticModelMetaService.getInstance()
                        .updateSemanticModel(
                            NameIdentifier.of(schNs, smName),
                            (SemanticModelEntity old) ->
                                semanticModel(old.id(), old.name(), List.of("m_other")));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                },
                () -> {
                  throw new IllegalStateException("simulated metric alter failure");
                }));
    assertEquals(rev, openManager.getTerm(LAKE, "LeafCleanupTerm").version());
    assertEquals(1, openManager.listTermBindings(LAKE, "LeafCleanupTerm", null, null, null).size());
    assertTrue(auditRecords.isEmpty());

    // Dropping the semantic model cleans up remaining m_keep binding.
    assertTrue(
        SemanticModelMetaService.getInstance()
            .deleteSemanticModel(NameIdentifier.of(schNs, smName)));
    assertEquals(rev + 1, openManager.getTerm(LAKE, "LeafCleanupTerm").version());
    assertEquals(1, auditRecords.size());
    assertEquals("DROP_SEMANTIC_MODEL", auditRecords.get(0).triggeringOperation());

    // 3. Non-cascade schema drop and non-cascade catalog drop clean up their own bindings.
    String ncCat = "non_cascade_cat";
    String ncSch = "non_cascade_sch";
    createAndInsertCatalog(LAKE, ncCat);
    createAndInsertSchema(LAKE, ncCat, ncSch);
    openManager.associateTerms(
        LAKE,
        MetadataObjects.of(ncCat, ncSch, MetadataObject.Type.SCHEMA),
        new String[] {"LeafCleanupTerm"},
        null);
    openManager.associateTerms(
        LAKE,
        MetadataObjects.of(null, ncCat, MetadataObject.Type.CATALOG),
        new String[] {"LeafCleanupTerm"},
        null);
    rev = openManager.getTerm(LAKE, "LeafCleanupTerm").version();
    auditRecords.clear();
    assertTrue(
        SchemaMetaService.getInstance().deleteSchema(NameIdentifier.of(LAKE, ncCat, ncSch), false));
    assertTrue(
        CatalogMetaService.getInstance().deleteCatalog(NameIdentifier.of(LAKE, ncCat), false));
    assertEquals(rev + 2, openManager.getTerm(LAKE, "LeafCleanupTerm").version());
    assertEquals(2, auditRecords.size());
    assertTrue(openManager.listTermBindings(LAKE, "LeafCleanupTerm", null, null, null).isEmpty());
  }

  /**
   * Regression test: racing MetalakeMetaService.deleteMetalake(cascade=true) against concurrent
   * leaf deletes (view, model, semantic model) sharing a bound term must never AB-BA deadlock.
   */
  @TestTemplate
  void testConcurrentMetalakeCascadeDropAndLeafDropNoDeadlock() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(60),
          () -> {
            for (int round = 0; round < 9; round++) {
              String tempLake = "ml_race_" + round;
              String tempCat = "cat_" + round;
              String tempSch = "sch_" + round;
              createAndInsertMakeLake(tempLake);
              createAndInsertCatalog(tempLake, tempCat);
              createAndInsertSchema(tempLake, tempCat, tempSch);
              Namespace tempSchNs = Namespace.of(tempLake, tempCat, tempSch);
              openManager.createTerm(tempLake, TermSpec.builder("MlRaceTerm").build());

              int mode = round % 3;
              NameIdentifier leafIdent;
              if (mode == 0) {
                backend.insert(createViewEntity(ids.incrementAndGet(), tempSchNs, "v_race"), false);
                leafIdent = NameIdentifier.of(tempSchNs, "v_race");
                openManager.associateTerms(
                    tempLake,
                    MetadataObjects.of(
                        List.of(tempCat, tempSch, "v_race"), MetadataObject.Type.VIEW),
                    new String[] {"MlRaceTerm"},
                    null);
              } else if (mode == 1) {
                backend.insert(
                    createModelEntity(
                        ids.incrementAndGet(), tempSchNs, "m_race", "c", 0, Map.of(), AUDIT_INFO),
                    false);
                leafIdent = NameIdentifier.of(tempSchNs, "m_race");
                openManager.associateTerms(
                    tempLake,
                    MetadataObjects.of(
                        List.of(tempCat, tempSch, "m_race"), MetadataObject.Type.MODEL),
                    new String[] {"MlRaceTerm"},
                    null);
              } else {
                SemanticModelMetaService.getInstance()
                    .insertSemanticModel(
                        SemanticModelEntity.builder()
                            .withId(ids.incrementAndGet())
                            .withName("sm_race")
                            .withNamespace(tempSchNs)
                            .withComment("race")
                            .withDefinition(
                                SemanticModelDefinition.builder()
                                    .withDatasets(
                                        new Dataset[] {
                                          Dataset.builder()
                                              .withName("d")
                                              .withSource(NameIdentifier.of(tempCat, tempSch, "t"))
                                              .build()
                                        })
                                    .withMetrics(
                                        new Metric[] {
                                          Metric.builder()
                                              .withName("rev")
                                              .withExpression(
                                                  Expression.builder()
                                                      .withDialects(
                                                          new DialectExpression[] {
                                                            DialectExpression.builder()
                                                                .withDialect("ansi")
                                                                .withExpression("COUNT(*)")
                                                                .build()
                                                          })
                                                      .build())
                                              .build()
                                        })
                                    .build())
                            .withProperties(Map.of())
                            .withAuditInfo(AUDIT_INFO)
                            .build(),
                        false);
                leafIdent = NameIdentifier.of(tempSchNs, "sm_race");
                openManager.associateMetricTerms(
                    tempLake,
                    MetadataObjects.of(
                        List.of(tempCat, tempSch, "sm_race"), MetadataObject.Type.SEMANTIC_MODEL),
                    "rev",
                    new String[] {"MlRaceTerm"},
                    null);
              }

              CyclicBarrier barrier = new CyclicBarrier(2);
              Future<?> metalakeDrop =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          MetalakeMetaService.getInstance()
                              .deleteMetalake(NameIdentifier.of(tempLake), true);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              Future<?> leafDrop =
                  pool.submit(
                      () -> {
                        try {
                          barrier.await(10, TimeUnit.SECONDS);
                          if (mode == 0) {
                            ViewMetaService.getInstance().deleteView(leafIdent);
                          } else if (mode == 1) {
                            backend.delete(leafIdent, Entity.EntityType.MODEL, false);
                          } else {
                            SemanticModelMetaService.getInstance().deleteSemanticModel(leafIdent);
                          }
                        } catch (NoSuchEntityException expectedWhenMetalakeWins) {
                          // Metalake cascade won first.
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
              metalakeDrop.get();
              leafDrop.get();
              GlossaryInvariants.assertBindings();
            }
          });
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * Regression test: overwriting a table name with a different table ID (stale registration
   * replacement) cleans up stale table and column bindings cleanly after the incoming table upsert.
   */
  @TestTemplate
  void testTableOverwriteStaleRegistrationCleansUpBindings() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("StaleTblTerm").build());
    insertTable("stale_tbl", List.of(col("old_col", 0)));
    MetadataObject staleTblObj =
        MetadataObjects.of(List.of(CAT, SCH, "stale_tbl"), MetadataObject.Type.TABLE);
    MetadataObject staleColObj =
        MetadataObjects.of(List.of(CAT, SCH, "stale_tbl", "old_col"), MetadataObject.Type.COLUMN);
    openManager.associateTerms(LAKE, staleTblObj, new String[] {"StaleTblTerm"}, null);
    openManager.associateTerms(LAKE, staleColObj, new String[] {"StaleTblTerm"}, null);
    long revBefore = openManager.getTerm(LAKE, "StaleTblTerm").version();
    auditRecords.clear();

    // Insert a brand-new table ID with the same name and overwrite=true.
    TableEntity replacement =
        TableEntity.builder()
            .withId(ids.incrementAndGet())
            .withName("stale_tbl")
            .withNamespace(schNs)
            .withColumns(List.of(col("new_col", 0)))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(replacement, true);

    assertTrue(openManager.listTermBindings(LAKE, "StaleTblTerm", null, null, null).isEmpty());
    assertEquals(revBefore + 1, openManager.getTerm(LAKE, "StaleTblTerm").version());
    assertEquals(1, auditRecords.size());
    assertEquals("DROP_TABLE", auditRecords.get(0).triggeringOperation());
    assertEquals(2, auditRecords.get(0).lostBindings().size());
  }

  @TestTemplate
  void testColdTableImportBeforeOpeningBindingTransaction() throws Exception {
    NameIdentifier ident = NameIdentifier.of(schNs, "external");
    MetadataObject table =
        MetadataObjects.of(List.of(CAT, SCH, "external"), MetadataObject.Type.TABLE);
    Mockito.when(env.internalTableDispatcher().tableExists(ident))
        .thenAnswer(
            inv -> {
              assertFalse(SqlSessions.hasActiveSession());
              insertTable("external", List.of(col("id", 0)));
              return true;
            });
    openManager.createTerm(LAKE, TermSpec.builder("Imported").build());
    openManager.associateTerms(LAKE, table, new String[] {"Imported"}, null);
    assertEquals(
        List.of("Imported"),
        openManager.listObjectTerms(LAKE, table, null).stream()
            .map(GlossaryTermEntity::name)
            .toList());
    Mockito.verify(env.internalTableDispatcher()).tableExists(ident);
  }

  @TestTemplate
  void testAuthorizerWithoutResolvedTargetSupportRejectsBindingBeforeImport() throws Exception {
    GravitinoAuthorizer legacy =
        Mockito.mock(GravitinoAuthorizer.class, AdditionalAnswers.delegatesTo(authorizer));
    Mockito.doCallRealMethod().when(legacy).supportsResolvedTargetAuthorization();
    env.setGravitinoAuthorizer(legacy);
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("Restricted").build()));
    MetadataObject cold = MetadataObjects.of(List.of(CAT, SCH, "cold"), MetadataObject.Type.TABLE);
    MetadataObject live = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "lakeOwner",
                () -> manager.associateTerms(LAKE, cold, new String[] {"Restricted"}, null)));
    assertThrows(
        ForbiddenException.class,
        () -> as("lakeOwner", () -> manager.listObjectTerms(LAKE, live, null)));
    assertThrows(
        ForbiddenException.class,
        () ->
            as("lakeOwner", () -> manager.listTermBindings(LAKE, "Restricted", null, null, null)));
    assertThrows(
        ForbiddenException.class,
        () ->
            as(
                "lakeOwner",
                () ->
                    manager.listTermBindings(
                        LAKE,
                        "Restricted",
                        TermBinding.Kind.IMPLEMENTED_BY,
                        Set.of(MetadataObject.Type.SEMANTIC_MODEL),
                        null)));
    Mockito.verifyNoInteractions(env.internalTableDispatcher());
    assertTrue(openManager.listObjectTerms(LAKE, live, null).isEmpty());
    GlossaryTermViews views = new GlossaryTermViews(manager);
    List<String> words = List.of("Restricted");
    assertEquals(
        0,
        as("lakeOwner", () -> views.listTerms(LAKE, null, null, null, List.of("absent"), true))
            .length);
    assertEquals(
        1, as("lakeOwner", () -> views.listTerms(LAKE, null, null, null, words, false)).length);
    assertThrows(
        ForbiddenException.class,
        () -> as("lakeOwner", () -> views.listTerms(LAKE, null, null, null, words, true)));
    openManager.associateTerms(LAKE, live, new String[] {"Restricted"}, null);
    assertThrows(
        ForbiddenException.class,
        () -> as("lakeOwner", () -> views.listTerms(LAKE, null, null, null, words, true)));
  }

  @TestTemplate
  void testOwnerReadFromOldSnapshotDoesNotChangeLaterAuthorization() throws Exception {
    NameIdentifier table = NameIdentifier.of(schNs, TBL);
    MetadataObject target = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    OwnerMetaService owners = OwnerMetaService.getInstance();
    owners.setOwner(
        table,
        Entity.EntityType.TABLE,
        NameIdentifierUtil.ofUser(LAKE, "writer"),
        Entity.EntityType.USER);
    ExecutorService changes = Executors.newSingleThreadExecutor();
    try {
      SqlSessions.readSnapshot(
          () -> {
            // Establish the binding reader's snapshot before another connection changes the owner.
            assertEquals(TBL, TableMetaService.getInstance().getTableByIdentifier(table).name());
            Future<?> updated =
                changes.submit(
                    () -> {
                      owners.setOwner(
                          table,
                          Entity.EntityType.TABLE,
                          NameIdentifierUtil.ofUser(LAKE, "tableAdmin"),
                          Entity.EntityType.USER);
                      authorizer.handleMetadataOwnerChange(
                          LAKE, null, table, Entity.EntityType.TABLE);
                    });
            try {
              updated.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new AssertionError(e);
            }
            assertTrue(
                authorizer.isOwner(
                    new UserPrincipal("writer"), LAKE, target, new AuthorizationRequestContext()));
            return null;
          });
      assertFalse(
          authorizer.isOwner(
              new UserPrincipal("writer"), LAKE, target, new AuthorizationRequestContext()));
      assertTrue(
          authorizer.isOwner(
              new UserPrincipal("tableAdmin"), LAKE, target, new AuthorizationRequestContext()));
    } finally {
      changes.shutdownNow();
    }
  }

  @TestTemplate
  void testListBindingsFindsVisibleTargetsAfterHiddenCandidates() throws Exception {
    user("metricReader", "viewDef", "readModel");
    GlossaryTermEntity term =
        openManager.createTerm(
            LAKE,
            TermSpec.builder("RestrictedTargets").withStatus(GlossaryTerm.Status.APPROVED).build());
    List<ColumnEntity> columns =
        IntStream.range(0, 1001).mapToObj(i -> col("col_" + i, i)).toList();
    insertTable("wide", columns);
    long lake = MetalakeMetaService.getInstance().getMetalakeIdByName(LAKE);
    SessionUtils.doMultipleWithCommit(
        () ->
            columns.forEach(
                column ->
                    GlossaryTermBindingService.getInstance()
                        .add(lake, term.id(), TargetType.COLUMN, column.id())));
    MetadataObject model =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    openManager.associateMetricTerms(
        LAKE, model, "net_revenue", new String[] {"RestrictedTargets"}, null);
    assertEquals(
        List.of(TermBinding.usedBy(model, "net_revenue")),
        as(
            "metricReader",
            () -> manager.listTermBindings(LAKE, "RestrictedTargets", null, null, null)));
    assertTrue(
        as(
                "metricReader",
                () ->
                    manager.listTermBindings(
                        LAKE, "RestrictedTargets", null, Set.of(MetadataObject.Type.COLUMN), null))
            .isEmpty());
    assertThrows(
        NoSuchTermException.class,
        () ->
            as(
                "metricReader",
                () ->
                    manager.listTermBindings(
                        LAKE, "RestrictedTargets", null, null, GlossaryTerm.Status.DRAFT)));
    GlossaryTermViews views = new GlossaryTermViews(manager);
    List<String> match = List.of("RestrictedTargets");
    openManager.createTerm(LAKE, TermSpec.builder("EmptyOne").build());
    openManager.createTerm(LAKE, TermSpec.builder("EmptyTwo").build());
    List<Integer> fetched = new ArrayList<>();
    List<Integer> probes = new ArrayList<>();
    try (MockedStatic<SqlSessions> ignored = observeBindingReads(fetched, probes)) {
      TermDTO preview =
          new GlossaryTermViews(openManager).listTerms(LAKE, null, null, null, match, true)[0];
      assertEquals(20, preview.getBindings().size());
      assertEquals(Boolean.TRUE, preview.getBindingsTruncated());
      assertEquals(List.of(21), fetched);
      assertEquals(List.of(), probes);
      fetched.clear();
      TermDTO[] mixed =
          new GlossaryTermViews(openManager)
              .listTerms(
                  LAKE,
                  null,
                  null,
                  null,
                  List.of("RestrictedTargets", "EmptyOne", "EmptyTwo"),
                  true);
      assertEquals(
          List.of(20, 0, 0), Arrays.stream(mixed).map(t -> t.getBindings().size()).toList());
      assertEquals(List.of(21), fetched);
      assertEquals(List.of(1), probes);
    }
    LookupTooBroadException perTerm = assertLookupStopsAtBindingBudget(views, match, 1000);
    assertEquals(
        "Glossary lookup is too broad. Omit embedded bindings or narrow the query.",
        perTerm.getMessage());
    TermDTO definition =
        as("metricReader", () -> views.listTerms(LAKE, null, null, null, match, false))[0];
    assertEquals("RestrictedTargets", definition.getName());
    assertNull(definition.getBindings());
    assertNull(definition.getBindingsTruncated());

    // Each root can prove that exactly 1,000 hidden targets exhaust its own list.
    // Ten such roots together exceed the request budget once the roots themselves are counted.
    for (int i = 0; i < 10; i++) {
      openManager.createTerm(LAKE, TermSpec.builder("Shared" + i).withSynonyms("group").build());
    }
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement insert =
            session
                .getConnection()
                .prepareStatement(
                    "INSERT INTO glossary_term_binding (metalake_id, term_id, target_type, target_id, deleted_at) "
                        + "SELECT t.metalake_id, t.term_id, b.target_type, b.target_id, 0 FROM glossary_term_meta t "
                        + "JOIN glossary_term_binding b ON b.metalake_id=t.metalake_id "
                        + "WHERE t.name LIKE 'Shared%' AND b.term_id=? AND b.target_type='COLUMN' AND b.target_id<=?")) {
      insert.setLong(1, term.id());
      insert.setLong(2, columns.get(999).id());
      assertEquals(10000, insert.executeUpdate());
      session.commit(true);
    }
    TermDTO allHidden =
        as("metricReader", () -> views.listTerms(LAKE, null, null, null, List.of("Shared0"), true))[
            0];
    assertEquals(List.of(), allHidden.getBindings());
    assertEquals(Boolean.FALSE, allHidden.getBindingsTruncated());
    LookupTooBroadException total = assertLookupStopsAtBindingBudget(views, List.of("group"), 9990);
    assertEquals(perTerm.getMessage(), total.getMessage());
    assertEquals(
        10,
        as("metricReader", () -> views.listTerms(LAKE, null, null, null, List.of("group"), false))
            .length);

    GlossaryManager hidden = manager.withTermVisibility(ignored -> false);
    assertThrows(
        NoSuchTermException.class,
        () -> hidden.listTermBindings(LAKE, "RestrictedTargets", null, null, null));
  }

  @TestTemplate
  void testLookupBatchesCandidatesAndTargetsAcrossTerms() throws Exception {
    List<ColumnEntity> columns = IntStream.range(0, 43).mapToObj(i -> col("col_" + i, i)).toList();
    insertTable("preview_targets", columns);
    long lake = MetalakeMetaService.getInstance().getMetalakeIdByName(LAKE);
    List<Long> termIds = new ArrayList<>();
    for (ColumnEntity column : columns) {
      GlossaryTermEntity term =
          openManager.createTerm(
              LAKE, TermSpec.builder(column.name()).withSynonyms("shared").build());
      termIds.add(term.id());
      SessionUtils.doMultipleWithCommit(
          () ->
              GlossaryTermBindingService.getInstance()
                  .add(lake, term.id(), TargetType.COLUMN, column.id()));
    }
    List<Integer> fetched = new ArrayList<>();
    List<Integer> probes = new ArrayList<>();
    List<Integer> resolved = new ArrayList<>();
    try (MockedStatic<SqlSessions> reads = observeBindingReads(fetched, probes);
        MockedStatic<MetadataObjectService> objects =
            Mockito.mockStatic(
                MetadataObjectService.class,
                call -> {
                  if (call.getMethod().getName().equals("resolveMetadataObjects")) {
                    resolved.add(call.<List<Long>>getArgument(1).size());
                  }
                  return call.callRealMethod();
                })) {
      TermDTO[] terms =
          new GlossaryTermViews(openManager)
              .listTerms(LAKE, null, null, null, List.of("shared"), true);
      assertEquals(columns.size(), terms.length);
      for (TermDTO term : terms) {
        assertEquals(1, term.getBindings().size());
        assertEquals(term.getName(), term.getBindings().get(0).getObject().name());
        assertEquals(Boolean.FALSE, term.getBindingsTruncated());
      }
      assertEquals(List.of(43), probes);
      assertTrue(fetched.size() <= 5, fetched::toString);
      assertEquals(columns.size(), fetched.stream().mapToInt(Integer::intValue).sum());
      assertTrue(resolved.size() <= 3, resolved::toString);
      assertEquals(columns.size(), resolved.stream().mapToInt(Integer::intValue).sum());
    }

    // The first page ends on the first binding of the 21st term. Its 20 additional bindings
    // cross the cursor boundary. Its neighbors must still receive only their own targets.
    SessionUtils.doMultipleWithCommit(
        () -> {
          for (ColumnEntity column : columns.subList(0, 20)) {
            GlossaryTermBindingService.getInstance()
                .add(lake, termIds.get(20), TargetType.COLUMN, column.id());
          }
        });
    TermDTO[] mixed =
        new GlossaryTermViews(openManager)
            .listTerms(LAKE, null, null, null, List.of("shared"), true);
    assertEquals(columns.size(), mixed.length);
    for (TermDTO term : mixed) {
      boolean spansPages = term.getName().equals(columns.get(20).name());
      List<String> expected =
          spansPages
              ? columns.subList(0, 20).stream().map(ColumnEntity::name).toList()
              : List.of(term.getName());
      assertEquals(expected, term.getBindings().stream().map(b -> b.getObject().name()).toList());
      assertEquals(spansPages, term.getBindingsTruncated());
    }
  }

  @TestTemplate
  void testLookupPreviewCountsVisibleBindingsAcrossBatches() throws Exception {
    List<ColumnEntity> hidden = new ArrayList<>();
    List<ColumnEntity> visible = new ArrayList<>();
    for (int i = 0; i < 21; i++) {
      int hiddenBefore = i == 0 ? 20 : 3;
      for (int j = 0; j < hiddenBefore; j++) {
        hidden.add(col("hidden_" + hidden.size(), hidden.size()));
      }
      visible.add(col("visible_" + i, i));
    }
    insertTable("hidden", hidden);
    insertTable("visible", visible);
    role(
        "denyHidden",
        SecurableObjects.parse(
            CAT + "." + SCH + ".hidden",
            MetadataObject.Type.TABLE,
            List.of(Privileges.deny("SELECT_TABLE"))));
    user("limitedColumns", "viewDef", "readTable", "denyHidden");
    long lake = MetalakeMetaService.getInstance().getMetalakeIdByName(LAKE);
    GlossaryTermEntity term = openManager.createTerm(LAKE, TermSpec.builder("ColumnTerm").build());
    GlossaryTermBindingService bindings = GlossaryTermBindingService.getInstance();
    SessionUtils.doMultipleWithCommit(
        () ->
            hidden.forEach(
                column -> bindings.add(lake, term.id(), TargetType.COLUMN, column.id())));
    GlossaryTermViews views = new GlossaryTermViews(manager);
    int previous = 0;
    for (int n : List.of(0, 1, 20, 21)) {
      List<ColumnEntity> added = visible.subList(previous, n);
      SessionUtils.doMultipleWithCommit(
          () ->
              added.forEach(
                  column -> bindings.add(lake, term.id(), TargetType.COLUMN, column.id())));
      TermDTO result =
          as(
              "limitedColumns",
              () -> views.listTerms(LAKE, null, null, null, List.of(term.name()), true))[0];
      assertEquals(
          visible.subList(0, Math.min(20, n)).stream().map(ColumnEntity::name).toList(),
          result.getBindings().stream().map(binding -> binding.getObject().name()).toList());
      assertEquals(n == 21, result.getBindingsTruncated());
      previous = n;
    }

    // The metric has a lower ID than the columns on the preceding pages.
    MetadataObject model =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    openManager.associateMetricTerms(LAKE, model, "net_revenue", new String[] {term.name()}, null);
    user("onlyMetrics", "viewDef", "readModel");
    TermDTO metrics =
        as(
            "onlyMetrics",
            () -> views.listTerms(LAKE, null, null, null, List.of(term.name()), true))[0];
    assertEquals(
        List.of("net_revenue"),
        metrics.getBindings().stream().map(TermBindingDTO::getMetric).toList());
    assertEquals(Boolean.FALSE, metrics.getBindingsTruncated());

    openManager.createTerm(LAKE, TermSpec.builder("Second").build());
    openManager.associateMetricTerms(LAKE, model, "net_revenue", new String[] {"Second"}, null);
    // Two roots, all columns through the 21st visible one, then the second root's hidden metric.
    // The first root's trailing metric must not consume the second root's budget.
    List<Integer> fetched = new ArrayList<>();
    try (MockedConstruction<TermLookupBudget> ignored =
            limitLookupTo(2 + hidden.size() + visible.size() + 1);
        MockedStatic<SqlSessions> reads = observeBindingReads(fetched, new ArrayList<>())) {
      TermDTO[] both =
          as(
              "limitedColumns",
              () -> views.listTerms(LAKE, null, null, null, List.of(term.name(), "Second"), true));
      assertEquals(2, both.length);
      assertEquals(Boolean.TRUE, both[0].getBindingsTruncated());
      assertEquals(20, both[0].getBindings().size());
      assertEquals(List.of(), both[1].getBindings());
      assertEquals(Boolean.FALSE, both[1].getBindingsTruncated());
      assertEquals(List.of(21, 21, 21, 21, 18, 1), fetched);
    }
  }

  @TestTemplate
  void testLookupBudgetCountsLiteralNameMatchesAcrossCandidatePages() {
    for (String name : List.of("Sales Team A", "Sales Team B", "Sales  Team C")) {
      openManager.createTerm(LAKE, TermSpec.builder(name).withSynonyms("group").build());
    }
    GlossaryTermViews views = new GlossaryTermViews(openManager);
    try (MockedConstruction<TermLookupBudget> ignored = limitLookupTo(1)) {
      assertArrayEquals(
          new String[] {"Sales  Team C"},
          views.listTermNames(LAKE, null, "Sales  Team", null, List.of("group")));
    }
    try (MockedConstruction<TermLookupBudget> ignored = limitLookupTo(1)) {
      assertThrows(
          LookupTooBroadException.class,
          () -> views.listTermNames(LAKE, null, "Sales Team", null, List.of("group")));
    }
  }

  @TestTemplate
  void testStatusFilteredRelationsUseOnlyTheEligibleLookupBudget() {
    GlossaryTerm.Status old = GlossaryTerm.Status.DEPRECATED;
    openManager.createTerm(LAKE, TermSpec.builder("Parent").build());
    openManager.createTerm(LAKE, TermSpec.builder("OldParent").withStatus(old).build());
    openManager.createTerm(LAKE, TermSpec.builder("Peer").build());
    openManager.createTerm(
        LAKE,
        TermSpec.builder("Root").withBroader("Parent", "OldParent").withRelated("Peer").build());
    openManager.createTerm(LAKE, TermSpec.builder("Child").withBroader("Root").build());
    openManager.createTerm(
        LAKE, TermSpec.builder("OldChild").withStatus(old).withBroader("Root").build());
    openManager.createTerm(
        LAKE, TermSpec.builder("OldPeer").withStatus(old).withRelated("Root").build());
    GlossaryTermViews views = new GlossaryTermViews(openManager);
    try (MockedConstruction<TermLookupBudget> ignored = limitLookupTo(4)) {
      TermDTO root =
          views
              .listTerms(LAKE, null, null, GlossaryTerm.Status.APPROVED, List.of("Root"), false)[0];
      assertArrayEquals(new String[] {"Parent"}, root.getBroader());
      assertArrayEquals(new String[] {"Child"}, root.getNarrower());
      assertArrayEquals(new String[] {"Peer"}, root.getRelated());
    }
    try (MockedConstruction<TermLookupBudget> ignored = limitLookupTo(4)) {
      assertThrows(
          LookupTooBroadException.class,
          () -> views.listTerms(LAKE, null, null, null, List.of("Root"), false));
    }
  }

  @TestTemplate
  void testReverseBindingChildrenAndDomainVisibility() throws Exception {
    String modelName = "three_metrics";
    SemanticModelMetaService.getInstance()
        .insertSemanticModel(
            semanticModel(ids.incrementAndGet(), modelName, List.of("one", "two", "three")), false);
    MetadataObject model =
        MetadataObjects.of(List.of(CAT, SCH, modelName), MetadataObject.Type.SEMANTIC_MODEL);
    MetadataObject table = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject view = MetadataObjects.of(List.of(CAT, SCH, VIEW), MetadataObject.Type.VIEW);
    GlossaryDomainFixtures.insertDomain(LAKE, "Visible", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    GlossaryDomainFixtures.insertDomain(LAKE, "Private", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    role(
        "readVisible",
        SecurableObjects.parse(
            "Visible", MetadataObject.Type.DOMAIN, List.of(Privileges.UseDomain.allow())));
    role("readView", SecurableObjects.ofMetalake(LAKE, List.of(Privileges.allow("SELECT_VIEW"))));
    user("limited", "manageDef", "readTable", "readModel", "readVisible", "readView");
    user("viewerOnly", "viewDef");
    openManager.createTerm(
        LAKE,
        TermSpec.builder("Approved")
            .withStatus(GlossaryTerm.Status.APPROVED)
            .withDomains("Visible", "Private")
            .build());
    openManager.createTerm(
        LAKE,
        TermSpec.builder("Other")
            .withStatus(GlossaryTerm.Status.APPROVED)
            .withDomains("Private")
            .build());
    openManager.createTerm(
        LAKE, TermSpec.builder("Draft").withStatus(GlossaryTerm.Status.DRAFT).build());
    String[] all = {"Approved", "Other", "Draft"};
    openManager.associateTerms(LAKE, table, all, null);
    openManager.associateTerms(LAKE, view, all, null);
    String[] columns = {"customer_id", "email", "is_active"};
    String[] metrics = {"one", "two", "three"};
    for (int i = 0; i < 3; i++) {
      MetadataObject col =
          MetadataObjects.of(List.of(CAT, SCH, TBL, columns[i]), MetadataObject.Type.COLUMN);
      openManager.associateTerms(LAKE, col, new String[] {all[i]}, null);
      openManager.associateMetricTerms(LAKE, model, metrics[i], new String[] {all[i]}, null);
    }
    GlossaryTermViews views = new GlossaryTermViews(manager);
    TermDTO[] matches =
        as(
            "limited",
            () ->
                views.listTerms(
                    LAKE,
                    null,
                    null,
                    GlossaryTerm.Status.APPROVED,
                    List.of("Approved", "Other"),
                    true));
    assertEquals(2, matches.length);
    assertArrayEquals(new String[] {"Visible"}, matches[0].getDomains());
    assertArrayEquals(new String[0], matches[1].getDomains());
    for (TermDTO match : matches) {
      assertEquals(4, match.getBindings().size());
      assertEquals(Boolean.FALSE, match.getBindingsTruncated());
    }
    assertArrayEquals(new String[] {"Private"}, openManager.getTerm(LAKE, "Other").domains());
    List<String> calls = new ArrayList<>();
    try (MockedStatic<SqlSessions> ignored =
        Mockito.mockStatic(
            SqlSessions.class,
            lookup -> {
              Object mapper = lookup.callRealMethod();
              if (!lookup.getMethod().getName().equals("getMapper")) return mapper;
              Class<?> type = lookup.getArgument(0);
              if (type != GlossaryTermBindingMapper.class
                  && type != SemanticModelMetricMapper.class
                  && type != TableColumnMapper.class) return mapper;
              return Mockito.mock(
                  type,
                  call -> {
                    calls.add(call.getMethod().getName());
                    return AdditionalAnswers.delegatesTo(mapper).answer(call);
                  });
            })) {
      for (String user : List.of("glossaryOnlyAdmin", "viewerOnly")) {
        for (MetadataObject target : List.of(table, model)) {
          assertThrows(
              ForbiddenException.class,
              () -> as(user, () -> views.listTargetTermsWithChildren(LAKE, target, null)));
        }
        assertThrows(
            ForbiddenException.class,
            () -> as(user, () -> views.listTargetTermNames(LAKE, model, "unknown", null)));
      }
      assertEquals(List.of(), calls);
      for (MetadataObject target : List.of(table, model)) {
        calls.clear();
        boolean isTable = target.type() == MetadataObject.Type.TABLE;
        ObjectTermsResponse result =
            as(
                "limited",
                () ->
                    views.listTargetTermsWithChildren(LAKE, target, GlossaryTerm.Status.APPROVED));
        assertEquals(
            isTable ? List.of("customer_id", "email") : List.of("one", "two"),
            result.getChildren().stream().map(ObjectTermsResponse.Child::getName).toList());
        assertEquals(isTable ? 2 : 0, result.getTerms().length);
        if (isTable) assertArrayEquals(new String[] {"Visible"}, result.getTerms()[0].getDomains());
        TermDTO withDomain = result.getChildren().get(0).getTerms().get(0);
        TermDTO withoutDomain = result.getChildren().get(1).getTerms().get(0);
        assertEquals("Approved", withDomain.getName());
        assertArrayEquals(new String[] {"Visible"}, withDomain.getDomains());
        assertEquals("Other", withoutDomain.getName());
        assertArrayEquals(new String[0], withoutDomain.getDomains());
        assertEquals(
            List.of(
                isTable ? "listByTable" : "listBySemanticModel",
                isTable ? "listColumnPOsByColumnIds" : "listSemanticModelMetricsByMetricIds"),
            calls);
        assertEquals(
            3,
            as("limited", () -> views.listTargetTermsWithChildren(LAKE, target, null))
                .getChildren()
                .size());
      }
    }
    assertArrayEquals(
        new String[] {"Approved", "Other"},
        as(
            "limited",
            () -> views.listTargetTermNames(LAKE, view, null, GlossaryTerm.Status.APPROVED)));
    assertThrows(
        IllegalArgumentException.class,
        () -> as("limited", () -> views.listTargetTermsWithChildren(LAKE, view, null)));
    assertArrayEquals(
        new String[0], as("limited", () -> views.listTargetTermNames(LAKE, model, null, null)));
    assertArrayEquals(
        new String[] {"Visible"},
        as("limited", () -> views.listDirectTargetTerms(LAKE, model, "one", null))[0].getDomains());
    assertThrows(
        NoSuchMetadataObjectException.class,
        () -> as("limited", () -> views.listTargetTermNames(LAKE, model, "absent", null)));
  }

  @TestTemplate
  void testBindingListSnapshotAcrossStatusAndBindingCommits() throws Exception {
    MetadataObject table = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    MetadataObject view = MetadataObjects.of(List.of(CAT, SCH, VIEW), MetadataObject.Type.VIEW);
    openManager.createTerm(
        LAKE, TermSpec.builder("Snapshot").withStatus(GlossaryTerm.Status.APPROVED).build());
    openManager.associateTerms(LAKE, table, new String[] {"Snapshot"}, null);
    as(
        "writer",
        () ->
            manager.createTerm(
                LAKE,
                TermSpec.builder("OwnedDraft").withStatus(GlossaryTerm.Status.DRAFT).build()));
    for (String reader : List.of("writer", "admin")) {
      assertThrows(
          NoSuchTermException.class,
          () ->
              as(
                  reader,
                  () ->
                      manager.listTermBindings(
                          LAKE, "OwnedDraft", null, null, GlossaryTerm.Status.APPROVED)));
    }
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      for (boolean changeStatusFirst : List.of(false, true)) {
        GlossaryManager paused =
            manager.withTermVisibility(
                root -> {
                  try {
                    writer
                        .submit(
                            () -> {
                              if (changeStatusFirst)
                                openManager.alterTerm(
                                    LAKE,
                                    "Snapshot",
                                    TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
                              openManager.associateTerms(
                                  LAKE, view, new String[] {"Snapshot"}, null);
                              if (!changeStatusFirst)
                                openManager.alterTerm(
                                    LAKE,
                                    "Snapshot",
                                    TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
                            })
                        .get(15, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new AssertionError("Writer could not commit during binding read", e);
                  }
                  return true;
                });
        assertEquals(
            List.of(TermBinding.implementedBy(table)),
            as(
                "lakeOwner",
                () ->
                    paused.listTermBindings(
                        LAKE, "Snapshot", null, null, GlossaryTerm.Status.APPROVED)));
        assertThrows(
            NoSuchTermException.class,
            () ->
                as(
                    "lakeOwner",
                    () ->
                        manager.listTermBindings(
                            LAKE, "Snapshot", null, null, GlossaryTerm.Status.APPROVED)));
        openManager.alterTerm(LAKE, "Snapshot", TermChange.setStatus(GlossaryTerm.Status.APPROVED));
        assertEquals(
            2,
            as(
                    "lakeOwner",
                    () ->
                        manager.listTermBindings(
                            LAKE, "Snapshot", null, null, GlossaryTerm.Status.APPROVED))
                .size());
        openManager.associateTerms(LAKE, view, null, new String[] {"Snapshot"});
      }
    } finally {
      writer.shutdownNow();
    }
  }

  @TestTemplate
  void testListBindingsReturnsEveryVisibleTable() throws Exception {
    GlossaryTermEntity term =
        openManager.createTerm(
            LAKE, TermSpec.builder("ManyTables").withStatus(GlossaryTerm.Status.APPROVED).build());
    long lake = MetalakeMetaService.getInstance().getMetalakeIdByName(LAKE);
    List<String> expected = new ArrayList<>();
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                TableMetaMapper.class,
                tables -> {
                  TablePO template = tables.selectTableByFullQualifiedName(LAKE, CAT, SCH, TBL);
                  for (int i = 0; i < 1001; i++) {
                    String name = "bound_table_" + i;
                    long id = ids.incrementAndGet();
                    tables.insertTableMeta(
                        TablePO.builder(template).withTableName(name).withTableId(id).build());
                    GlossaryTermBindingService.getInstance()
                        .add(lake, term.id(), TargetType.TABLE, id);
                    expected.add(name);
                  }
                }));
    Set<MetadataObject.Type> types = Set.of(MetadataObject.Type.TABLE);
    try (MockedStatic<GlossaryTermBindingSQLProviderFactory> queries =
        Mockito.mockStatic(
            GlossaryTermBindingSQLProviderFactory.class, Mockito.CALLS_REAL_METHODS)) {
      List<TermBinding> bindings =
          as(
              "viewerWithTableRead",
              () ->
                  manager.listTermBindings(
                      LAKE, "ManyTables", TermBinding.Kind.IMPLEMENTED_BY, types, null));
      assertEquals(expected, bindings.stream().map(binding -> binding.object().name()).toList());
      queries.verify(
          () ->
              GlossaryTermBindingSQLProviderFactory.listByTerm(lake, term.id(), List.of("TABLE")));
    }
    assertTrue(
        as(
                "viewerWithTableRead",
                () ->
                    manager.listTermBindings(
                        LAKE, "ManyTables", TermBinding.Kind.USED_BY, types, null))
            .isEmpty());
  }

  @TestTemplate
  void testExactMetricIdentity() throws Exception {
    SemanticModelMetaService.getInstance()
        .insertSemanticModel(
            semanticModel(ids.incrementAndGet(), "exact_names", List.of("units", "units ")), false);
    MetadataObject target =
        MetadataObjects.of(List.of(CAT, SCH, "exact_names"), MetadataObject.Type.SEMANTIC_MODEL);
    openManager.createTerm(LAKE, TermSpec.builder("UnitsTerm").build());
    openManager.associateMetricTerms(LAKE, target, "units", new String[] {"UnitsTerm"}, null);
    assertEquals(List.of(), openManager.listMetricTerms(LAKE, target, "units ", null));
  }

  @TestTemplate
  void testBindingFailureRollsBackAndRejectsNestedWrites() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("FirstTerm").build());
    openManager.createTerm(LAKE, TermSpec.builder("SecondTerm").build());
    long before = openManager.getTerm(LAKE, "FirstTerm").version();
    AtomicInteger calls = new AtomicInteger();
    GlossaryManager failing =
        new GlossaryManager(
            ids::incrementAndGet,
            false,
            (a, b, persist) -> {
              if (calls.incrementAndGet() == 2)
                throw new IllegalStateException("second write failed");
              persist.run();
            });
    MetadataObject target = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    SessionUtils.doMultipleWithCommit(
        () ->
            assertThrows(
                IllegalStateException.class,
                () ->
                    failing.associateTerms(
                        LAKE, target, new String[] {"FirstTerm", "SecondTerm"}, null)));
    assertEquals(0, calls.get());
    assertThrows(
        IllegalStateException.class,
        () -> failing.associateTerms(LAKE, target, new String[] {"FirstTerm", "SecondTerm"}, null));
    assertEquals(2, calls.get());
    assertEquals(
        List.of(),
        openManager.listObjectTerms(LAKE, target, null).stream()
            .map(GlossaryTermEntity::name)
            .toList());
    assertEquals(before, openManager.getTerm(LAKE, "FirstTerm").version());
  }

  @TestTemplate
  void testTargetDropRecordsModifier() throws Exception {
    MetadataObject target = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    as(
        "writer",
        () ->
            openManager.createTerm(
                LAKE, TermSpec.builder("AuditTerm").withSynonyms("first", "second").build()));
    as("writer", () -> openManager.associateTerms(LAKE, target, new String[] {"AuditTerm"}, null));
    GlossaryTermEntity before = openManager.getTerm(LAKE, "AuditTerm");
    as("dropper", () -> TableMetaService.getInstance().deleteTable(NameIdentifier.of(schNs, TBL)));
    GlossaryTermEntity after = openManager.getTerm(LAKE, "AuditTerm");
    assertEquals(before.version() + 1, after.version());
    assertArrayEquals(before.synonyms(), after.synonyms());
    assertEquals("writer", after.auditInfo().creator());
    assertEquals(before.auditInfo().createTime(), after.auditInfo().createTime());
    assertFalse(
        after.auditInfo().lastModifiedTime().isBefore(before.auditInfo().lastModifiedTime()));
    assertEquals("dropper", after.auditInfo().lastModifier());
  }

  @TestTemplate
  void testBindAndReadCaseInsensitiveTarget() throws Exception {
    openManager.createTerm(LAKE, TermSpec.builder("RevenueTerm").build());
    Capability insensitive =
        new Capability() {
          @Override
          public CapabilityResult caseSensitiveOnName(Scope scope) {
            return CapabilityResult.unsupported("case insensitive");
          }
        };
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(insensitive);
    Mockito.reset(env.catalogManager());
    Mockito.when(env.catalogManager().doWithCatalog(Mockito.any(), Mockito.any()))
        .thenAnswer(
            inv -> {
              ThrowableFunction<BaseCatalog<?>, Object> action = inv.getArgument(1);
              return action.apply(catalog);
            });
    MetadataObject target =
        MetadataObjects.of(
            List.of(CAT, SCH, TBL.toUpperCase(Locale.ROOT)), MetadataObject.Type.TABLE);
    NameIdentifier canonical =
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of(schNs, TBL.toUpperCase(Locale.ROOT)),
            Capability.Scope.TABLE,
            env.catalogManager(),
            () -> env.entityStore());
    assertEquals(NameIdentifier.of(schNs, TBL), canonical);
    openManager.associateTerms(LAKE, target, new String[] {"RevenueTerm"}, null);
    assertEquals(1, openManager.listObjectTerms(LAKE, target, null).size());
  }

  @TestTemplate
  void testMetricWithUnmatchedTrailingSpace() throws Exception {
    MetadataObject target =
        MetadataObjects.of(List.of(CAT, SCH, SMODEL), MetadataObject.Type.SEMANTIC_MODEL);
    openManager.createTerm(LAKE, TermSpec.builder("MissingMetric").build());
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            openManager.associateMetricTerms(
                LAKE, target, "net_revenue ", new String[] {"MissingMetric"}, null));
  }

  @TestTemplate
  void testBindingListAuthorizationSkipsPerTargetTableNameLookups() throws Exception {
    as("lakeOwner", () -> manager.createTerm(LAKE, TermSpec.builder("BatchTerm").build()));
    for (int i = 0; i < 5; i++) {
      String tbl = "bound_tbl_" + i;
      insertTable(tbl, List.of(col("c1", 0)));
      MetadataObject tblObj = MetadataObjects.of(List.of(CAT, SCH, tbl), MetadataObject.Type.TABLE);
      as("lakeOwner", () -> manager.associateTerms(LAKE, tblObj, new String[] {"BatchTerm"}, null));
    }
    TableMetaService tableService = Mockito.spy(TableMetaService.getInstance());
    try (MockedStatic<TableMetaService> services =
        Mockito.mockStatic(TableMetaService.class, Mockito.CALLS_REAL_METHODS)) {
      services.when(TableMetaService::getInstance).thenReturn(tableService);
      List<TermBinding> bindings =
          as(
              "viewerWithTableRead",
              () -> manager.listTermBindings(LAKE, "BatchTerm", null, null, null));
      assertEquals(5, bindings.size());
      Mockito.verify(tableService, Mockito.never())
          .getTableIdBySchemaIdAndName(Mockito.anyLong(), Mockito.anyString());
    }
  }

  private static MockedStatic<SqlSessions> observeBindingReads(
      List<Integer> fetched, List<Integer> probes) {
    return Mockito.mockStatic(
        SqlSessions.class,
        lookup -> {
          Object mapper = lookup.callRealMethod();
          if (lookup.getMethod().getName().equals("getMapper")
              && lookup.getArgument(0) == GlossaryTermBindingMapper.class) {
            return Mockito.mock(
                GlossaryTermBindingMapper.class,
                call -> {
                  Object result = AdditionalAnswers.delegatesTo(mapper).answer(call);
                  if (call.getMethod().getName().equals("listBindingCandidates")) {
                    fetched.add(((List<?>) result).size());
                  } else if (call.getMethod().getName().equals("listTermsWithBindings")) {
                    probes.add(((List<?>) result).size());
                  }
                  return result;
                });
          }
          return mapper;
        });
  }

  private static MockedConstruction<TermLookupBudget> limitLookupTo(int remaining) {
    TermLookupBudget budget = new TermLookupBudget();
    budget.consume(budget.remaining() - remaining);
    return Mockito.mockConstruction(
        TermLookupBudget.class,
        Mockito.withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(budget)));
  }

  private LookupTooBroadException assertLookupStopsAtBindingBudget(
      GlossaryTermViews views, List<String> words, int expected) {
    AtomicInteger resolved = new AtomicInteger();
    try (MockedStatic<MetadataObjectService> ignored =
        Mockito.mockStatic(
            MetadataObjectService.class,
            call -> {
              if (call.getMethod().getName().equals("resolveMetadataObjects")) {
                resolved.addAndGet(call.<List<Long>>getArgument(1).size());
              }
              return call.callRealMethod();
            })) {
      LookupTooBroadException error =
          assertThrows(
              LookupTooBroadException.class,
              () -> as("metricReader", () -> views.listTerms(LAKE, null, null, null, words, true)));
      assertEquals(expected, resolved.get());
      return error;
    }
  }

  private TableEntity insertTable(String name, List<ColumnEntity> columns) throws Exception {
    TableEntity entity =
        TableEntity.builder()
            .withId(ids.incrementAndGet())
            .withName(name)
            .withNamespace(schNs)
            .withColumns(columns)
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(entity, false);
    return entity;
  }

  private ColumnEntity col(String name, int position) {
    return ColumnEntity.builder()
        .withId(ids.incrementAndGet())
        .withName(name)
        .withPosition(position)
        .withDataType(Types.StringType.get())
        .withNullable(true)
        .withAutoIncrement(false)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private SemanticModelEntity semanticModel(long id, String name, List<String> metricNames) {
    return semanticModelWithExpression(id, name, metricNames, "COUNT(*)");
  }

  private SemanticModelEntity semanticModelWithExpression(
      long id, String name, List<String> metricNames, String sqlExpression) {
    Metric[] metrics =
        metricNames.stream()
            .map(
                metricName ->
                    Metric.builder()
                        .withName(metricName)
                        .withExpression(
                            Expression.builder()
                                .withDialects(
                                    new DialectExpression[] {
                                      DialectExpression.builder()
                                          .withDialect("ansi")
                                          .withExpression(sqlExpression)
                                          .build()
                                    })
                                .build())
                        .build())
            .toArray(Metric[]::new);
    return SemanticModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(schNs)
        .withComment("test semantic model")
        .withDefinition(
            SemanticModelDefinition.builder()
                .withDatasets(
                    new Dataset[] {
                      Dataset.builder()
                          .withName("orders")
                          .withSource(NameIdentifier.of(CAT, SCH, TBL))
                          .build()
                    })
                .withMetrics(metrics)
                .build())
        .withProperties(Map.of())
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private void role(String name, SecurableObject... objects) throws Exception {
    RoleEntity role =
        RoleEntity.builder()
            .withId(ids.incrementAndGet())
            .withName(name)
            .withNamespace(NamespaceUtil.ofRole(LAKE))
            .withSecurableObjects(List.of(objects))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(role, false);
  }

  private void user(String name, String... roles) throws Exception {
    List<Long> roleIds = new ArrayList<>();
    for (String r : roles) {
      RoleEntity roleEntity =
          backend.get(NameIdentifierUtil.ofRole(LAKE, r), Entity.EntityType.ROLE);
      roleIds.add(roleEntity.id());
    }
    UserEntity user =
        UserEntity.builder()
            .withId(ids.incrementAndGet())
            .withName(name)
            .withNamespace(NamespaceUtil.ofUser(LAKE))
            .withRoleNames(List.of(roles))
            .withRoleIds(roleIds)
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(user, false);
  }

  private static <T> T as(String user, PrivilegedExceptionAction<T> action) throws Exception {
    return PrincipalUtils.doAs(new UserPrincipal(user), action);
  }
}
