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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.GlossaryInvariants;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.BindingLossAuditRecord;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.CleanupAuditListener;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.LostBinding;
import org.apache.gravitino.storage.relational.service.GlossaryTermBindingService.TargetType;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.SemanticModelMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.ViewMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
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
    AtomicInteger handlerCalls = new AtomicInteger();
    GlossaryManager hooked =
        new GlossaryManager(
            ids::incrementAndGet,
            false,
            (before, after, persist) -> {
              handlerCalls.incrementAndGet();
              persist.run();
            });
    hooked.createTerm(LAKE, TermSpec.builder("ReconcileTerm").build());
    MetadataObject colActive =
        MetadataObjects.of(List.of(CAT, SCH, TBL, "is_active"), MetadataObject.Type.COLUMN);
    hooked.associateTerms(LAKE, colActive, new String[] {"ReconcileTerm"}, null);
    assertEquals(2, handlerCalls.get()); // create + bind both went through TermChangeHandler.

    long versionBefore = hooked.getTerm(LAKE, "ReconcileTerm").version();
    NameIdentifier tableIdent = NameIdentifier.of(schNs, TBL);
    TableEntity storedTable = TableMetaService.getInstance().getTableByIdentifier(tableIdent);

    // Simulate external column drop followed by failed local store cleanup.
    auditRecords.clear();
    assertThrows(
        RuntimeException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> {
                  try {
                    TableMetaService.getInstance()
                        .updateTable(
                            tableIdent,
                            (TableEntity old) ->
                                TableEntity.builder()
                                    .withId(old.id())
                                    .withName(old.name())
                                    .withNamespace(old.namespace())
                                    .withColumns(
                                        old.columns().stream()
                                            .filter(c -> !c.name().equals("is_active"))
                                            .toList())
                                    .withAuditInfo(AUDIT_INFO)
                                    .build());
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                },
                () -> {
                  throw new IllegalStateException(
                      "Simulated local store failure after external DDL");
                }));
    // Local column, binding, and revision rolled back; no cleanup-success audit emitted.
    assertEquals(versionBefore, hooked.getTerm(LAKE, "ReconcileTerm").version());
    assertEquals(1, hooked.listTermBindings(LAKE, "ReconcileTerm", null, null, null).size());
    assertTrue(auditRecords.isEmpty());

    // Subsequent load/overwrite reconciliation (insertTable with overwrite=true,
    // reuseStoredColumnIds)
    // drops the removed column and cleans up its binding atomically.
    TableEntity reloadedWithoutIsActive =
        TableEntity.builder()
            .withId(storedTable.id())
            .withName(storedTable.name())
            .withNamespace(storedTable.namespace())
            .withColumns(
                List.of(
                    ColumnEntity.builder()
                        .withId(ids.incrementAndGet())
                        .withName("customer_id")
                        .withPosition(0)
                        .withDataType(Types.StringType.get())
                        .withNullable(true)
                        .withAutoIncrement(false)
                        .withAuditInfo(AUDIT_INFO)
                        .build(),
                    ColumnEntity.builder()
                        .withId(ids.incrementAndGet())
                        .withName("email")
                        .withPosition(1)
                        .withDataType(Types.StringType.get())
                        .withNullable(true)
                        .withAutoIncrement(false)
                        .withAuditInfo(AUDIT_INFO)
                        .build()))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(reloadedWithoutIsActive, true);
    assertEquals(versionBefore + 1, hooked.getTerm(LAKE, "ReconcileTerm").version());
    assertTrue(hooked.listTermBindings(LAKE, "ReconcileTerm", null, null, null).isEmpty());
    assertEquals(1, auditRecords.size());
    assertEquals("RECONCILE_TABLE_COLUMNS", auditRecords.get(0).triggeringOperation());

    // Failed local table drop leaves stale registration and its binding intact (not repaired).
    MetadataObject tableObj = MetadataObjects.of(List.of(CAT, SCH, TBL), MetadataObject.Type.TABLE);
    hooked.associateTerms(LAKE, tableObj, new String[] {"ReconcileTerm"}, null);
    long beforeDropFail = hooked.getTerm(LAKE, "ReconcileTerm").version();
    auditRecords.clear();
    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> TableMetaService.getInstance().deleteTable(tableIdent),
                () -> {
                  throw new IllegalStateException("Local table drop failed after external drop");
                }));
    assertEquals(beforeDropFail, hooked.getTerm(LAKE, "ReconcileTerm").version());
    assertEquals(1, hooked.listTermBindings(LAKE, "ReconcileTerm", null, null, null).size());
    assertTrue(auditRecords.isEmpty());
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
