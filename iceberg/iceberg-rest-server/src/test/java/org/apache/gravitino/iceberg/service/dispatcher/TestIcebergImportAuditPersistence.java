/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.catalog.ViewOperationDispatcher;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.lock.TreeLock;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogPoller;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

/** Exercises IRC hooks, core imports and real database writes together. */
public class TestIcebergImportAuditPersistence extends TestJDBCBackend {
  private static final String METALAKE = "audit_metalake";
  private static final String CATALOG = "audit_catalog";
  private static final AuditInfo ALICE =
      AuditInfo.builder().withCreator("alice").withCreateTime(Instant.EPOCH).build();

  @Nullable private static Object previousConfig;
  @Nullable private static Object previousIdGenerator;
  @Nullable private static Object previousIdResolver;

  @BeforeAll
  static void saveBackendExtensionGlobals() throws IllegalAccessException {
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
    previousIdResolver =
        FieldUtils.readStaticField(EntityIdService.class, "entityIdResolver", true);
  }

  @AfterAll
  static void restoreBackendExtensionGlobals() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", previousIdGenerator, true);
    FieldUtils.writeStaticField(
        EntityIdService.class, "entityIdResolver", previousIdResolver, true);
  }

  @TestTemplate
  void testTableHookFillsAuditAfterOrdinaryLoad() throws Exception {
    try (Fixture f = new Fixture()) {
      NameIdentifier schema = f.ident("table_schema");
      f.schemas.createSchema(schema, "", Map.of("k1", "v1", "k2", "v2"));
      NameIdentifier table = NameIdentifier.of(METALAKE, CATALOG, schema.name(), "table");
      IcebergTableOperationDispatcher backendDispatcher =
          mock(IcebergTableOperationDispatcher.class);
      CreateTableRequest request = mock(CreateTableRequest.class);
      when(request.name()).thenReturn(table.name());
      Namespace namespace = Namespace.of(schema.name());
      when(backendDispatcher.createTable(f.context, namespace, request))
          .thenAnswer(
              call -> {
                f.source.createTable(
                    table,
                    new Column[] {Column.of("col", Types.StringType.get())},
                    "",
                    Map.of(),
                    new Transform[0],
                    Distributions.NONE,
                    new SortOrder[0],
                    Indexes.EMPTY_INDEXES);
                FieldUtils.writeField(
                    f.source.loadTable(table), "auditInfo", AuditInfo.EMPTY, true);
                f.tables.loadTable(table);
                Assertions.assertNull(
                    f.store
                        .get(table, Entity.EntityType.TABLE, TableEntity.class)
                        .auditInfo()
                        .creator());
                return mock(LoadTableResponse.class);
              });
      new IcebergTableHookDispatcher(
              backendDispatcher, mock(IcebergNamespaceOperationDispatcher.class))
          .createTable(f.context, namespace, request);
      TableEntity stored = f.store.get(table, Entity.EntityType.TABLE, TableEntity.class);
      Assertions.assertEquals("alice", stored.auditInfo().creator());
      Assertions.assertEquals(
          stored.auditInfo().creator(), stored.columns().get(0).auditInfo().creator());
      Assertions.assertEquals(
          stored.auditInfo().createTime(), stored.columns().get(0).auditInfo().createTime());
    }
  }

  @TestTemplate
  void testTableAuditRecoveryPreservesColumnFields() throws Exception {
    try (Fixture f = new Fixture()) {
      NameIdentifier schema = f.ident("column_audit_schema");
      f.schemas.createSchema(schema, "", Map.of("k1", "v1", "k2", "v2"));
      NameIdentifier table = NameIdentifier.of(METALAKE, CATALOG, schema.name(), "table");
      Column[] columns = {
        Column.of(
            "missing",
            Types.StringType.get(),
            "comment",
            false,
            false,
            Column.DEFAULT_VALUE_NOT_SET),
        Column.of("modified", Types.StringType.get()),
        Column.of("owned", Types.StringType.get())
      };
      f.source.createTable(
          table,
          columns,
          "",
          Map.of(),
          new Transform[0],
          Distributions.NONE,
          new SortOrder[0],
          Indexes.EMPTY_INDEXES);
      FieldUtils.writeField(f.source.loadTable(table), "auditInfo", AuditInfo.EMPTY, true);
      f.tables.loadTable(table);
      AuditInfo modified =
          AuditInfo.builder()
              .withCreateTime(Instant.EPOCH.plusSeconds(1))
              .withLastModifier("modifier")
              .withLastModifiedTime(Instant.EPOCH.plusSeconds(2))
              .build();
      AuditInfo owned =
          AuditInfo.builder()
              .withCreator("bob")
              .withCreateTime(Instant.EPOCH.plusSeconds(3))
              .withLastModifier("editor")
              .withLastModifiedTime(Instant.EPOCH.plusSeconds(4))
              .build();
      TableEntity before =
          f.store.update(
              table,
              TableEntity.class,
              Entity.EntityType.TABLE,
              current ->
                  TableEntity.builder()
                      .withId(current.id())
                      .withName(current.name())
                      .withNamespace(current.namespace())
                      .withComment(current.comment())
                      .withProperties(current.properties())
                      .withAuditInfo(current.auditInfo())
                      .withColumns(
                          current.columns().stream()
                              .map(
                                  column ->
                                      ColumnEntity.toColumnEntity(
                                          columns[column.position()],
                                          column.position(),
                                          column.id(),
                                          "modified".equals(column.name())
                                              ? modified
                                              : "owned".equals(column.name())
                                                  ? owned
                                                  : AuditInfo.EMPTY))
                              .collect(Collectors.toList()))
                      .build());
      f.tables.importTable(table, ALICE);
      TableEntity stored = f.store.get(table, Entity.EntityType.TABLE, TableEntity.class);
      Map<String, ColumnEntity> storedColumns =
          stored.columns().stream().collect(Collectors.toMap(ColumnEntity::name, column -> column));
      Assertions.assertEquals("alice", stored.auditInfo().creator());
      Assertions.assertEquals("alice", storedColumns.get("missing").auditInfo().creator());
      Assertions.assertEquals(Instant.EPOCH, storedColumns.get("missing").auditInfo().createTime());
      Assertions.assertEquals("alice", storedColumns.get("modified").auditInfo().creator());
      Assertions.assertEquals(
          modified.createTime(), storedColumns.get("modified").auditInfo().createTime());
      Assertions.assertEquals(
          modified.lastModifier(), storedColumns.get("modified").auditInfo().lastModifier());
      Assertions.assertEquals(
          modified.lastModifiedTime(),
          storedColumns.get("modified").auditInfo().lastModifiedTime());
      Assertions.assertEquals(columns.length, storedColumns.size());
      for (ColumnEntity expected : before.columns()) {
        ColumnEntity actual = storedColumns.get(expected.name());
        if ("owned".equals(expected.name())) {
          Assertions.assertEquals(expected, actual);
        }
        Assertions.assertEquals(expected.id(), actual.id());
        Assertions.assertEquals(expected.name(), actual.name());
        Assertions.assertEquals(expected.position(), actual.position());
        Assertions.assertEquals(expected.dataType(), actual.dataType());
        Assertions.assertEquals(expected.comment(), actual.comment());
        Assertions.assertEquals(expected.nullable(), actual.nullable());
        Assertions.assertEquals(expected.autoIncrement(), actual.autoIncrement());
        Assertions.assertEquals(expected.defaultValue(), actual.defaultValue());
      }
    }
  }

  @TestTemplate
  void testTableImportReturnsRecreatedEntityWithoutOldAudit() throws Exception {
    try (Fixture f = new Fixture()) {
      NameIdentifier schema = f.ident("recreated_table_schema");
      f.schemas.createSchema(schema, "", Map.of("k1", "v1", "k2", "v2"));
      NameIdentifier table = NameIdentifier.of(METALAKE, CATALOG, schema.name(), "table");
      f.source.createTable(
          table,
          new Column[] {Column.of("col", Types.StringType.get())},
          "",
          Map.of(),
          new Transform[0],
          Distributions.NONE,
          new SortOrder[0],
          Indexes.EMPTY_INDEXES);
      FieldUtils.writeField(f.source.loadTable(table), "auditInfo", AuditInfo.EMPTY, true);
      f.tables.loadTable(table);
      TableEntity observed = f.store.get(table, Entity.EntityType.TABLE, TableEntity.class);
      AtomicBoolean replaced = new AtomicBoolean();
      doAnswer(
              call -> {
                if (replaced.compareAndSet(false, true)) {
                  f.store.delete(table, Entity.EntityType.TABLE);
                  TableEntity recreated =
                      TableEntity.builder()
                          .withId(RandomIdGenerator.INSTANCE.nextId())
                          .withName(observed.name())
                          .withNamespace(observed.namespace())
                          .withColumns(
                              Arrays.asList(
                                  ColumnEntity.toColumnEntity(
                                      Column.of("col", Types.StringType.get()),
                                      0,
                                      RandomIdGenerator.INSTANCE.nextId(),
                                      AuditInfo.EMPTY)))
                          .withAuditInfo(AuditInfo.EMPTY)
                          .build();
                  f.store.put(recreated, false);
                }
                return call.callRealMethod();
              })
          .when(f.store)
          .update(eq(table), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
      Assertions.assertNull(f.tables.importTable(table, ALICE).auditInfo().creator());
      TableEntity stored = f.store.get(table, Entity.EntityType.TABLE, TableEntity.class);
      Assertions.assertTrue(replaced.get());
      Assertions.assertNotEquals(observed.id(), stored.id());
      Assertions.assertNull(stored.auditInfo().creator());
      Assertions.assertNull(stored.auditInfo().createTime());
      Assertions.assertNull(stored.columns().get(0).auditInfo().creator());
      Assertions.assertNull(f.tables.loadTable(table).auditInfo().creator());
    }
  }

  @TestTemplate
  void testViewHookFillsAuditAfterOrdinaryLoad() throws Exception {
    try (Fixture f = new Fixture()) {
      NameIdentifier schema = f.ident("view_schema");
      f.schemas.createSchema(schema, "", Map.of("k1", "v1", "k2", "v2"));
      NameIdentifier view = NameIdentifier.of(METALAKE, CATALOG, schema.name(), "view");
      IcebergViewOperationDispatcher backendDispatcher = mock(IcebergViewOperationDispatcher.class);
      CreateViewRequest request = mock(CreateViewRequest.class);
      when(request.name()).thenReturn(view.name());
      Namespace namespace = Namespace.of(schema.name());
      when(backendDispatcher.createView(f.context, namespace, request))
          .thenAnswer(
              call -> {
                f.source.views.put(view, sourceView(view.name(), Map.of()));
                f.views.loadView(view);
                return mock(LoadViewResponse.class);
              });
      new IcebergViewHookDispatcher(
              backendDispatcher, mock(IcebergNamespaceOperationDispatcher.class), METALAKE)
          .createView(f.context, namespace, request);
      Assertions.assertEquals(
          "alice",
          f.store.get(view, Entity.EntityType.VIEW, ViewEntity.class).auditInfo().creator());
    }
  }

  @TestTemplate
  void testNamespaceHookAuditsOnlyCreatedNamespaces() throws Exception {
    try (Fixture f = new Fixture()) {
      for (int scenario = 0; scenario < 3; scenario++) {
        boolean parentExists = scenario != 0;
        boolean knownAudit = scenario == 1;
        String root = "parent_" + scenario;
        Namespace parent = Namespace.of(root);
        Namespace leaf = Namespace.of(root, "leaf");
        if (parentExists) {
          f.createSourceSchema(
              root,
              knownAudit
                  ? AuditInfo.builder()
                      .withCreator("original")
                      .withCreateTime(Instant.EPOCH)
                      .build()
                  : AuditInfo.EMPTY);
        }
        IcebergNamespaceOperationDispatcher backendDispatcher =
            mock(IcebergNamespaceOperationDispatcher.class);
        when(backendDispatcher.namespaceExists(f.context, parent)).thenReturn(parentExists);
        CreateNamespaceRequest request =
            CreateNamespaceRequest.builder().withNamespace(leaf).build();
        when(backendDispatcher.createNamespace(f.context, request))
            .thenAnswer(
                call -> {
                  if (!parentExists) {
                    f.createSourceSchema(root, AuditInfo.EMPTY);
                  }
                  f.createSourceSchema(root + ":leaf", AuditInfo.EMPTY);
                  return mock(CreateNamespaceResponse.class);
                });
        AtomicBoolean ordinaryLoadWon = new AtomicBoolean();
        doAnswer(
                call -> {
                  // The backend branch lock has been released, but the hook has not imported yet.
                  if (ordinaryLoadWon.compareAndSet(false, true)) {
                    f.schemas.loadSchema(f.ident(root));
                    f.schemas.loadSchema(f.ident(root + ":leaf"));
                  }
                  return call.callRealMethod();
                })
            .when(f.schemas)
            .importSchema(any(), any());
        new IcebergNamespaceHookDispatcher(backendDispatcher).createNamespace(f.context, request);
        SchemaEntity parentEntity =
            f.store.get(f.ident(root), Entity.EntityType.SCHEMA, SchemaEntity.class);
        SchemaEntity leafEntity =
            f.store.get(f.ident(root + ":leaf"), Entity.EntityType.SCHEMA, SchemaEntity.class);
        Assertions.assertEquals(
            parentExists ? knownAudit ? "original" : null : "alice",
            parentEntity.auditInfo().creator());
        Assertions.assertEquals("alice", leafEntity.auditInfo().creator());
        Assertions.assertNotNull(leafEntity.auditInfo().createTime());
        if (parentExists) {
          Assertions.assertEquals(
              knownAudit ? Instant.EPOCH : null, parentEntity.auditInfo().createTime());
        }
      }
    }
  }

  @TestTemplate
  void testOverlappingViewImportsKeepCreator() throws Exception {
    try (Fixture f = new Fixture()) {
      f.schemas.createSchema(f.ident("overlap"), "", Map.of("k1", "v1", "k2", "v2"));
      // Separate servers have separate tree locks; only the real store coordinates these writes.
      LockManager locks = mock(LockManager.class);
      when(locks.createTreeLock(any())).thenAnswer(call -> mock(TreeLock.class));
      f.install("lockManager", locks);
      for (boolean sourceId : new boolean[] {false, true}) {
        for (boolean emptyFirst : new boolean[] {false, true}) {
          NameIdentifier ident =
              NameIdentifier.of(
                  METALAKE, CATALOG, "overlap", "view_" + sourceId + "_" + emptyFirst);
          Map<String, String> properties =
              sourceId
                  ? Map.of(
                      StringIdentifier.ID_KEY,
                      StringIdentifier.fromId(RandomIdGenerator.INSTANCE.nextId()).toString())
                  : Map.of();
          f.source.views.put(ident, sourceView(ident.name(), properties));
          CountDownLatch pendingInsert = new CountDownLatch(1);
          CountDownLatch winnerCommitted = new CountDownLatch(1);
          doAnswer(
                  call -> {
                    ViewEntity incoming = call.getArgument(0);
                    if ((incoming.auditInfo().creator() == null) == emptyFirst) {
                      pendingInsert.countDown();
                      Assertions.assertTrue(winnerCommitted.await(10, TimeUnit.SECONDS));
                    }
                    return call.callRealMethod();
                  })
              .when(f.store)
              .put(any(ViewEntity.class), anyBoolean());
          ExecutorService executor = Executors.newSingleThreadExecutor();
          try {
            Future<View> loser =
                executor.submit(
                    () -> f.views.importView(ident, emptyFirst ? AuditInfo.EMPTY : ALICE));
            Assertions.assertTrue(pendingInsert.await(10, TimeUnit.SECONDS));
            f.views.importView(ident, emptyFirst ? ALICE : AuditInfo.EMPTY);
            long winnerId = f.store.get(ident, Entity.EntityType.VIEW, ViewEntity.class).id();
            winnerCommitted.countDown();
            Assertions.assertEquals("alice", loser.get(10, TimeUnit.SECONDS).auditInfo().creator());
            ViewEntity stored = f.store.get(ident, Entity.EntityType.VIEW, ViewEntity.class);
            Assertions.assertEquals(winnerId, stored.id());
            Assertions.assertEquals("alice", stored.auditInfo().creator());
            Assertions.assertEquals(Instant.EPOCH, stored.auditInfo().createTime());
          } finally {
            winnerCommitted.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
          }
        }
      }
    }
  }

  private static View sourceView(String name, Map<String, String> properties) {
    View view = mock(View.class);
    when(view.name()).thenReturn(name);
    when(view.properties()).thenReturn(properties);
    when(view.auditInfo()).thenReturn(AuditInfo.EMPTY);
    when(view.columns()).thenReturn(new Column[0]);
    when(view.representations())
        .thenReturn(
            new Representation[] {
              SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build()
            });
    return view;
  }

  private class Fixture implements AutoCloseable {
    private final Map<String, Object> previous = new HashMap<>();
    private final RelationalEntityStore store;
    private final CatalogManager catalogs;
    private final SecretManager secrets;
    private final SchemaOperationDispatcher schemas;
    private final TableOperationDispatcher tables;
    private final ViewOperationDispatcher views;
    private final TestCatalogOperations source;
    private final IcebergRequestContext context;
    private final Class<?> contextHolder;
    private final Object previousContext;

    private Fixture() throws Exception {
      Config config = new Config(false) {};
      config.set(Configs.CATALOG_LOAD_ISOLATED, false);
      config.set(Configs.CACHE_ENABLED, false);
      config.set(Configs.SCHEMA_SEPARATOR, ":");
      install("config", config);
      install("idGenerator", RandomIdGenerator.INSTANCE);
      install("lockManager", new LockManager(config));
      store = spy(new RelationalEntityStore());
      FieldUtils.writeField(store, "backend", backend, true);
      FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
      // The extension owns the real backend; background cache polling is outside these tests.
      FieldUtils.writeField(
          store, "entityChangeLogPoller", mock(EntityChangeLogPoller.class), true);
      createAndInsertMakeLake(METALAKE);
      secrets = new SecretManager(config);
      catalogs = new CatalogManager(config, store, RandomIdGenerator.INSTANCE, secrets);
      install("catalogManager", catalogs);
      install("entityStore", store);
      catalogs.createCatalog(
          NameIdentifier.of(METALAKE, CATALOG),
          Catalog.Type.RELATIONAL,
          "test",
          "",
          Map.of(
              TestCatalog.PROPERTY_KEY1,
              "v1",
              TestCatalog.PROPERTY_KEY2,
              "v2",
              TestCatalog.PROPERTY_KEY5_PREFIX + "1",
              "v3"));
      source =
          catalogs.doWithCatalog(
              NameIdentifier.of(METALAKE, CATALOG),
              c -> (TestCatalogOperations) ((TestCatalog) c).ops());
      schemas =
          spy(new SchemaOperationDispatcher(catalogs, store, RandomIdGenerator.INSTANCE, secrets));
      tables =
          new TableOperationDispatcher(
              catalogs, store, RandomIdGenerator.INSTANCE, () -> schemas, secrets);
      views =
          new ViewOperationDispatcher(
              catalogs, store, RandomIdGenerator.INSTANCE, () -> schemas, secrets);
      install("internalSchemaDispatcher", schemas);
      install("internalTableDispatcher", tables);
      install("internalViewDispatcher", views);
      install("internalOwnerDispatcher", mock(OwnerDispatcher.class));
      contextHolder =
          Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
              .filter(c -> c.getSimpleName().equals("InstanceHolder"))
              .findFirst()
              .orElseThrow();
      previousContext = FieldUtils.readStaticField(contextHolder, "INSTANCE", true);
      IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
      when(provider.getMetalakeName()).thenReturn(METALAKE);
      when(provider.getDefaultCatalogName()).thenReturn(CATALOG);
      IcebergRESTServerContext.create(provider, false, false, true, null);
      context = mock(IcebergRequestContext.class);
      when(context.catalogName()).thenReturn(CATALOG);
      when(context.userName()).thenReturn("alice");
    }

    private void install(String field, Object value) throws IllegalAccessException {
      if (!previous.containsKey(field)) {
        previous.put(field, FieldUtils.readField(GravitinoEnv.getInstance(), field, true));
      }
      FieldUtils.writeField(GravitinoEnv.getInstance(), field, value, true);
    }

    private NameIdentifier ident(String schema) {
      return NameIdentifier.of(METALAKE, CATALOG, schema);
    }

    private void createSourceSchema(String name, AuditInfo audit) throws Exception {
      Object schema = source.createSchema(ident(name), "", Map.of("k1", "v1", "k2", "v2"));
      FieldUtils.writeField(schema, "auditInfo", audit, true);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
      catalogs.close();
      secrets.close();
      FieldUtils.writeStaticField(contextHolder, "INSTANCE", previousContext, true);
      for (Map.Entry<String, Object> entry : previous.entrySet()) {
        FieldUtils.writeField(GravitinoEnv.getInstance(), entry.getKey(), entry.getValue(), true);
      }
    }
  }
}
