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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * An external object (for example one created through the Iceberg REST server) can carry a {@code
 * gravitino.identifier} property that names the id of another, live entity. Importing that object
 * must never reuse the other entity: the existing entity keeps its name, its parent and its id, so
 * its owner and role grants stay with it, and the new object is imported under a fresh id.
 */
public class TestImportStringIdentifierReuse extends TestJDBCBackend {

  private static final String METALAKE = "reuse_metalake";
  private static final String CATALOG = "reuse_catalog";
  private static final String OTHER_CATALOG = "reuse_other_catalog";
  private static final String SCHEMA = "reuse_schema";

  private Config previousConfig;
  private Object previousLockManager;
  private RelationalEntityStore store;
  private CatalogManager catalogManager;
  private ViewCatalog viewCatalog;
  private TableCatalog tableCatalog;
  private SupportsSchemas schemaCatalog;

  @BeforeAll
  public void captureEnvironment() throws IllegalAccessException {
    previousConfig = GravitinoEnv.getInstance().config();
    previousLockManager = FieldUtils.readField(GravitinoEnv.getInstance(), "lockManager", true);
  }

  @BeforeEach
  public void prepare() throws Exception {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    createAndInsertCatalog(METALAKE, OTHER_CATALOG);
    createAndInsertSchema(METALAKE, CATALOG, SCHEMA);
    createAndInsertSchema(METALAKE, OTHER_CATALOG, SCHEMA);

    Config lockConfig = mock(Config.class);
    doReturn(100000L).when(lockConfig).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(lockConfig).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(lockConfig).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "lockManager", new LockManager(lockConfig), true);

    Config config = new Config(false) {};
    config.set(Configs.CACHE_ENABLED, false);
    store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);

    viewCatalog = mock(ViewCatalog.class);
    tableCatalog = mock(TableCatalog.class);
    schemaCatalog = mock(SupportsSchemas.class);
    catalogManager = mockCatalogManager();
  }

  @AfterEach
  public void restoreEnvironment() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", previousLockManager, true);
  }

  @TestTemplate
  public void testImportedViewCannotAdoptAnotherViewsId() throws IOException {
    Namespace ns = NamespaceUtil.ofView(METALAKE, CATALOG, SCHEMA);
    long existingId = RandomIdGenerator.INSTANCE.nextId();
    NameIdentifier existingIdent = NameIdentifier.of(ns, "existing_view");
    store.put(createViewEntity(existingId, ns, existingIdent.name()), false);

    // The new view carries the existing entity's id in its properties.
    NameIdentifier newIdent = NameIdentifier.of(ns, "new_view");
    View newView = mock(View.class);
    when(newView.name()).thenReturn(newIdent.name());
    when(newView.properties()).thenReturn(carriedIdProperties(existingId));
    when(newView.auditInfo()).thenReturn(AUDIT_INFO);
    when(newView.columns()).thenReturn(new Column[0]);
    when(newView.representations())
        .thenReturn(
            new Representation[] {
              SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build()
            });
    when(viewCatalog.loadView(newIdent)).thenReturn(newView);
    // The existing entity is still live in the catalog, so the new view is not a rename of it.
    when(viewCatalog.viewExists(existingIdent)).thenReturn(true);

    newViewDispatcher().loadView(newIdent);

    assertExistingUntouched(existingIdent, Entity.EntityType.VIEW, ViewEntity.class, existingId);
    assertNotAdopted(newIdent, Entity.EntityType.VIEW, ViewEntity.class, existingId);
  }

  @TestTemplate
  public void testImportedViewRenamedExternallyKeepsItsId() throws IOException {
    Namespace ns = NamespaceUtil.ofView(METALAKE, CATALOG, SCHEMA);
    long viewId = RandomIdGenerator.INSTANCE.nextId();
    NameIdentifier oldIdent = NameIdentifier.of(ns, "old_view");
    store.put(createViewEntity(viewId, ns, oldIdent.name()), false);

    // The external system renamed old_view to new_view: the old name is gone from the catalog.
    NameIdentifier newIdent = NameIdentifier.of(ns, "new_view");
    View renamedView = mock(View.class);
    when(renamedView.name()).thenReturn(newIdent.name());
    when(renamedView.properties()).thenReturn(carriedIdProperties(viewId));
    when(renamedView.auditInfo()).thenReturn(AUDIT_INFO);
    when(renamedView.columns()).thenReturn(new Column[0]);
    when(renamedView.representations())
        .thenReturn(
            new Representation[] {
              SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build()
            });
    when(viewCatalog.loadView(newIdent)).thenReturn(renamedView);
    when(viewCatalog.viewExists(oldIdent)).thenReturn(false);

    newViewDispatcher().loadView(newIdent);

    assertEquals(viewId, store.get(newIdent, Entity.EntityType.VIEW, ViewEntity.class).id());
    assertFalse(store.exists(oldIdent, Entity.EntityType.VIEW));
  }

  @TestTemplate
  public void testImportedTableCannotAdoptAnotherCatalogsTableId() throws IOException {
    // The existing entity lives in another catalog; only its id leaks into the new table.
    Namespace existingNs = NamespaceUtil.ofTable(METALAKE, OTHER_CATALOG, SCHEMA);
    long existingId = RandomIdGenerator.INSTANCE.nextId();
    NameIdentifier existingIdent = NameIdentifier.of(existingNs, "existing_table");
    store.put(createTableEntity(existingId, existingNs, existingIdent.name(), AUDIT_INFO), false);

    NameIdentifier newIdent =
        NameIdentifier.of(NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA), "new_table");
    Table newTable = mock(Table.class);
    when(newTable.name()).thenReturn(newIdent.name());
    when(newTable.properties()).thenReturn(carriedIdProperties(existingId));
    when(newTable.auditInfo()).thenReturn(AUDIT_INFO);
    when(newTable.columns()).thenReturn(new Column[0]);
    when(tableCatalog.loadTable(newIdent)).thenReturn(newTable);

    TableOperationDispatcher dispatcher =
        new TableOperationDispatcher(
            catalogManager,
            store,
            RandomIdGenerator.INSTANCE,
            () -> mock(SchemaDispatcher.class),
            mock(SecretManager.class));
    dispatcher.loadTable(newIdent);

    assertExistingUntouched(existingIdent, Entity.EntityType.TABLE, TableEntity.class, existingId);
    assertNotAdopted(newIdent, Entity.EntityType.TABLE, TableEntity.class, existingId);
  }

  @TestTemplate
  public void testImportedSchemaCannotAdoptAnotherCatalogsSchemaId() throws IOException {
    Namespace existingNs = NamespaceUtil.ofSchema(METALAKE, OTHER_CATALOG);
    long existingId = RandomIdGenerator.INSTANCE.nextId();
    NameIdentifier existingIdent = NameIdentifier.of(existingNs, "existing_schema");
    store.put(createSchemaEntity(existingId, existingNs, existingIdent.name(), AUDIT_INFO), false);

    NameIdentifier newIdent =
        NameIdentifier.of(NamespaceUtil.ofSchema(METALAKE, CATALOG), "new_schema");
    Schema newSchema = mock(Schema.class);
    when(newSchema.name()).thenReturn(newIdent.name());
    when(newSchema.properties()).thenReturn(carriedIdProperties(existingId));
    when(newSchema.auditInfo()).thenReturn(AUDIT_INFO);
    when(schemaCatalog.loadSchema(newIdent)).thenReturn(newSchema);

    SchemaOperationDispatcher dispatcher =
        new SchemaOperationDispatcher(
            catalogManager, store, RandomIdGenerator.INSTANCE, mock(SecretManager.class));
    dispatcher.loadSchema(newIdent);

    assertExistingUntouched(
        existingIdent, Entity.EntityType.SCHEMA, SchemaEntity.class, existingId);
    assertNotAdopted(newIdent, Entity.EntityType.SCHEMA, SchemaEntity.class, existingId);
  }

  private ViewOperationDispatcher newViewDispatcher() {
    return new ViewOperationDispatcher(
        catalogManager,
        store,
        RandomIdGenerator.INSTANCE,
        () -> mock(SchemaDispatcher.class),
        mock(SecretManager.class));
  }

  private static Map<String, String> carriedIdProperties(long existingId) {
    return ImmutableMap.of(
        StringIdentifier.ID_KEY, StringIdentifier.fromId(existingId).toString(), "k", "v");
  }

  private <E extends Entity & HasIdentifier> void assertExistingUntouched(
      NameIdentifier existingIdent, Entity.EntityType type, Class<E> clazz, long existingId)
      throws IOException {
    assertTrue(
        store.exists(existingIdent, type),
        "Importing a foreign object must not rename or move the existing entity " + existingIdent);
    assertEquals(existingId, store.get(existingIdent, type, clazz).id());
  }

  private <E extends Entity & HasIdentifier> void assertNotAdopted(
      NameIdentifier newIdent, Entity.EntityType type, Class<E> clazz, long existingId)
      throws IOException {
    // The new object is a new external object: it is imported under a fresh id.
    assertTrue(store.exists(newIdent, type), "The new object must still be imported");
    assertNotEquals(
        existingId,
        store.get(newIdent, type, clazz).id(),
        "The imported object must not adopt the existing entity's id");
  }

  @SuppressWarnings("unchecked")
  private CatalogManager mockCatalogManager() throws Exception {
    BasePropertiesMetadata emptyMetadata =
        new BasePropertiesMetadata() {
          @Override
          protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
            return Collections.emptyMap();
          }
        };
    HasPropertyMetadata propertyMetadata = mock(HasPropertyMetadata.class);
    when(propertyMetadata.tablePropertiesMetadata()).thenReturn(emptyMetadata);
    when(propertyMetadata.schemaPropertiesMetadata()).thenReturn(emptyMetadata);

    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    doAnswer(inv -> inv.getArgument(0)).when(wrapper).detachConnectorResult(any());
    // Only isManagedEntity reaches doWithCatalog here: none of these objects is managed.
    doReturn(false).when(wrapper).doWithCatalog(any());
    doAnswer(inv -> ((ThrowableFunction<ViewCatalog, ?>) inv.getArgument(0)).apply(viewCatalog))
        .when(wrapper)
        .doWithViewOps(any());
    doAnswer(inv -> ((ThrowableFunction<TableCatalog, ?>) inv.getArgument(0)).apply(tableCatalog))
        .when(wrapper)
        .doWithTableOps(any());
    doAnswer(
            inv ->
                ((ThrowableFunction<SupportsSchemas, ?>) inv.getArgument(0)).apply(schemaCatalog))
        .when(wrapper)
        .doWithSchemaOps(any());
    doAnswer(
            inv ->
                ((ThrowableFunction<HasPropertyMetadata, ?>) inv.getArgument(0))
                    .apply(propertyMetadata))
        .when(wrapper)
        .doWithPropertiesMeta(any());

    CatalogManager manager = mock(CatalogManager.class);
    doAnswer(
            inv ->
                ((ThrowableFunction<CatalogManager.CatalogWrapper, ?>) inv.getArgument(1))
                    .apply(wrapper))
        .when(manager)
        .doWithCatalogWrapper(any(), any());
    return manager;
  }
}
