/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.EntityType.SCHEMA;
import static com.datastrato.gravitino.Entity.EntityType.TABLE;
import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.TestBasePropertiesMetadata.COMMENT_KEY;
import static com.datastrato.gravitino.TestFilesetPropertiesMetadata.TEST_FILESET_HIDDEN_KEY;
import static com.datastrato.gravitino.connector.BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.TestColumn;
import com.datastrato.gravitino.TestEntityStore;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TestCatalogOperationDispatcher {

  private static EntityStore entityStore;

  private static final IdGenerator idGenerator = new RandomIdGenerator();

  private static final String metalake = "metalake";

  private static final String catalog = "catalog";

  private static CatalogManager catalogManager;

  private static Config config;

  private static CatalogOperationDispatcher dispatcher;

  @BeforeAll
  public static void setUp() throws IOException {
    config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = spy(new TestEntityStore.InMemoryEntityStore());
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    BaseMetalake metalakeEntity =
        new BaseMetalake.Builder()
            .withId(1L)
            .withName(metalake)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();
    entityStore.put(metalakeEntity, true);

    catalogManager = new CatalogManager(config, entityStore, idGenerator);
    dispatcher = new CatalogOperationDispatcher(catalogManager, entityStore, idGenerator);

    NameIdentifier ident = NameIdentifier.of(metalake, catalog);
    Map<String, String> props = ImmutableMap.of();
    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, "test", "comment", props);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }

    if (catalogManager != null) {
      catalogManager.close();
      catalogManager = null;
    }
  }

  @BeforeEach
  public void beforeStart() throws IOException {
    reset(entityStore);
  }

  @Test
  public void testCreateAndListSchemas() throws IOException {
    Namespace ns = Namespace.of(metalake, catalog);

    NameIdentifier schemaIdent = NameIdentifier.of(ns, "schema1");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("schema1", schema.name());
    Assertions.assertEquals("comment", schema.comment());
    testProperties(props, schema.properties());

    // Test required table properties exception
    Map<String, String> illegalTableProperties =
        new HashMap<String, String>() {
          {
            put("k2", "v2");
          }
        };

    testPropertyException(
        () -> dispatcher.createSchema(schemaIdent, "comment", illegalTableProperties),
        "Properties are required and must be set");

    // Test reserved table properties exception
    illegalTableProperties.put(COMMENT_KEY, "table comment");
    illegalTableProperties.put(ID_KEY, "gravitino.v1.uidfdsafdsa");
    testPropertyException(
        () -> dispatcher.createSchema(schemaIdent, "comment", illegalTableProperties),
        "Properties are reserved and cannot be set",
        "comment",
        "gravitino.identifier");

    // Check if the Schema entity is stored in the EntityStore
    SchemaEntity schemaEntity = entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class);
    Assertions.assertNotNull(schemaEntity);
    Assertions.assertEquals("schema1", schemaEntity.name());
    Assertions.assertNotNull(schemaEntity.id());

    Optional<NameIdentifier> ident1 =
        Arrays.stream(dispatcher.listSchemas(ns))
            .filter(s -> s.name().equals("schema1"))
            .findFirst();
    Assertions.assertTrue(ident1.isPresent());

    // Test when the entity store failed to put the schema entity
    doThrow(new IOException()).when(entityStore).put(any(), anyBoolean());
    NameIdentifier schemaIdent2 = NameIdentifier.of(ns, "schema2");
    Schema schema2 = dispatcher.createSchema(schemaIdent2, "comment", props);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("schema2", schema2.name());
    Assertions.assertEquals("comment", schema2.comment());
    testProperties(props, schema2.properties());

    // Check if the Schema entity is stored in the EntityStore
    Assertions.assertFalse(entityStore.exists(schemaIdent2, SCHEMA));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> entityStore.get(schemaIdent2, SCHEMA, SchemaEntity.class));

    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", schema2.auditInfo().creator());
  }

  @Test
  public void testCreateAndLoadSchema() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema11");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);

    Schema loadedSchema = dispatcher.loadSchema(schemaIdent);
    Assertions.assertEquals(schema.name(), loadedSchema.name());
    Assertions.assertEquals(schema.comment(), loadedSchema.comment());
    testProperties(schema.properties(), loadedSchema.properties());
    // Audit info is gotten from entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadedSchema.auditInfo().creator());

    // Case 2: Test if the schema is not found in entity store
    doThrow(new NoSuchEntityException("mock error")).when(entityStore).get(any(), any(), any());
    Schema loadedSchema1 = dispatcher.loadSchema(schemaIdent);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedSchema1.auditInfo().creator());

    // Case 3: Test if entity store is failed to get the schema entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).get(any(), any(), any());
    Schema loadedSchema2 = dispatcher.loadSchema(schemaIdent);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedSchema2.auditInfo().creator());

    // Case 4: Test if the fetched schema entity is matched.
    reset(entityStore);
    SchemaEntity unmatchedEntity =
        new SchemaEntity.Builder()
            .withId(1L)
            .withName("schema11")
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(AuthConstants.ANONYMOUS_USER)
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).get(any(), any(), any());
    Schema loadedSchema3 = dispatcher.loadSchema(schemaIdent);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedSchema2.auditInfo().creator());
  }

  @Test
  public void testCreateAndAlterSchema() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema21");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);

    // Test immutable schema properties
    SchemaChange[] illegalChange =
        new SchemaChange[] {SchemaChange.setProperty(COMMENT_KEY, "new comment")};
    testPropertyException(
        () -> dispatcher.alterSchema(schemaIdent, illegalChange),
        "Property comment is immutable or reserved, cannot be set");

    SchemaChange[] changes =
        new SchemaChange[] {
          SchemaChange.setProperty("k3", "v3"), SchemaChange.removeProperty("k1")
        };

    Schema alteredSchema = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema.name());
    Assertions.assertEquals(schema.comment(), alteredSchema.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredSchema.properties());
    // Audit info is gotten from gravitino entity store.
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredSchema.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredSchema.auditInfo().lastModifier());

    // Case 2: Test if the schema is not found in entity store
    doThrow(new NoSuchEntityException("mock error"))
        .when(entityStore)
        .update(any(), any(), any(), any());
    Schema alteredSchema1 = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema1.name());
    Assertions.assertEquals(schema.comment(), alteredSchema1.comment());
    testProperties(expectedProps, alteredSchema1.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", alteredSchema1.auditInfo().creator());
    Assertions.assertEquals("test", alteredSchema1.auditInfo().lastModifier());

    // Case 3: Test if entity store is failed to get the schema entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).update(any(), any(), any(), any());
    Schema alteredSchema2 = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema2.name());
    Assertions.assertEquals(schema.comment(), alteredSchema2.comment());
    testProperties(expectedProps, alteredSchema2.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", alteredSchema2.auditInfo().creator());
    Assertions.assertEquals("test", alteredSchema1.auditInfo().lastModifier());

    // Case 4: Test if the fetched schema entity is matched.
    reset(entityStore);
    SchemaEntity unmatchedEntity =
        new SchemaEntity.Builder()
            .withId(1L)
            .withName("schema21")
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(AuthConstants.ANONYMOUS_USER)
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).update(any(), any(), any(), any());
    Schema alteredSchema3 = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema3.name());
    Assertions.assertEquals(schema.comment(), alteredSchema3.comment());
    testProperties(expectedProps, alteredSchema3.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", alteredSchema3.auditInfo().creator());
    Assertions.assertEquals("test", alteredSchema1.auditInfo().lastModifier());
  }

  @Test
  public void testCreateAndDropSchema() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema31");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);

    boolean dropped = dispatcher.dropSchema(schemaIdent, false);
    Assertions.assertTrue(dropped);

    // Test if entity store is failed to drop the schema entity
    dispatcher.createSchema(schemaIdent, "comment", props);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(
        RuntimeException.class, () -> dispatcher.dropSchema(schemaIdent, false));
  }

  @Test
  public void testCreateAndListTables() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema41");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, "table1");
    Column[] columns =
        new Column[] {
          new TestColumn.Builder().withName("col1").withType(Types.StringType.get()).build(),
          new TestColumn.Builder().withName("col2").withType(Types.StringType.get()).build()
        };

    Table table1 = dispatcher.createTable(tableIdent1, columns, "comment", props, new Transform[0]);
    Assertions.assertEquals("table1", table1.name());
    Assertions.assertEquals("comment", table1.comment());
    testProperties(props, table1.properties());
    Assertions.assertEquals(0, table1.partitioning().length);
    Assertions.assertArrayEquals(columns, table1.columns());

    // Test required table properties exception
    Map<String, String> illegalTableProperties =
        new HashMap<String, String>() {
          {
            put("k2", "v2");
          }
        };
    testPropertyException(
        () ->
            dispatcher.createTable(
                tableIdent1, columns, "comment", illegalTableProperties, new Transform[0]),
        "Properties are required and must be set");

    // Test reserved table properties exception
    illegalTableProperties.put(COMMENT_KEY, "table comment");
    illegalTableProperties.put(ID_KEY, "gravitino.v1.uidfdsafdsa");
    testPropertyException(
        () ->
            dispatcher.createTable(
                tableIdent1, columns, "comment", illegalTableProperties, new Transform[0]),
        "Properties are reserved and cannot be set",
        "comment",
        "gravitino.identifier");

    // Check if the Table entity is stored in the EntityStore
    TableEntity tableEntity = entityStore.get(tableIdent1, TABLE, TableEntity.class);
    Assertions.assertNotNull(tableEntity);
    Assertions.assertEquals("table1", tableEntity.name());

    Assertions.assertFalse(table1.properties().containsKey(ID_KEY));

    Optional<NameIdentifier> ident1 =
        Arrays.stream(dispatcher.listTables(tableNs))
            .filter(s -> s.name().equals("table1"))
            .findFirst();
    Assertions.assertTrue(ident1.isPresent());

    // Test when the entity store failed to put the table entity
    doThrow(new IOException()).when(entityStore).put(any(), anyBoolean());
    NameIdentifier tableIdent2 = NameIdentifier.of(tableNs, "table2");
    Table table2 = dispatcher.createTable(tableIdent2, columns, "comment", props, new Transform[0]);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("table2", table2.name());
    Assertions.assertEquals("comment", table2.comment());
    testProperties(props, table2.properties());

    // Check if the Table entity is stored in the EntityStore
    Assertions.assertFalse(entityStore.exists(tableIdent2, TABLE));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> entityStore.get(tableIdent2, TABLE, TableEntity.class));

    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", table2.auditInfo().creator());
  }

  @Test
  public void testCreateAndLoadTable() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema51");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, "table11");
    Column[] columns =
        new Column[] {
          new TestColumn.Builder().withName("col1").withType(Types.StringType.get()).build(),
          new TestColumn.Builder().withName("col2").withType(Types.StringType.get()).build()
        };

    Table table1 = dispatcher.createTable(tableIdent1, columns, "comment", props, new Transform[0]);
    Table loadedTable1 = dispatcher.loadTable(tableIdent1);
    Assertions.assertEquals(table1.name(), loadedTable1.name());
    Assertions.assertEquals(table1.comment(), loadedTable1.comment());
    testProperties(table1.properties(), loadedTable1.properties());
    Assertions.assertEquals(0, loadedTable1.partitioning().length);
    Assertions.assertArrayEquals(table1.columns(), loadedTable1.columns());
    // Audit info is gotten from the entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadedTable1.auditInfo().creator());

    // Case 2: Test if the table entity is not found in the entity store
    reset(entityStore);
    doThrow(new NoSuchEntityException("")).when(entityStore).get(any(), any(), any());
    Table loadedTable2 = dispatcher.loadTable(tableIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable2.auditInfo().creator());

    // Case 3: Test if the entity store is failed to get the table entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).get(any(), any(), any());
    Table loadedTable3 = dispatcher.loadTable(tableIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable3.auditInfo().creator());

    // Case 4: Test if the table entity is not matched
    reset(entityStore);
    TableEntity tableEntity =
        new TableEntity.Builder()
            .withId(1L)
            .withName("table11")
            .withNamespace(tableNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(tableEntity).when(entityStore).get(any(), any(), any());
    Table loadedTable4 = dispatcher.loadTable(tableIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable4.auditInfo().creator());
  }

  @Test
  public void testCreateAndAlterTable() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema61");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table21");
    Column[] columns =
        new Column[] {
          new TestColumn.Builder().withName("col1").withType(Types.StringType.get()).build(),
          new TestColumn.Builder().withName("col2").withType(Types.StringType.get()).build()
        };

    Table table = dispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);

    // Test immutable table properties
    TableChange[] illegalChange =
        new TableChange[] {TableChange.setProperty(COMMENT_KEY, "new comment")};
    testPropertyException(
        () -> dispatcher.alterTable(tableIdent, illegalChange),
        "Property comment is immutable or reserved, cannot be set");

    TableChange[] changes =
        new TableChange[] {TableChange.setProperty("k3", "v3"), TableChange.removeProperty("k1")};

    Table alteredTable = dispatcher.alterTable(tableIdent, changes);
    Assertions.assertEquals(table.name(), alteredTable.name());
    Assertions.assertEquals(table.comment(), alteredTable.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredTable.properties());
    // Audit info is gotten from gravitino entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().lastModifier());

    // Case 2: Test if the table entity is not found in the entity store
    reset(entityStore);
    doThrow(new NoSuchEntityException("")).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable2 = dispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable2.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable2.auditInfo().lastModifier());

    // Case 3: Test if the entity store is failed to update the table entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable3 = dispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable3.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable3.auditInfo().lastModifier());

    // Case 4: Test if the table entity is not matched
    reset(entityStore);
    TableEntity unmatchedEntity =
        new TableEntity.Builder()
            .withId(1L)
            .withName("table21")
            .withNamespace(tableNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable4 = dispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable4.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable4.auditInfo().lastModifier());
  }

  @Test
  public void testCreateAndDropTable() throws IOException {
    NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, "schema71", "table31");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Column[] columns =
        new Column[] {
          new TestColumn.Builder().withName("col1").withType(Types.StringType.get()).build(),
          new TestColumn.Builder().withName("col2").withType(Types.StringType.get()).build()
        };
    Table table = dispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);

    boolean dropped = dispatcher.dropTable(tableIdent);
    Assertions.assertTrue(dropped);

    // Test if the entity store is failed to drop the table entity
    dispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(RuntimeException.class, () -> dispatcher.dropTable(tableIdent));
  }

  @Test
  public void testCreateAndListFilesets() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema81");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset1");
    Fileset fileset1 =
        dispatcher.createFileset(filesetIdent1, "comment", Fileset.Type.MANAGED, "test", props);
    Assertions.assertEquals("fileset1", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("test", fileset1.storageLocation());

    NameIdentifier[] idents = dispatcher.listFilesets(filesetNs);
    Assertions.assertEquals(1, idents.length);
    Assertions.assertEquals(filesetIdent1, idents[0]);

    Map<String, String> illegalProps = ImmutableMap.of("k2", "v2");
    testPropertyException(
        () ->
            dispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps),
        "Properties are required and must be set");

    Map<String, String> illegalProps2 = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () ->
            dispatcher.createFileset(
                filesetIdent1, "comment", Fileset.Type.MANAGED, "test", illegalProps2),
        "Properties are reserved and cannot be set",
        "gravitino.identifier");
  }

  @Test
  public void testCreateAndLoadFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema91");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "location", "schema91");
    dispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset11");
    Fileset fileset1 =
        dispatcher.createFileset(filesetIdent1, "comment", Fileset.Type.MANAGED, null, props);
    Assertions.assertEquals("fileset11", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertNull(fileset1.storageLocation());

    Fileset loadedFileset1 = dispatcher.loadFileset(filesetIdent1);
    Assertions.assertEquals(fileset1.name(), loadedFileset1.name());
    Assertions.assertEquals(fileset1.comment(), loadedFileset1.comment());
    testProperties(props, loadedFileset1.properties());
    Assertions.assertEquals(fileset1.type(), loadedFileset1.type());
    Assertions.assertEquals(fileset1.storageLocation(), loadedFileset1.storageLocation());
  }

  @Test
  public void testCreateAndAlterFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema101");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset21");
    Fileset fileset1 =
        dispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "fileset21", props);
    Assertions.assertEquals("fileset21", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("fileset21", fileset1.storageLocation());

    FilesetChange[] changes =
        new FilesetChange[] {
          FilesetChange.setProperty("k3", "v3"), FilesetChange.removeProperty("k1")
        };

    Fileset alteredFileset = dispatcher.alterFileset(filesetIdent1, changes);
    Assertions.assertEquals(fileset1.name(), alteredFileset.name());
    Assertions.assertEquals(fileset1.comment(), alteredFileset.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredFileset.properties());

    // Test immutable fileset properties
    FilesetChange[] illegalChange =
        new FilesetChange[] {FilesetChange.setProperty(GRAVITINO_MANAGED_ENTITY, "test")};
    testPropertyException(
        () -> dispatcher.alterFileset(filesetIdent1, illegalChange),
        "Property gravitino.managed.entity is immutable or reserved, cannot be set");
  }

  @Test
  public void testCreateAndDropFileset() {
    Namespace filesetNs = Namespace.of(metalake, catalog, "schema111");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(NameIdentifier.of(filesetNs.levels()), "comment", props);

    NameIdentifier filesetIdent1 = NameIdentifier.of(filesetNs, "fileset31");
    Fileset fileset1 =
        dispatcher.createFileset(
            filesetIdent1, "comment", Fileset.Type.MANAGED, "fileset31", props);
    Assertions.assertEquals("fileset31", fileset1.name());
    Assertions.assertEquals("comment", fileset1.comment());
    testProperties(props, fileset1.properties());
    Assertions.assertEquals(Fileset.Type.MANAGED, fileset1.type());
    Assertions.assertEquals("fileset31", fileset1.storageLocation());

    boolean dropped = dispatcher.dropFileset(filesetIdent1);
    Assertions.assertTrue(dropped);
  }

  @Test
  public void testGetCatalogIdentifier() {
    CatalogOperationDispatcher dispatcher = new CatalogOperationDispatcher(null, null, null);

    NameIdentifier id1 = NameIdentifier.of("a");
    assertThrows(IllegalNamespaceException.class, () -> dispatcher.getCatalogIdentifier(id1));

    NameIdentifier id2 = NameIdentifier.of("a", "b");
    assertEquals(dispatcher.getCatalogIdentifier(id2), NameIdentifier.of("a", "b"));

    NameIdentifier id3 = NameIdentifier.of("a", "b", "c");
    assertEquals(dispatcher.getCatalogIdentifier(id3), NameIdentifier.of("a", "b"));

    NameIdentifier id4 = NameIdentifier.of("a", "b", "c", "d");
    assertEquals(dispatcher.getCatalogIdentifier(id4), NameIdentifier.of("a", "b"));

    NameIdentifier id5 = NameIdentifier.of("a", "b", "c", "d", "e");
    assertEquals(dispatcher.getCatalogIdentifier(id5), NameIdentifier.of("a", "b"));
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });
    Assertions.assertFalse(testProps.containsKey(StringIdentifier.ID_KEY));
    Assertions.assertFalse(testProps.containsKey(GRAVITINO_MANAGED_ENTITY));
    Assertions.assertFalse(testProps.containsKey(TEST_FILESET_HIDDEN_KEY));
  }

  private void testPropertyException(Executable operation, String... errorMessage) {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, operation);
    for (String msg : errorMessage) {
      Assertions.assertTrue(exception.getMessage().contains(msg));
    }
  }
}
