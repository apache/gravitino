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
package org.apache.gravitino.catalog.lakehouse.generic;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_RETENTION_SECS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGenericCatalogOperations {
  private static final String STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String METALAKE_NAME = "metalake_for_lakehouse_test";
  private static final String CATALOG_NAME = "lakehouse_catalog_test";

  private static EntityStore store;
  private static IdGenerator idGenerator;
  private static GenericCatalogOperations ops;
  private static GenericCatalogOperations opsWithFakeProvider;

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(STORE_PATH);

    // The following properties are used to create the JDBC connection; they are just for test, in
    // the real world, they will be set automatically by the configuration file if you set
    // ENTITY_RELATIONAL_STORE as EMBEDDED_ENTITY_RELATIONAL_STORE.
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);

    File f = FileUtils.getFile(STORE_PATH);
    f.deleteOnExit();

    when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    when(config.get(ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)).thenReturn(3L);
    when(config.get(ENTITY_CHANGE_LOG_RETENTION_SECS)).thenReturn(24 * 60 * 60L);
    when(config.get(ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)).thenReturn(60 * 60L);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(false);

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    idGenerator = RandomIdGenerator.INSTANCE;

    // Create the metalake and catalog
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE_NAME)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(auditInfo)
            .withName(METALAKE_NAME)
            .build();
    store.put(metalake, false);

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG_NAME)
            .withNamespace(Namespace.of(METALAKE_NAME))
            .withProvider("generic-lakehouse")
            .withType(Catalog.Type.RELATIONAL)
            .withAuditInfo(auditInfo)
            .build();
    store.put(catalog, false);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);

    ops = new GenericCatalogOperations(store, idGenerator);

    // A second instance, initialized so that it uses the test provider, to exercise the location
    // release callbacks. The shared instance above is never initialized and has no provider.
    opsWithFakeProvider = new GenericCatalogOperations(store, idGenerator);
    opsWithFakeProvider.initialize(
        ImmutableMap.of(
            GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER,
            FakeTableLocationProvider.NAME),
        null /* CatalogInfo, unused */,
        new GenericCatalog());
  }

  @BeforeEach
  public void resetProvider() {
    FakeTableLocationProvider.reset();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    ops.close();
    opsWithFakeProvider.close();
    store.close();
    FileUtils.deleteDirectory(new File(STORE_PATH));
  }

  @Test
  public void testConnectionNotSupported() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> ops.testConnection(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME)));
  }

  @Test
  public void testSchemaOperations() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties = StringIdentifier.newPropertiesWithId(stringId, null);

    ops.createSchema(schemaIdent, "schema comment", properties);
    Schema loadedSchema = ops.loadSchema(schemaIdent);

    Assertions.assertEquals(schemaName, loadedSchema.name());
    Assertions.assertEquals("schema comment", loadedSchema.comment());
    Assertions.assertEquals(properties, loadedSchema.properties());

    // Test create schema with the same name
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> ops.createSchema(schemaIdent, "schema comment", properties));

    // Test create schema in a non-existent catalog
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () ->
            ops.createSchema(
                NameIdentifierUtil.ofSchema(METALAKE_NAME, "non-existent-catalog", schemaName),
                "schema comment",
                properties));

    // Test load a non-existent schema
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            ops.loadSchema(
                NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, "non-existent-schema")));

    // Test load a non-existent schema in a non-existent catalog
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            ops.loadSchema(
                NameIdentifierUtil.ofSchema(
                    METALAKE_NAME, "non-existent-catalog", "non-existent-schema")));

    // Create another schema
    String schemaName2 = randomSchemaName();
    NameIdentifier schemaIdent2 =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName2);
    StringIdentifier stringId2 = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties2 = StringIdentifier.newPropertiesWithId(stringId2, null);

    ops.createSchema(schemaIdent2, "schema comment 2", properties2);

    // Test list schemas
    NameIdentifier[] idents = ops.listSchemas(Namespace.of(METALAKE_NAME, CATALOG_NAME));

    Set<NameIdentifier> resultSet = Arrays.stream(idents).collect(Collectors.toSet());
    Assertions.assertTrue(resultSet.contains(schemaIdent));
    Assertions.assertTrue(resultSet.contains(schemaIdent2));

    // Test list schemas in a non-existent catalog
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () -> ops.listSchemas(Namespace.of(METALAKE_NAME, "non-existent-catalog")));

    // Test drop schema
    Assertions.assertTrue(ops.dropSchema(schemaIdent, false));
    Assertions.assertFalse(ops.dropSchema(schemaIdent, false));
    Assertions.assertTrue(ops.dropSchema(schemaIdent2, false));
    Assertions.assertFalse(ops.dropSchema(schemaIdent2, false));

    // Test drop non-existent schema
    Assertions.assertFalse(
        ops.dropSchema(
            NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, "non-existent-schema"),
            false));

    // Test drop schema in a non-existent catalog
    Assertions.assertFalse(
        ops.dropSchema(
            NameIdentifierUtil.ofSchema(METALAKE_NAME, "non-existent-catalog", schemaName2),
            false));
  }

  @Test
  public void testValidateProvisionedLocationAcceptsDirectoryPath() {
    NameIdentifier tableIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "table1");

    Assertions.assertEquals(
        "s3://bucket/db/table/",
        GenericCatalogOperations.validateProvisionedLocation(
            "s3://bucket/db/table/", "default", tableIdent));
  }

  @Test
  public void testValidateProvisionedLocationRejectsBlank() {
    NameIdentifier tableIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "table1");

    for (String location : Arrays.asList(null, "", "   ")) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  GenericCatalogOperations.validateProvisionedLocation(
                      location, "custom", tableIdent));
      Assertions.assertTrue(
          e.getMessage().contains("returned a null or blank location"), e.getMessage());
      Assertions.assertTrue(e.getMessage().contains("custom"), e.getMessage());
    }
  }

  @Test
  public void testValidateProvisionedLocationKeepsPathVerbatim() {
    NameIdentifier tableIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema1", "table1");

    // The shape of the path belongs to the provider: a location without a trailing slash is
    // accepted and stored exactly as returned, so that the provider unprovisioning it later sees
    // the same string it handed out.
    Assertions.assertEquals(
        "s3://bucket/db/9f1c2e04-6b3a-4a17-bd6e-1c0a5f2d8e77",
        GenericCatalogOperations.validateProvisionedLocation(
            "s3://bucket/db/9f1c2e04-6b3a-4a17-bd6e-1c0a5f2d8e77", "custom", tableIdent));
  }

  @Test
  public void testDropTableUnprovisionsItsLocation() {
    NameIdentifier schemaIdent = createSchema();
    NameIdentifier tableIdent = createTable(schemaIdent, "table1");

    Assertions.assertTrue(opsWithFakeProvider.dropTable(tableIdent));

    List<TableLocationContext> unprovisioned = FakeTableLocationProvider.unprovisioned();
    Assertions.assertEquals(1, unprovisioned.size());
    TableLocationContext context = unprovisioned.get(0);
    Assertions.assertEquals(tableIdent, context.tableIdentifier());
    // The location the provider has to hand back reaches it through the stored table properties,
    // which are read before the table is removed.
    Assertions.assertEquals(
        locationOf("table1"), context.tableProperties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(
        FakeTableDelegator.TABLE_FORMAT,
        context.tableProperties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals(schemaIdent.name(), context.schema().name());
  }

  @Test
  public void testPurgeTableUnprovisionsItsLocation() {
    NameIdentifier schemaIdent = createSchema();
    NameIdentifier tableIdent = createTable(schemaIdent, "table1");

    Assertions.assertTrue(opsWithFakeProvider.purgeTable(tableIdent));

    List<TableLocationContext> unprovisioned = FakeTableLocationProvider.unprovisioned();
    Assertions.assertEquals(1, unprovisioned.size());
    Assertions.assertEquals(tableIdent, unprovisioned.get(0).tableIdentifier());
    Assertions.assertEquals(
        locationOf("table1"), unprovisioned.get(0).tableProperties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testDropNonExistentTableDoesNotUnprovision() {
    NameIdentifier schemaIdent = createSchema();
    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "no-such-table");

    Assertions.assertFalse(opsWithFakeProvider.dropTable(tableIdent));
    Assertions.assertTrue(FakeTableLocationProvider.unprovisioned().isEmpty());
  }

  @Test
  public void testDropSchemaWithCascadeUnprovisionsEveryTableLocation() {
    NameIdentifier schemaIdent = createSchema();
    NameIdentifier tableIdent1 = createTable(schemaIdent, "table1");
    NameIdentifier tableIdent2 = createTable(schemaIdent, "table2");

    // Load both tables so that their format is cached, to tell an invalidated cache apart from a
    // cache that was never populated.
    opsWithFakeProvider.loadTable(tableIdent1);
    opsWithFakeProvider.loadTable(tableIdent2);
    Assertions.assertNotNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent1));
    Assertions.assertNotNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent2));

    Assertions.assertTrue(opsWithFakeProvider.dropSchema(schemaIdent, true /* cascade */));

    Set<NameIdentifier> unprovisionedIdents =
        FakeTableLocationProvider.unprovisioned().stream()
            .map(TableLocationContext::tableIdentifier)
            .collect(Collectors.toSet());
    Assertions.assertEquals(Set.of(tableIdent1, tableIdent2), unprovisionedIdents);

    // The cascade also has to clear the cached table formats, which it only does when it goes
    // through the catalog level dropTable.
    Assertions.assertNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent1));
    Assertions.assertNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent2));
  }

  @Test
  public void testFailedUnprovisionStillReportsTheDropAsSuccessful() {
    NameIdentifier schemaIdent = createSchema();
    NameIdentifier tableIdent = createTable(schemaIdent, "table1");
    FakeTableLocationProvider.failOnUnprovision(true);

    // The table is already gone by the time the provider is called, so failing the request would
    // report a drop that did happen as unsuccessful. The failure is logged instead.
    Assertions.assertTrue(opsWithFakeProvider.dropTable(tableIdent));
    Assertions.assertEquals(1, FakeTableLocationProvider.unprovisioned().size());
    Assertions.assertThrows(
        NoSuchTableException.class, () -> opsWithFakeProvider.loadTable(tableIdent));
  }

  @Test
  public void testFailedUnprovisionDoesNotAbortACascadingSchemaDrop() {
    NameIdentifier schemaIdent = createSchema();
    createTable(schemaIdent, "table1");
    createTable(schemaIdent, "table2");
    FakeTableLocationProvider.failOnUnprovision(true);

    Assertions.assertTrue(opsWithFakeProvider.dropSchema(schemaIdent, true /* cascade */));

    // Both tables are attempted, rather than the first failure leaving the second one behind.
    Assertions.assertEquals(2, FakeTableLocationProvider.unprovisioned().size());
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> opsWithFakeProvider.loadSchema(schemaIdent));
  }

  private NameIdentifier createSchema() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    opsWithFakeProvider.createSchema(
        schemaIdent, "schema comment", StringIdentifier.newPropertiesWithId(stringId, null));
    return schemaIdent;
  }

  /**
   * Puts a table entity straight into the entity store, instead of going through createTable, so
   * that the drop paths can be exercised without a real table format behind them.
   */
  private NameIdentifier createTable(NameIdentifier schemaIdent, String tableName) {
    TableEntity table =
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(tableName)
            .withNamespace(Namespace.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name()))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withProperties(
                ImmutableMap.of(
                    Table.PROPERTY_TABLE_FORMAT,
                    FakeTableDelegator.TABLE_FORMAT,
                    Table.PROPERTY_LOCATION,
                    locationOf(tableName)))
            .build();
    Assertions.assertDoesNotThrow(() -> store.put(table, false));
    return NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), tableName);
  }

  private static String locationOf(String tableName) {
    return FakeTableLocationProvider.LOCATION_PREFIX + tableName + "/";
  }

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }
}
