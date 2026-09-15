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
import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.apache.gravitino.Entity.EntityType.TABLE;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
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
  private static GenericCatalogOperations opsWithDefaultProvider;

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

    // A second instance, initialized so that it uses the test provider, to exercise the provision
    // and unprovision callbacks. The shared instance above is never initialized and has no
    // provider.
    opsWithFakeProvider = new GenericCatalogOperations(store, idGenerator);
    opsWithFakeProvider.initialize(
        ImmutableMap.of(
            GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER,
            FakeTableLocationProvider.NAME),
        null /* CatalogInfo, unused */,
        new GenericCatalog());

    // A third instance left on the built-in provider, to show that an external table carrying its
    // own location is resolved the same way whichever provider a catalog is configured with.
    opsWithDefaultProvider = new GenericCatalogOperations(store, idGenerator);
    opsWithDefaultProvider.initialize(
        ImmutableMap.of(), null /* CatalogInfo, unused */, new GenericCatalog());
  }

  @BeforeEach
  public void resetProvider() {
    FakeTableLocationProvider.reset();
    FakeTableDelegator.reset();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    ops.close();
    opsWithFakeProvider.close();
    opsWithDefaultProvider.close();
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
  public void testACatalogPropertyWithANullValueDoesNotFailInitialization() throws IOException {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(
        GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER, FakeTableLocationProvider.NAME);
    conf.put("a-property-with-no-value", null);

    // A null property value must not be able to fail the creation of a whole catalog. This is the
    // catalog-level counterpart of testAPropertyWithANullValueReachesTheProvider.
    GenericCatalogOperations catalogOps = new GenericCatalogOperations(store, idGenerator);
    catalogOps.initialize(conf, null /* CatalogInfo, unused */, new GenericCatalog());

    try {
      NameIdentifier schemaIdent = createSchema();
      Table created =
          createTableThroughCatalog(catalogOps, schemaIdent, "table1", ImmutableMap.of());
      Assertions.assertEquals(
          locationOf("table1"), created.properties().get(Table.PROPERTY_LOCATION));
    } finally {
      catalogOps.close();
    }
  }

  @Test
  public void testDroppingATableReadsItsEntityOnlyOnce() throws IOException {
    NameIdentifier schemaIdent = createSchema();
    NameIdentifier tableIdent = createTable(schemaIdent, "table1");

    EntityStore spiedStore = Mockito.spy(store);
    GenericCatalogOperations catalogOps = new GenericCatalogOperations(spiedStore, idGenerator);
    catalogOps.initialize(
        ImmutableMap.of(
            GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER,
            FakeTableLocationProvider.NAME),
        null /* CatalogInfo, unused */,
        new GenericCatalog());

    try {
      Assertions.assertTrue(catalogOps.dropTable(tableIdent));

      // The properties read to build the unprovision context already carry the table format, so
      // resolving the operations to drop with must not go back to the store for the same entity.
      Mockito.verify(spiedStore, Mockito.times(1)).get(tableIdent, TABLE, TableEntity.class);
    } finally {
      catalogOps.close();
    }
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

  @Test
  public void testAnExternalTableWithALocationReachesTheProviderToo() {
    NameIdentifier schemaIdent = createSchema();
    String suppliedLocation = "s3://caller-owned-bucket/existing/data";

    createTableThroughCatalog(
        opsWithFakeProvider,
        schemaIdent,
        "external_table",
        ImmutableMap.of(
            Table.PROPERTY_EXTERNAL, "true", Table.PROPERTY_LOCATION, suppliedLocation));

    // Registering data that already exists is the case a provider most needs to see rather than
    // have decided for it: both facts it would rule on are on the context, so it can return the
    // supplied path unchanged, place the table elsewhere, or refuse the creation outright.
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());
    TableLocationContext context = FakeTableLocationProvider.provisioned().get(0);
    Assertions.assertTrue(
        Boolean.parseBoolean(context.tableProperties().get(Table.PROPERTY_EXTERNAL)));
    Assertions.assertEquals(
        suppliedLocation, context.tableProperties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testASuppliedLocationIsUnchangedUnderTheBuiltInProvider() {
    NameIdentifier schemaIdent = createSchema();
    // Without a trailing slash on purpose: with one there would be no normalization left to
    // preserve, and preserving it is the whole claim being made here.
    String suppliedLocation = "s3://caller-owned-bucket/existing/data";

    Table external =
        createTableThroughCatalog(
            opsWithDefaultProvider,
            schemaIdent,
            "external1",
            ImmutableMap.of(
                Table.PROPERTY_EXTERNAL, "true", Table.PROPERTY_LOCATION, suppliedLocation));
    Table managed =
        createTableThroughCatalog(
            opsWithDefaultProvider,
            schemaIdent,
            "managed1",
            ImmutableMap.of(Table.PROPERTY_LOCATION, suppliedLocation));

    // Routing a supplied location through the provider is not a behaviour change for a catalog on
    // the built-in one: returning it verbatim is that provider's first branch, and it is the same
    // branch the catalog used to apply itself, trailing slash included.
    Assertions.assertEquals(
        suppliedLocation + "/", external.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(
        suppliedLocation + "/", managed.properties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testACallerSuppliedLocationReachesTheProvider() {
    NameIdentifier schemaIdent = createSchema();
    String suppliedLocation = "s3://caller-owned-bucket/managed";

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider,
            schemaIdent,
            "managed_table",
            ImmutableMap.of(Table.PROPERTY_LOCATION, suppliedLocation));

    // The catalog does not decide on the provider's behalf. A supplied location is passed through
    // as the `location` property of the context, and what happens to it is the provider's call: a
    // provider enforcing a placement policy exists precisely for this request, and one that keeps
    // the caller's choice returns it unchanged. This provider allocates, so it allocates.
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());
    Assertions.assertEquals(
        suppliedLocation,
        FakeTableLocationProvider.provisioned()
            .get(0)
            .tableProperties()
            .get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(
        FakeTableLocationProvider.LOCATION_PREFIX + "managed_table/",
        created.properties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testAnUnusedProvisionedLocationIsNotHandedBack() {
    NameIdentifier schemaIdent = createSchema();
    String preExistingLocation = "s3://already-there/exist-ok-table/";
    FakeTableDelegator.useLocationInstead(preExistingLocation);

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider, schemaIdent, "exist_ok_table", ImmutableMap.of());

    // A format may decline the location it was given and still report success: an EXIST_OK
    // creation mode returns the table that already exists, at the location it already had.
    Assertions.assertEquals(preExistingLocation, created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());

    // The location provisioned for this call is leaked, silently, and that is deliberate rather
    // than overlooked. The table this was called for is alive, so a provider that reclaims by
    // table identity would delete a live registration if the drop callback fired here. Reclaiming
    // the stray allocation is left to the provider's own reconciliation, which the contract
    // already requires for four other cases it cannot be told about either.
    Assertions.assertTrue(
        FakeTableLocationProvider.unprovisioned().isEmpty(),
        "A creation must never reach the drop callback, even when its location went unused");
  }

  @Test
  public void testAPropertyWithANullValueReachesTheProvider() {
    NameIdentifier schemaIdent = createSchema();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("a-property-with-no-value", null);

    // The catalog accepts a null property value, so building the context must not be what turns
    // such a request into a failure.
    Table created =
        createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "null_value_table", properties);

    Assertions.assertEquals(
        locationOf("null_value_table"), created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());
    Map<String, String> seen = FakeTableLocationProvider.provisioned().get(0).tableProperties();
    Assertions.assertTrue(seen.containsKey("a-property-with-no-value"));
    Assertions.assertNull(seen.get("a-property-with-no-value"));
  }

  @Test
  public void testProvisionedLocationIsKeptWhenTheFormatUsesIt() {
    NameIdentifier schemaIdent = createSchema();

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider, schemaIdent, "normal_table", ImmutableMap.of());

    Assertions.assertEquals(
        locationOf("normal_table"), created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());
    Assertions.assertTrue(
        FakeTableLocationProvider.unprovisioned().isEmpty(),
        "Creating a table must not reach the drop callback");
  }

  @Test
  public void testExternalTableWithoutLocationStillConsultsProvider() {
    NameIdentifier schemaIdent = createSchema();

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider,
            schemaIdent,
            "external_no_location",
            ImmutableMap.of(Table.PROPERTY_EXTERNAL, "true"));

    // There is no existing location to preserve here, so this case is left as it was: the table
    // format decides whether an external table without a location is valid at all.
    Assertions.assertEquals(
        locationOf("external_no_location"), created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());
  }

  @Test
  public void testDroppingExternalTableSkipsUnprovision() {
    NameIdentifier schemaIdent = createSchema();
    String suppliedLocation = "s3://caller-owned-bucket/existing/data/";
    createTableThroughCatalog(
        opsWithFakeProvider,
        schemaIdent,
        "external_table",
        ImmutableMap.of(
            Table.PROPERTY_EXTERNAL, "true", Table.PROPERTY_LOCATION, suppliedLocation));
    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "external_table");

    Assertions.assertTrue(opsWithFakeProvider.dropTable(tableIdent));

    // The data under an external table's location is not the catalog's to reclaim: the table
    // formats leave it in place on drop, and asking a provider to hand the location back would
    // invite it to delete exactly that data. Leaking beats deleting, so the callback is skipped.
    Assertions.assertTrue(FakeTableLocationProvider.unprovisioned().isEmpty());
  }

  @Test
  public void testDroppingExternalTableSkipsUnprovisionInACascade() {
    NameIdentifier schemaIdent = createSchema();
    createTableThroughCatalog(
        opsWithFakeProvider,
        schemaIdent,
        "external_table",
        ImmutableMap.of(
            Table.PROPERTY_EXTERNAL,
            "true",
            Table.PROPERTY_LOCATION,
            "s3://caller-owned-bucket/existing/data/"));
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "managed_table", ImmutableMap.of());

    Assertions.assertTrue(opsWithFakeProvider.dropSchema(schemaIdent, true /* cascade */));

    // Only the managed table's location comes back; the external one is left alone, exactly as on
    // a single drop.
    List<TableLocationContext> unprovisioned = FakeTableLocationProvider.unprovisioned();
    Assertions.assertEquals(1, unprovisioned.size());
    Assertions.assertEquals("managed_table", unprovisioned.get(0).tableIdentifier().name());
  }

  @Test
  public void testACascadeResolvesTheSchemaOnlyOnce() {
    NameIdentifier schemaIdent = createSchema();
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "t1", ImmutableMap.of());
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "t2", ImmutableMap.of());
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "t3", ImmutableMap.of());

    Assertions.assertTrue(opsWithFakeProvider.dropSchema(schemaIdent, true /* cascade */));

    // Three tables, one parent schema. Every context hands back the very same Schema instance,
    // which it could only do if the cascade resolved it once and shared it.
    List<TableLocationContext> unprovisioned = FakeTableLocationProvider.unprovisioned();
    Assertions.assertEquals(3, unprovisioned.size());
    Schema resolvedOnce = unprovisioned.get(0).schema();
    Assertions.assertSame(resolvedOnce, unprovisioned.get(1).schema());
    Assertions.assertSame(resolvedOnce, unprovisioned.get(2).schema());
  }

  @Test
  public void testPurgingAnExternalTableStillUnprovisions() {
    NameIdentifier schemaIdent = createSchema();
    String suppliedLocation = "s3://caller-owned-bucket/existing/data/";
    createTableThroughCatalog(
        opsWithFakeProvider,
        schemaIdent,
        "external_purged",
        ImmutableMap.of(
            Table.PROPERTY_EXTERNAL, "true", Table.PROPERTY_LOCATION, suppliedLocation));
    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "external_purged");

    Assertions.assertTrue(opsWithFakeProvider.purgeTable(tableIdent));

    // The mirror of testDroppingExternalTableSkipsUnprovision, and the reason that one cannot be
    // stated as "external tables are never unprovisioned". A purge is the explicit request to
    // destroy the data, and LanceTableOperations#purgeTable honours it for an external table --
    // deleting the dataset its dropTable leaves alone. Skipping here would leak an allocation
    // whose data the catalog had just deleted.
    List<TableLocationContext> unprovisioned = FakeTableLocationProvider.unprovisioned();
    Assertions.assertEquals(1, unprovisioned.size());
    Assertions.assertTrue(
        Boolean.parseBoolean(unprovisioned.get(0).tableProperties().get(Table.PROPERTY_EXTERNAL)));
    // The location handed back is the one that was stored, which for this catalog is the one the
    // provider itself returned when it saw the supplied value.
    Assertions.assertEquals(
        FakeTableLocationProvider.LOCATION_PREFIX + "external_purged/",
        unprovisioned.get(0).tableProperties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testAnExternalTableIsUnprovisionedOnPurgeButNotOnDrop() {
    NameIdentifier schemaIdent = createSchema();
    Map<String, String> external =
        ImmutableMap.of(
            Table.PROPERTY_EXTERNAL, "true",
            Table.PROPERTY_LOCATION, "s3://caller-owned-bucket/existing/");
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "to_drop", external);
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "to_purge", external);

    opsWithFakeProvider.dropTable(
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "to_drop"));
    opsWithFakeProvider.purgeTable(
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "to_purge"));

    // The whole of the drop-versus-purge distinction, stated where it is actually made. The
    // provider is told about the purge and not about the drop, which is the catalog's decision to
    // make: by the time the callback runs the data is gone, so there is nothing left for a
    // provider to decide differently, and the skipped case is the one where reclaiming by path
    // would destroy a user's data.
    List<TableLocationContext> unprovisioned = FakeTableLocationProvider.unprovisioned();
    Assertions.assertEquals(1, unprovisioned.size());
    Assertions.assertEquals("to_purge", unprovisioned.get(0).tableIdentifier().name());
  }

  @Test
  public void testAFormatFailingAfterRemovingMetadataStillInvalidatesTheFormatCache() {
    NameIdentifier schemaIdent = createSchema();
    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "half_dropped", ImmutableMap.of());
    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "half_dropped");
    Assertions.assertNotNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent));

    FakeTableDelegator.failAfterRemovingMetadata(true);
    Assertions.assertThrows(
        RuntimeException.class, () -> opsWithFakeProvider.dropTable(tableIdent));

    // The metadata is gone even though the drop threw, so a cached format here would be read for
    // whatever is created next under the same name.
    Assertions.assertNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent));
    // Not unprovisioned: the format may equally have failed before touching storage, and handing
    // back a location that still has data under it is the unrecoverable direction.
    Assertions.assertTrue(FakeTableLocationProvider.unprovisioned().isEmpty());
  }

  @Test
  public void testANullProviderNameFallsBackToTheBuiltInProvider() throws IOException {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER, null);
    conf.put("a-property-with-no-value", null);

    // getOrDefault returns decode(null) rather than the default when the key is present with a
    // null value, so without the fallback this would fail the whole catalog with "must not be
    // blank" -- on a property the server accepted, and in a catalog that tolerates null values
    // everywhere else.
    GenericCatalogOperations catalogOps = new GenericCatalogOperations(store, idGenerator);
    catalogOps.initialize(conf, null /* CatalogInfo, unused */, new GenericCatalog());

    try {
      NameIdentifier schemaIdent = createSchema("s3://schema-bucket/db");
      Table created =
          createTableThroughCatalog(catalogOps, schemaIdent, "table1", ImmutableMap.of());

      // The built-in chain, not the test provider: a null name means the property was not set.
      Assertions.assertEquals(
          "s3://schema-bucket/db/table1/", created.properties().get(Table.PROPERTY_LOCATION));
      Assertions.assertTrue(FakeTableLocationProvider.provisioned().isEmpty());
    } finally {
      catalogOps.close();
    }
  }

  @Test
  public void testDroppingAnEmptySchemaDoesNotResolveIt() throws IOException {
    EntityStore spy = Mockito.spy(store);
    GenericCatalogOperations catalogOps = new GenericCatalogOperations(spy, idGenerator);
    catalogOps.initialize(
        ImmutableMap.of(
            GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER,
            FakeTableLocationProvider.NAME),
        null /* CatalogInfo, unused */,
        new GenericCatalog());

    try {
      NameIdentifier schemaIdent = createSchema();
      Mockito.clearInvocations(spy);

      Assertions.assertTrue(catalogOps.dropSchema(schemaIdent, true /* cascade */));

      // The schema is resolved to build the contexts of the tables being cascaded, so with no
      // tables there is nothing to resolve it for. Dropping the schema itself is a single delete,
      // so this read is the catalog's alone and every drop of an empty schema would pay for it.
      Mockito.verify(spy, Mockito.never())
          .get(Mockito.eq(schemaIdent), Mockito.eq(SCHEMA), Mockito.any());
    } finally {
      catalogOps.close();
    }
  }

  @Test
  public void testAProviderRefusingTheCreationLeavesNoTable() {
    NameIdentifier schemaIdent = createSchema();
    FakeTableLocationProvider.provisionLocation(null);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            createTableThroughCatalog(
                opsWithFakeProvider, schemaIdent, "never_created", ImmutableMap.of()));

    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "never_created");
    Assertions.assertFalse(opsWithFakeProvider.tableExists(tableIdent));
    // The format cache is populated after a successful creation, so a refused one must leave no
    // entry behind for a later table of the same name to read.
    Assertions.assertNull(opsWithFakeProvider.tableFormatCache().getIfPresent(tableIdent));
  }

  private NameIdentifier createSchema() {
    return createSchema(null);
  }

  /**
   * Creates a schema, optionally carrying a {@code location} of its own so that the built-in
   * provider has a level to derive a table location from.
   */
  private NameIdentifier createSchema(@Nullable String location) {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    StringIdentifier stringId = StringIdentifier.fromId(idGenerator.nextId());
    Map<String, String> properties =
        location == null ? null : ImmutableMap.of(Table.PROPERTY_LOCATION, location);
    opsWithFakeProvider.createSchema(
        schemaIdent, "schema comment", StringIdentifier.newPropertiesWithId(stringId, properties));
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

  /**
   * Creates a table through the catalog level createTable, so that the location resolution under
   * test actually runs, unlike {@link #createTable(NameIdentifier, String)} which bypasses it.
   */
  private Table createTableThroughCatalog(
      GenericCatalogOperations catalogOps,
      NameIdentifier schemaIdent,
      String tableName,
      Map<String, String> properties) {
    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), tableName);
    // A plain map rather than an ImmutableMap: a creation request may carry a property whose
    // value is null, and the test helper must be able to pass one through.
    Map<String, String> allProperties = Maps.newHashMap(properties);
    allProperties.put(Table.PROPERTY_TABLE_FORMAT, FakeTableDelegator.TABLE_FORMAT);
    allProperties.putAll(
        StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(idGenerator.nextId()), null));

    return catalogOps.createTable(
        tableIdent,
        new Column[0],
        "table comment",
        allProperties,
        new Transform[0],
        null /* distribution */,
        new SortOrder[0],
        new Index[0]);
  }

  private static String locationOf(String tableName) {
    return FakeTableLocationProvider.LOCATION_PREFIX + tableName + "/";
  }

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }

  @Test
  public void testABlankProvisionedLocationFailsTheCreation() {
    NameIdentifier schemaIdent = createSchema();

    for (String location : Arrays.asList(null, "", "   ")) {
      FakeTableLocationProvider.provisionLocation(location);
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  createTableThroughCatalog(
                      opsWithFakeProvider, schemaIdent, "blank_location", ImmutableMap.of()));
      Assertions.assertTrue(
          e.getMessage().contains("returned a null or blank location"), e.getMessage());
      Assertions.assertTrue(
          e.getMessage().contains(FakeTableLocationProvider.NAME), e.getMessage());
    }
  }

  @Test
  public void testAProvisionedLocationIsStoredVerbatim() {
    NameIdentifier schemaIdent = createSchema();
    // No trailing slash: the shape of the path belongs to the provider, so the catalog stores it
    // exactly as returned rather than normalizing it, and the provider unprovisioning it later
    // sees the same string it handed out.
    String opaque = "testing://bucket/9f1c2e04-6b3a-4a17-bd6e-1c0a5f2d8e77";
    FakeTableLocationProvider.provisionLocation(opaque);

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider, schemaIdent, "verbatim_location", ImmutableMap.of());

    Assertions.assertEquals(opaque, created.properties().get(Table.PROPERTY_LOCATION));

    // A trailing slash is left alone for the same reason. The catalog does append one when it
    // compares the location it handed out with the one the created table reports, so that a
    // provider is not told its location was declined over a slash; that normalization belongs to
    // the comparison and must not reach the stored property.
    String directory = "testing://bucket/9f1c2e04-6b3a-4a17-bd6e-1c0a5f2d8e77/";
    FakeTableLocationProvider.provisionLocation(directory);

    Table createdInDirectory =
        createTableThroughCatalog(
            opsWithFakeProvider, schemaIdent, "verbatim_directory", ImmutableMap.of());

    Assertions.assertEquals(
        directory, createdInDirectory.properties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testANonCanonicalExternalValueIsReadTheSameWayTheFormatsReadIt() {
    // `external=yes` is not external to the table formats: they read the flag with
    // Boolean.parseBoolean or an equalsIgnoreCase("true"), so they see false and treat the table
    // as managed, deleting its data on drop. If the catalog disagreed and read it as external, it
    // would skip handing the location back for a table whose data was just deleted: the data lost
    // and the allocation leaked at once. So the drop must reach the provider.
    NameIdentifier schemaIdent = createSchema();
    createTableThroughCatalog(
        opsWithFakeProvider,
        schemaIdent,
        "yes_table",
        ImmutableMap.of(Table.PROPERTY_EXTERNAL, "yes"));
    NameIdentifier tableIdent =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaIdent.name(), "yes_table");

    Assertions.assertTrue(opsWithFakeProvider.dropTable(tableIdent));

    Assertions.assertEquals(
        1,
        FakeTableLocationProvider.unprovisioned().size(),
        "'yes' is not read as external by the formats, so the catalog must not read it so either");
  }

  @Test
  public void testTheCatalogPropertiesReachTheProvider() throws IOException {
    // The built-in provider's last fallback is the catalog's own `location`, and with no
    // initialization callback the only way it can see that property is through the context. This
    // is the end-to-end wiring of it: a catalog that has a location, a schema that does not, and a
    // table that must therefore land under {catalog location}/{schema}/{table}/.
    GenericCatalogOperations catalogOps = new GenericCatalogOperations(store, idGenerator);
    catalogOps.initialize(
        ImmutableMap.of(Catalog.PROPERTY_LOCATION, "s3://catalog-owned"),
        null /* CatalogInfo, unused */,
        new GenericCatalog());
    try {
      NameIdentifier schemaIdent = createSchema();

      Table created =
          createTableThroughCatalog(catalogOps, schemaIdent, "catalog_level", ImmutableMap.of());

      Assertions.assertEquals(
          "s3://catalog-owned/" + schemaIdent.name() + "/catalog_level/",
          created.properties().get(Table.PROPERTY_LOCATION));
    } finally {
      catalogOps.close();
    }
  }

  @Test
  public void testManagedTableWithoutLocationConsultsProvider() {
    NameIdentifier schemaIdent = createSchema();

    createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "managed_table", ImmutableMap.of());

    // The fourth cell of the external x supplied-location matrix: no location and not external is
    // the ordinary case the provider exists for.
    List<TableLocationContext> provisioned = FakeTableLocationProvider.provisioned();
    Assertions.assertEquals(1, provisioned.size());
    Assertions.assertEquals("managed_table", provisioned.get(0).tableIdentifier().name());
    Assertions.assertFalse(
        Boolean.parseBoolean(provisioned.get(0).tableProperties().get(Table.PROPERTY_EXTERNAL)));
  }
}
