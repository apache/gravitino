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

    // A second instance, initialized so that it uses the test provider, to exercise the location
    // release callbacks. The shared instance above is never initialized and has no provider.
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
  public void testExternalTableWithLocationSkipsProvider() {
    NameIdentifier schemaIdent = createSchema();
    // Deliberately without a trailing slash, so that the only normalization the stored value may
    // have gone through is the one the built-in provider applies to a table-level location.
    String suppliedLocation = "s3://caller-owned-bucket/existing/data";

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider,
            schemaIdent,
            "external_table",
            ImmutableMap.of(
                Table.PROPERTY_EXTERNAL, "true", Table.PROPERTY_LOCATION, suppliedLocation));

    // An external table registers data that already exists. Handing it a freshly allocated path
    // would orphan that data while still reporting the creation as successful.
    Assertions.assertEquals(
        suppliedLocation + "/", created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertTrue(
        FakeTableLocationProvider.provisioned().isEmpty(),
        "The provider must not be consulted for an external table carrying its own location");
  }

  @Test
  public void testExternalTableLocationIsIdenticalUnderDefaultProvider() {
    NameIdentifier schemaIdent = createSchema();
    // Without a trailing slash on purpose: with one, the bypass and the built-in provider agree
    // trivially and this test would prove nothing about the normalization the bypass has to keep.
    String suppliedLocation = "s3://caller-owned-bucket/existing/data";
    Map<String, String> properties =
        ImmutableMap.of(Table.PROPERTY_EXTERNAL, "true", Table.PROPERTY_LOCATION, suppliedLocation);

    Table underFakeProvider =
        createTableThroughCatalog(opsWithFakeProvider, schemaIdent, "external1", properties);
    Table underDefaultProvider =
        createTableThroughCatalog(opsWithDefaultProvider, schemaIdent, "external2", properties);

    // The bypass is not a behaviour change for deployments on the built-in provider: it honoured
    // the supplied location already, and still resolves to the same value, trailing slash included.
    Assertions.assertEquals(
        underDefaultProvider.properties().get(Table.PROPERTY_LOCATION),
        underFakeProvider.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(
        suppliedLocation + "/", underDefaultProvider.properties().get(Table.PROPERTY_LOCATION));
  }

  @Test
  public void testCallerSuppliedLocationSkipsProviderEvenForAManagedTable() {
    NameIdentifier schemaIdent = createSchema();
    String suppliedLocation = "s3://caller-owned-bucket/managed";

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider,
            schemaIdent,
            "managed_table",
            ImmutableMap.of(Table.PROPERTY_LOCATION, suppliedLocation));

    // The provider is consulted only when the catalog is the one choosing the location. A caller
    // that supplies one is usually pointing at data that is already there -- a Lance registration
    // carries no external flag -- and the catalog cannot tell that apart from an override, so it
    // keeps the supplied value in both cases, exactly as it did before the provider existed.
    Assertions.assertEquals(
        suppliedLocation + "/", created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertTrue(
        FakeTableLocationProvider.provisioned().isEmpty(),
        "The provider must not be consulted for a request that carries its own location");
  }

  @Test
  public void testProvisionedLocationIsHandedBackWhenTheFormatDoesNotUseIt() {
    NameIdentifier schemaIdent = createSchema();
    String preExistingLocation = "s3://already-there/exist-ok-table/";
    FakeTableDelegator.useLocationInstead(preExistingLocation);

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider, schemaIdent, "exist_ok_table", ImmutableMap.of());

    // A format may decline the location it was given and still report success: an EXIST_OK
    // creation mode returns the table that already exists, at the location it already had. Nothing
    // can have been written to the location provisioned for this call, so it goes back rather than
    // leaking once per retried create.
    Assertions.assertEquals(preExistingLocation, created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(1, FakeTableLocationProvider.provisioned().size());
    Assertions.assertEquals(1, FakeTableLocationProvider.released().size());
    Assertions.assertEquals(
        locationOf("exist_ok_table"),
        FakeTableLocationProvider.released().get(0).tableProperties().get(Table.PROPERTY_LOCATION));

    // The table this was called for is alive. A provider that reclaims by table identity rather
    // than by path would delete a live registration if the drop callback were reused here, so the
    // drop callback must not fire on the creation path at all.
    Assertions.assertTrue(
        FakeTableLocationProvider.unprovisioned().isEmpty(),
        "The drop callback must not be used to release a location of a table that exists");
  }

  @Test
  public void testUnusedLocationIsNotReleasedByDefault() {
    // The built-in provider derives the location, so this schema has to carry one.
    NameIdentifier schemaIdent = createSchema("s3://schema-owned/");
    FakeTableDelegator.useLocationInstead("s3://already-there/exist-ok-table/");

    // The built-in provider inherits the default body of releaseUnusedLocation, which does nothing.
    // Leaking a stray allocation is the deliberate default; a provider that reclaims real storage
    // opts in.
    Table created =
        createTableThroughCatalog(
            opsWithDefaultProvider, schemaIdent, "exist_ok_default", ImmutableMap.of());

    Assertions.assertEquals(
        "s3://already-there/exist-ok-table/", created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertTrue(FakeTableLocationProvider.released().isEmpty());
    Assertions.assertTrue(FakeTableLocationProvider.unprovisioned().isEmpty());
  }

  @Test
  public void testATrailingSlashAloneDoesNotMakeALocationLookUnused() {
    NameIdentifier schemaIdent = createSchema();
    String provisioned = locationOf("slash_table");
    String withoutSlash = provisioned.substring(0, provisioned.length() - 1);
    FakeTableDelegator.useLocationInstead(withoutSlash);

    Table created =
        createTableThroughCatalog(
            opsWithFakeProvider, schemaIdent, "slash_table", ImmutableMap.of());

    // The catalog adds the trailing slash itself, so a format storing the location without one is
    // using the location it was given, not declining it.
    Assertions.assertEquals(withoutSlash, created.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertTrue(
        FakeTableLocationProvider.released().isEmpty(),
        "A location differing only by a trailing slash is the same location");
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
        FakeTableLocationProvider.released().isEmpty(),
        "A location the format actually used must not be handed back");
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
  public void testContextReportsWhetherTheTableIsExternal() {
    Assertions.assertTrue(
        TableLocationContext.builder()
            .withTableIdentifier(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "s", "t"))
            .withTableProperties(ImmutableMap.of(Table.PROPERTY_EXTERNAL, "TRUE"))
            .withSchema(Mockito.mock(Schema.class))
            .build()
            .isExternal());

    // Absent, and unparseable, both mean "not external" rather than an error.
    Assertions.assertFalse(
        TableLocationContext.builder()
            .withTableIdentifier(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "s", "t"))
            .withTableProperties(ImmutableMap.of())
            .withSchema(Mockito.mock(Schema.class))
            .build()
            .isExternal());
    Assertions.assertFalse(
        TableLocationContext.builder()
            .withTableIdentifier(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "s", "t"))
            .withTableProperties(ImmutableMap.of(Table.PROPERTY_EXTERNAL, "not-a-boolean"))
            .withSchema(Mockito.mock(Schema.class))
            .build()
            .isExternal());
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
  }
}
