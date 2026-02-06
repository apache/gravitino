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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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
  private static GenericCatalog genericCatalog;

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
    genericCatalog = new GenericCatalog();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    ops.close();
    store.close();
    FileUtils.deleteDirectory(new File(STORE_PATH));
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
  public void testCreateExternalIcebergTable() throws Exception {
    initializeCatalogOps();

    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    Map<String, String> schemaProperties =
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()),
            Collections.singletonMap(Schema.PROPERTY_LOCATION, "/tmp/iceberg_schema"));
    ops.createSchema(schemaIdent, "schema comment", schemaProperties);

    NameIdentifier tableIdent =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, schemaName, "iceberg_table");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(Table.PROPERTY_TABLE_FORMAT, "iceberg");
    tableProperties.put(Table.PROPERTY_EXTERNAL, "true");
    tableProperties.put(Table.PROPERTY_LOCATION, "/tmp/iceberg_table");
    tableProperties =
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), tableProperties);

    Table createdTable =
        ops.createTable(
            tableIdent, columns, "table comment", tableProperties, null, null, null, null);

    Assertions.assertEquals("iceberg", createdTable.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals("true", createdTable.properties().get(Table.PROPERTY_EXTERNAL));
    Assertions.assertEquals(
        "/tmp/iceberg_table/", createdTable.properties().get(Table.PROPERTY_LOCATION));

    Assertions.assertThrows(UnsupportedOperationException.class, () -> ops.purgeTable(tableIdent));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(tableIdent, TableChange.setProperty(Table.PROPERTY_EXTERNAL, "false")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                tableIdent, TableChange.setProperty(Table.PROPERTY_TABLE_FORMAT, "lance")));

    Assertions.assertTrue(ops.dropTable(tableIdent));
  }

  @Test
  public void testCreateIcebergTableWithoutExternal() throws Exception {
    initializeCatalogOps();

    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    Map<String, String> schemaProperties =
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()),
            Collections.singletonMap(Schema.PROPERTY_LOCATION, "/tmp/iceberg_schema_2"));
    ops.createSchema(schemaIdent, "schema comment", schemaProperties);

    NameIdentifier tableIdent =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, schemaName, "iceberg_table_no_ext");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(Table.PROPERTY_TABLE_FORMAT, "iceberg");
    tableProperties.put(Table.PROPERTY_LOCATION, "/tmp/iceberg_table_2");
    Map<String, String> tablePropertiesFinal =
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(idGenerator.nextId()), tableProperties);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                tableIdent,
                columns,
                "table comment",
                tablePropertiesFinal,
                null,
                null,
                null,
                null));
  }

  private void initializeCatalogOps() {
    CatalogInfo catalogInfo =
        new CatalogInfo(
            idGenerator.nextId(),
            CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "generic-lakehouse",
            null,
            Collections.emptyMap(),
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build(),
            Namespace.of(METALAKE_NAME));
    ops.initialize(Collections.emptyMap(), catalogInfo, genericCatalog);
  }

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }
}
