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
package org.apache.gravitino.catalog.lakehouse.iceberg;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.lakehouse.generic.GenericCatalogOperations;
import org.apache.gravitino.catalog.lakehouse.generic.GenericCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.generic.GenericSchemaPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.generic.GenericTablePropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.generic.LakehouseTableDelegator;
import org.apache.gravitino.catalog.lakehouse.generic.LakehouseTableDelegatorFactory;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergTableDelegator {

  private static final String STORE_PATH =
      "/tmp/gravitino_test_iceberg_" + UUID.randomUUID().toString().replace("-", "");
  private static final String METALAKE_NAME = "metalake_for_iceberg_generic_test";
  private static final String CATALOG_NAME = "lakehouse_generic_catalog";

  private static EntityStore store;
  private static IdGenerator idGenerator;
  private static GenericCatalogOperations ops;

  @BeforeAll
  public static void setUp() throws Exception {
    Config config = Mockito.mock(Config.class);
    when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(STORE_PATH);
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
    when(config.get(Configs.CACHE_ENABLED)).thenReturn(false);

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    idGenerator = RandomIdGenerator.INSTANCE;

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
            .withProvider("lakehouse-generic")
            .withType(Catalog.Type.RELATIONAL)
            .withAuditInfo(auditInfo)
            .build();
    store.put(catalog, false);

    ops = new GenericCatalogOperations(store, idGenerator);
    ops.initialize(
        Collections.emptyMap(),
        Mockito.mock(CatalogInfo.class),
        new GenericPropertiesMetadata());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (ops != null) {
      ops.close();
    }
    if (store != null) {
      store.close();
    }
    FileUtils.deleteDirectory(new File(STORE_PATH));
  }

  @Test
  public void testDelegatorIsRegistered() {
    Map<String, LakehouseTableDelegator> delegators =
        LakehouseTableDelegatorFactory.tableDelegators();
    Set<String> formats = delegators.keySet();
    Assertions.assertTrue(formats.contains(IcebergTableDelegator.ICEBERG_TABLE_FORMAT));
  }

  @Test
  public void testCreateAndDropIcebergTable()
      throws NoSuchSchemaException, SchemaAlreadyExistsException, TableAlreadyExistsException,
          NoSuchTableException, IOException {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema(METALAKE_NAME, CATALOG_NAME, schemaName);
    Map<String, String> schemaProperties =
        StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(idGenerator.nextId()), null);

    Schema schema = ops.createSchema(schemaIdent, "schema comment", schemaProperties);
    Assertions.assertEquals(schemaName, schema.name());

    String tableName = "tbl_" + UUID.randomUUID().toString().replace("-", "");
    NameIdentifier tableIdent =
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, schemaName, tableName);

    Map<String, String> tableProperties =
        StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(idGenerator.nextId()), null);
    Path tableLocation = Files.createTempDirectory("iceberg_generic_tbl");
    tableProperties.put(Table.PROPERTY_LOCATION, tableLocation.toString());
    tableProperties.put(Table.PROPERTY_TABLE_FORMAT, IcebergTableDelegator.ICEBERG_TABLE_FORMAT);
    tableProperties.put(Table.PROPERTY_EXTERNAL, "true");

    Column[] columns =
        new Column[] {
          Column.of("id", Types.LongType.get(), "id column"),
          Column.of("data", Types.StringType.get(), "data column")
        };

    Table createdTable =
        ops.createTable(
            tableIdent,
            columns,
            "table comment",
            tableProperties,
            Transforms.EMPTY_TRANSFORM,
            null,
            null,
            null);

    Assertions.assertEquals(tableName, createdTable.name());
    Assertions.assertEquals(
        IcebergTableDelegator.ICEBERG_TABLE_FORMAT,
        createdTable.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals(
        tableLocation.toString() + "/",
        createdTable.properties().get(Table.PROPERTY_LOCATION));

    Table loadedTable = ops.loadTable(tableIdent);
    Assertions.assertEquals(
        createdTable.properties().get(Table.PROPERTY_LOCATION),
        loadedTable.properties().get(Table.PROPERTY_LOCATION));

    Assertions.assertTrue(ops.dropTable(tableIdent));
    Assertions.assertFalse(ops.dropTable(tableIdent));
    FileUtils.deleteDirectory(tableLocation.toFile());
  }

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }

  private static class GenericPropertiesMetadata implements HasPropertyMetadata {
    private final PropertiesMetadata catalogProps = new GenericCatalogPropertiesMetadata();
    private final PropertiesMetadata schemaProps = new GenericSchemaPropertiesMetadata();
    private final PropertiesMetadata tableProps = new GenericTablePropertiesMetadata();

    @Override
    public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
      return catalogProps;
    }

    @Override
    public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
      return schemaProps;
    }

    @Override
    public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
      return tableProps;
    }
  }
}
