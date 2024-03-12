/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogDorisIT extends AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(CatalogDorisIT.class);

  private static final String provider = "jdbc-doris";
  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";

  private static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  public String metalakeName = GravitinoITUtils.genRandomName("doris_it_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("doris_it_catalog");
  public String schemaName = GravitinoITUtils.genRandomName("doris_it_schema");
  public String tableName = GravitinoITUtils.genRandomName("doris_it_table");

  public String table_comment = "table_comment";

  // Doris doesn't support schema comment
  public String schema_comment = null;
  public String DORIS_COL_NAME1 = "doris_col_name1";
  public String DORIS_COL_NAME2 = "doris_col_name2";
  public String DORIS_COL_NAME3 = "doris_col_name3";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private GravitinoMetaLake metalake;

  protected Catalog catalog;

  @BeforeAll
  public void startup() throws IOException {

    if (!ITUtils.EMBEDDED_TEST_MODE.equals(AbstractIT.testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-doris/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }

    containerSuite.startDorisContainer();

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    AbstractIT.client.dropMetalake(NameIdentifier.of(metalakeName));
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private static void waitForDorisOperation() {
    // TODO: use a better way to wait for the operation to complete
    // see: https://doris.apache.org/docs/1.2/advanced/alter-table/schema-change/
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  private void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(metalakeName, catalogName, schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().dropTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), true);
  }

  private void createMetalake() {
    GravitinoMetaLake[] gravitinoMetaLakes = AbstractIT.client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetaLake createdMetalake =
        AbstractIT.client.createMetalake(
            NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitinoMetaLake loadMetalake =
        AbstractIT.client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    DorisContainer dorisContainer = containerSuite.getDorisContainer();

    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            dorisContainer.getContainerIpAddress(), DorisContainer.FE_MYSQL_PORT);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), DorisContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), DorisContainer.PASSWORD);

    Catalog createdCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            provider,
            "doris catalog comment",
            catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    String propKey = "key";
    String propValue = "value";
    Map<String, String> prop = Maps.newHashMap();
    prop.put(propKey, propValue);

    Schema createdSchema = catalog.asSchemas().createSchema(ident, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());

    Assertions.assertEquals(createdSchema.properties().get(propKey), propValue);
  }

  @Test
  void testDropDorisSchema() {
    String schemaName = GravitinoITUtils.genRandomName("doris_it_schema_dropped").toLowerCase();

    catalog
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, catalogName, schemaName),
            null,
            ImmutableMap.<String, String>builder().build());

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            createColumns(),
            "Created by gravitino client",
            createProperties(),
            Transforms.EMPTY_TRANSFORM,
            createDistribution(),
            null);

    // Try to drop a database, and cascade equals to false, it should not be allowed.
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), false);
    // Check the database still exists
    catalog.asSchemas().loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaName));

    // Try to drop a database, and cascade equals to true, it should be allowed.
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), true);
    // Check database has been dropped
    SupportsSchemas schemas = catalog.asSchemas();
    NameIdentifier of = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          schemas.loadSchema(of);
        });
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName(DORIS_COL_NAME1)
            .withDataType(Types.IntegerType.get())
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(DORIS_COL_NAME2)
            .withDataType(Types.DateType.get())
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(DORIS_COL_NAME3)
            .withDataType(Types.VarCharType.of(10))
            .withComment("col_3_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3};
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("replication_allocation", "tag.location.default: 1");
    return properties;
  }

  private Distribution createDistribution() {
    return Distributions.hash(32, NamedReference.field(DORIS_COL_NAME1));
  }

  @Test
  void testCreateAndLoadDorisSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    Namespace namespace = Namespace.of(metalakeName, catalogName);

    // test list schemas
    NameIdentifier[] nameIdentifiers = schemas.listSchemas(namespace);
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // test create schema already exists
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    Map<String, NameIdentifier> schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertTrue(schemaMap.containsKey(testSchemaName));

    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> {
          schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap());
        });

    // test drop schema
    Assertions.assertTrue(schemas.dropSchema(schemaIdent, false));

    // check schema is deleted
    // 1. check by load schema
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent));

    // 2. check by list schema
    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertFalse(schemaMap.containsKey(testSchemaName));

    // test drop schema not exists
    NameIdentifier notExistsSchemaIdent = NameIdentifier.of(metalakeName, catalogName, "no-exits");
    Assertions.assertFalse(schemas.dropSchema(notExistsSchemaIdent, false));
  }

  @Test
  void testCreateAndLoadDorisTable() {
    // create a table
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    ColumnDTO[] columns = createColumns();

    Distribution distribution = createDistribution();

    Index[] indexes =
        new Index[] {
          Indexes.of(Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}})
        };

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            distribution,
            null,
            indexes);
    Assertions.assertEquals(createdTable.name(), tableName);
    Map<String, String> resultProp = createdTable.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertEquals(createdTable.columns().length, columns.length);

    for (int i = 0; i < columns.length; i++) {
      AbstractIT.assertColumn(columns[i], createdTable.columns()[i]);
    }

    // test load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(table_comment, loadTable.comment());
    resultProp = loadTable.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertEquals(loadTable.columns().length, columns.length);
    for (int i = 0; i < columns.length; i++) {
      AbstractIT.assertColumn(columns[i], loadTable.columns()[i]);
    }
  }

  @Test
  void testDorisIndex() {
    String tableName = GravitinoITUtils.genRandomName("test_add_index");

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    ColumnDTO[] columns = createColumns();

    Distribution distribution = createDistribution();

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            distribution,
            null);
    Assertions.assertEquals(createdTable.name(), tableName);

    // add index test.
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}}));

    waitForDorisOperation();
    Table table =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Index[] indexes = table.index();
    assertEquals(1, indexes.length);

    // delete index and add new column and index.
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.deleteIndex("k1_index", true),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY, "k2_index", new String[][] {{DORIS_COL_NAME2}}));

    waitForDorisOperation();

    table =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    indexes = table.index();
    assertEquals(1, indexes.length);
    assertEquals("k2_index", indexes[0].name());
  }
}
