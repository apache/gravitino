/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.partitions.Partitioning;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.catalog.jdbc.mysql.service.MysqlService;
import com.datastrato.gravitino.integration.test.catalog.jdbc.utils.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
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
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;

@Tag("gravitino-docker-it")
public class CatalogMysqlIT extends AbstractIT {
  public static String metalakeName = GravitinoITUtils.genRandomName("mysql_it_metalake");
  public static String catalogName = GravitinoITUtils.genRandomName("mysql_it_catalog");
  public static String schemaName = GravitinoITUtils.genRandomName("mysql_it_schema");
  public static String tableName = GravitinoITUtils.genRandomName("mysql_it_table");
  public static String alertTableName = "alert_table_name";
  public static String table_comment = "table_comment";

  public static String schema_comment = "schema_comment";
  public static String MYSQL_COL_NAME1 = "mysql_col_name1";
  public static String MYSQL_COL_NAME2 = "mysql_col_name2";
  public static String MYSQL_COL_NAME3 = "mysql_col_name3";
  public static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";
  private static final String provider = "jdbc-mysql";

  private static GravitinoMetaLake metalake;

  private static Catalog catalog;

  private static MysqlService mysqlService;

  private static MySQLContainer<?> MYSQL_CONTAINER;

  protected static final String TEST_DB_NAME = RandomUtils.nextInt(10000) + "_test_db";

  public static final String mysqlImageName = "mysql:8.0";

  @BeforeAll
  public static void startup() throws IOException {

    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-mysql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }

    MYSQL_CONTAINER =
        new MySQLContainer<>(mysqlImageName)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    MYSQL_CONTAINER.start();
    mysqlService = new MysqlService(MYSQL_CONTAINER);
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public static void stop() {
    clearTableAndSchema();
    client.dropMetalake(NameIdentifier.of(metalakeName));
    mysqlService.close();
    MYSQL_CONTAINER.stop();
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private static void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(metalakeName, catalogName, schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().purgeTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), false);
  }

  private static void createMetalake() {
    GravitinoMetaLake[] gravitinoMetaLakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitinoMetaLake loadMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            MYSQL_CONTAINER.getJdbcUrl(), 0, MYSQL_CONTAINER.getJdbcUrl().lastIndexOf("/")));
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), MYSQL_CONTAINER.getDriverClassName());
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), MYSQL_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), MYSQL_CONTAINER.getPassword());

    Catalog createdCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            provider,
            "comment",
            catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private static void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();

    Schema createdSchema = catalog.asSchemas().createSchema(ident, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName(MYSQL_COL_NAME1)
            .withDataType(Types.IntegerType.get())
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(MYSQL_COL_NAME2)
            .withDataType(Types.DateType.get())
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(MYSQL_COL_NAME3)
            .withDataType(Types.StringType.get())
            .withComment("col_3_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3};
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    return properties;
  }

  @Test
  void testOperationMysqlSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    Namespace namespace = Namespace.of(metalakeName, catalogName);
    // list schema check.
    NameIdentifier[] nameIdentifiers = schemas.listSchemas(namespace);
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    NameIdentifier[] mysqlNamespaces = mysqlService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(mysqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    Map<String, NameIdentifier> schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertTrue(schemaMap.containsKey(testSchemaName));

    mysqlNamespaces = mysqlService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(mysqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap()));

    // drop schema check.
    schemas.dropSchema(schemaIdent, false);
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent));
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> mysqlService.loadSchema(schemaIdent));

    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertFalse(schemaMap.containsKey(testSchemaName));
    Assertions.assertFalse(
        schemas.dropSchema(NameIdentifier.of(metalakeName, catalogName, "no-exits"), false));
    TableCatalog tableCatalog = catalog.asTableCatalog();

    // create failed check.
    NameIdentifier table =
        NameIdentifier.of(metalakeName, catalogName, testSchemaName, "test_table");
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            tableCatalog.createTable(
                table,
                createColumns(),
                table_comment,
                createProperties(),
                null,
                Distributions.NONE,
                null));
    // drop schema failed check.
    Assertions.assertFalse(schemas.dropSchema(schemaIdent, true));
    Assertions.assertFalse(schemas.dropSchema(schemaIdent, false));
    Assertions.assertFalse(tableCatalog.dropTable(table));
    mysqlNamespaces = mysqlService.listSchemas(Namespace.empty());
    schemaNames =
        Arrays.stream(mysqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));
  }

  @Test
  void testCreateAndLoadMysqlTable() {
    // Create table from Gravitino API
    ColumnDTO[] columns = createColumns();

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    final SortOrderDTO[] sortOrders = SortOrderDTO.EMPTY_SORT;

    Partitioning[] partitioning = Partitioning.EMPTY_PARTITIONING;

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            partitioning,
            distribution,
            sortOrders);
    Assertions.assertEquals(createdTable.name(), tableName);
    Map<String, String> resultProp = createdTable.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertEquals(createdTable.columns().length, columns.length);

    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(createdTable.columns()[i], columns[i]);
    }

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
      Assertions.assertEquals(columns[i], loadTable.columns()[i]);
    }
  }

  @Test
  void testAlterAndDropMysqlTable() {
    ColumnDTO[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            columns,
            table_comment,
            createProperties());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog
              .asTableCatalog()
              .alterTable(
                  NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                  TableChange.rename(alertTableName),
                  TableChange.updateComment(table_comment + "_new"));
        });

    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            TableChange.rename(alertTableName));

    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {MYSQL_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnType(new String[] {MYSQL_COL_NAME1}, Types.IntegerType.get()));

    Table table =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName));
    Assertions.assertEquals(alertTableName, table.name());

    Assertions.assertEquals(MYSQL_COL_NAME1, table.columns()[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), table.columns()[0].dataType());

    Assertions.assertEquals("col_2_new", table.columns()[1].name());
    Assertions.assertEquals(Types.DateType.get(), table.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", table.columns()[1].comment());

    Assertions.assertEquals(MYSQL_COL_NAME3, table.columns()[2].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[2].dataType());
    Assertions.assertEquals("col_3_comment", table.columns()[2].comment());

    Assertions.assertEquals("col_4", table.columns()[3].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[3].dataType());
    Assertions.assertNull(table.columns()[3].comment());

    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName("name")
            .withDataType(Types.StringType.get())
            .withComment("comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName("address")
            .withDataType(Types.StringType.get())
            .withComment("comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName("date_of_birth")
            .withDataType(Types.DateType.get())
            .withComment("comment")
            .build();
    ColumnDTO[] newColumns = new ColumnDTO[] {col1, col2, col3};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(
            metalakeName,
            catalogName,
            schemaName,
            GravitinoITUtils.genRandomName("CatalogJdbcIT_table"));
    catalog
        .asTableCatalog()
        .createTable(
            tableIdentifier,
            newColumns,
            table_comment,
            ImmutableMap.of(),
            Partitioning.EMPTY_PARTITIONING,
            Distributions.NONE,
            new SortOrder[0]);

    NotFoundException notFoundException =
        assertThrows(
            NotFoundException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .alterTable(
                        tableIdentifier,
                        TableChange.updateColumnPosition(
                            new String[] {"no_column"}, TableChange.ColumnPosition.first())));
    Assertions.assertTrue(notFoundException.getMessage().contains("no_column"));

    catalog
        .asTableCatalog()
        .alterTable(
            tableIdentifier,
            TableChange.updateColumnPosition(
                new String[] {col1.name()}, TableChange.ColumnPosition.after(col2.name())));

    Table updateColumnPositionTable = catalog.asTableCatalog().loadTable(tableIdentifier);

    Column[] updateCols = updateColumnPositionTable.columns();
    Assertions.assertEquals(3, updateCols.length);
    Assertions.assertEquals(col2.name(), updateCols[0].name());
    Assertions.assertEquals(col1.name(), updateCols[1].name());
    Assertions.assertEquals(col3.name(), updateCols[2].name());

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asTableCatalog()
                .alterTable(
                    tableIdentifier,
                    TableChange.deleteColumn(new String[] {col3.name()}, true),
                    TableChange.deleteColumn(new String[] {col2.name()}, true)));
    Table delColTable = catalog.asTableCatalog().loadTable(tableIdentifier);
    Assertions.assertEquals(1, delColTable.columns().length);
    Assertions.assertEquals(col1.name(), delColTable.columns()[0].name());

    Assertions.assertDoesNotThrow(
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier);
        });
  }

  @Test
  void testDropMySQLDatabase() {
    String schemaName = GravitinoITUtils.genRandomName("mysql_schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("mysql_table").toLowerCase();

    catalog
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, catalogName, schemaName),
            "Created by gravitino client",
            ImmutableMap.<String, String>builder().build());

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            createColumns(),
            "Created by gravitino client",
            ImmutableMap.<String, String>builder().build());

    // Try to drop MySQL database without cascade equals to false, it should not be allowed.
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), false);
    // Check database still exists
    catalog.asSchemas().loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaName));

    // Try to drop MySQL database with cascade equals to true, it should be allowed.
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), true);
    // Check database has been dropped
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            catalog
                .asSchemas()
                .loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaName)));
  }
}
