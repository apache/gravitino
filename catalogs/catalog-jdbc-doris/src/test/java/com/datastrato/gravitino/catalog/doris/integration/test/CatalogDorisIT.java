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
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.rel.Column;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogDorisIT extends AbstractIT {

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

  // Because the creation of Schema Change is an asynchronous process, we need to wait for a while
  // For more information, you can refer to the comment in
  // DorisTableOperations.generateAlterTableSql().
  private static final long MAX_WAIT_IN_SECONDS = 30;

  private static final long WAIT_INTERVAL_IN_SECONDS = 1;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private GravitinoMetalake metalake;

  protected Catalog catalog;

  protected String mysqlDriverDownloadUrl = DOWNLOAD_JDBC_DRIVER_URL;

  @BeforeAll
  public void startup() throws IOException {

    if (!ITUtils.EMBEDDED_TEST_MODE.equals(AbstractIT.testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-doris/libs");
      JdbcDriverDownloader.downloadJdbcDriver(mysqlDriverDownloadUrl, tmpPath.toString());
    }

    containerSuite.startDorisContainer();

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    AbstractIT.client.dropMetalake(metalakeName);
  }

  @AfterEach
  public void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(metalakeName, catalogName, schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().dropTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetaLakes = AbstractIT.client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetalake createdMetalake =
        AbstractIT.client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = AbstractIT.client.loadMetalake(metalakeName);
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
            catalogName,
            Catalog.Type.RELATIONAL,
            provider,
            "doris catalog comment",
            catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    String propKey = "key";
    String propValue = "value";
    Map<String, String> prop = Maps.newHashMap();
    prop.put(propKey, propValue);

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());

    Assertions.assertEquals(createdSchema.properties().get(propKey), propValue);
  }

  private Column[] createColumns() {
    Column col1 = Column.of(DORIS_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(DORIS_COL_NAME2, Types.VarCharType.of(10), "col_2_comment");
    Column col3 = Column.of(DORIS_COL_NAME3, Types.VarCharType.of(10), "col_3_comment");

    return new Column[] {col1, col2, col3};
  }

  private Map<String, String> createTableProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("replication_allocation", "tag.location.default: 1");
    return properties;
  }

  private Distribution createDistribution() {
    return Distributions.hash(2, NamedReference.field(DORIS_COL_NAME1));
  }

  @Test
  void testDorisSchemaBasicOperation() {
    SupportsSchemas schemas = catalog.asSchemas();

    // test list schemas
    NameIdentifier[] nameIdentifiers = schemas.listSchemas();
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // test create schema already exists
    String testSchemaName = GravitinoITUtils.genRandomName("create_schema_test");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap());

    nameIdentifiers = schemas.listSchemas();
    Map<String, NameIdentifier> schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertTrue(schemaMap.containsKey(testSchemaName));

    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> {
          schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap());
        });

    // test drop schema
    Assertions.assertTrue(schemas.dropSchema(schemaIdent.name(), false));

    // check schema is deleted
    // 1. check by load schema
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));

    // 2. check by list schema
    nameIdentifiers = schemas.listSchemas();
    schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertFalse(schemaMap.containsKey(testSchemaName));

    // test drop schema not exists
    NameIdentifier notExistsSchemaIdent = NameIdentifier.of(metalakeName, catalogName, "no-exits");
    Assertions.assertFalse(schemas.dropSchema(notExistsSchemaIdent.name(), false));
  }

  @Test
  void testDropDorisSchema() {
    String schemaName = GravitinoITUtils.genRandomName("doris_it_schema_dropped").toLowerCase();

    catalog.asSchemas().createSchema(schemaName, "test_comment", ImmutableMap.of("key", "value"));

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            createColumns(),
            "Created by gravitino client",
            createTableProperties(),
            Transforms.EMPTY_TRANSFORM,
            createDistribution(),
            null);

    // Try to drop a database, and cascade equals to false, it should not be allowed.
    Assertions.assertFalse(catalog.asSchemas().dropSchema(schemaName, false));

    // Check the database still exists
    catalog.asSchemas().loadSchema(schemaName);

    // Try to drop a database, and cascade equals to true, it should be allowed.
    Assertions.assertTrue(catalog.asSchemas().dropSchema(schemaName, true));

    // Check database has been dropped
    SupportsSchemas schemas = catalog.asSchemas();
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          schemas.loadSchema(schemaName);
        });
  }

  @Test
  void testDorisTableBasicOperation() {
    // create a table
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Index[] indexes =
        new Index[] {
          Indexes.of(Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}})
        };

    Map<String, String> properties = createTableProperties();
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

    ITUtils.assertionsTableInfo(
        tableName, table_comment, Arrays.asList(columns), properties, indexes, createdTable);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName, table_comment, Arrays.asList(columns), properties, indexes, loadTable);

    // rename table
    String newTableName = GravitinoITUtils.genRandomName("new_table_name");
    tableCatalog.alterTable(tableIdentifier, TableChange.rename(newTableName));
    NameIdentifier newTableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, newTableName);
    Table renamedTable = tableCatalog.loadTable(newTableIdentifier);
    ITUtils.assertionsTableInfo(
        newTableName, table_comment, Arrays.asList(columns), properties, indexes, renamedTable);
  }

  @Test
  void testAlterDorisTable() {
    // create a table
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Index[] indexes =
        new Index[] {
          Indexes.of(Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}})
        };

    Map<String, String> properties = createTableProperties();
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

    ITUtils.assertionsTableInfo(
        tableName, table_comment, Arrays.asList(columns), properties, indexes, createdTable);

    // Alter column type
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.updateColumnType(new String[] {DORIS_COL_NAME3}, Types.VarCharType.of(255)));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                ITUtils.assertColumn(
                    Column.of(DORIS_COL_NAME3, Types.VarCharType.of(255), "col_3_comment"),
                    tableCatalog.loadTable(tableIdentifier).columns()[2]));

    // update column comment
    // Alter column type
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.updateColumnComment(new String[] {DORIS_COL_NAME3}, "new_comment"));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                ITUtils.assertColumn(
                    Column.of(DORIS_COL_NAME3, Types.VarCharType.of(255), "new_comment"),
                    tableCatalog.loadTable(tableIdentifier).columns()[2]));

    // add new column
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {"col_4"}, Types.VarCharType.of(255), "col_4_comment", true));
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    4, tableCatalog.loadTable(tableIdentifier).columns().length));

    ITUtils.assertColumn(
        Column.of("col_4", Types.VarCharType.of(255), "col_4_comment"),
        tableCatalog.loadTable(tableIdentifier).columns()[3]);

    // change column position
    // TODO: change column position is unstable, add it later

    // drop column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.deleteColumn(new String[] {"col_4"}, true));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    3, tableCatalog.loadTable(tableIdentifier).columns().length));
  }

  @Test
  void testDorisIndex() {
    String tableName = GravitinoITUtils.genRandomName("test_add_index");

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
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

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    1,
                    tableCatalog
                        .loadTable(
                            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName))
                        .index()
                        .length));

    // delete index and add new column and index.
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.deleteIndex("k1_index", true),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY, "k2_index", new String[][] {{DORIS_COL_NAME2}}));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    1,
                    tableCatalog
                        .loadTable(
                            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName))
                        .index()
                        .length));
  }
}
