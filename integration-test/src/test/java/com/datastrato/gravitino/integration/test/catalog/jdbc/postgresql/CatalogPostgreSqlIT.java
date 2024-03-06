/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

import static com.datastrato.gravitino.dto.util.DTOConverters.toFunctionArg;
import static com.datastrato.gravitino.integration.test.catalog.jdbc.TestJdbcAbstractIT.assertColumn;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql.service.PostgreSqlService;
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
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.UnparsedExpression;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Decimal;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.PostgreSQLContainer;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogPostgreSqlIT extends AbstractIT {
  public static final String DEFAULT_POSTGRES_IMAGE = "postgres:13";
  public static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://jdbc.postgresql.org/download/postgresql-42.7.0.jar";

  public String metalakeName = GravitinoITUtils.genRandomName("postgresql_it_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("postgresql_it_catalog");
  public String schemaName = GravitinoITUtils.genRandomName("postgresql_it_schema");
  public String tableName = GravitinoITUtils.genRandomName("postgresql_it_table");
  public String alertTableName = "alert_table_name";
  public String table_comment = "table_comment";
  public String schema_comment = "schema_comment";
  public String POSTGRESQL_COL_NAME1 = "postgresql_col_name1";
  public String POSTGRESQL_COL_NAME2 = "postgresql_col_name2";
  public String POSTGRESQL_COL_NAME3 = "postgresql_col_name3";
  private final String provider = "jdbc-postgresql";

  private GravitinoMetaLake metalake;

  private Catalog catalog;

  private PostgreSqlService postgreSqlService;

  private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

  protected final String TEST_DB_NAME = GravitinoITUtils.genRandomName("test_db");

  protected String postgreImageName = DEFAULT_POSTGRES_IMAGE;

  @BeforeAll
  public void startup() throws IOException {

    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-postgresql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }

    POSTGRESQL_CONTAINER =
        new PostgreSQLContainer<>(postgreImageName)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    POSTGRESQL_CONTAINER.start();
    postgreSqlService = new PostgreSqlService(POSTGRESQL_CONTAINER);
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    client.dropMetalake(NameIdentifier.of(metalakeName));
    postgreSqlService.close();
    POSTGRESQL_CONTAINER.stop();
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(metalakeName, catalogName, schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().dropTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), false);
  }

  private void createMetalake() {
    GravitinoMetaLake[] gravitinoMetaLakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitinoMetaLake loadMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    try {
      String jdbcUrl = POSTGRESQL_CONTAINER.getJdbcUrl();
      String database = new URI(jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1)).getPath();
      catalogProperties.put(
          JdbcConfig.JDBC_DRIVER.getKey(), POSTGRESQL_CONTAINER.getDriverClassName());
      catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
      catalogProperties.put(JdbcConfig.JDBC_DATABASE.getKey(), database);
      catalogProperties.put(JdbcConfig.USERNAME.getKey(), POSTGRESQL_CONTAINER.getUsername());
      catalogProperties.put(JdbcConfig.PASSWORD.getKey(), POSTGRESQL_CONTAINER.getPassword());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

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

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);

    Schema createdSchema =
        catalog.asSchemas().createSchema(ident, schema_comment, Collections.EMPTY_MAP);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    Assertions.assertEquals(createdSchema.comment(), loadSchema.comment());
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName(POSTGRESQL_COL_NAME1)
            .withDataType(Types.IntegerType.get())
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(POSTGRESQL_COL_NAME2)
            .withDataType(Types.DateType.get())
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(POSTGRESQL_COL_NAME3)
            .withDataType(Types.StringType.get())
            .withComment("col_3_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3};
  }

  private ColumnDTO[] columnsWithSpecialNames() {
    return new ColumnDTO[] {
      new ColumnDTO.Builder()
          .withName("integer")
          .withDataType(Types.IntegerType.get())
          .withComment("integer")
          .build(),
      new ColumnDTO.Builder()
          .withName("long")
          .withDataType(Types.LongType.get())
          .withComment("long")
          .build(),
      new ColumnDTO.Builder()
          .withName("float")
          .withDataType(Types.FloatType.get())
          .withComment("float")
          .build(),
      new ColumnDTO.Builder()
          .withName("double")
          .withDataType(Types.DoubleType.get())
          .withComment("double")
          .build(),
      new ColumnDTO.Builder()
          .withName("decimal")
          .withDataType(Types.DecimalType.of(10, 3))
          .withComment("decimal")
          .build(),
      new ColumnDTO.Builder()
          .withName("date")
          .withDataType(Types.DateType.get())
          .withComment("date")
          .build(),
      new ColumnDTO.Builder()
          .withName("time")
          .withDataType(Types.TimeType.get())
          .withComment("time")
          .build(),
      new ColumnDTO.Builder()
          .withName("binary")
          .withDataType(Types.TimestampType.withoutTimeZone())
          .withComment("binary")
          .build()
    };
  }

  @Test
  void testCreateTableWithSpecialColumnNames() {
    // Create table from Gravitino API
    ColumnDTO[] columns = columnsWithSpecialNames();

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    SortOrder[] sortOrders = new SortOrder[0];
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        sortOrders);

    Table t = tableCatalog.loadTable(tableIdentifier);
    Optional<Column> column =
        Arrays.stream(t.columns()).filter(c -> c.name().equals("binary")).findFirst();
    Assertions.assertTrue(column.isPresent());
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    return properties;
  }

  @Test
  void testOperationPostgreSqlSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    Namespace namespace = Namespace.of(metalakeName, catalogName);
    // list schema check.
    NameIdentifier[] nameIdentifiers = schemas.listSchemas(namespace);
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    NameIdentifier[] postgreSqlNamespaces = postgreSqlService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(postgreSqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    Map<String, NameIdentifier> schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertTrue(schemaMap.containsKey(testSchemaName));

    postgreSqlNamespaces = postgreSqlService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(postgreSqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> {
          schemas.createSchema(schemaIdent, schema_comment, emptyMap);
        });

    // drop schema check.
    schemas.dropSchema(schemaIdent, false);
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent));
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> postgreSqlService.loadSchema(schemaIdent));

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
    postgreSqlNamespaces = postgreSqlService.listSchemas(Namespace.empty());
    schemaNames =
        Arrays.stream(postgreSqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));
  }

  @Test
  void testCreateAndLoadPostgreSqlTable() {
    // Create table from Gravitino API
    ColumnDTO[] columns = createColumns();

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    SortOrder[] sortOrders = new SortOrder[0];
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;

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
      assertColumn(columns[i], createdTable.columns()[i]);
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
      assertColumn(columns[i], loadTable.columns()[i]);
    }
  }

  @Test
  void testAlterAndDropPostgreSqlTable() {
    ColumnDTO[] columns = createColumns();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                columns,
                table_comment,
                createProperties());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());
    Assertions.assertNull(table.auditInfo().lastModifier());
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

    // rename table
    table =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                TableChange.rename(alertTableName));
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().lastModifier());

    // update table
    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {POSTGRESQL_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnType(
                new String[] {POSTGRESQL_COL_NAME1}, Types.IntegerType.get()));

    table =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName));
    Assertions.assertEquals(alertTableName, table.name());

    Assertions.assertEquals(POSTGRESQL_COL_NAME1, table.columns()[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), table.columns()[0].dataType());
    Assertions.assertTrue(table.columns()[0].nullable());

    Assertions.assertEquals("col_2_new", table.columns()[1].name());
    Assertions.assertEquals(Types.DateType.get(), table.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", table.columns()[1].comment());
    Assertions.assertTrue(table.columns()[1].nullable());

    Assertions.assertEquals(POSTGRESQL_COL_NAME3, table.columns()[2].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[2].dataType());
    Assertions.assertEquals("col_3_comment", table.columns()[2].comment());
    Assertions.assertTrue(table.columns()[2].nullable());

    Assertions.assertEquals("col_4", table.columns()[3].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[3].dataType());
    Assertions.assertNull(table.columns()[3].comment());
    Assertions.assertTrue(table.columns()[3].nullable());
    Assertions.assertNotNull(table.auditInfo());
    Assertions.assertNotNull(table.auditInfo().createTime());
    Assertions.assertNotNull(table.auditInfo().creator());
    Assertions.assertNotNull(table.auditInfo().lastModifiedTime());
    Assertions.assertNotNull(table.auditInfo().lastModifier());

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
            metalakeName, catalogName, schemaName, GravitinoITUtils.genRandomName("jdbc_it_table"));
    catalog
        .asTableCatalog()
        .createTable(
            tableIdentifier,
            newColumns,
            table_comment,
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0]);

    // delete column
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
  void testCreateAndLoadSchema() {
    String testSchemaName = "test";
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, testSchemaName);

    Schema schema = catalog.asSchemas().createSchema(ident, "comment", null);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    Assertions.assertEquals("comment", schema.comment());
    schema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    Assertions.assertEquals("comment", schema.comment());

    // test null comment
    testSchemaName = "test2";
    ident = NameIdentifier.of(metalakeName, catalogName, testSchemaName);

    schema = catalog.asSchemas().createSchema(ident, null, null);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    // todo: Gravitino put id to comment, makes comment is empty string not null.
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
    schema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
  }

  @Test
  void testListSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asSchemas().listSchemas(Namespace.of(metalakeName, catalogName));
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains("public"));
  }

  @Test
  public void testBackQuoteTable() {
    Column col1 = Column.of("create", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("delete", Types.IntegerType.get(), "number", false, false, null);
    Column col3 = Column.of("show", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("status", Types.VarCharType.of(255), "code", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4};
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "abc");
    Assertions.assertDoesNotThrow(
        () ->
            tableCatalog.createTable(
                tableIdentifier,
                newColumns,
                table_comment,
                Collections.emptyMap(),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0],
                Indexes.EMPTY_INDEXES));

    Assertions.assertDoesNotThrow(() -> tableCatalog.loadTable(tableIdentifier));

    Assertions.assertDoesNotThrow(
        () ->
            tableCatalog.alterTable(
                tableIdentifier,
                new TableChange[] {
                  TableChange.addColumn(new String[] {"int"}, Types.StringType.get()),
                  TableChange.deleteColumn(new String[] {"create"}, true),
                  TableChange.renameColumn(new String[] {"delete"}, "varchar")
                }));
    Assertions.assertDoesNotThrow(
        () ->
            tableCatalog.alterTable(
                tableIdentifier, new TableChange[] {TableChange.rename("test")}));

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));
  }

  @Test
  void testCreateIndexTable() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.VarCharType.of(100), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    Index[] indexes =
        new Index[] {
          Indexes.primary("k1_pk", new String[][] {{"col_2"}, {"col_1"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}}),
          Indexes.unique("u3_key", new String[][] {{"col_5"}, {"col_4"}}),
          Indexes.unique("u4_key", new String[][] {{"col_2"}, {"col_4"}, {"col_3"}}),
          Indexes.unique("u5_key", new String[][] {{"col_5"}, {"col_3"}, {"col_2"}}),
          Indexes.unique("u6_key", new String[][] {{"col_1"}, {"col_3"}, {"col_2"}, {"col_4"}}),
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);

    // Test create many indexes with name success.
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            newColumns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            indexes);
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indexes, createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indexes, table);

    // Test create index complex fields fail.
    NameIdentifier id = NameIdentifier.of(metalakeName, catalogName, schemaName, "test_failed");
    SortOrder[] sortOrder = new SortOrder[0];
    Index[] primaryIndex =
        new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_1", "col_2"}})};
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              tableCatalog.createTable(
                  id,
                  newColumns,
                  table_comment,
                  properties,
                  Transforms.EMPTY_TRANSFORM,
                  Distributions.NONE,
                  sortOrder,
                  primaryIndex);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in PostgreSQL"));

    Index[] primaryIndex2 =
        new Index[] {Indexes.unique("u1_key", new String[][] {{"col_2", "col_3"}})};
    illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              tableCatalog.createTable(
                  id,
                  newColumns,
                  table_comment,
                  properties,
                  Transforms.EMPTY_TRANSFORM,
                  Distributions.NONE,
                  sortOrder,
                  primaryIndex2);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in PostgreSQL"));

    // Test create index with empty name success.
    table =
        tableCatalog.createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, "test_null_key"),
            newColumns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            new Index[] {
              Indexes.of(
                  Index.IndexType.UNIQUE_KEY,
                  null,
                  new String[][] {{"col_1"}, {"col_3"}, {"col_4"}}),
              Indexes.of(Index.IndexType.UNIQUE_KEY, null, new String[][] {{"col_4"}}),
            });
    Assertions.assertEquals(2, table.index().length);
    Assertions.assertNotNull(table.index()[0].name());
    Assertions.assertNotNull(table.index()[1].name());

    // Test create index with same col success.
    table =
        tableCatalog.createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, "many_index"),
            newColumns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            new Index[] {
              Indexes.unique("u4_key_2", new String[][] {{"col_2"}, {"col_3"}, {"col_4"}}),
              Indexes.unique("u5_key_3", new String[][] {{"col_2"}, {"col_3"}, {"col_4"}}),
            });
    Assertions.assertEquals(1, table.index().length);
    Assertions.assertEquals("u4_key_2", table.index()[0].name());
  }

  @Test
  void testColumnDefaultValue() {
    Column col1 =
        Column.of(
            "col_1",
            Types.FloatType.get(),
            "col_1_comment",
            false,
            false,
            FunctionExpression.of("random"));
    Column col2 =
        Column.of(
            "col_2",
            Types.TimestampType.withoutTimeZone(),
            "col_2_comment",
            false,
            false,
            FunctionExpression.of("current_timestamp"));
    Column col3 =
        Column.of("col_3", Types.VarCharType.of(255), "col_3_comment", true, false, Literals.NULL);
    Column col4 = Column.of("col_4", Types.StringType.get(), "col_4_comment", false, false, null);
    Column col5 =
        Column.of(
            "col_5",
            Types.VarCharType.of(255),
            "col_5_comment",
            true,
            false,
            Literals.stringLiteral("current_timestamp"));
    Column col6 =
        Column.of(
            "col_6",
            Types.IntegerType.get(),
            "col_6_comment",
            true,
            false,
            Literals.integerLiteral(1000));

    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5, col6};

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(
                    metalakeName,
                    catalogName,
                    schemaName,
                    GravitinoITUtils.genRandomName("pg_it_table")),
                newColumns,
                null,
                ImmutableMap.of());

    Assertions.assertEquals(
        toFunctionArg(UnparsedExpression.of("random()")), createdTable.columns()[0].defaultValue());
    Assertions.assertEquals(
        toFunctionArg(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP),
        createdTable.columns()[1].defaultValue());
    Assertions.assertEquals(toFunctionArg(Literals.NULL), createdTable.columns()[2].defaultValue());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, createdTable.columns()[3].defaultValue());
    Assertions.assertEquals(
        toFunctionArg(Literals.varcharLiteral(255, "current_timestamp")),
        createdTable.columns()[4].defaultValue());
    Assertions.assertEquals(
        toFunctionArg(Literals.integerLiteral(1000)), createdTable.columns()[5].defaultValue());
  }

  @Test
  void testColumnDefaultValueConverter() {
    // test convert from MySQL to Gravitino
    String tableName = GravitinoITUtils.genRandomName("test_default_value");
    String fullTableName = TEST_DB_NAME + "." + schemaName + "." + tableName;
    String sql =
        "CREATE TABLE "
            + fullTableName
            + " (\n"
            + "    int_col_1 int default 431,\n"
            + "    int_col_2 int default floor(random() * 100),\n"
            + "    /*Default values must be specified as the same type in PostgreSQL\n"
            + "    int_col_3 int default '3.321'::int,*/\n"
            + "    double_col_1 double precision default 123.45,\n"
            + "    varchar20_col_1 varchar(20) default (10),\n"
            + "    varchar100_col_1 varchar(100) default 'CURRENT_TIMESTAMP',\n"
            + "    varchar200_col_1 varchar(200) default 'curdate()',\n"
            + "    varchar200_col_2 varchar(200) default current_date,\n"
            + "    varchar200_col_3 varchar(200) default current_timestamp,\n"
            + "    date_col_1 date default current_date,\n"
            + "    date_col_2 date,\n"
            + "    date_col_3 date default (current_date + interval '1 year'),\n"
            + "    date_col_4 date default current_date,\n"
            // todo: uncomment when we support timestamp in PG catalog
            // + "    timestamp_col_1 timestamp default '2012-12-31 11:30:45',\n"
            + "    decimal_6_2_col_1 decimal(6, 2) default 1.2\n"
            + ");";
    System.out.println(sql);
    postgreSqlService.executeQuery(sql);
    Table loadedTable =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));

    for (Column column : loadedTable.columns()) {
      switch (column.name()) {
        case "int_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.integerLiteral(431)), column.defaultValue());
          break;
        case "int_col_2":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("floor((random() * (100)::double precision))")),
              column.defaultValue());
          break;
        case "int_col_3":
          Assertions.assertEquals(toFunctionArg(Literals.integerLiteral(3)), column.defaultValue());
          break;
        case "double_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.doubleLiteral(123.45)), column.defaultValue());
          break;
        case "varchar20_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.varcharLiteral(20, "10")), column.defaultValue());
          break;
        case "varchar100_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.varcharLiteral(100, "CURRENT_TIMESTAMP")),
              column.defaultValue());
          break;
        case "varchar200_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.varcharLiteral(200, "curdate()")), column.defaultValue());
          break;
        case "varchar200_col_2":
          Assertions.assertEquals(
              toFunctionArg(Literals.varcharLiteral(200, "CURRENT_DATE")), column.defaultValue());
          break;
        case "varchar200_col_3":
          Assertions.assertEquals(
              toFunctionArg(Literals.varcharLiteral(200, "CURRENT_TIMESTAMP")),
              column.defaultValue());
          break;
        case "date_col_1":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("CURRENT_DATE")), column.defaultValue());
          break;
        case "date_col_2":
          Assertions.assertEquals(toFunctionArg(Literals.NULL), column.defaultValue());
          break;
        case "date_col_3":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("(CURRENT_DATE + '1 year'::interval)")),
              column.defaultValue());
          break;
        case "date_col_4":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("CURRENT_DATE")), column.defaultValue());
          break;
        case "timestamp_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.timestampLiteral("2012-12-31T11:30:45")),
              column.defaultValue());
          break;
        case "timestamp_col_2":
          Assertions.assertEquals(
              toFunctionArg(Literals.timestampLiteral("1983-09-05T00:00:00")),
              column.defaultValue());
          break;
        case "decimal_6_2_col_1":
          Assertions.assertEquals(
              toFunctionArg(Literals.decimalLiteral(Decimal.of("1.2", 6, 2))),
              column.defaultValue());
          break;
        default:
          Assertions.fail("Unexpected column name: " + column.name());
      }
    }
  }

  @Test
  void testPGSpecialTableName() {
    // Test create many indexes with name success.
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String t1_name = "t112";
    Column t1_col = Column.of(t1_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns = {t1_col};

    Index[] t1_indexes = {Indexes.unique("u1_key", new String[][] {{t1_name}})};

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t1_name);
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        t1_indexes);

    String t2_name = "t212";
    Column t2_col = Column.of(t2_name, Types.LongType.get(), "id", false, false, null);
    Index[] t2_indexes = {Indexes.unique("u2_key", new String[][] {{t2_name}})};
    columns = new Column[] {t2_col};
    tableIdentifier = NameIdentifier.of(metalakeName, catalogName, schemaName, t2_name);
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        t2_indexes);

    String t3_name = "t_12";
    Column t3_col = Column.of(t3_name, Types.LongType.get(), "id", false, false, null);
    Index[] t3_indexes = {Indexes.unique("u3_key", new String[][] {{t3_name}})};
    columns = new Column[] {t3_col};
    tableIdentifier = NameIdentifier.of(metalakeName, catalogName, schemaName, t3_name);
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        t3_indexes);

    String t4_name = "_1__";
    Column t4_col = Column.of(t4_name, Types.LongType.get(), "id", false, false, null);
    Index[] t4_indexes = {Indexes.unique("u4_key", new String[][] {{t4_name}})};
    columns = new Column[] {t4_col};
    tableIdentifier = NameIdentifier.of(metalakeName, catalogName, schemaName, t4_name);
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        t4_indexes);

    Table t1 =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, t1_name));
    Arrays.stream(t1.columns()).anyMatch(c -> Objects.equals(c.name(), "t112"));
    assertionsTableInfo(t1_name, table_comment, Arrays.asList(t1_col), properties, t1_indexes, t1);

    Table t2 =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, t2_name));
    Arrays.stream(t2.columns()).anyMatch(c -> Objects.equals(c.name(), "t212"));
    assertionsTableInfo(t2_name, table_comment, Arrays.asList(t2_col), properties, t2_indexes, t2);

    Table t3 =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, t3_name));
    Arrays.stream(t3.columns()).anyMatch(c -> Objects.equals(c.name(), "t_12"));
    assertionsTableInfo(t3_name, table_comment, Arrays.asList(t3_col), properties, t3_indexes, t3);

    Table t4 =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, t4_name));
    Arrays.stream(t4.columns()).anyMatch(c -> Objects.equals(c.name(), "_1__"));
    assertionsTableInfo(t4_name, table_comment, Arrays.asList(t4_col), properties, t4_indexes, t4);
  }

  @Test
  void testPGTableNameCaseSensitive() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.IntegerType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    Index[] indexes = new Index[0];
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "tablename");
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            newColumns,
            "low case table name",
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            indexes);
    assertionsTableInfo(
        "tablename",
        "low case table name",
        Arrays.asList(newColumns),
        properties,
        indexes,
        createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    assertionsTableInfo(
        "tablename", "low case table name", Arrays.asList(newColumns), properties, indexes, table);

    // Test create table with same name but different case
    NameIdentifier tableIdentifier2 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "TABLENAME");

    Column[] upperTableColumns = new Column[] {col1, col4, col5};
    Table tableAgain =
        Assertions.assertDoesNotThrow(
            () ->
                tableCatalog.createTable(
                    tableIdentifier2,
                    upperTableColumns,
                    "upper case table name",
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new SortOrder[0],
                    indexes));
    Assertions.assertEquals("TABLENAME", tableAgain.name());

    table = tableCatalog.loadTable(tableIdentifier2);
    Assertions.assertEquals("TABLENAME", table.name());
  }

  @Test
  void testPGListTable() {

    String schemaPrefix = GravitinoITUtils.genRandomName("postgresql_it_schema");
    String schemaName1 = schemaPrefix + "_";
    String schemaName2 = schemaPrefix + "_a";
    String schemaName3 = schemaPrefix + "1";
    String schemaName4 = schemaPrefix + "1a";
    String schemaName5 = schemaPrefix + "aaa";

    String[] dbs = {schemaName1, schemaName2, schemaName3, schemaName4, schemaName5};

    for (int i = 0; i < dbs.length; i++) {
      catalog
          .asSchemas()
          .createSchema(
              NameIdentifier.of(metalakeName, catalogName, dbs[i]), dbs[i], Maps.newHashMap());
    }

    String tableName1 = "table1";
    String tableName2 = "table2";
    String tableName3 = "table3";
    String tableName4 = "table4";
    String tableName5 = "table5";

    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.IntegerType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    String[] tables = {tableName1, tableName2, tableName3, tableName4, tableName5};

    for (int i = 0; i < dbs.length; i++) {
      catalog
          .asTableCatalog()
          .createTable(
              NameIdentifier.of(metalakeName, catalogName, dbs[i], tables[i]),
              newColumns,
              dbs[i] + "." + tables[i],
              Maps.newHashMap(),
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              new Index[0]);
    }

    // list table in schema1
    for (int i = 0; i < 5; i++) {
      NameIdentifier[] tableNames =
          catalog.asTableCatalog().listTables(Namespace.of(metalakeName, catalogName, dbs[i]));
      Assertions.assertEquals(1, tableNames.length);
      Assertions.assertEquals(tables[i], tableNames[0].name());
    }
  }

  @Test
  void testPostgreSQLSchemaNameCaseSensitive() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.VarCharType.of(255), "code", false, false, null);
    Column col3 = Column.of("col_3", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3};

    Index[] indexes = new Index[] {Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}})};

    String[] schemas = {"db_", "db_1", "db_2", "db12"};
    SupportsSchemas schemaSupport = catalog.asSchemas();

    for (String schema : schemas) {
      NameIdentifier schemaIdentifier = NameIdentifier.of(metalakeName, catalogName, schema);
      schemaSupport.createSchema(schemaIdentifier, null, Collections.emptyMap());
      Assertions.assertNotNull(schemaSupport.loadSchema(schemaIdentifier));
    }

    Set<String> schemaNames =
        Arrays.stream(schemaSupport.listSchemas(Namespace.of(metalakeName, catalogName)))
            .map(NameIdentifier::name)
            .collect(Collectors.toSet());

    Assertions.assertTrue(schemaNames.containsAll(Arrays.asList(schemas)));

    String tableName = "test1";
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();

    for (String schema : schemas) {
      tableCatalog.createTable(
          NameIdentifier.of(metalakeName, catalogName, schema, tableName),
          newColumns,
          table_comment,
          properties,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          new SortOrder[0],
          indexes);
      tableCatalog.createTable(
          NameIdentifier.of(
              metalakeName, catalogName, schema, GravitinoITUtils.genRandomName("test2")),
          newColumns,
          table_comment,
          properties,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          new SortOrder[0],
          Indexes.EMPTY_INDEXES);
    }

    for (String schema : schemas) {
      NameIdentifier[] nameIdentifiers =
          tableCatalog.listTables(Namespace.of(metalakeName, catalogName, schema));
      Assertions.assertEquals(2, nameIdentifiers.length);
      Assertions.assertTrue(
          Arrays.stream(nameIdentifiers)
              .map(NameIdentifier::name)
              .collect(Collectors.toSet())
              .stream()
              .anyMatch(n -> n.equals(tableName)));
    }
  }

  @Test
  void testUnparsedTypeConverter() {
    String tableName = GravitinoITUtils.genRandomName("test_unparsed_type");
    postgreSqlService.executeQuery(
        String.format("CREATE TABLE %s.%s (bit_col bit);", schemaName, tableName));
    Table loadedTable =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Assertions.assertEquals(Types.UnparsedType.of("bit"), loadedTable.columns()[0].dataType());
  }

  @Test
  void testOperationTableIndex() {
    String tableName = GravitinoITUtils.genRandomName("test_add_index");
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.VarCharType.of(255), "code", false, false, null);
    Column col3 = Column.of("col_3", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3};
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        newColumns,
        table_comment,
        createProperties(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        Indexes.EMPTY_INDEXES);

    // add index test.
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.addIndex(
            Index.IndexType.UNIQUE_KEY, "u1_key", new String[][] {{"col_2"}, {"col_3"}}),
        TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "pk1_key", new String[][] {{"col_1"}}));

    Table table =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Index[] indexes =
        new Index[] {
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.primary("pk1_key", new String[][] {{"col_1"}})
        };
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), createProperties(), indexes, table);

    // delete index and add new column and index.
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.deleteIndex("u1_key", true),
        TableChange.addColumn(
            new String[] {"col_4"},
            Types.VarCharType.of(255),
            TableChange.ColumnPosition.defaultPos()),
        TableChange.addIndex(Index.IndexType.UNIQUE_KEY, "u2_key", new String[][] {{"col_4"}}));

    indexes =
        new Index[] {
          Indexes.primary("pk1_key", new String[][] {{"col_1"}}),
          Indexes.unique("u2_key", new String[][] {{"col_4"}})
        };
    table =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), null, true, false, null);
    newColumns = new Column[] {col1, col2, col3, col4};
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), createProperties(), indexes, table);

    // Add a previously existing index
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.addIndex(
            Index.IndexType.UNIQUE_KEY, "u1_key", new String[][] {{"col_2"}, {"col_3"}}),
        TableChange.addIndex(
            Index.IndexType.UNIQUE_KEY, "u3_key", new String[][] {{"col_1"}, {"col_4"}}));

    indexes =
        new Index[] {
          Indexes.primary("pk1_key", new String[][] {{"col_1"}}),
          Indexes.unique("u2_key", new String[][] {{"col_4"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u3_key", new String[][] {{"col_1"}, {"col_4"}})
        };
    table =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), createProperties(), indexes, table);
  }

  @Test
  void testAddColumnAutoIncrement() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "uid", false, false, null);
    Column col2 = Column.of("col_2", Types.DateType.get(), "comment", false, false, null);
    Column col3 = Column.of("col_3", Types.VarCharType.of(255), "code", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4};
    String tableName = "auto_increment_table";

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        newColumns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        Indexes.EMPTY_INDEXES);
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {"col_5"},
            Types.LongType.get(),
            "id",
            TableChange.ColumnPosition.defaultPos(),
            false,
            true));

    Table table = tableCatalog.loadTable(tableIdentifier);

    Column col5 = Column.of("col_5", Types.LongType.get(), "id", false, true, null);
    newColumns = new Column[] {col1, col2, col3, col4, col5};
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        Indexes.EMPTY_INDEXES,
        table);

    // Test drop auto increment column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_5"}, false));
    table = tableCatalog.loadTable(tableIdentifier);
    col5 = Column.of("col_5", Types.LongType.get(), "id", false, false, null);
    newColumns = new Column[] {col1, col2, col3, col4, col5};
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        Indexes.EMPTY_INDEXES,
        table);

    // Test add auto increment column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_5"}, true));
    table = tableCatalog.loadTable(tableIdentifier);
    col5 = Column.of("col_5", Types.LongType.get(), "id", false, true, null);
    newColumns = new Column[] {col1, col2, col3, col4, col5};
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        Indexes.EMPTY_INDEXES,
        table);
  }
}
