/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import static com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata.GRAVITINO_ENGINE_KEY;
import static com.datastrato.gravitino.dto.util.DTOConverters.toFunctionArg;
import static com.datastrato.gravitino.integration.test.catalog.jdbc.TestJdbcAbstractIT.assertColumn;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.catalog.jdbc.mysql.service.MysqlService;
import com.datastrato.gravitino.integration.test.catalog.jdbc.utils.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Column.ColumnImpl;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.MySQLContainer;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogMysqlIT extends AbstractIT {
  private static final String provider = "jdbc-mysql";
  public static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";

  public String metalakeName = GravitinoITUtils.genRandomName("mysql_it_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("mysql_it_catalog");
  public String schemaName = GravitinoITUtils.genRandomName("mysql_it_schema");
  public String tableName = GravitinoITUtils.genRandomName("mysql_it_table");
  public String alertTableName = "alert_table_name";
  public String table_comment = "table_comment";

  // MySQL doesn't support schema comment
  public String schema_comment = null;
  public String MYSQL_COL_NAME1 = "mysql_col_name1";
  public String MYSQL_COL_NAME2 = "mysql_col_name2";
  public String MYSQL_COL_NAME3 = "mysql_col_name3";
  public String MYSQL_COL_NAME4 = "mysql_col_name4";
  public String MYSQL_COL_NAME5 = "mysql_col_name5";

  private GravitinoMetaLake metalake;

  protected Catalog catalog;

  private MysqlService mysqlService;

  private MySQLContainer<?> MYSQL_CONTAINER;

  protected final String TEST_DB_NAME = RandomUtils.nextInt(10000) + "_test_db";

  public static final String defaultMysqlImageName = "mysql:8.0";

  protected String mysqlImageName = defaultMysqlImageName;

  boolean SupportColumnDefaultValueExpression() {
    return true;
  }

  @BeforeAll
  public void startup() throws IOException {

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
  public void stop() {
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

  private void createSchema() {
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
    properties.put(GRAVITINO_ENGINE_KEY, "InnoDB");
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

    final SortOrder[] sortOrders = new SortOrder[0];

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
  void testColumnNameWithKeyWords() {
    // Create table from Gravitino API
    ColumnDTO[] columns =
        new ColumnDTO[] {
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
              .build()
        };

    String name = GravitinoITUtils.genRandomName("table") + "_keyword";
    NameIdentifier tableIdentifier = NameIdentifier.of(metalakeName, catalogName, schemaName, name);
    Distribution distribution = Distributions.NONE;

    final SortOrder[] sortOrders = new SortOrder[0];

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
    Assertions.assertEquals(createdTable.name(), name);
  }

  @Test
  // MySQL support column default value expression after 8.0.13
  // see https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html
  @EnabledIf("SupportColumnDefaultValueExpression")
  void testColumnDefaultValue() {
    Column col1 =
        Column.of(
            MYSQL_COL_NAME1,
            Types.IntegerType.get(),
            "col_1_comment",
            false,
            false,
            FunctionExpression.of("rand"));
    Column col2 =
        Column.of(
            MYSQL_COL_NAME2,
            Types.TimestampType.withoutTimeZone(),
            "col_2_comment",
            false,
            false,
            FunctionExpression.of("current_timestamp"));
    Column col3 =
        Column.of(
            MYSQL_COL_NAME3,
            Types.VarCharType.of(255),
            "col_3_comment",
            true,
            false,
            Literals.NULL);
    Column col4 =
        Column.of(MYSQL_COL_NAME4, Types.StringType.get(), "col_4_comment", false, false, null);
    Column col5 =
        Column.of(
            MYSQL_COL_NAME5,
            Types.VarCharType.of(255),
            "col_5_comment",
            true,
            false,
            Literals.stringLiteral("current_timestamp"));

    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(
                    metalakeName,
                    catalogName,
                    schemaName,
                    GravitinoITUtils.genRandomName("mysql_it_table")),
                newColumns,
                null,
                ImmutableMap.of());

    Assertions.assertEquals(
        toFunctionArg(UnparsedExpression.of("rand()")), createdTable.columns()[0].defaultValue());
    Assertions.assertEquals(
        toFunctionArg(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP),
        createdTable.columns()[1].defaultValue());
    Assertions.assertEquals(toFunctionArg(Literals.NULL), createdTable.columns()[2].defaultValue());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, createdTable.columns()[3].defaultValue());
    Assertions.assertEquals(
        new LiteralDTO.Builder()
            .withValue("current_timestamp")
            .withDataType(Types.VarCharType.of(255))
            .build(),
        createdTable.columns()[4].defaultValue());
  }

  @Test
  // MySQL support column default value expression after 8.0.13
  // see https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html
  @EnabledIf("SupportColumnDefaultValueExpression")
  void testColumnDefaultValueConverter() {
    // test convert from MySQL to Gravitino
    String tableName = GravitinoITUtils.genRandomName("test_default_value");
    String fullTableName = schemaName + "." + tableName;
    String sql =
        "CREATE TABLE "
            + fullTableName
            + " (\n"
            + "  int_col_1 int default 0x01AF,\n"
            + "  int_col_2 int default (rand()),\n"
            + "  int_col_3 int default '3.321',\n"
            + "  double_col_1 double default 123.45,\n"
            + "  varchar20_col_1 varchar(20) default (10),\n"
            + "  varchar100_col_1 varchar(100) default 'CURRENT_TIMESTAMP',\n"
            + "  varchar200_col_1 varchar(200) default 'curdate()',\n"
            + "  varchar200_col_2 varchar(200) default (curdate()),\n"
            + "  varchar200_col_3 varchar(200) default (CURRENT_TIMESTAMP),\n"
            // todo: uncomment when we support datetime type
            + "  /* we don't support datetime type now\n"
            + "  datetime_col_1 datetime default CURRENT_TIMESTAMP,\n"
            + "  datetime_col_2 datetime default current_timestamp,\n"
            + "  datetime_col_3 datetime default null,\n"
            + "  datetime_col_4 datetime default 19830905, */\n"
            + "  date_col_1 date default (CURRENT_DATE),\n"
            + "  date_col_2 date,\n"
            + "  date_col_3 date DEFAULT (CURRENT_DATE + INTERVAL 1 YEAR),\n"
            + "  date_col_4 date DEFAULT (CURRENT_DATE),\n"
            + "  timestamp_col_1 timestamp default '2012-12-31 11:30:45',\n"
            + "  timestamp_col_2 timestamp default 19830905,\n"
            + "  decimal_6_2_col_1 decimal(6, 2) default 1.2\n"
            + ");\n";

    mysqlService.executeQuery(sql);
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
              toFunctionArg(UnparsedExpression.of("rand()")), column.defaultValue());
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
              toFunctionArg(UnparsedExpression.of("10")), column.defaultValue());
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
              toFunctionArg(UnparsedExpression.of("curdate()")), column.defaultValue());
          break;
        case "varchar200_col_3":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("now()")), column.defaultValue());
          break;
        case "date_col_1":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("curdate()")), column.defaultValue());
          break;
        case "date_col_2":
          Assertions.assertEquals(toFunctionArg(Literals.NULL), column.defaultValue());
          break;
        case "date_col_3":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("(curdate() + interval 1 year)")),
              column.defaultValue());
          break;
        case "date_col_4":
          Assertions.assertEquals(
              toFunctionArg(UnparsedExpression.of("curdate()")), column.defaultValue());
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
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0]);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    TableChange change =
        TableChange.updateColumnPosition(
            new String[] {"no_column"}, TableChange.ColumnPosition.first());
    NotFoundException notFoundException =
        assertThrows(
            NotFoundException.class, () -> tableCatalog.alterTable(tableIdentifier, change));
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
            null,
            ImmutableMap.<String, String>builder().build());

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            createColumns(),
            "Created by gravitino client",
            ImmutableMap.<String, String>builder().build());

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

  @Test
  void testCreateTableIndex() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.ByteType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}}),
          Indexes.unique("u3_key", new String[][] {{"col_5"}, {"col_4"}}),
          Indexes.unique("u4_key", new String[][] {{"col_2"}, {"col_3"}, {"col_4"}}),
          Indexes.unique("u5_key", new String[][] {{"col_3"}, {"col_2"}, {"col_4"}}),
          Indexes.unique("u6_key", new String[][] {{"col_3"}, {"col_4"}, {"col_1"}, {"col_2"}}),
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);

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

    NameIdentifier id = NameIdentifier.of(metalakeName, catalogName, schemaName, "test_failed");
    Index[] indexes2 =
        new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_1", "col_2"}})};
    SortOrder[] sortOrder = new SortOrder[0];
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
                  indexes2);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in MySQL"));

    Index[] indexes3 = new Index[] {Indexes.unique("u1_key", new String[][] {{"col_2", "col_3"}})};
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
                  indexes3);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in MySQL"));

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
  }

  @Test
  public void testAutoIncrement() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, true, null);
    Column col2 = Column.of("col_2", Types.ByteType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}})
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);

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
    // Test create auto increment key success.
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indexes, createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indexes, table);

    // Test alter table. auto increment exist.
    // UpdateColumnType
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.updateColumnType(new String[] {"col_1"}, Types.IntegerType.get()));
    table = tableCatalog.loadTable(tableIdentifier);
    Column[] alterColumns =
        new Column[] {
          Column.of("col_1", Types.IntegerType.get(), "id", false, true, null),
          col2,
          col3,
          col4,
          col5
        };
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(alterColumns), properties, indexes, table);

    // UpdateColumnComment
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnComment(new String[] {"col_1"}, "new_id_comment"));
    table = tableCatalog.loadTable(tableIdentifier);
    alterColumns =
        new Column[] {
          Column.of("col_1", Types.IntegerType.get(), "new_id_comment", false, true, null),
          col2,
          col3,
          col4,
          col5
        };
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(alterColumns), properties, indexes, table);

    // RenameColumn
    tableCatalog.alterTable(
        tableIdentifier, TableChange.renameColumn(new String[] {"col_1"}, "col_1_1"));
    table = tableCatalog.loadTable(tableIdentifier);
    alterColumns =
        new Column[] {
          Column.of("col_1_1", Types.IntegerType.get(), "new_id_comment", false, true, null),
          col2,
          col3,
          col4,
          col5
        };
    indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}})
        };
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(alterColumns), properties, indexes, table);

    tableCatalog.dropTable(tableIdentifier);

    // Test create auto increment fail(No index)
    RuntimeException runtimeException =
        assertThrows(
            RuntimeException.class,
            () ->
                tableCatalog.createTable(
                    tableIdentifier,
                    newColumns,
                    table_comment,
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new SortOrder[0],
                    Indexes.EMPTY_INDEXES));
    Assertions.assertTrue(
        StringUtils.contains(
            runtimeException.getMessage(),
            "Incorrect table definition; there can be only one auto column and it must be defined as a key"));

    // Test create auto increment fail(Many index col)
    ColumnImpl column = Column.of("col_6", Types.LongType.get(), "id2", false, true, null);
    SortOrder[] sortOrder = new SortOrder[0];
    Index[] index2 =
        new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}, {"col_6"}})};

    runtimeException =
        assertThrows(
            RuntimeException.class,
            () ->
                tableCatalog.createTable(
                    tableIdentifier,
                    new Column[] {col1, col2, col3, col4, col5, column},
                    table_comment,
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    sortOrder,
                    index2));
    Assertions.assertTrue(
        StringUtils.contains(
            runtimeException.getMessage(),
            "Only one column can be auto-incremented. There are multiple auto-increment columns in your table: [col_1,col_6]"));
  }

  @Test
  public void testSchemaComment() {
    String testSchemaName = "test";
    NameIdentifier identer = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    RuntimeException exception =
        Assertions.assertThrowsExactly(
            RuntimeException.class,
            () -> catalog.asSchemas().createSchema(identer, "comment", null));
    Assertions.assertTrue(
        exception.getMessage().contains("MySQL doesn't support set schema comment: comment"));

    // test null comment
    testSchemaName = "test2";
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Schema schema = catalog.asSchemas().createSchema(ident, "", null);
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
    schema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
  }

  @Test
  public void testBackQuoteTable() {
    Column col1 = Column.of("create", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("delete", Types.ByteType.get(), "yes", false, false, null);
    Column col3 = Column.of("show", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("status", Types.VarCharType.of(255), "code", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4};
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "table");
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
                  TableChange.addColumn(
                      new String[] {"int"},
                      Types.StringType.get(),
                      TableChange.ColumnPosition.after("status")),
                  TableChange.deleteColumn(new String[] {"create"}, true),
                  TableChange.renameColumn(new String[] {"delete"}, "varchar")
                }));

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));
  }

  @Test
  void testMySQLSpecialTableName() {
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
  void testMySQLTableNameCaseSensitive() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.ByteType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    Index[] indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}})
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "tableName");
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
        "tableName", table_comment, Arrays.asList(newColumns), properties, indexes, createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    assertionsTableInfo(
        "tableName", table_comment, Arrays.asList(newColumns), properties, indexes, table);

    // Test create table with same name but different case
    NameIdentifier tableIdentifier2 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "TABLENAME");

    Table tableAgain =
        Assertions.assertDoesNotThrow(
            () ->
                tableCatalog.createTable(
                    tableIdentifier2,
                    newColumns,
                    table_comment,
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new SortOrder[0],
                    indexes));
    Assertions.assertEquals("TABLENAME", tableAgain.name());

    table = tableCatalog.loadTable(tableIdentifier2);
    assertionsTableInfo(
        "TABLENAME", table_comment, Arrays.asList(newColumns), properties, indexes, table);
  }

  @Test
  void testMySQLSchemaNameCaseSensitive() {
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
    mysqlService.executeQuery(
        String.format("CREATE TABLE %s.%s (bit_col bit);", schemaName, tableName));
    Table loadedTable =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Assertions.assertEquals(Types.UnparsedType.of("BIT"), loadedTable.columns()[0].dataType());
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
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY,
            Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            new String[][] {{"col_1"}}));

    Table table =
        tableCatalog.loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Index[] indexes =
        new Index[] {
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}})
        };
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), createProperties(), indexes, table);

    // delete index and add new column and index.
    tableCatalog.alterTable(
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
        TableChange.deleteIndex("u1_key", false),
        TableChange.addColumn(
            new String[] {"col_4"},
            Types.VarCharType.of(255),
            TableChange.ColumnPosition.defaultPos()),
        TableChange.addIndex(Index.IndexType.UNIQUE_KEY, "u2_key", new String[][] {{"col_4"}}));

    indexes =
        new Index[] {
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}}),
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
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}}),
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
    Column col2 = Column.of("col_2", Types.ByteType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    String tableName = "auto_increment_table";
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

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

    // Test add auto increment ,but not insert index. will failed.
    RuntimeException runtimeException =
        assertThrows(
            RuntimeException.class,
            () ->
                tableCatalog.alterTable(
                    tableIdentifier,
                    TableChange.addColumn(
                        new String[] {"col_6"},
                        Types.LongType.get(),
                        "id",
                        TableChange.ColumnPosition.defaultPos(),
                        false,
                        true)));
    Assertions.assertTrue(
        StringUtils.contains(
            runtimeException.getMessage(),
            "Incorrect table definition; there can be only one auto column and it must be defined as a key"));

    // Test add auto increment success.
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {"col_6"},
            Types.LongType.get(),
            "id",
            TableChange.ColumnPosition.defaultPos(),
            false,
            true),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY,
            Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            new String[][] {{"col_6"}}));

    Table table = tableCatalog.loadTable(tableIdentifier);

    Column col6 = Column.of("col_6", Types.LongType.get(), "id", false, true, null);
    Index[] indices = new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_6"}})};
    newColumns = new Column[] {col1, col2, col3, col4, col5, col6};
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indices, table);

    // Test the auto-increment property of modified fields
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_6"}, false));
    table = tableCatalog.loadTable(tableIdentifier);
    col6 = Column.of("col_6", Types.LongType.get(), "id", false, false, null);
    indices = new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_6"}})};
    newColumns = new Column[] {col1, col2, col3, col4, col5, col6};
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indices, table);

    // Add the auto-increment attribute to the field
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_6"}, true));
    table = tableCatalog.loadTable(tableIdentifier);
    col6 = Column.of("col_6", Types.LongType.get(), "id", false, true, null);
    indices = new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_6"}})};
    newColumns = new Column[] {col1, col2, col3, col4, col5, col6};
    assertionsTableInfo(
        tableName, table_comment, Arrays.asList(newColumns), properties, indices, table);
  }
}
