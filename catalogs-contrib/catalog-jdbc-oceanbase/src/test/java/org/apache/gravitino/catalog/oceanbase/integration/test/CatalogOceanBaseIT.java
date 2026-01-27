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
package org.apache.gravitino.catalog.oceanbase.integration.test;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.oceanbase.integration.test.service.OceanBaseService;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.OceanBaseContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Column.ColumnImpl;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogOceanBaseIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogOceanBaseIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String provider = "jdbc-oceanbase";

  private final String metalakeName = GravitinoITUtils.genRandomName("oceanbase_it_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("oceanbase_it_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("oceanbase_it_schema");
  private final String tableName = GravitinoITUtils.genRandomName("oceanbase_it_table");
  private final String alertTableName = "alert_table_name";
  private final String table_comment = "table_comment";

  // OceanBase doesn't support schema comment
  private final String schema_comment = null;
  private final String OCEANBASE_COL_NAME1 = "oceanbase_col_name1";
  private final String OCEANBASE_COL_NAME2 = "oceanbase_col_name2";
  private final String OCEANBASE_COL_NAME3 = "oceanbase_col_name3";
  private final String OCEANBASE_COL_NAME4 = "oceanbase_col_name4";
  private final String OCEANBASE_COL_NAME5 = "oceanbase_col_name5";

  private GravitinoMetalake metalake;

  protected Catalog catalog;

  private OceanBaseService oceanBaseService;

  private OceanBaseContainer OCEANBASE_CONTAINER;

  private String TEST_DB_NAME;

  boolean SupportColumnDefaultValueExpression() {
    return true;
  }

  @BeforeAll
  public void startup() throws IOException, SQLException {
    TEST_DB_NAME = "oceanbase_catalog_oceanbase_it";

    containerSuite.startOceanBaseContainer();
    OCEANBASE_CONTAINER = containerSuite.getOceanBaseContainer();

    oceanBaseService = new OceanBaseService(OCEANBASE_CONTAINER, TEST_DB_NAME);
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    try {
      clearTableAndSchema();
      metalake.dropCatalog(catalogName);
      client.dropMetalake(metalakeName);
      oceanBaseService.close();
    } catch (Exception e) {
      LOG.info("Fail to stop.", e);
    }
  }

  @AfterEach
  public void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().dropTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  private void createCatalog() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            OCEANBASE_CONTAINER.getJdbcUrl(TEST_DB_NAME),
            0,
            OCEANBASE_CONTAINER.getJdbcUrl(TEST_DB_NAME).lastIndexOf("/")));
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), OCEANBASE_CONTAINER.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), OCEANBASE_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), OCEANBASE_CONTAINER.getPassword());

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    Map<String, String> prop = Maps.newHashMap();

    Schema createdSchema = catalog.asSchemas().createSchema(schemaName, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));
  }

  private Column[] createColumns() {
    Column col1 = Column.of(OCEANBASE_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(OCEANBASE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(OCEANBASE_COL_NAME3, Types.StringType.get(), "col_3_comment");

    return new Column[] {col1, col2, col3};
  }

  private Column[] createColumnsWithDefaultValue() {
    return new Column[] {
      Column.of(
          OCEANBASE_COL_NAME1,
          Types.FloatType.get(),
          "col_1_comment",
          false,
          false,
          Literals.of("1.23", Types.FloatType.get())),
      Column.of(
          OCEANBASE_COL_NAME2,
          Types.TimestampType.withoutTimeZone(),
          "col_2_comment",
          false,
          false,
          FunctionExpression.of("current_timestamp")),
      Column.of(
          OCEANBASE_COL_NAME3,
          Types.VarCharType.of(255),
          "col_3_comment",
          true,
          false,
          Literals.NULL),
      Column.of(
          OCEANBASE_COL_NAME4,
          Types.IntegerType.get(),
          "col_4_comment",
          false,
          false,
          Literals.of("1000", Types.IntegerType.get())),
      Column.of(
          OCEANBASE_COL_NAME5,
          Types.DecimalType.of(3, 2),
          "col_5_comment",
          true,
          false,
          Literals.of("1.23", Types.DecimalType.of(3, 2)))
    };
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    return properties;
  }

  @Test
  void testOperationOceanBaseSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    Namespace namespace = Namespace.of(metalakeName, catalogName);
    // list schema check.
    String[] nameIdentifiers = schemas.listSchemas();
    Set<String> schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertTrue(schemaNames.contains(schemaName));

    NameIdentifier[] oceanBaseNamespaces = oceanBaseService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(oceanBaseNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(testSchemaName, schema_comment, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas();
    schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    oceanBaseNamespaces = oceanBaseService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(oceanBaseNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> {
          schemas.createSchema(testSchemaName, schema_comment, emptyMap);
        });

    // drop schema check.
    schemas.dropSchema(testSchemaName, false);
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(testSchemaName));
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> oceanBaseService.loadSchema(schemaIdent));

    nameIdentifiers = schemas.listSchemas();
    schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    Assertions.assertFalse(schemas.dropSchema("no-exits", false));
    TableCatalog tableCatalog = catalog.asTableCatalog();

    // create failed check.
    NameIdentifier table = NameIdentifier.of(testSchemaName, "test_table");
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
    Assertions.assertFalse(schemas.dropSchema(schemaIdent.name(), true));
    Assertions.assertFalse(schemas.dropSchema(schemaIdent.name(), false));
    Assertions.assertFalse(tableCatalog.dropTable(table));
    oceanBaseNamespaces = oceanBaseService.listSchemas(Namespace.empty());
    schemaNames =
        Arrays.stream(oceanBaseNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));
  }

  @Test
  void testTestConnection() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            OCEANBASE_CONTAINER.getJdbcUrl(TEST_DB_NAME),
            0,
            OCEANBASE_CONTAINER.getJdbcUrl(TEST_DB_NAME).lastIndexOf("/")));
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), OCEANBASE_CONTAINER.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), OCEANBASE_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), "wrong_password");

    Exception exception =
        assertThrows(
            ConnectionFailedException.class,
            () ->
                metalake.testConnection(
                    GravitinoITUtils.genRandomName("oceanbase_it_catalog"),
                    Catalog.Type.RELATIONAL,
                    provider,
                    "comment",
                    catalogProperties));
    Assertions.assertTrue(exception.getMessage().contains("Access denied for user"));
  }

  @Test
  void testCreateAndLoadOceanBaseTable() {
    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    final SortOrder[] sortOrders = new SortOrder[0];

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

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(table_comment, loadTable.comment());
    Map<String, String> resultProp = loadTable.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertEquals(loadTable.columns().length, columns.length);
    for (int i = 0; i < columns.length; i++) {
      ITUtils.assertColumn(columns[i], loadTable.columns()[i]);
    }
  }

  @Test
  void testColumnNameWithKeyWords() {
    // Create table from Gravitino API
    Column[] columns = {
      Column.of("integer", Types.IntegerType.get(), "integer"),
      Column.of("long", Types.LongType.get(), "long"),
      Column.of("float", Types.FloatType.get(), "float"),
      Column.of("double", Types.DoubleType.get(), "double"),
      Column.of("decimal", Types.DecimalType.of(10, 3), "decimal"),
      Column.of("date", Types.DateType.get(), "date"),
      Column.of("time", Types.TimeType.get(), "time")
    };

    String name = GravitinoITUtils.genRandomName("table") + "_keyword";
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, name);
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
  @EnabledIf("SupportColumnDefaultValueExpression")
  void testColumnDefaultValue() {
    Column col1 =
        Column.of(
            OCEANBASE_COL_NAME1,
            Types.IntegerType.get(),
            "col_1_comment",
            false,
            false,
            Literals.integerLiteral(1));
    Column col2 =
        Column.of(
            OCEANBASE_COL_NAME2,
            Types.TimestampType.withoutTimeZone(),
            "col_2_comment",
            false,
            false,
            FunctionExpression.of("current_timestamp"));
    Column col3 =
        Column.of(
            OCEANBASE_COL_NAME3,
            Types.VarCharType.of(255),
            "col_3_comment",
            true,
            false,
            Literals.NULL);
    Column col4 =
        Column.of(OCEANBASE_COL_NAME4, Types.StringType.get(), "col_4_comment", false, false, null);
    Column col5 =
        Column.of(
            OCEANBASE_COL_NAME5,
            Types.VarCharType.of(255),
            "col_5_comment",
            true,
            false,
            Literals.stringLiteral("current_timestamp"));

    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    NameIdentifier tableIdent =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("oceanbase_it_table"));
    catalog.asTableCatalog().createTable(tableIdent, newColumns, null, ImmutableMap.of());
    Table createdTable = catalog.asTableCatalog().loadTable(tableIdent);
    Assertions.assertEquals(
        DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, createdTable.columns()[1].defaultValue());
    Assertions.assertEquals(Literals.NULL, createdTable.columns()[2].defaultValue());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, createdTable.columns()[3].defaultValue());
    Assertions.assertEquals(
        Literals.varcharLiteral(255, "current_timestamp"),
        createdTable.columns()[4].defaultValue());
  }

  @Test
  @EnabledIf("SupportColumnDefaultValueExpression")
  void testColumnDefaultValueConverter() {
    // test convert from OceanBase to Gravitino
    String tableName = GravitinoITUtils.genRandomName("test_default_value");
    String fullTableName = schemaName + "." + tableName;
    String sql =
        "CREATE TABLE "
            + fullTableName
            + " (\n"
            + "  int_col_1 int default 0x01AF,\n"
            + "  int_col_2 int default 6,\n"
            + "  int_col_3 int default '3.321',\n"
            + "  unsigned_int_col_1 INT UNSIGNED default 1,\n"
            + "  unsigned_bigint_col_1 BIGINT(20) UNSIGNED default 0,\n"
            + "  double_col_1 double default 123.45,\n"
            + "  varchar100_col_1 varchar(100) default 'CURRENT_TIMESTAMP',\n"
            + "  varchar200_col_1 varchar(200) default 'curdate()',\n"
            + "  time_col_1 time default '00:00:00',\n"
            + "  datetime_col_1 datetime default CURRENT_TIMESTAMP,\n"
            + "  datetime_col_2 datetime default current_timestamp,\n"
            + "  datetime_col_3 datetime default null,\n"
            + "  datetime_col_4 datetime default 19830905,\n"
            + "  date_col_2 date,\n"
            + "  date_col_5 date DEFAULT '2024-04-01',\n"
            + "  timestamp_col_1 timestamp default '2012-12-31 11:30:45',\n"
            + "  timestamp_col_2 timestamp default 19830905,\n"
            + "  timestamp_col_3 timestamp(6) default CURRENT_TIMESTAMP(6),\n"
            + "  decimal_6_2_col_1 decimal(6, 2) default 1.2,\n"
            + "  bit_col_1 bit default b'1'\n"
            + ");\n";

    oceanBaseService.executeQuery(sql);
    Table loadedTable =
        catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));

    for (Column column : loadedTable.columns()) {
      switch (column.name()) {
        case "int_col_1":
          Assertions.assertEquals(Literals.integerLiteral(431), column.defaultValue());
          break;
        case "int_col_2":
          Assertions.assertEquals(Literals.integerLiteral(6), column.defaultValue());
          break;
        case "int_col_3":
          Assertions.assertEquals(Literals.integerLiteral(3), column.defaultValue());
          break;
        case "unsigned_int_col_1":
          Assertions.assertEquals(Literals.unsignedIntegerLiteral(1L), column.defaultValue());
          break;
        case "unsigned_bigint_col_1":
          Assertions.assertEquals(
              Literals.unsignedLongLiteral(Decimal.of("0")), column.defaultValue());
          break;
        case "double_col_1":
          Assertions.assertEquals(Literals.doubleLiteral(123.45), column.defaultValue());
          break;
        case "varchar100_col_1":
          Assertions.assertEquals(
              Literals.varcharLiteral(100, "CURRENT_TIMESTAMP"), column.defaultValue());
          break;
        case "varchar200_col_1":
          Assertions.assertEquals(Literals.varcharLiteral(200, "curdate()"), column.defaultValue());
          break;
        case "time_col_1":
          Assertions.assertEquals(Literals.timeLiteral("00:00:00"), column.defaultValue());
          break;
        case "datetime_col_1":
        case "datetime_col_2":
        case "timestamp_col_3":
          Assertions.assertEquals(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, column.defaultValue());
          break;
        case "datetime_col_3":
          Assertions.assertEquals(Literals.NULL, column.defaultValue());
          break;
        case "datetime_col_4":
          Assertions.assertEquals(
              Literals.timestampLiteral("1983-09-05T00:00"), column.defaultValue());
          break;
        case "date_col_1":
          Assertions.assertEquals(UnparsedExpression.of("curdate()"), column.defaultValue());
          break;
        case "date_col_2":
          Assertions.assertEquals(Literals.NULL, column.defaultValue());
          break;
        case "date_col_5":
          Assertions.assertEquals(
              Literals.of("2024-04-01", Types.DateType.get()), column.defaultValue());
          break;
        case "timestamp_col_1":
          Assertions.assertEquals(
              Literals.timestampLiteral("2012-12-31T11:30:45"), column.defaultValue());
          break;
        case "timestamp_col_2":
          Assertions.assertEquals(
              Literals.timestampLiteral("1983-09-05T00:00:00"), column.defaultValue());
          break;
        case "decimal_6_2_col_1":
          Assertions.assertEquals(
              Literals.decimalLiteral(Decimal.of("1.2", 6, 2)), column.defaultValue());
          break;
        case "bit_col_1":
          Assertions.assertEquals(UnparsedExpression.of("b'1'"), column.defaultValue());
          break;
        default:
          Assertions.fail(
              "Unexpected column name: "
                  + column.name()
                  + ", default value: "
                  + column.defaultValue());
      }
    }
  }

  @Test
  void testColumnTypeConverter() {
    // test convert from OceanBase to Gravitino
    String tableName = GravitinoITUtils.genRandomName("test_type_converter");
    String fullTableName = schemaName + "." + tableName;
    String sql =
        "CREATE TABLE "
            + fullTableName
            + " (\n"
            + "  tinyint_col tinyint,\n"
            + "  smallint_col smallint,\n"
            + "  int_col int,\n"
            + "  bigint_col bigint,\n"
            + "  float_col float,\n"
            + "  double_col double,\n"
            + "  date_col date,\n"
            + "  time_col time,\n"
            + "  timestamp_col timestamp,\n"
            + "  datetime_col datetime,\n"
            + "  decimal_6_2_col decimal(6, 2),\n"
            + "  varchar20_col varchar(20),\n"
            + "  text_col text,\n"
            + "  binary_col binary,\n"
            + "  blob_col blob\n"
            + ");\n";

    oceanBaseService.executeQuery(sql);
    Table loadedTable =
        catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));

    for (Column column : loadedTable.columns()) {
      switch (column.name()) {
        case "tinyint_col":
          Assertions.assertEquals(Types.ByteType.get(), column.dataType());
          break;
        case "smallint_col":
          Assertions.assertEquals(Types.ShortType.get(), column.dataType());
          break;
        case "int_col":
          Assertions.assertEquals(Types.IntegerType.get(), column.dataType());
          break;
        case "bigint_col":
          Assertions.assertEquals(Types.LongType.get(), column.dataType());
          break;
        case "float_col":
          Assertions.assertEquals(Types.FloatType.get(), column.dataType());
          break;
        case "double_col":
          Assertions.assertEquals(Types.DoubleType.get(), column.dataType());
          break;
        case "date_col":
          Assertions.assertEquals(Types.DateType.get(), column.dataType());
          break;
        case "time_col":
          Assertions.assertEquals(Types.TimeType.of(0), column.dataType());
          break;
        case "timestamp_col":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(0), column.dataType());
          break;
        case "datetime_col":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(0), column.dataType());
          break;
        case "decimal_6_2_col":
          Assertions.assertEquals(Types.DecimalType.of(6, 2), column.dataType());
          break;
        case "varchar20_col":
          Assertions.assertEquals(Types.VarCharType.of(20), column.dataType());
          break;
        case "text_col":
          Assertions.assertEquals(Types.StringType.get(), column.dataType());
          break;
        case "binary_col":
          Assertions.assertEquals(Types.BinaryType.get(), column.dataType());
          break;
        case "blob_col":
          Assertions.assertEquals(Types.ExternalType.of("BLOB"), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected column name: " + column.name());
      }
    }
  }

  @Test
  void testAlterAndDropOceanBaseTable() {
    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName), columns, table_comment, createProperties());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog
              .asTableCatalog()
              .alterTable(
                  NameIdentifier.of(schemaName, tableName),
                  TableChange.rename(alertTableName),
                  TableChange.updateComment(table_comment + "_new"));
        });

    catalog
        .asTableCatalog()
        .alterTable(NameIdentifier.of(schemaName, tableName), TableChange.rename(alertTableName));

    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {OCEANBASE_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnType(
                new String[] {OCEANBASE_COL_NAME1}, Types.IntegerType.get()));

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, alertTableName));
    Assertions.assertEquals(alertTableName, table.name());

    Assertions.assertEquals(OCEANBASE_COL_NAME1, table.columns()[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), table.columns()[0].dataType());

    Assertions.assertEquals("col_2_new", table.columns()[1].name());
    Assertions.assertEquals(Types.DateType.get(), table.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", table.columns()[1].comment());

    Assertions.assertEquals(OCEANBASE_COL_NAME3, table.columns()[2].name());
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

    Column col1 = Column.of("name", Types.StringType.get(), "comment");
    Column col2 = Column.of("address", Types.StringType.get(), "comment");
    Column col3 = Column.of("date_of_birth", Types.DateType.get(), "comment");

    Column[] newColumns = new Column[] {col1, col2, col3};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("catalog_jdbc_it_table"));
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
  void testUpdateColumnDefaultValue() {
    Column[] columns = createColumnsWithDefaultValue();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName), columns, null, ImmutableMap.of());

    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());
    Assertions.assertNull(table.auditInfo().lastModifier());
    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(schemaName, tableName),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[0].name()}, Literals.of("1.2345", Types.FloatType.get())),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[1].name()}, FunctionExpression.of("current_timestamp")),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[2].name()}, Literals.of("hello", Types.VarCharType.of(255))),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[3].name()}, Literals.of("2000", Types.IntegerType.get())),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[4].name()}, Literals.of("2.34", Types.DecimalType.of(3, 2))));

    table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));

    Assertions.assertEquals(
        Literals.of("1.2345", Types.FloatType.get()), table.columns()[0].defaultValue());
    Assertions.assertEquals(
        FunctionExpression.of("current_timestamp"), table.columns()[1].defaultValue());
    Assertions.assertEquals(
        Literals.of("hello", Types.VarCharType.of(255)), table.columns()[2].defaultValue());
    Assertions.assertEquals(
        Literals.of("2000", Types.IntegerType.get()), table.columns()[3].defaultValue());
    Assertions.assertEquals(
        Literals.of("2.34", Types.DecimalType.of(3, 2)), table.columns()[4].defaultValue());
  }

  @Test
  void testDropOceanBaseDatabase() {
    String schemaName = GravitinoITUtils.genRandomName("oceanbase_schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("oceanbase_table").toLowerCase();

    catalog
        .asSchemas()
        .createSchema(schemaName, null, ImmutableMap.<String, String>builder().build());

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createColumns(),
            "Created by Gravitino client",
            ImmutableMap.<String, String>builder().build());

    // Try to drop a database, and cascade equals to false, it should not be
    // allowed.
    Throwable excep =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.asSchemas().dropSchema(schemaName, false));
    Assertions.assertTrue(excep.getMessage().contains("the value of cascade should be true."));

    // Check the database still exists
    catalog.asSchemas().loadSchema(schemaName);

    // Try to drop a database, and cascade equals to true, it should be allowed.
    catalog.asSchemas().dropSchema(schemaName, true);
    // Check database has been dropped
    SupportsSchemas schemas = catalog.asSchemas();
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          schemas.loadSchema(schemaName);
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

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

    NameIdentifier id = NameIdentifier.of(schemaName, "test_failed");
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
            "Index does not support complex fields in this Catalog"));

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
            "Index does not support complex fields in this Catalog"));

    NameIdentifier tableIdent = NameIdentifier.of(schemaName, "test_null_key");
    tableCatalog.createTable(
        tableIdent,
        newColumns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        new Index[] {
          Indexes.of(
              Index.IndexType.UNIQUE_KEY, null, new String[][] {{"col_1"}, {"col_3"}, {"col_4"}}),
          Indexes.of(Index.IndexType.UNIQUE_KEY, null, new String[][] {{"col_4"}}),
        });
    table = tableCatalog.loadTable(tableIdent);

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

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(alterColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(alterColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(alterColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

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
    final String testSchemaName = "test";
    RuntimeException exception =
        Assertions.assertThrowsExactly(
            UnsupportedOperationException.class,
            () -> catalog.asSchemas().createSchema(testSchemaName, "comment", null));
    Assertions.assertTrue(
        exception.getMessage().contains("Doesn't support setting schema comment: comment"));

    // test null comment
    String testSchemaName2 = "test2";
    Schema schema = catalog.asSchemas().createSchema(testSchemaName2, "", null);
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
    schema = catalog.asSchemas().loadSchema(testSchemaName2);
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
    catalog.asSchemas().dropSchema(testSchemaName2, true);
  }

  @Test
  public void testBackQuoteTable() {
    Column col1 = Column.of("create", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("delete", Types.ByteType.get(), "yes", false, false, null);
    Column col3 = Column.of("show", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("status", Types.VarCharType.of(255), "code", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4};
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, "table");
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
  void testOceanBaseSpecialTableName() {
    // Test create many indexes with name success.
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();

    String t1_name = "t112";
    Column t1_col = Column.of(t1_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns = {t1_col};

    Index[] t1_indexes = {Indexes.unique("u1_key", new String[][] {{t1_name}})};

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, t1_name);
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
    tableIdentifier = NameIdentifier.of(schemaName, t2_name);
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
    tableIdentifier = NameIdentifier.of(schemaName, t3_name);
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
    tableIdentifier = NameIdentifier.of(schemaName, t4_name);
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        t4_indexes);

    Table t1 = tableCatalog.loadTable(NameIdentifier.of(schemaName, t1_name));
    Arrays.stream(t1.columns()).anyMatch(c -> Objects.equals(c.name(), "t112"));
    ITUtils.assertionsTableInfo(
        t1_name,
        table_comment,
        Arrays.asList(t1_col),
        properties,
        t1_indexes,
        Transforms.EMPTY_TRANSFORM,
        t1);

    Table t2 = tableCatalog.loadTable(NameIdentifier.of(schemaName, t2_name));
    Arrays.stream(t2.columns()).anyMatch(c -> Objects.equals(c.name(), "t212"));
    ITUtils.assertionsTableInfo(
        t2_name,
        table_comment,
        Arrays.asList(t2_col),
        properties,
        t2_indexes,
        Transforms.EMPTY_TRANSFORM,
        t2);

    Table t3 = tableCatalog.loadTable(NameIdentifier.of(schemaName, t3_name));
    Arrays.stream(t3.columns()).anyMatch(c -> Objects.equals(c.name(), "t_12"));
    ITUtils.assertionsTableInfo(
        t3_name,
        table_comment,
        Arrays.asList(t3_col),
        properties,
        t3_indexes,
        Transforms.EMPTY_TRANSFORM,
        t3);

    Table t4 = tableCatalog.loadTable(NameIdentifier.of(schemaName, t4_name));
    Arrays.stream(t4.columns()).anyMatch(c -> Objects.equals(c.name(), "_1__"));
    ITUtils.assertionsTableInfo(
        t4_name,
        table_comment,
        Arrays.asList(t4_col),
        properties,
        t4_indexes,
        Transforms.EMPTY_TRANSFORM,
        t4);
  }

  @Test
  void testOceanBaseIllegalTableName() {
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String table_name = "t123";

    String t1_name = table_name + "`; DROP TABLE important_table; -- ";
    Column t1_col = Column.of(t1_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns = {t1_col};
    Index[] t1_indexes = {Indexes.unique("u1_key", new String[][] {{t1_name}})};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t1_name);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier,
              columns,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t1_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier);
        });

    String t2_name = table_name + "`; SLEEP(10); -- ";
    Column t2_col = Column.of(t2_name, Types.LongType.get(), "id", false, false, null);
    Index[] t2_indexes = {Indexes.unique("u2_key", new String[][] {{t2_name}})};
    Column[] columns2 = new Column[] {t2_col};
    NameIdentifier tableIdentifier2 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t2_name);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier2,
              columns2,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t2_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier2);
        });

    String t3_name =
        table_name + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Column t3_col = Column.of(t3_name, Types.LongType.get(), "id", false, false, null);
    Index[] t3_indexes = {Indexes.unique("u3_key", new String[][] {{t3_name}})};
    Column[] columns3 = new Column[] {t3_col};
    NameIdentifier tableIdentifier3 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t3_name);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier3,
              columns3,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t3_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier3);
        });

    String invalidInput = StringUtils.repeat("a", 65);
    Column t4_col = Column.of(invalidInput, Types.LongType.get(), "id", false, false, null);
    Index[] t4_indexes = {Indexes.unique("u4_key", new String[][] {{invalidInput}})};
    Column[] columns4 = new Column[] {t4_col};
    NameIdentifier tableIdentifier4 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, invalidInput);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier4,
              columns4,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t4_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier4);
        });
  }

  @Test
  void testNameSpec() {
    // test operate illegal schema name from OceanBase
    String testSchemaName = "//";
    String sql = String.format("CREATE DATABASE `%s`", testSchemaName);
    oceanBaseService.executeQuery(sql);

    Schema schema = catalog.asSchemas().loadSchema(testSchemaName);
    Assertions.assertEquals(testSchemaName, schema.name());

    String[] schemaIdents = catalog.asSchemas().listSchemas();
    Assertions.assertTrue(Arrays.stream(schemaIdents).anyMatch(s -> s.equals(testSchemaName)));

    Assertions.assertTrue(catalog.asSchemas().dropSchema(testSchemaName, false));
    Assertions.assertFalse(catalog.asSchemas().schemaExists(testSchemaName));

    // test operate illegal table name from OceanBase
    oceanBaseService.executeQuery(sql);
    String testTableName = "//";
    sql = String.format("CREATE TABLE `%s`.`%s` (id int)", testSchemaName, testTableName);
    oceanBaseService.executeQuery(sql);
    NameIdentifier tableIdent = NameIdentifier.of(testSchemaName, testTableName);

    Table table = catalog.asTableCatalog().loadTable(tableIdent);
    Assertions.assertEquals(testTableName, table.name());

    NameIdentifier[] tableIdents =
        catalog.asTableCatalog().listTables(Namespace.of(testSchemaName));
    Assertions.assertTrue(Arrays.stream(tableIdents).anyMatch(t -> t.name().equals(testTableName)));

    Assertions.assertTrue(catalog.asTableCatalog().dropTable(tableIdent));
    Assertions.assertFalse(catalog.asTableCatalog().tableExists(tableIdent));
    Assertions.assertFalse(catalog.asTableCatalog().purgeTable(tableIdent));
    catalog.asSchemas().dropSchema(testSchemaName, true);

    // sql injection
    String schemaName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    String comment = null;

    // should throw an exception with string that might contain SQL injection
    String sqlInjection = schemaName + "`; DROP TABLE important_table; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().createSchema(sqlInjection, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().dropSchema(sqlInjection, false);
        });

    String sqlInjection1 = schemaName + "`; SLEEP(10); -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().createSchema(sqlInjection1, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().dropSchema(sqlInjection1, false);
        });

    String sqlInjection2 =
        schemaName + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().createSchema(sqlInjection2, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().dropSchema(sqlInjection2, false);
        });

    // should throw an exception with input that has more than 64 characters
    String invalidInput = StringUtils.repeat("a", 65);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().createSchema(invalidInput, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asSchemas().dropSchema(invalidInput, false);
        });
  }

  @Test
  void testOceanBaseSchemaNameCaseSensitive() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.VarCharType.of(255), "code", false, false, null);
    Column col3 = Column.of("col_3", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3};

    Index[] indexes = new Index[] {Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}})};

    String[] schemas = {"db_", "db_1", "db_2", "db12"};
    SupportsSchemas schemaSupport = catalog.asSchemas();

    for (String schema : schemas) {
      schemaSupport.createSchema(schema, null, Collections.emptyMap());
      Assertions.assertNotNull(schemaSupport.loadSchema(schema));
    }

    Set<String> schemaNames = Sets.newHashSet(schemaSupport.listSchemas());

    Assertions.assertTrue(schemaNames.containsAll(Arrays.asList(schemas)));

    String tableName = "test1";
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();

    for (String schema : schemas) {
      tableCatalog.createTable(
          NameIdentifier.of(schema, tableName),
          newColumns,
          table_comment,
          properties,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          new SortOrder[0],
          indexes);
      tableCatalog.createTable(
          NameIdentifier.of(schema, GravitinoITUtils.genRandomName("test2")),
          newColumns,
          table_comment,
          properties,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          new SortOrder[0],
          Indexes.EMPTY_INDEXES);
    }

    for (String schema : schemas) {
      NameIdentifier[] nameIdentifiers = tableCatalog.listTables(Namespace.of(schema));
      Assertions.assertEquals(2, nameIdentifiers.length);
      Assertions.assertTrue(
          Arrays.stream(nameIdentifiers)
              .map(NameIdentifier::name)
              .collect(Collectors.toSet())
              .stream()
              .anyMatch(n -> n.equals(tableName)));
    }

    for (String schema : schemas) {
      schemaSupport.dropSchema(schema, true);
    }
  }

  @Test
  void testUnparsedTypeConverter() {
    String tableName = GravitinoITUtils.genRandomName("test_unparsed_type");
    oceanBaseService.executeQuery(
        String.format("CREATE TABLE %s.%s (bit_col bit);", schemaName, tableName));
    Table loadedTable =
        catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertEquals(Types.ExternalType.of("BIT"), loadedTable.columns()[0].dataType());
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
        NameIdentifier.of(schemaName, tableName),
        newColumns,
        table_comment,
        createProperties(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        Indexes.EMPTY_INDEXES);

    // add index test.
    tableCatalog.alterTable(
        NameIdentifier.of(schemaName, tableName),
        TableChange.addIndex(
            Index.IndexType.UNIQUE_KEY, "u1_key", new String[][] {{"col_2"}, {"col_3"}}));

    tableCatalog.alterTable(
        NameIdentifier.of(schemaName, tableName),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY,
            Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            new String[][] {{"col_1"}}));

    Table table = tableCatalog.loadTable(NameIdentifier.of(schemaName, tableName));
    Index[] indexes =
        new Index[] {
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.createMysqlPrimaryKey(new String[][] {{"col_1"}})
        };
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        createProperties(),
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

    // delete index and add new column and index.
    tableCatalog.alterTable(
        NameIdentifier.of(schemaName, tableName),
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
    table = tableCatalog.loadTable(NameIdentifier.of(schemaName, tableName));
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), null, true, false, null);
    newColumns = new Column[] {col1, col2, col3, col4};
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        createProperties(),
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

    // Add a previously existing index
    tableCatalog.alterTable(
        NameIdentifier.of(schemaName, tableName),
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
    table = tableCatalog.loadTable(NameIdentifier.of(schemaName, tableName));
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        createProperties(),
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);
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

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indices,
        Transforms.EMPTY_TRANSFORM,
        table);

    // Test the auto-increment property of modified fields
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_6"}, false));
    table = tableCatalog.loadTable(tableIdentifier);
    col6 = Column.of("col_6", Types.LongType.get(), "id", false, false, null);
    indices = new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_6"}})};
    newColumns = new Column[] {col1, col2, col3, col4, col5, col6};
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indices,
        Transforms.EMPTY_TRANSFORM,
        table);

    // Add the auto-increment attribute to the field
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_6"}, true));
    table = tableCatalog.loadTable(tableIdentifier);
    col6 = Column.of("col_6", Types.LongType.get(), "id", false, true, null);
    indices = new Index[] {Indexes.createMysqlPrimaryKey(new String[][] {{"col_6"}})};
    newColumns = new Column[] {col1, col2, col3, col4, col5, col6};
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        indices,
        Transforms.EMPTY_TRANSFORM,
        table);
  }

  @Test
  void testAddColumnDefaultValue() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "uid", true, false, null);
    Column col2 = Column.of("col_2", Types.ByteType.get(), "yes", true, false, null);
    Column col3 = Column.of("col_3", Types.VarCharType.of(255), "comment", true, false, null);
    String tableName = "default_value_table";
    Column[] newColumns = new Column[] {col1, col2, col3};

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
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

    Column col4 =
        Column.of("col_4", Types.LongType.get(), "col4", false, false, Literals.longLiteral(1000L));
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {col4.name()},
            col4.dataType(),
            col4.comment(),
            TableChange.ColumnPosition.defaultPos(),
            col4.nullable(),
            col4.autoIncrement(),
            col4.defaultValue()));

    Table table = tableCatalog.loadTable(tableIdentifier);
    newColumns = new Column[] {col1, col2, col3, col4};

    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        Indexes.EMPTY_INDEXES,
        Transforms.EMPTY_TRANSFORM,
        table);
  }

  @Test
  public void testOceanBaseIntegerTypes() {
    Column col1 = Column.of("col_1", Types.ByteType.get(), "byte type", true, false, null);
    Column col2 =
        Column.of("col_2", Types.ByteType.unsigned(), "byte unsigned type", true, false, null);
    Column col3 = Column.of("col_3", Types.ShortType.get(), "short type", true, false, null);
    Column col4 =
        Column.of("col_4", Types.ShortType.unsigned(), "short unsigned type ", true, false, null);
    Column col5 = Column.of("col_5", Types.IntegerType.get(), "integer type", true, false, null);
    Column col6 =
        Column.of(
            "col_6", Types.IntegerType.unsigned(), "integer unsigned type", true, false, null);
    Column col7 = Column.of("col_7", Types.LongType.get(), "long type", true, false, null);
    Column col8 =
        Column.of("col_8", Types.LongType.unsigned(), "long unsigned type", true, false, null);
    String tableName = "default_integer_types_table";
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5, col6, col7, col8};

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
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

    Table table = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(8, table.columns().length);
    Column[] columns = table.columns();
    Assertions.assertEquals(columns[0].dataType().simpleString(), "byte");
    Assertions.assertEquals(columns[1].dataType().simpleString(), "byte unsigned");
    Assertions.assertEquals(columns[2].dataType().simpleString(), "short");
    Assertions.assertEquals(columns[3].dataType().simpleString(), "short unsigned");
    Assertions.assertEquals(columns[4].dataType().simpleString(), "integer");
    Assertions.assertEquals(columns[5].dataType().simpleString(), "integer unsigned");
    Assertions.assertEquals(columns[6].dataType().simpleString(), "long");
    Assertions.assertEquals(columns[7].dataType().simpleString(), "long unsigned");
  }

  @Test
  void testTimeTypePrecision() {
    String tableName = GravitinoITUtils.genRandomName("test_time_precision");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    columns =
        ArrayUtils.addAll(
            columns,
            // time type
            Column.of("time_col", Types.TimeType.get()),
            Column.of("time_col_0", Types.TimeType.of(0)),
            Column.of("time_col_1", Types.TimeType.of(1)),
            Column.of("time_col_3", Types.TimeType.of(3)),
            Column.of("time_col_6", Types.TimeType.of(6)),
            // timestamp type
            Column.of("timestamp_col", Types.TimestampType.withTimeZone()),
            Column.of("timestamp_col_0", Types.TimestampType.withTimeZone(0)),
            Column.of("timestamp_col_1", Types.TimestampType.withTimeZone(1)),
            Column.of("timestamp_col_3", Types.TimestampType.withTimeZone(3)),
            Column.of("timestamp_col_6", Types.TimestampType.withTimeZone(6)),
            // datetime type (without time zone)
            Column.of("datetime_col", Types.TimestampType.withoutTimeZone()),
            Column.of("datetime_col_0", Types.TimestampType.withoutTimeZone(0)),
            Column.of("datetime_col_1", Types.TimestampType.withoutTimeZone(1)),
            Column.of("datetime_col_3", Types.TimestampType.withoutTimeZone(3)),
            Column.of("datetime_col_6", Types.TimestampType.withoutTimeZone(6)));

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);

    Table loadTable = tableCatalog.loadTable(tableIdentifier);

    // Verify time type precisions
    Column[] timeColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("time_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(5, timeColumns.length);
    for (Column column : timeColumns) {
      switch (column.name()) {
        case "time_col":
        case "time_col_0":
          Assertions.assertEquals(Types.TimeType.of(0), column.dataType());
          break;
        case "time_col_1":
          Assertions.assertEquals(Types.TimeType.of(1), column.dataType());
          break;
        case "time_col_3":
          Assertions.assertEquals(Types.TimeType.of(3), column.dataType());
          break;
        case "time_col_6":
          Assertions.assertEquals(Types.TimeType.of(6), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected time column: " + column.name());
      }
    }

    // Verify timestamp type precisions
    Column[] timestampColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("timestamp_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(5, timestampColumns.length);
    for (Column column : timestampColumns) {
      switch (column.name()) {
        case "timestamp_col":
        case "timestamp_col_0":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(0), column.dataType());
          break;
        case "timestamp_col_1":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(1), column.dataType());
          break;
        case "timestamp_col_3":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(3), column.dataType());
          break;
        case "timestamp_col_6":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(6), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected timestamp column: " + column.name());
      }
    }

    // Verify datetime type precisions
    Column[] datetimeColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("datetime_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(5, datetimeColumns.length);
    for (Column column : datetimeColumns) {
      switch (column.name()) {
        case "datetime_col":
        case "datetime_col_0":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(0), column.dataType());
          break;
        case "datetime_col_1":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(1), column.dataType());
          break;
        case "datetime_col_3":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(3), column.dataType());
          break;
        case "datetime_col_6":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(6), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected datetime column: " + column.name());
      }
    }
  }
}
