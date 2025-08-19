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
package org.apache.gravitino.catalog.postgresql.integration.test;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.apache.gravitino.catalog.postgresql.integration.test.service.PostgreSqlService;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.PGImageName;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
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
import org.apache.gravitino.rel.types.Types.IntegerType;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogPostgreSqlIT extends BaseIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  public static final PGImageName DEFAULT_POSTGRES_IMAGE = PGImageName.VERSION_13;

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

  private GravitinoMetalake metalake;

  private Catalog catalog;

  private PostgreSqlService postgreSqlService;

  private PostgreSQLContainer POSTGRESQL_CONTAINER;

  protected final TestDatabaseName TEST_DB_NAME = TestDatabaseName.PG_CATALOG_POSTGRESQL_IT;

  protected PGImageName postgreImageName = DEFAULT_POSTGRES_IMAGE;

  @BeforeAll
  public void startup() throws IOException, SQLException {
    containerSuite.startPostgreSQLContainer(TEST_DB_NAME, postgreImageName);
    POSTGRESQL_CONTAINER = containerSuite.getPostgreSQLContainer(postgreImageName);

    postgreSqlService = new PostgreSqlService(POSTGRESQL_CONTAINER, TEST_DB_NAME);
    createMetalake();
    catalog = createCatalog(catalogName);
    createSchema(schemaName);
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    String[] schemaNames = catalog.asSchemas().listSchemas();
    for (String schemaName : schemaNames) {
      catalog.asSchemas().dropSchema(schemaName, true);
    }
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
    client.disableMetalake(metalakeName);
    client.dropMetalake(metalakeName);
    postgreSqlService.close();
  }

  @AfterEach
  public void resetSchema() {
    clearTableAndSchema();
    createSchema(schemaName);
  }

  private void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().dropTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  private Catalog createCatalog(String catalogName) throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    String jdbcUrl = POSTGRESQL_CONTAINER.getJdbcUrl(TEST_DB_NAME);
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), POSTGRESQL_CONTAINER.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DATABASE.getKey(), TEST_DB_NAME.toString());
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), POSTGRESQL_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), POSTGRESQL_CONTAINER.getPassword());

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    return loadCatalog;
  }

  private void createSchema(String schemaName) {

    Schema createdSchema =
        catalog.asSchemas().createSchema(schemaName, schema_comment, Collections.EMPTY_MAP);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    Assertions.assertEquals(createdSchema.comment(), loadSchema.comment());
  }

  private Column[] createColumns() {
    Column col1 = Column.of(POSTGRESQL_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(POSTGRESQL_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(POSTGRESQL_COL_NAME3, Types.StringType.get(), "col_3_comment");

    return new Column[] {col1, col2, col3};
  }

  private Column[] columnsWithSpecialNames() {
    return new Column[] {
      Column.of("integer", Types.IntegerType.get(), "integer"),
      Column.of("long", Types.LongType.get(), "long"),
      Column.of("float", Types.FloatType.get(), "float"),
      Column.of("double", Types.DoubleType.get(), "double"),
      Column.of("decimal", Types.DecimalType.of(10, 3), "decimal"),
      Column.of("date", Types.DateType.get(), "date"),
      Column.of("time", Types.TimeType.get(), "time"),
      Column.of("binary", Types.TimestampType.withoutTimeZone(), "binary")
    };
  }

  private Column[] columnsWithDefaultValue() {
    return new Column[] {
      Column.of(
          "col_1",
          Types.FloatType.get(),
          "col_1_comment",
          false,
          false,
          FunctionExpression.of("random")),
      Column.of("col_2", Types.VarCharType.of(255), "col_2_comment", true, false, Literals.NULL),
      Column.of("col_3", Types.StringType.get(), "col_3_comment", false, false, null),
      Column.of(
          "col_4",
          Types.IntegerType.get(),
          "col_4_comment",
          true,
          false,
          Literals.integerLiteral(1000)),
      Column.of(
          "col_5",
          Types.TimestampType.withoutTimeZone(),
          "col_5_comment",
          true,
          false,
          Literals.NULL)
    };
  }

  @Test
  void testTestConnection() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    String jdbcUrl = POSTGRESQL_CONTAINER.getJdbcUrl(TEST_DB_NAME);
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), POSTGRESQL_CONTAINER.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DATABASE.getKey(), TEST_DB_NAME.toString());
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), POSTGRESQL_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), "wrong_password");

    Exception exception =
        assertThrows(
            ConnectionFailedException.class,
            () ->
                metalake.testConnection(
                    GravitinoITUtils.genRandomName("postgresql_it_catalog"),
                    Catalog.Type.RELATIONAL,
                    provider,
                    "comment",
                    catalogProperties));
    Assertions.assertTrue(
        exception.getMessage().contains("password authentication failed for user"));
  }

  @Test
  void testCreateTableWithArrayType() {
    String tableName = GravitinoITUtils.genRandomName("postgresql_it_array_table");
    Column col = Column.of("array", Types.ListType.of(IntegerType.get(), false), "col_4_comment");
    Column[] columns = new Column[] {col};

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(tableIdentifier, columns, null, ImmutableMap.of());
    Table createdTable = tableCatalog.loadTable(tableIdentifier);

    Assertions.assertEquals(tableName, createdTable.name());
    Assertions.assertEquals(columns.length, createdTable.columns().length);
    for (int i = 0; i < columns.length; i++) {
      ITUtils.assertColumn(columns[i], createdTable.columns()[i]);
    }

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(columns.length, loadTable.columns().length);
    for (int i = 0; i < columns.length; i++) {
      ITUtils.assertColumn(columns[i], loadTable.columns()[i]);
    }
  }

  @Test
  void testCreateTableWithSpecialColumnNames() {
    // Create table from Gravitino API
    Column[] columns = columnsWithSpecialNames();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
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

  @Test
  void testCreateUpperCaseSchemaAndTable() {
    // Create table from Gravitino API
    Column[] columns = columnsWithSpecialNames();

    String tableN = GravitinoITUtils.genRandomName("postgresql_it_table").toUpperCase();
    String schemaN = GravitinoITUtils.genRandomName("postgresql_it_schema").toUpperCase();

    // Create a schema with upper case name
    catalog.asSchemas().createSchema(schemaN, schema_comment, Collections.EMPTY_MAP);
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaN, tableN);
    Distribution distribution = Distributions.NONE;

    SortOrder[] sortOrders = new SortOrder[0];
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    // Create a table with upper case name
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

    boolean result = tableCatalog.dropTable(tableIdentifier);
    Assertions.assertTrue(result);

    Assertions.assertThrows(Exception.class, () -> tableCatalog.loadTable(tableIdentifier));

    // Test drop schema with upper-case name
    result = catalog.asSchemas().dropSchema(schemaN, false);

    // Test whether drop the schema succussfully.
    Assertions.assertTrue(result);
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
    String[] nameIdentifiers = schemas.listSchemas();
    Set<String> schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertTrue(schemaNames.contains(schemaName));

    NameIdentifier[] postgreSqlNamespaces = postgreSqlService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(postgreSqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(testSchemaName, schema_comment, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas();
    schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    postgreSqlNamespaces = postgreSqlService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(postgreSqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
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
        NoSuchSchemaException.class, () -> postgreSqlService.loadSchema(schemaIdent));

    nameIdentifiers = schemas.listSchemas();
    schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    Assertions.assertFalse(schemas.dropSchema("no_exits", false));
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
    postgreSqlNamespaces = postgreSqlService.listSchemas(Namespace.empty());
    schemaNames =
        Arrays.stream(postgreSqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));
  }

  @Test
  void testSchemaWithIllegalName() {
    SupportsSchemas schemas = catalog.asSchemas();
    String schemaName = RandomNameUtils.genRandomName("ct_db");

    // should throw an exception with string that might contain SQL injection
    String sqlInjection = schemaName + "; DROP TABLE important_table; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(sqlInjection, null, null);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(sqlInjection, false);
        });

    String sqlInjection1 = schemaName + "; SELECT pg_sleep(10);";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(sqlInjection1, null, null);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(sqlInjection1, false);
        });

    String sqlInjection2 =
        schemaName + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(sqlInjection2, null, null);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(sqlInjection2, false);
        });

    // should throw an exception with input that has more than 63 characters
    String invalidInput = StringUtils.repeat("a", 64);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(invalidInput, null, null);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(invalidInput, false);
        });

    // should throw an exception with schema name that starts with special character
    String invalidInput2 = RandomNameUtils.genRandomName("$test_db");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(invalidInput2, null, null);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(invalidInput2, false);
        });
  }

  @Test
  void testCreateAndLoadPostgreSqlTable() {
    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
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
  void testAlterAndDropPostgreSqlTable() {
    Column[] columns = createColumns();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
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
                  NameIdentifier.of(schemaName, tableName),
                  TableChange.rename(alertTableName),
                  TableChange.updateComment(table_comment + "_new"));
        });

    // rename table
    table =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(schemaName, tableName), TableChange.rename(alertTableName));
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().lastModifier());

    // update table
    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {POSTGRESQL_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnType(
                new String[] {POSTGRESQL_COL_NAME1}, Types.IntegerType.get()));

    table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, alertTableName));
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

    Column col1 = Column.of("name", Types.StringType.get(), "comment");
    Column col2 = Column.of("address", Types.StringType.get(), "comment");
    Column col3 = Column.of("date_of_birth", Types.DateType.get(), "comment");

    Column[] newColumns = new Column[] {col1, col2, col3};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("jdbc_it_table"));
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

  //  @Test TODO(mchades): https://github.com/apache/gravitino/issues/6134
  void testCreateAndLoadSchema() throws SQLException {
    String testSchemaName = "test";

    Schema schema = catalog.asSchemas().createSchema(testSchemaName, "comment", null);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    Assertions.assertEquals("comment", schema.comment());
    schema = catalog.asSchemas().loadSchema(testSchemaName);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    Assertions.assertEquals("comment", schema.comment());

    // test null comment
    String testSchemaName2 = "test2";

    schema = catalog.asSchemas().createSchema(testSchemaName2, null, null);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    // todo: Gravitino put id to comment, makes comment is empty string not null.
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));
    schema = catalog.asSchemas().loadSchema(testSchemaName2);
    Assertions.assertEquals("anonymous", schema.auditInfo().creator());
    Assertions.assertTrue(StringUtils.isEmpty(schema.comment()));

    // test register PG service to multiple catalogs
    String newCatalogName = GravitinoITUtils.genRandomName("new_catalog");
    Catalog newCatalog = createCatalog(newCatalogName);
    newCatalog.asSchemas().loadSchema(testSchemaName2);
    Assertions.assertTrue(catalog.asSchemas().dropSchema(testSchemaName2, false));
    createSchema(testSchemaName2);
    SupportsSchemas schemaOps = newCatalog.asSchemas();
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> schemaOps.loadSchema(testSchemaName2));
    // recovered by re-build the catalog
    Assertions.assertTrue(metalake.dropCatalog(newCatalogName, true));
    newCatalog = createCatalog(newCatalogName);
    Schema loadedSchema = newCatalog.asSchemas().loadSchema(testSchemaName2);
    Assertions.assertEquals(testSchemaName2, loadedSchema.name());

    Assertions.assertTrue(metalake.dropCatalog(newCatalogName, true));
  }

  @Test
  void testListSchema() {
    String[] nameIdentifiers = catalog.asSchemas().listSchemas();
    Set<String> schemaNames = Sets.newHashSet(nameIdentifiers);
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
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, "abc");
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

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

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

    // Test create index complex fields fail.
    NameIdentifier id = NameIdentifier.of(schemaName, "test_failed");
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

    // Test create index with same col success.
    tableIdent = NameIdentifier.of(schemaName, "many_index");
    tableCatalog.createTable(
        tableIdent,
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
    table = tableCatalog.loadTable(tableIdent);

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
    Column col7 =
        Column.of(
            "col_7",
            Types.DateType.get(),
            "col_7_comment",
            false,
            false,
            Literals.dateLiteral("2024-01-01"));
    Column col8 =
        Column.of(
            "col_8",
            Types.TimestampType.withoutTimeZone(),
            "col_8_comment",
            false,
            false,
            Literals.timestampLiteral("2024-01-01T01:01:01"));
    Column col9 =
        Column.of(
            "col_9",
            Types.TimeType.get(),
            "col_9_comment",
            false,
            false,
            Literals.timeLiteral("01:01:01"));

    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5, col6, col7, col8, col9};

    NameIdentifier tableIdent =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("pg_it_table"));
    catalog.asTableCatalog().createTable(tableIdent, newColumns, null, ImmutableMap.of());
    Table loadedTable = catalog.asTableCatalog().loadTable(tableIdent);

    Assertions.assertEquals(
        UnparsedExpression.of("random()"), loadedTable.columns()[0].defaultValue());
    Assertions.assertEquals(
        DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, loadedTable.columns()[1].defaultValue());
    Assertions.assertEquals(Literals.NULL, loadedTable.columns()[2].defaultValue());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, loadedTable.columns()[3].defaultValue());
    Assertions.assertEquals(
        Literals.varcharLiteral(255, "current_timestamp"), loadedTable.columns()[4].defaultValue());
    Assertions.assertEquals(Literals.integerLiteral(1000), loadedTable.columns()[5].defaultValue());
    Assertions.assertEquals(
        Literals.dateLiteral(LocalDate.of(2024, 1, 1)), loadedTable.columns()[6].defaultValue());
    Assertions.assertEquals(
        Literals.timestampLiteral(LocalDateTime.of(2024, 1, 1, 1, 1, 1)),
        loadedTable.columns()[7].defaultValue());
    Assertions.assertEquals(
        Literals.timeLiteral(LocalTime.of(1, 1, 1)), loadedTable.columns()[8].defaultValue());
  }

  @Test
  void testUpdateColumnDefaultValue() {
    Column[] columns = columnsWithDefaultValue();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
                columns,
                table_comment,
                createProperties());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());
    Assertions.assertNull(table.auditInfo().lastModifier());
    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(schemaName, tableName),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[0].name()}, Literals.of("1.234", Types.FloatType.get())),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[1].name()}, Literals.of("hello", Types.VarCharType.of(255))),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[2].name()}, Literals.of("world", Types.StringType.get())),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[3].name()}, Literals.of(2000, Types.IntegerType.get())),
            TableChange.updateColumnDefaultValue(
                new String[] {columns[4].name()}, FunctionExpression.of("current_timestamp")));

    table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));

    Assertions.assertEquals(
        Literals.of("1.234", Types.FloatType.get()), table.columns()[0].defaultValue());
    Assertions.assertEquals(
        Literals.of("hello", Types.VarCharType.of(255)), table.columns()[1].defaultValue());
    Assertions.assertEquals(
        Literals.of("world", Types.StringType.get()), table.columns()[2].defaultValue());
    Assertions.assertEquals(
        Literals.of(2000, Types.IntegerType.get()), table.columns()[3].defaultValue());
    Assertions.assertEquals(
        FunctionExpression.of("current_timestamp"), table.columns()[4].defaultValue());
  }

  @Test
  void testColumnDefaultValueConverter() {
    // test convert from MySQL to Gravitino
    String tableName = GravitinoITUtils.genRandomName("test_default_value");
    String fullTableName = String.format("%s.%s.%s", TEST_DB_NAME, schemaName, tableName);
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
            + "    date_col_5 date default '2012-12-31',\n"
            + "    decimal_6_2_col_1 decimal(6, 2) default 1.2,\n"
            + "    timestamp_col_1 timestamp default '2012-12-31 11:30:45',\n"
            + "    time_col_1 time default '11:30:45'\n"
            + ");";
    System.out.println(sql);
    postgreSqlService.executeQuery(sql);
    Table loadedTable =
        catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));

    for (Column column : loadedTable.columns()) {
      switch (column.name()) {
        case "int_col_1":
          Assertions.assertEquals(Literals.integerLiteral(431), column.defaultValue());
          break;
        case "int_col_2":
          Assertions.assertEquals(
              UnparsedExpression.of("floor((random() * (100)::double precision))"),
              column.defaultValue());
          break;
        case "int_col_3":
          Assertions.assertEquals(Literals.integerLiteral(3), column.defaultValue());
          break;
        case "double_col_1":
          Assertions.assertEquals(Literals.doubleLiteral(123.45), column.defaultValue());
          break;
        case "varchar20_col_1":
          Assertions.assertEquals(Literals.varcharLiteral(20, "10"), column.defaultValue());
          break;
        case "varchar100_col_1":
          Assertions.assertEquals(
              Literals.varcharLiteral(100, "CURRENT_TIMESTAMP"), column.defaultValue());
          break;
        case "varchar200_col_1":
          Assertions.assertEquals(Literals.varcharLiteral(200, "curdate()"), column.defaultValue());
          break;
        case "varchar200_col_2":
          Assertions.assertEquals(
              Literals.varcharLiteral(200, "CURRENT_DATE"), column.defaultValue());
          break;
        case "varchar200_col_3":
          Assertions.assertEquals(
              Literals.varcharLiteral(200, "CURRENT_TIMESTAMP"), column.defaultValue());
          break;
        case "date_col_1":
          Assertions.assertEquals(UnparsedExpression.of("CURRENT_DATE"), column.defaultValue());
          break;
        case "date_col_2":
          Assertions.assertEquals(Literals.NULL, column.defaultValue());
          break;
        case "date_col_3":
          Assertions.assertEquals(
              UnparsedExpression.of("(CURRENT_DATE + '1 year'::interval)"), column.defaultValue());
          break;
        case "date_col_4":
          Assertions.assertEquals(UnparsedExpression.of("CURRENT_DATE"), column.defaultValue());
          break;
        case "date_col_5":
          Assertions.assertEquals(Literals.dateLiteral("2012-12-31"), column.defaultValue());
          break;
        case "timestamp_col_1":
          Assertions.assertEquals(
              Literals.timestampLiteral("2012-12-31T11:30:45"), column.defaultValue());
          break;
        case "time_col_1":
          Assertions.assertEquals(Literals.timeLiteral("11:30:45"), column.defaultValue());
          break;
        case "decimal_6_2_col_1":
          Assertions.assertEquals(
              Literals.decimalLiteral(Decimal.of("1.2", 6, 2)), column.defaultValue());
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
  void testPGIllegalTableName() {
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

    String invalidInput = StringUtils.repeat("a", 64);
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

    String invalidInput2 = RandomNameUtils.genRandomName("$test_db");
    Column t5_col = Column.of(invalidInput2, Types.LongType.get(), "id", false, false, null);
    Index[] t5_indexes = {Indexes.unique("u5_key", new String[][] {{invalidInput2}})};
    Column[] columns5 = new Column[] {t5_col};
    NameIdentifier tableIdentifier5 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, invalidInput2);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier5,
              columns5,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t5_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier5);
        });
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
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, "tablename");
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
    ITUtils.assertionsTableInfo(
        "tablename",
        "low case table name",
        Arrays.asList(newColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        createdTable);
    Table table = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        "tablename",
        "low case table name",
        Arrays.asList(newColumns),
        properties,
        indexes,
        Transforms.EMPTY_TRANSFORM,
        table);

    // Test create table with same name but different case
    NameIdentifier tableIdentifier2 = NameIdentifier.of(schemaName, "TABLENAME");

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
      catalog.asSchemas().createSchema(dbs[i], dbs[i], Maps.newHashMap());
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
              NameIdentifier.of(dbs[i], tables[i]),
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
      NameIdentifier[] tableNames = catalog.asTableCatalog().listTables(Namespace.of(dbs[i]));
      Assertions.assertEquals(1, tableNames.length);
      Assertions.assertEquals(tables[i], tableNames[0].name());
    }
  }

  @Test
  void testCreateSameTableInDifferentSchema() {
    String schemaPrefix = GravitinoITUtils.genRandomName("postgresql_it_schema");
    String schemaName1 = schemaPrefix + "1a";
    String schemaName2 = schemaPrefix + "_a";
    String schemaName3 = schemaPrefix + "__";

    String[] dbs = {schemaName1, schemaName2, schemaName3};
    for (int i = 0; i < dbs.length; i++) {
      catalog.asSchemas().createSchema(dbs[i], dbs[i], Maps.newHashMap());
    }

    String tableName1 = "table1";
    String tableName2 = "table2";
    String tableName3 = "table3";
    Column col1 = Column.of("col_1", Types.LongType.get(), "id", false, false, null);
    Column col2 = Column.of("col_2", Types.IntegerType.get(), "yes", false, false, null);
    Column col3 = Column.of("col_3", Types.DateType.get(), "comment", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "code", false, false, null);
    Column col5 = Column.of("col_5", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4, col5};

    String[] tables = {tableName1, tableName2, tableName3};

    for (int i = 0; i < dbs.length; i++) {
      for (int j = 0; j < tables.length; j++) {
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(dbs[i], tables[j]),
                newColumns,
                dbs[i] + "." + tables[j],
                Maps.newHashMap(),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0],
                new Index[0]);
      }
    }

    // list table in schema
    for (int i = 0; i < dbs.length; i++) {
      NameIdentifier[] tableNames = catalog.asTableCatalog().listTables(Namespace.of(dbs[i]));
      Assertions.assertEquals(3, tableNames.length);
      String[] realNames =
          Arrays.stream(tableNames).map(NameIdentifier::name).toArray(String[]::new);
      Assertions.assertArrayEquals(tables, realNames);

      final int idx = i;
      for (String n : realNames) {
        Table t =
            Assertions.assertDoesNotThrow(
                () -> catalog.asTableCatalog().loadTable(NameIdentifier.of(dbs[idx], n)));
        Assertions.assertEquals(n, t.name());

        // Test the table1 is the `1a`.`table1` not `_a`.`table1` or `__`.`table1`
        Assertions.assertEquals(dbs[idx] + "." + n, t.comment());
      }
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
  }

  @Test
  void testUnparsedTypeConverter() {
    String tableName = GravitinoITUtils.genRandomName("test_unparsed_type");
    postgreSqlService.executeQuery(
        String.format("CREATE TABLE %s.%s (bit_col bit);", schemaName, tableName));
    Table loadedTable =
        catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertEquals(Types.ExternalType.of("bit"), loadedTable.columns()[0].dataType());
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
            Index.IndexType.UNIQUE_KEY, "u1_key", new String[][] {{"col_2"}, {"col_3"}}),
        TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "pk1_key", new String[][] {{"col_1"}}));

    Table table = tableCatalog.loadTable(NameIdentifier.of(schemaName, tableName));
    Index[] indexes =
        new Index[] {
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.primary("pk1_key", new String[][] {{"col_1"}})
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
          Indexes.primary("pk1_key", new String[][] {{"col_1"}}),
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
    Column col2 = Column.of("col_2", Types.DateType.get(), "comment", false, false, null);
    Column col3 = Column.of("col_3", Types.VarCharType.of(255), "code", false, false, null);
    Column col4 = Column.of("col_4", Types.VarCharType.of(255), "config", false, false, null);
    Column[] newColumns = new Column[] {col1, col2, col3, col4};
    String tableName = "auto_increment_table";

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
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        Indexes.EMPTY_INDEXES,
        Transforms.EMPTY_TRANSFORM,
        table);

    // Test drop auto increment column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_5"}, false));
    table = tableCatalog.loadTable(tableIdentifier);
    col5 = Column.of("col_5", Types.LongType.get(), "id", false, false, null);
    newColumns = new Column[] {col1, col2, col3, col4, col5};
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(newColumns),
        properties,
        Indexes.EMPTY_INDEXES,
        Transforms.EMPTY_TRANSFORM,
        table);

    // Test add auto increment column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.updateColumnAutoIncrement(new String[] {"col_5"}, true));
    table = tableCatalog.loadTable(tableIdentifier);
    col5 = Column.of("col_5", Types.LongType.get(), "id", false, true, null);
    newColumns = new Column[] {col1, col2, col3, col4, col5};
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
  void testAddColumnDefaultValue() {
    Column col1 = Column.of("col_1", Types.LongType.get(), "uid", true, false, null);
    Column col2 = Column.of("col_2", Types.DateType.get(), "comment", true, false, null);
    Column[] newColumns = new Column[] {col1, col2};
    String tableName = "default_value_table";

    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, newColumns[0].defaultValue());

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

    Column col3 =
        Column.of("col_3", Types.LongType.get(), "id", false, false, Literals.longLiteral(1000L));
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {col3.name()},
            col3.dataType(),
            col3.comment(),
            TableChange.ColumnPosition.defaultPos(),
            col3.nullable(),
            col3.autoIncrement(),
            col3.defaultValue()));

    Table table = tableCatalog.loadTable(tableIdentifier);

    newColumns = new Column[] {col1, col2, col3};
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
  void testGetPGDriver() {
    Assertions.assertDoesNotThrow(
        () -> DriverManager.getDriver("jdbc:postgresql://dummy_address:12345/"));
    Assertions.assertThrows(
        Exception.class,
        () -> DriverManager.getDriver("jdbc:postgresql://dummy_address:dummy_port/"));
  }

  @Test
  void testTimeTypePrecision() {
    String tableName = GravitinoITUtils.genRandomName("test_time_precision");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    columns =
        ArrayUtils.addAll(
            columns,
            // timestamp without time zone
            Column.of("timestamp_col", Types.TimestampType.withoutTimeZone()),
            Column.of("timestamp_col_0", Types.TimestampType.withoutTimeZone(0)),
            Column.of("timestamp_col_1", Types.TimestampType.withoutTimeZone(1)),
            Column.of("timestamp_col_3", Types.TimestampType.withoutTimeZone(3)),
            Column.of("timestamp_col_6", Types.TimestampType.withoutTimeZone(6)),
            // timestamp with time zone
            Column.of("timestamptz_col", Types.TimestampType.withTimeZone()),
            Column.of("timestamptz_col_0", Types.TimestampType.withTimeZone(0)),
            Column.of("timestamptz_col_1", Types.TimestampType.withTimeZone(1)),
            Column.of("timestamptz_col_3", Types.TimestampType.withTimeZone(3)),
            Column.of("timestamptz_col_6", Types.TimestampType.withTimeZone(6)),
            // time without time zone
            Column.of("time_col", Types.TimeType.get()),
            Column.of("time_col_0", Types.TimeType.of(0)),
            Column.of("time_col_1", Types.TimeType.of(1)),
            Column.of("time_col_3", Types.TimeType.of(3)),
            Column.of("time_col_6", Types.TimeType.of(6)));

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        null);

    Table loadTable = tableCatalog.loadTable(tableIdentifier);

    // Verify timestamp type precisions
    Column[] timestampColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("timestamp_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(5, timestampColumns.length);
    for (Column column : timestampColumns) {
      switch (column.name()) {
        case "timestamp_col":
        case "timestamp_col_6":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(6), column.dataType());
          break;
        case "timestamp_col_0":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(0), column.dataType());
          break;
        case "timestamp_col_1":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(1), column.dataType());
          break;
        case "timestamp_col_3":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(3), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected timestamp column: " + column.name());
      }
    }

    // Verify timestamptz type precisions
    Column[] timestamptzColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("timestamptz_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(5, timestamptzColumns.length);
    for (Column column : timestamptzColumns) {
      switch (column.name()) {
        case "timestamptz_col":
        case "timestamptz_col_6":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(6), column.dataType());
          break;
        case "timestamptz_col_0":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(0), column.dataType());
          break;
        case "timestamptz_col_1":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(1), column.dataType());
          break;
        case "timestamptz_col_3":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(3), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected timestamptz column: " + column.name());
      }
    }

    // Verify time type precisions
    Column[] timeColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("time_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(5, timeColumns.length);
    for (Column column : timeColumns) {
      switch (column.name()) {
        case "time_col":
        case "time_col_6":
          Assertions.assertEquals(Types.TimeType.of(6), column.dataType());
          break;
        case "time_col_0":
          Assertions.assertEquals(Types.TimeType.of(0), column.dataType());
          break;
        case "time_col_1":
          Assertions.assertEquals(Types.TimeType.of(1), column.dataType());
          break;
        case "time_col_3":
          Assertions.assertEquals(Types.TimeType.of(3), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected time column: " + column.name());
      }
    }
  }
}
