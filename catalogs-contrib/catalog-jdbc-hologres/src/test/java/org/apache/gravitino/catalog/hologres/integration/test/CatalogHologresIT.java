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
package org.apache.gravitino.catalog.hologres.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.hologres.integration.test.service.HologresService;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for the Hologres JDBC catalog.
 *
 * <p>Since Hologres is a cloud service hosted on Alibaba Cloud, these tests require a real Hologres
 * instance. Set the following environment variables to enable these tests:
 *
 * <ul>
 *   <li>{@code GRAVITINO_HOLOGRES_JDBC_URL} - Hologres JDBC URL (e.g.
 *       jdbc:postgresql://host:port/db)
 *   <li>{@code GRAVITINO_HOLOGRES_USERNAME} - Hologres username
 *   <li>{@code GRAVITINO_HOLOGRES_PASSWORD} - Hologres password
 * </ul>
 */
@EnabledIf(value = "hologresIsConfigured", disabledReason = "Hologres is not configured")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogHologresIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogHologresIT.class);
  private static final String PROVIDER = "jdbc-hologres";

  public static final String HOLOGRES_JDBC_URL = System.getenv("GRAVITINO_HOLOGRES_JDBC_URL");
  public static final String HOLOGRES_USERNAME = System.getenv("GRAVITINO_HOLOGRES_USERNAME");
  public static final String HOLOGRES_PASSWORD = System.getenv("GRAVITINO_HOLOGRES_PASSWORD");

  private final String metalakeName = GravitinoITUtils.genRandomName("hologres_it_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("hologres_it_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("hologres_it_schema");
  private final String tableName = GravitinoITUtils.genRandomName("hologres_it_table");
  private final String tableComment = "table_comment";

  private final String HOLOGRES_COL_NAME1 = "hologres_col_name1";
  private final String HOLOGRES_COL_NAME2 = "hologres_col_name2";
  private final String HOLOGRES_COL_NAME3 = "hologres_col_name3";

  private GravitinoMetalake metalake;
  protected Catalog catalog;
  private HologresService hologresService;

  protected static boolean hologresIsConfigured() {
    return StringUtils.isNotBlank(HOLOGRES_JDBC_URL)
        && StringUtils.isNotBlank(HOLOGRES_USERNAME)
        && StringUtils.isNotBlank(HOLOGRES_PASSWORD);
  }

  @BeforeAll
  public void startup() throws IOException {
    hologresService = new HologresService(HOLOGRES_JDBC_URL, HOLOGRES_USERNAME, HOLOGRES_PASSWORD);
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
      hologresService.close();
    } catch (Exception e) {
      LOG.error("Failed to stop.", e);
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

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), HOLOGRES_JDBC_URL);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), HOLOGRES_USERNAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), HOLOGRES_PASSWORD);

    // Extract database name from JDBC URL for jdbc-database config
    String database = extractDatabaseFromUrl(HOLOGRES_JDBC_URL);
    if (database != null) {
      catalogProperties.put(JdbcConfig.JDBC_DATABASE.getKey(), database);
    }

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "comment", catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private String extractDatabaseFromUrl(String url) {
    // JDBC URL format: jdbc:postgresql://host:port/database
    int lastSlash = url.lastIndexOf('/');
    int questionMark = url.indexOf('?', lastSlash);
    if (lastSlash >= 0) {
      if (questionMark >= 0) {
        return url.substring(lastSlash + 1, questionMark);
      }
      return url.substring(lastSlash + 1);
    }
    return null;
  }

  private void createSchema() {
    Map<String, String> prop = Maps.newHashMap();
    Schema createdSchema = catalog.asSchemas().createSchema(schemaName, null, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
  }

  private Column[] createColumns() {
    Column col1 = Column.of(HOLOGRES_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(HOLOGRES_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HOLOGRES_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  private Map<String, String> createProperties() {
    return Maps.newHashMap();
  }

  @Test
  void testOperationHologresSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    Namespace namespace = Namespace.of(metalakeName, catalogName);

    // List schema check
    String[] nameIdentifiers = schemas.listSchemas();
    Set<String> schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertTrue(schemaNames.contains(schemaName));

    NameIdentifier[] hologresSchemas = hologresService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(hologresSchemas).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // Create schema check
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(testSchemaName, null, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas();
    schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    hologresSchemas = hologresService.listSchemas(namespace);
    schemaNames =
        Arrays.stream(hologresSchemas).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(testSchemaName, null, emptyMap));

    // Drop schema check
    schemas.dropSchema(testSchemaName, false);
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(testSchemaName));
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> hologresService.loadSchema(schemaIdent));

    nameIdentifiers = schemas.listSchemas();
    schemaNames = Sets.newHashSet(nameIdentifiers);
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    Assertions.assertFalse(schemas.dropSchema("no_exits", false));
  }

  @Test
  void testCreateAndLoadHologresTable() {
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;
    SortOrder[] sortOrders = new SortOrder[0];
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
    Map<String, String> properties = createProperties();

    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier, columns, tableComment, properties, partitioning, distribution, sortOrders);

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(tableComment, loadTable.comment());
    Assertions.assertEquals(columns.length, loadTable.columns().length);
    for (int i = 0; i < columns.length; i++) {
      ITUtils.assertColumn(columns[i], loadTable.columns()[i]);
    }
  }

  @Test
  void testColumnTypeConverter() {
    Column[] columns =
        new Column[] {
          Column.of("bool_col", Types.BooleanType.get(), "bool column"),
          Column.of("int2_col", Types.ShortType.get(), "int2 column"),
          Column.of("int4_col", Types.IntegerType.get(), "int4 column"),
          Column.of("int8_col", Types.LongType.get(), "int8 column"),
          Column.of("float4_col", Types.FloatType.get(), "float4 column"),
          Column.of("float8_col", Types.DoubleType.get(), "float8 column"),
          Column.of("text_col", Types.StringType.get(), "text column"),
          Column.of("varchar_col", Types.VarCharType.of(100), "varchar column"),
          Column.of("date_col", Types.DateType.get(), "date column"),
          Column.of("timestamp_col", Types.TimestampType.withoutTimeZone(), "timestamp column"),
          Column.of("timestamptz_col", Types.TimestampType.withTimeZone(), "timestamptz column"),
          Column.of("numeric_col", Types.DecimalType.of(10, 2), "numeric column"),
          Column.of("bytea_col", Types.BinaryType.get(), "bytea column"),
        };

    String testTableName = GravitinoITUtils.genRandomName("type_converter_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, testTableName);
    TableCatalog tableCatalog = catalog.asTableCatalog();

    tableCatalog.createTable(
        tableIdentifier,
        columns,
        "type converter test",
        ImmutableMap.of(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);

    Table loadedTable = tableCatalog.loadTable(tableIdentifier);

    for (Column column : loadedTable.columns()) {
      switch (column.name()) {
        case "bool_col":
          Assertions.assertEquals(Types.BooleanType.get(), column.dataType());
          break;
        case "int2_col":
          Assertions.assertEquals(Types.ShortType.get(), column.dataType());
          break;
        case "int4_col":
          Assertions.assertEquals(Types.IntegerType.get(), column.dataType());
          break;
        case "int8_col":
          Assertions.assertEquals(Types.LongType.get(), column.dataType());
          break;
        case "float4_col":
          Assertions.assertEquals(Types.FloatType.get(), column.dataType());
          break;
        case "float8_col":
          Assertions.assertEquals(Types.DoubleType.get(), column.dataType());
          break;
        case "text_col":
          Assertions.assertEquals(Types.StringType.get(), column.dataType());
          break;
        case "varchar_col":
          Assertions.assertEquals(Types.VarCharType.of(100), column.dataType());
          break;
        case "date_col":
          Assertions.assertEquals(Types.DateType.get(), column.dataType());
          break;
        case "timestamp_col":
          Assertions.assertTrue(column.dataType() instanceof Types.TimestampType);
          Assertions.assertFalse(((Types.TimestampType) column.dataType()).hasTimeZone());
          break;
        case "timestamptz_col":
          Assertions.assertTrue(column.dataType() instanceof Types.TimestampType);
          Assertions.assertTrue(((Types.TimestampType) column.dataType()).hasTimeZone());
          break;
        case "numeric_col":
          Assertions.assertEquals(Types.DecimalType.of(10, 2), column.dataType());
          break;
        case "bytea_col":
          Assertions.assertEquals(Types.BinaryType.get(), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected column name: " + column.name());
      }
    }
  }

  @Test
  void testAlterAndDropHologresTable() {
    Column[] columns = createColumns();
    String alterTableName = GravitinoITUtils.genRandomName("alter_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, alterTableName);

    catalog
        .asTableCatalog()
        .createTable(tableIdentifier, columns, tableComment, createProperties());

    // Test rename table
    String newTableName = GravitinoITUtils.genRandomName("renamed_table");
    catalog.asTableCatalog().alterTable(tableIdentifier, TableChange.rename(newTableName));

    NameIdentifier newTableIdentifier = NameIdentifier.of(schemaName, newTableName);
    Table table = catalog.asTableCatalog().loadTable(newTableIdentifier);
    Assertions.assertEquals(newTableName, table.name());

    // Test update table comment
    catalog
        .asTableCatalog()
        .alterTable(newTableIdentifier, TableChange.updateComment(tableComment + "_new"));
    table = catalog.asTableCatalog().loadTable(newTableIdentifier);
    Assertions.assertTrue(table.comment().contains(tableComment + "_new"));

    // Test add column
    catalog
        .asTableCatalog()
        .alterTable(
            newTableIdentifier,
            TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()));
    table = catalog.asTableCatalog().loadTable(newTableIdentifier);
    Assertions.assertEquals(4, table.columns().length);
    Assertions.assertEquals("col_4", table.columns()[3].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[3].dataType());

    // Test rename column
    catalog
        .asTableCatalog()
        .alterTable(
            newTableIdentifier,
            TableChange.renameColumn(new String[] {HOLOGRES_COL_NAME2}, "col_2_new"));
    table = catalog.asTableCatalog().loadTable(newTableIdentifier);
    Assertions.assertEquals("col_2_new", table.columns()[1].name());

    // Test update column comment
    catalog
        .asTableCatalog()
        .alterTable(
            newTableIdentifier,
            TableChange.updateColumnComment(new String[] {HOLOGRES_COL_NAME1}, "new_comment"));
    table = catalog.asTableCatalog().loadTable(newTableIdentifier);
    Assertions.assertEquals("new_comment", table.columns()[0].comment());

    // Test delete column
    catalog
        .asTableCatalog()
        .alterTable(newTableIdentifier, TableChange.deleteColumn(new String[] {"col_4"}, true));
    table = catalog.asTableCatalog().loadTable(newTableIdentifier);
    Assertions.assertEquals(3, table.columns().length);

    // Test drop table
    Assertions.assertTrue(catalog.asTableCatalog().dropTable(newTableIdentifier));
    Assertions.assertFalse(catalog.asTableCatalog().dropTable(newTableIdentifier));
  }

  @Test
  void testCreateTableWithDistribution() {
    Column col1 = Column.of("id", Types.LongType.get(), "id column", false, false, null);
    Column col2 = Column.of("name", Types.StringType.get(), "name column", true, false, null);
    Column col3 = Column.of("value", Types.IntegerType.get(), "value column", true, false, null);
    Column[] columns = new Column[] {col1, col2, col3};

    String distTableName = GravitinoITUtils.genRandomName("dist_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, distTableName);

    Distribution distribution = Distributions.hash(0, NamedReference.field("id"));

    Index[] indexes = new Index[] {Indexes.primary("pk", new String[][] {{"id"}})};

    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        "table with distribution",
        ImmutableMap.of(),
        Transforms.EMPTY_TRANSFORM,
        distribution,
        new SortOrder[0],
        indexes);

    Table loadedTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(distTableName, loadedTable.name());

    // Verify distribution
    Distribution loadedDist = loadedTable.distribution();
    Assertions.assertNotNull(loadedDist);
    Assertions.assertNotEquals(Distributions.NONE, loadedDist);
  }

  @Test
  void testCreateTableWithProperties() {
    Column col1 = Column.of("id", Types.LongType.get(), "id column", false, false, null);
    Column col2 = Column.of("name", Types.StringType.get(), "name column", true, false, null);
    Column[] columns = new Column[] {col1, col2};

    String propsTableName = GravitinoITUtils.genRandomName("props_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, propsTableName);

    Map<String, String> properties = Maps.newHashMap();
    properties.put("orientation", "column");
    properties.put("time_to_live_in_seconds", "3600");

    Index[] indexes = new Index[] {Indexes.primary("pk", new String[][] {{"id"}})};

    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        "table with properties",
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        indexes);

    Table loadedTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(propsTableName, loadedTable.name());

    Map<String, String> loadedProps = loadedTable.properties();
    Assertions.assertEquals("column", loadedProps.get("orientation"));
    Assertions.assertEquals("3600", loadedProps.get("time_to_live_in_seconds"));
  }

  @Test
  void testCreateTableWithPrimaryKey() {
    Column col1 = Column.of("id", Types.LongType.get(), "id column", false, false, null);
    Column col2 = Column.of("name", Types.StringType.get(), "name column", true, false, null);
    Column col3 = Column.of("value", Types.IntegerType.get(), "value column", true, false, null);
    Column[] columns = new Column[] {col1, col2, col3};

    String pkTableName = GravitinoITUtils.genRandomName("pk_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, pkTableName);

    Index[] indexes =
        new Index[] {Indexes.primary("pk_id_name", new String[][] {{"id"}, {"name"}})};

    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        "table with primary key",
        ImmutableMap.of(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        indexes);

    Table loadedTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(pkTableName, loadedTable.name());

    // Verify primary key index exists
    Index[] loadedIndexes = loadedTable.index();
    Assertions.assertTrue(loadedIndexes.length > 0);

    boolean hasPrimaryKey = false;
    for (Index index : loadedIndexes) {
      if (index.type() == Index.IndexType.PRIMARY_KEY) {
        hasPrimaryKey = true;
        // Verify the primary key columns
        Set<String> pkColumns =
            Arrays.stream(index.fieldNames()).flatMap(Arrays::stream).collect(Collectors.toSet());
        Assertions.assertTrue(pkColumns.contains("id"));
        Assertions.assertTrue(pkColumns.contains("name"));
      }
    }
    Assertions.assertTrue(hasPrimaryKey, "Table should have a primary key");
  }

  @Test
  void testColumnDefaultValue() {
    Column col1 =
        Column.of(
            HOLOGRES_COL_NAME1,
            Types.IntegerType.get(),
            "col_1_comment",
            false,
            false,
            Literals.integerLiteral(42));
    Column col2 =
        Column.of(
            HOLOGRES_COL_NAME2,
            Types.VarCharType.of(255),
            "col_2_comment",
            true,
            false,
            Literals.NULL);
    Column col3 =
        Column.of(
            HOLOGRES_COL_NAME3,
            Types.BooleanType.get(),
            "col_3_comment",
            false,
            false,
            Literals.booleanLiteral(true));

    Column[] newColumns = new Column[] {col1, col2, col3};

    String defaultValueTableName = GravitinoITUtils.genRandomName("default_value_table");
    NameIdentifier tableIdent = NameIdentifier.of(schemaName, defaultValueTableName);

    Index[] indexes = new Index[] {Indexes.primary("pk", new String[][] {{HOLOGRES_COL_NAME1}})};

    catalog
        .asTableCatalog()
        .createTable(
            tableIdent,
            newColumns,
            null,
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            indexes);

    Table createdTable = catalog.asTableCatalog().loadTable(tableIdent);
    Assertions.assertEquals(Literals.NULL, createdTable.columns()[1].defaultValue());
  }

  @Test
  void testSchemaComment() {
    String testSchemaName = GravitinoITUtils.genRandomName("schema_comment_test");

    // Hologres supports schema comment via COMMENT ON SCHEMA
    Schema schema = catalog.asSchemas().createSchema(testSchemaName, "test schema comment", null);
    Assertions.assertNotNull(schema);

    Schema loadedSchema = catalog.asSchemas().loadSchema(testSchemaName);
    Assertions.assertEquals("test schema comment", loadedSchema.comment());

    // Clean up
    catalog.asSchemas().dropSchema(testSchemaName, true);
  }

  @Test
  void testDropHologresSchema() {
    String testSchemaName = GravitinoITUtils.genRandomName("hologres_drop_schema").toLowerCase();
    String testTableName = GravitinoITUtils.genRandomName("hologres_drop_table").toLowerCase();

    catalog
        .asSchemas()
        .createSchema(testSchemaName, null, ImmutableMap.<String, String>builder().build());

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(testSchemaName, testTableName),
            createColumns(),
            "Created by Gravitino client",
            ImmutableMap.<String, String>builder().build());

    // Try to drop a schema with cascade = true
    catalog.asSchemas().dropSchema(testSchemaName, true);

    // Check schema has been dropped
    SupportsSchemas schemas = catalog.asSchemas();
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(testSchemaName));
  }
}
