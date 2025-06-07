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
package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import static org.apache.gravitino.catalog.lakehouse.paimon.GravitinoPaimonTable.PAIMON_PRIMARY_KEY_INDEX_NAME;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;
import static org.apache.gravitino.rel.indexes.Indexes.primary;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonBackendCatalogWrapper;
import org.apache.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

public abstract class CatalogPaimonBaseIT extends BaseIT {

  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static final TestDatabaseName TEST_DB_NAME =
      TestDatabaseName.PG_TEST_PAIMON_CATALOG_MULTIPLE_JDBC_LOAD;
  protected static MySQLContainer mySQLContainer;
  protected String WAREHOUSE;
  protected String TYPE;
  protected String URI;
  protected String jdbcUser;
  protected String jdbcPassword;
  protected Catalog catalog;
  protected org.apache.paimon.catalog.Catalog paimonCatalog;
  protected SparkSession spark;
  protected String metalakeName = GravitinoITUtils.genRandomName("paimon_it_metalake");
  protected String catalogName = GravitinoITUtils.genRandomName("paimon_it_catalog");
  protected String schemaName = GravitinoITUtils.genRandomName("paimon_it_schema");
  protected static final String schema_comment = "schema_comment";

  protected static final String provider = "lakehouse-paimon";
  private static final String catalog_comment = "catalog_comment";
  private static final String table_comment = "table_comment";
  private static final String PAIMON_COL_NAME1 = "paimon_col_name1";
  private static final String PAIMON_COL_NAME2 = "paimon_col_name2";
  private static final String PAIMON_COL_NAME3 = "paimon_col_name3";
  private static final String PAIMON_COL_NAME4 = "paimon_col_name4";
  private static final String PAIMON_COL_NAME5 = "paimon_col_name5";
  private static final String alertTableName = "alert_table_name";
  private static String INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE = "INSERT INTO paimon.%s VALUES %s";
  private static final String SELECT_ALL_TEMPLATE = "SELECT * FROM paimon.%s";
  private static final String DEFAULT_DB = "default";
  protected GravitinoMetalake metalake;
  private Map<String, String> catalogProperties;

  @BeforeAll
  public void startup() {
    startNecessaryContainers();
    catalogProperties = initPaimonCatalogProperties();
    createMetalake();
    createCatalog();
    createSchema();
    initSparkEnv();
  }

  protected void startNecessaryContainers() {
    containerSuite.startHiveContainer();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName);
    client.disableMetalake(metalakeName);
    client.dropMetalake(metalakeName);
    if (spark != null) {
      spark.close();
    }
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  protected abstract Map<String, String> initPaimonCatalogProperties();

  @Test
  void testPaimonSchemaOperations() throws DatabaseNotExistException {
    SupportsSchemas schemas = catalog.asSchemas();

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    schemaProperties.put("key1", "val1");
    schemaProperties.put("key2", "val2");
    schemas.createSchema(schemaIdent.name(), schema_comment, schemaProperties);

    Set<String> schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(testSchemaName));
    List<String> paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertTrue(paimonDatabaseNames.contains(testSchemaName));

    // load schema check.
    Schema schema = schemas.loadSchema(schemaIdent.name());
    Assertions.assertEquals(testSchemaName, schema.name());

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent.name(), schema_comment, emptyMap));

    // alter schema check.
    // alter schema operation is unsupported.
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> schemas.alterSchema(schemaIdent.name(), SchemaChange.setProperty("k1", "v1")));

    // drop schema check.
    schemas.dropSchema(schemaIdent.name(), false);
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));
    Assertions.assertThrows(
        DatabaseNotExistException.class,
        () -> {
          paimonCatalog.loadDatabaseProperties(schemaIdent.name());
        });

    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    Assertions.assertFalse(schemas.dropSchema(schemaIdent.name(), false));
    Assertions.assertFalse(schemas.dropSchema("no-exits", false));

    // list schema check.
    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertFalse(paimonDatabaseNames.contains(testSchemaName));
  }

  @Test
  void testCreateTableWithNullComment() {
    String tableName = GravitinoITUtils.genRandomName("paimon_table_with_null_comment");
    Column[] columns = createColumns();
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(tableIdentifier, columns, null, null, null, null, null);
    Assertions.assertNull(createdTable.comment());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertNull(loadTable.comment());
  }

  @Test
  void testCreateAndLoadPaimonTable()
      throws org.apache.paimon.catalog.Catalog.TableNotExistException {
    String tableName = GravitinoITUtils.genRandomName("create_and_load_paimon_table");

    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
    SortOrder[] sortOrders = SortOrders.NONE;
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

    // catalog load check
    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, tableName));
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertTrue(table.comment().isPresent());
    Assertions.assertEquals(table_comment, table.comment().get());
    resultProp = table.options();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }

    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;

    TableSchema schema = fileStoreTable.schema();
    Assertions.assertEquals(schema.fields().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(columns[i].name(), schema.fieldNames().get(i));
    }
    Assertions.assertEquals(partitioning.length, fileStoreTable.partitionKeys().size());

    Assertions.assertThrows(
        TableAlreadyExistsException.class,
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier,
                    columns,
                    table_comment,
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    distribution,
                    sortOrders));
  }

  @Test
  void testCreateAndLoadPaimonPartitionedTable()
      throws org.apache.paimon.catalog.Catalog.TableNotExistException {
    String tableName = GravitinoITUtils.genRandomName("create_and_load_paimon_partitioned_table");

    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    Transform[] partitioning =
        new Transform[] {identity(PAIMON_COL_NAME1), identity(PAIMON_COL_NAME3)};
    String[] partitionKeys = new String[] {PAIMON_COL_NAME1, PAIMON_COL_NAME3};
    SortOrder[] sortOrders = SortOrders.NONE;
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
    Assertions.assertEquals(createdTable.comment(), table_comment);
    Assertions.assertArrayEquals(partitioning, createdTable.partitioning());
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
    Assertions.assertArrayEquals(partitioning, loadTable.partitioning());
    String[] loadedPartitionKeys =
        Arrays.stream(loadTable.partitioning())
            .map(
                transform -> {
                  NamedReference[] references = transform.references();
                  Assertions.assertTrue(
                      references.length == 1
                          && references[0] instanceof NamedReference.FieldReference);
                  NamedReference.FieldReference fieldReference =
                      (NamedReference.FieldReference) references[0];
                  return fieldReference.fieldName()[0];
                })
            .toArray(String[]::new);
    Assertions.assertArrayEquals(partitionKeys, loadedPartitionKeys);
    Assertions.assertEquals(loadTable.columns().length, columns.length);
    for (int i = 0; i < columns.length; i++) {
      assertColumn(columns[i], loadTable.columns()[i]);
    }

    // catalog load check
    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, tableName));
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertTrue(table.comment().isPresent());
    Assertions.assertEquals(table_comment, table.comment().get());
    resultProp = table.options();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertArrayEquals(partitionKeys, table.partitionKeys().toArray(new String[0]));
    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;

    TableSchema schema = fileStoreTable.schema();
    Assertions.assertEquals(schema.fields().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(columns[i].name(), schema.fieldNames().get(i));
    }
    Assertions.assertArrayEquals(partitionKeys, schema.partitionKeys().toArray(new String[0]));
  }

  @Test
  void testCreateAndLoadPaimonPrimaryKeyTable()
      throws org.apache.paimon.catalog.Catalog.TableNotExistException {
    String tableName = GravitinoITUtils.genRandomName("create_and_load_paimon_primary_key_table");

    // Create table from Gravitino API
    Column[] columns = createColumns();
    ArrayList<Column> newColumns = new ArrayList<>(Arrays.asList(columns));
    Column col5 =
        Column.of(
            PAIMON_COL_NAME5,
            Types.StringType.get(),
            "col_5_comment",
            false,
            false,
            Column.DEFAULT_VALUE_NOT_SET);
    newColumns.add(col5);
    columns = newColumns.toArray(new Column[0]);

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    Transform[] partitioning =
        new Transform[] {identity(PAIMON_COL_NAME1), identity(PAIMON_COL_NAME3)};
    String[] partitionKeys = new String[] {PAIMON_COL_NAME1, PAIMON_COL_NAME3};

    String[] primaryKeys = new String[] {PAIMON_COL_NAME5};
    Index[] indexes =
        Collections.singletonList(
                primary(
                    PAIMON_PRIMARY_KEY_INDEX_NAME,
                    new String[][] {new String[] {PAIMON_COL_NAME5}}))
            .toArray(new Index[0]);

    Map<String, String> properties = createProperties();

    SortOrder[] sortOrders = SortOrders.NONE;
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            partitioning,
            distribution,
            sortOrders,
            indexes);
    Assertions.assertEquals(createdTable.name(), tableName);
    Assertions.assertEquals(createdTable.comment(), table_comment);
    Assertions.assertArrayEquals(partitioning, createdTable.partitioning());
    Assertions.assertEquals(indexes.length, createdTable.index().length);
    for (int i = 0; i < indexes.length; i++) {
      Assertions.assertEquals(indexes[i].name(), createdTable.index()[i].name());
      Assertions.assertEquals(indexes[i].type(), createdTable.index()[i].type());
      Assertions.assertArrayEquals(indexes[i].fieldNames(), createdTable.index()[i].fieldNames());
    }
    Assertions.assertEquals(createdTable.columns().length, columns.length);
    for (int i = 0; i < columns.length; i++) {
      assertColumn(columns[i], createdTable.columns()[i]);
    }

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(table_comment, loadTable.comment());
    Assertions.assertArrayEquals(partitioning, loadTable.partitioning());
    String[] loadedPartitionKeys =
        Arrays.stream(loadTable.partitioning())
            .map(
                transform -> {
                  NamedReference[] references = transform.references();
                  Assertions.assertTrue(
                      references.length == 1
                          && references[0] instanceof NamedReference.FieldReference);
                  NamedReference.FieldReference fieldReference =
                      (NamedReference.FieldReference) references[0];
                  return fieldReference.fieldName()[0];
                })
            .toArray(String[]::new);
    Assertions.assertArrayEquals(partitionKeys, loadedPartitionKeys);
    Assertions.assertEquals(indexes.length, loadTable.index().length);
    for (int i = 0; i < indexes.length; i++) {
      Assertions.assertEquals(indexes[i].name(), loadTable.index()[i].name());
      Assertions.assertEquals(indexes[i].type(), loadTable.index()[i].type());
      Assertions.assertArrayEquals(indexes[i].fieldNames(), loadTable.index()[i].fieldNames());
    }
    Assertions.assertEquals(loadTable.columns().length, columns.length);
    for (int i = 0; i < columns.length; i++) {
      assertColumn(columns[i], loadTable.columns()[i]);
    }

    // catalog load check
    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, tableName));
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertTrue(table.comment().isPresent());
    Assertions.assertEquals(table_comment, table.comment().get());
    Assertions.assertArrayEquals(partitionKeys, table.partitionKeys().toArray(new String[0]));
    Assertions.assertArrayEquals(primaryKeys, table.primaryKeys().toArray(new String[0]));
    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;

    TableSchema schema = fileStoreTable.schema();
    Assertions.assertEquals(schema.fields().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(columns[i].name(), schema.fieldNames().get(i));
    }
    Assertions.assertArrayEquals(partitionKeys, schema.partitionKeys().toArray(new String[0]));
    Assertions.assertArrayEquals(primaryKeys, schema.primaryKeys().toArray(new String[0]));
  }

  @Test
  void testCreateTableWithTimestampColumn()
      throws org.apache.paimon.catalog.Catalog.TableNotExistException {
    Column col1 = Column.of("paimon_column_1", Types.TimestampType.withTimeZone(), "col_1_comment");
    Column col2 =
        Column.of("paimon_column_2", Types.TimestampType.withoutTimeZone(), "col_2_comment");

    Column[] columns = new Column[] {col1, col2};

    String timestampTableName = "timestamp_table";

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, timestampTableName);

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(tableIdentifier, columns, table_comment, properties);
    Assertions.assertEquals("paimon_column_1", createdTable.columns()[0].name());
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(), createdTable.columns()[0].dataType());
    Assertions.assertEquals("col_1_comment", createdTable.columns()[0].comment());
    Assertions.assertTrue(createdTable.columns()[0].nullable());

    Assertions.assertEquals("paimon_column_2", createdTable.columns()[1].name());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(), createdTable.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", createdTable.columns()[1].comment());
    Assertions.assertTrue(createdTable.columns()[1].nullable());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals("paimon_column_1", loadTable.columns()[0].name());
    Assertions.assertEquals(Types.TimestampType.withTimeZone(6), loadTable.columns()[0].dataType());
    Assertions.assertEquals("col_1_comment", loadTable.columns()[0].comment());
    Assertions.assertTrue(loadTable.columns()[0].nullable());

    Assertions.assertEquals("paimon_column_2", loadTable.columns()[1].name());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(6), loadTable.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", loadTable.columns()[1].comment());
    Assertions.assertTrue(loadTable.columns()[1].nullable());

    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, timestampTableName));
    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;
    TableSchema tableSchema = fileStoreTable.schema();
    Assertions.assertEquals("paimon_column_1", tableSchema.fields().get(0).name());
    Assertions.assertEquals(
        new LocalZonedTimestampType().nullable(), tableSchema.fields().get(0).type());
    Assertions.assertEquals("col_1_comment", tableSchema.fields().get(0).description());

    Assertions.assertEquals("paimon_column_2", tableSchema.fields().get(1).name());
    Assertions.assertEquals(new TimestampType().nullable(), tableSchema.fields().get(1).type());
    Assertions.assertEquals("col_2_comment", tableSchema.fields().get(1).description());
  }

  @Test
  void testListAndDropPaimonTable() throws DatabaseNotExistException {
    Column[] columns = createColumns();

    String tableName1 = "table_1";

    NameIdentifier table1 = NameIdentifier.of(schemaName, tableName1);

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        table1,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);
    NameIdentifier[] nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_1", nameIdentifiers[0].name());

    List<String> tableIdentifiers = paimonCatalog.listTables(schemaName);
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0));

    String tableName2 = "table_2";

    NameIdentifier table2 = NameIdentifier.of(schemaName, tableName2);
    tableCatalog.createTable(
        table2,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);
    nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(2, nameIdentifiers.length);
    Assertions.assertEquals("table_1", nameIdentifiers[0].name());
    Assertions.assertEquals("table_2", nameIdentifiers[1].name());

    tableIdentifiers = paimonCatalog.listTables(schemaName);
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0));
    Assertions.assertEquals("table_2", tableIdentifiers.get(1));

    Assertions.assertDoesNotThrow(() -> tableCatalog.purgeTable(table1));

    nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_2", nameIdentifiers[0].name());

    Assertions.assertDoesNotThrow(() -> tableCatalog.purgeTable(table2));
    Namespace schemaNamespace = Namespace.of(schemaName);
    nameIdentifiers = tableCatalog.listTables(schemaNamespace);
    Assertions.assertEquals(0, nameIdentifiers.length);

    Assertions.assertEquals(0, paimonCatalog.listTables(schemaName).size());
  }

  @Test
  public void testAlterPaimonTable() {
    String tableName = GravitinoITUtils.genRandomName("alter_paimon_table");

    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName), columns, table_comment, createProperties());

    // The RenameTable operation cannot be performed together with other operations.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .asTableCatalog()
                .alterTable(
                    NameIdentifier.of(schemaName, tableName),
                    TableChange.rename(alertTableName),
                    TableChange.updateComment(table_comment + "_new")));

    // rename table.
    catalog
        .asTableCatalog()
        .alterTable(NameIdentifier.of(schemaName, tableName), TableChange.rename(alertTableName));

    // other operations.
    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.setProperty("key2", "val2_new"),
            TableChange.removeProperty("key1"),
            TableChange.addColumn(
                new String[] {"paimon_col_name5_for_add"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {PAIMON_COL_NAME2}, "paimon_col_name2_new"),
            TableChange.updateColumnComment(new String[] {PAIMON_COL_NAME1}, "comment_new"),
            TableChange.updateColumnType(new String[] {PAIMON_COL_NAME1}, Types.StringType.get()),
            TableChange.updateColumnNullability(new String[] {PAIMON_COL_NAME1}, false));

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, alertTableName));
    Assertions.assertEquals(alertTableName, table.name());
    Assertions.assertEquals("val2_new", table.properties().get("key2"));

    Assertions.assertEquals(PAIMON_COL_NAME1, table.columns()[0].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[0].dataType());
    Assertions.assertEquals("comment_new", table.columns()[0].comment());
    Assertions.assertFalse(table.columns()[0].nullable());

    Assertions.assertEquals("paimon_col_name2_new", table.columns()[1].name());
    Assertions.assertEquals(Types.DateType.get(), table.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", table.columns()[1].comment());

    Assertions.assertEquals(PAIMON_COL_NAME3, table.columns()[2].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[2].dataType());
    Assertions.assertEquals("col_3_comment", table.columns()[2].comment());

    Assertions.assertEquals(PAIMON_COL_NAME4, table.columns()[3].name());
    Assertions.assertEquals(columns[3].dataType(), table.columns()[3].dataType());
    Assertions.assertEquals("col_4_comment", table.columns()[3].comment());

    Assertions.assertEquals("paimon_col_name5_for_add", table.columns()[4].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[4].dataType());
    Assertions.assertNull(table.columns()[4].comment());

    // test add column with exceptions
    // 1. with column default value
    TableChange withDefaultValue =
        TableChange.addColumn(
            new String[] {"newColumn"}, Types.ByteType.get(), "comment", Literals.NULL);
    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .alterTable(NameIdentifier.of(schemaName, alertTableName), withDefaultValue));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "Paimon set column default value through table properties instead of column info"));

    // 2. with column autoIncrement
    TableChange withAutoIncrement =
        TableChange.addColumn(
            new String[] {"newColumn"}, Types.ByteType.get(), "comment", null, true, true);
    exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .alterTable(NameIdentifier.of(schemaName, alertTableName), withAutoIncrement));
    Assertions.assertTrue(
        exception.getMessage().contains("Paimon does not support auto increment column"));

    // update column position
    Column col1 = Column.of("name", Types.StringType.get(), "comment");
    Column col2 = Column.of("address", Types.StringType.get(), "comment");
    Column col3 = Column.of("date_of_birth", Types.StringType.get(), "comment");

    Column[] newColumns = new Column[] {col1, col2, col3};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("new_alter_paimon_table"));
    catalog
        .asTableCatalog()
        .createTable(
            tableIdentifier,
            newColumns,
            table_comment,
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0],
            new Index[0]);

    catalog
        .asTableCatalog()
        .alterTable(
            tableIdentifier,
            TableChange.updateColumnPosition(
                new String[] {col1.name()}, TableChange.ColumnPosition.after(col2.name())),
            TableChange.updateColumnPosition(
                new String[] {col3.name()}, TableChange.ColumnPosition.first()));

    Table updateColumnPositionTable = catalog.asTableCatalog().loadTable(tableIdentifier);

    Column[] updateCols = updateColumnPositionTable.columns();
    Assertions.assertEquals(3, updateCols.length);
    Assertions.assertEquals(col3.name(), updateCols[0].name());
    Assertions.assertEquals(col2.name(), updateCols[1].name());
    Assertions.assertEquals(col1.name(), updateCols[2].name());

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
  }

  @Test
  void testOperationDataOfPaimonTable() {
    Column[] columns = createColumns();
    String testTableName = GravitinoITUtils.genRandomName("test_table");
    SortOrder[] sortOrders = SortOrders.NONE;
    Transform[] transforms = Transforms.EMPTY_TRANSFORM;
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, testTableName),
            columns,
            table_comment,
            createProperties(),
            transforms,
            Distributions.NONE,
            sortOrders);
    List<String> values = getValues();
    String dbTable = String.join(".", schemaName, testTableName);
    // insert data
    String insertSQL =
        String.format(INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE, dbTable, String.join(", ", values));
    spark.sql(insertSQL);

    // select data
    Dataset<Row> sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, dbTable));
    Assertions.assertEquals(4, sql.count());
    Row[] result = (Row[]) sql.sort(PAIMON_COL_NAME1).collect();
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    for (int i = 0; i < result.length; i++) {
      LocalDate previousDay = currentDate.minusDays(i + 1);
      Assertions.assertEquals(
          String.format(
              "[%s,%s,data%s,[%s,string%s,[%s,inner%s]]]",
              i + 1, previousDay.format(formatter), i + 1, (i + 1) * 10, i + 1, i + 1, i + 1),
          result[i].toString());
    }

    // update data
    spark.sql(
        String.format(
            "UPDATE paimon.%s SET %s = 100 WHERE %s = 1",
            dbTable, PAIMON_COL_NAME1, PAIMON_COL_NAME1));
    sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, dbTable));
    Assertions.assertEquals(4, sql.count());
    result = (Row[]) sql.sort(PAIMON_COL_NAME1).collect();
    for (int i = 0; i < result.length; i++) {
      if (i == result.length - 1) {
        LocalDate previousDay = currentDate.minusDays(1);
        Assertions.assertEquals(
            String.format(
                "[100,%s,data%s,[%s,string%s,[%s,inner%s]]]",
                previousDay.format(formatter), 1, 10, 1, 1, 1),
            result[i].toString());
      } else {
        LocalDate previousDay = currentDate.minusDays(i + 2);
        Assertions.assertEquals(
            String.format(
                "[%s,%s,data%s,[%s,string%s,[%s,inner%s]]]",
                i + 2, previousDay.format(formatter), i + 2, (i + 2) * 10, i + 2, i + 2, i + 2),
            result[i].toString());
      }
    }
    // delete data
    spark.sql(String.format("DELETE FROM paimon.%s WHERE %s = 100", dbTable, PAIMON_COL_NAME1));
    sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, dbTable));
    Assertions.assertEquals(3, sql.count());
    result = (Row[]) sql.sort(PAIMON_COL_NAME1).collect();
    for (int i = 0; i < result.length; i++) {
      LocalDate previousDay = currentDate.minusDays(i + 2);
      Assertions.assertEquals(
          String.format(
              "[%s,%s,data%s,[%s,string%s,[%s,inner%s]]]",
              i + 2, previousDay.format(formatter), i + 2, (i + 2) * 10, i + 2, i + 2, i + 2),
          result[i].toString());
    }
  }

  @Test
  void testTimeTypePrecision() throws org.apache.paimon.catalog.Catalog.TableNotExistException {
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
          Assertions.assertEquals(Types.TimeType.of(0), column.dataType());
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
          Assertions.assertEquals(Types.TimestampType.withTimeZone(6), column.dataType());
          break;
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
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(6), column.dataType());
          break;
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

    // Verify Paimon catalog type conversion
    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, tableName));
    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;
    TableSchema schema = fileStoreTable.schema();

    // Verify field types in Paimon schema
    for (DataField field : schema.fields()) {
      String fieldName = field.name();
      org.apache.paimon.types.DataType fieldType = field.type();

      if (fieldName.startsWith("time_col")) {
        Assertions.assertInstanceOf(
            TimeType.class,
            fieldType,
            String.format(
                "Field %s should be TimeType but was %s",
                fieldName, fieldType.getClass().getSimpleName()));
      } else if (fieldName.startsWith("timestamp_col")) {
        Assertions.assertInstanceOf(
            LocalZonedTimestampType.class,
            fieldType,
            String.format(
                "Field %s should be LocalZonedTimestampType but was %s",
                fieldName, fieldType.getClass().getSimpleName()));
      } else if (fieldName.startsWith("datetime_col")) {
        Assertions.assertInstanceOf(
            TimestampType.class,
            fieldType,
            String.format(
                "Field %s should be TimestampType but was %s",
                fieldName, fieldType.getClass().getSimpleName()));
      }
    }
  }

  private static @NotNull List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      String structValue =
          String.format(
              "STRUCT(%d, 'string%d', %s)",
              i * 10, // integer_field
              i, // string_field
              String.format(
                  "STRUCT(%d, 'inner%d')",
                  i, i) // struct_field, alternating NULL and non-NULL values
              );
      values.add(
          String.format("(%d, date_sub(current_date(), %d), 'data%d', %s)", i, i, i, structValue));
    }
    return values;
  }

  private void clearTableAndSchema() {
    SupportsSchemas supportsSchema = catalog.asSchemas();
    Arrays.stream(supportsSchema.listSchemas())
        .forEach(
            schema -> {
              // can not drop default database for hive backend.
              if (!DEFAULT_DB.equalsIgnoreCase(schema)) {
                supportsSchema.dropSchema(schema, true);
              }
            });
  }

  private void createMetalake() {
    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, catalog_comment, catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);
    catalog = loadCatalog;

    String type =
        catalogProperties
            .get(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND)
            .toLowerCase(Locale.ROOT);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(type), "Paimon Catalog backend type can not be null or empty.");
    catalogProperties.put(PaimonCatalogPropertiesMetadata.PAIMON_METASTORE, type);

    // Why needs this conversion? Because PaimonCatalogOperations#initialize will try to convert
    // Gravitino general S3 properties to Paimon specific S3 properties.
    Map<String, String> copy = CatalogUtils.toInnerProperty(catalogProperties, true);

    PaimonBackendCatalogWrapper paimonBackendCatalogWrapper =
        CatalogUtils.loadCatalogBackend(new PaimonConfig(copy));
    paimonCatalog = paimonBackendCatalogWrapper.getCatalog();
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();
    prop.put("key1", "val1");
    prop.put("key2", "val2");

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
  }

  private Column[] createColumns() {
    Column col1 = Column.of(PAIMON_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(PAIMON_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(PAIMON_COL_NAME3, Types.StringType.get(), "col_3_comment");
    Types.StructType structTypeInside =
        Types.StructType.of(
            Types.StructType.Field.notNullField("integer_field_inside", Types.IntegerType.get()),
            Types.StructType.Field.notNullField(
                "string_field_inside", Types.StringType.get(), "string field inside"));
    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.notNullField("integer_field", Types.IntegerType.get()),
            Types.StructType.Field.notNullField(
                "string_field", Types.StringType.get(), "string field"),
            Types.StructType.Field.nullableField("struct_field", structTypeInside, "struct field"));
    Column col4 = Column.of(PAIMON_COL_NAME4, structType, "col_4_comment");
    return new Column[] {col1, col2, col3, col4};
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  protected void initSparkEnv() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Paimon Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE)
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
  }

  protected void assertColumn(Column expectedColumn, Column actualColumn) {
    Assertions.assertEquals(expectedColumn.name(), actualColumn.name());
    Assertions.assertEquals(expectedColumn.dataType(), actualColumn.dataType());
    Assertions.assertEquals(expectedColumn.comment(), actualColumn.comment());
    Assertions.assertEquals(expectedColumn.nullable(), actualColumn.nullable());
    Assertions.assertEquals(expectedColumn.autoIncrement(), actualColumn.autoIncrement());
    Assertions.assertEquals(expectedColumn.defaultValue(), actualColumn.defaultValue());
  }
}
