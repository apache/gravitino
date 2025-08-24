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
package org.apache.gravitino.catalog.lakehouse.iceberg.integration.test;

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable.DEFAULT_ICEBERG_PROVIDER;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable.ICEBERG_AVRO_FILE_FORMAT;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable.ICEBERG_ORC_FILE_FORMAT;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable.ICEBERG_PARQUET_FILE_FORMAT;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable.PROP_PROVIDER;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergSchemaPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortField;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class CatalogIcebergBaseIT extends BaseIT {

  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected String WAREHOUSE;
  protected String URIS;
  protected String TYPE;

  private static final String alertTableName = "alert_table_name";
  private static final String table_comment = "table_comment";
  private static final String schema_comment = "schema_comment";
  private static final String ICEBERG_COL_NAME1 = "iceberg_col_name1";
  private static final String ICEBERG_COL_NAME2 = "iceberg_col_name2";
  private static final String ICEBERG_COL_NAME3 = "iceberg_col_name3";
  private static final String ICEBERG_COL_NAME4 = "iceberg_col_name4";
  private static final String provider = "lakehouse-iceberg";
  private static final String SELECT_ALL_TEMPLATE = "SELECT * FROM iceberg.%s";
  private static String INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE =
      "INSERT INTO iceberg.%s VALUES %s";
  private String metalakeName = GravitinoITUtils.genRandomName("iceberg_it_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("iceberg_it_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("iceberg_it_schema");
  private String tableName = GravitinoITUtils.genRandomName("iceberg_it_table");
  protected GravitinoMetalake metalake;
  private Catalog catalog;
  private org.apache.iceberg.catalog.Catalog icebergCatalog;
  private org.apache.iceberg.catalog.SupportsNamespaces icebergSupportsNamespaces;
  private SparkSession spark;

  @BeforeAll
  public void startup() throws Exception {
    ignoreIcebergRestService = false;
    super.startIntegrationTest();
    containerSuite.startHiveContainer();
    initIcebergCatalogProperties();
    createMetalake();
    createCatalog();
    createSchema();
    initSparkEnv();
  }

  @AfterAll
  public void stop() throws Exception {
    try {
      clearTableAndSchema();
      metalake.disableCatalog(catalogName);
      metalake.dropCatalog(catalogName);
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName);
    } finally {
      if (spark != null) {
        spark.close();
      }
      super.stopIntegrationTest();
    }
  }

  @AfterEach
  public void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  // AbstractIT#startIntegrationTest() is static, so we couldn't inject catalog info
  // if startIntegrationTest() is auto invoked by Junit. So here we override
  // startIntegrationTest() to disable the auto invoke by junit.
  @BeforeAll
  public void startIntegrationTest() {}

  @AfterAll
  public void stopIntegrationTest() {}

  protected abstract void initIcebergCatalogProperties();

  private void initSparkEnv() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Iceberg Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.uri", URIS)
            .config("spark.sql.catalog.iceberg.type", TYPE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
  }

  private void clearTableAndSchema() {
    if (catalog.asSchemas().schemaExists(schemaName)) {
      NameIdentifier[] nameIdentifiers =
          catalog.asTableCatalog().listTables(Namespace.of(schemaName));
      for (NameIdentifier nameIdentifier : nameIdentifiers) {
        catalog.asTableCatalog().dropTable(nameIdentifier);
      }
      catalog.asSchemas().dropSchema(schemaName, false);
    }
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
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");
    String icebergCatalogBackendName = "iceberg-catalog-name-test";

    catalogProperties.put(IcebergConfig.CATALOG_BACKEND.getKey(), TYPE);
    catalogProperties.put(IcebergConfig.CATALOG_URI.getKey(), URIS);
    if (!"rest".equalsIgnoreCase(TYPE)) {
      catalogProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), WAREHOUSE);
    }
    catalogProperties.put(IcebergConfig.CATALOG_BACKEND_NAME.getKey(), icebergCatalogBackendName);

    Map<String, String> icebergCatalogProperties = Maps.newHashMap();
    icebergCatalogProperties.put(IcebergConfig.CATALOG_URI.getKey(), URIS);
    if (!"rest".equalsIgnoreCase(TYPE)) {
      icebergCatalogProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), WAREHOUSE);
    }
    icebergCatalogProperties.put(
        IcebergConfig.CATALOG_BACKEND_NAME.getKey(), icebergCatalogBackendName);

    icebergCatalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.valueOf(TYPE.toUpperCase(Locale.ROOT)),
            new IcebergConfig(icebergCatalogProperties));
    if (icebergCatalog instanceof SupportsNamespaces) {
      icebergSupportsNamespaces = (org.apache.iceberg.catalog.SupportsNamespaces) icebergCatalog;
    }

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(schemaName);
    Map<String, String> prop = Maps.newHashMap();
    prop.put("key1", "val1");
    prop.put("key2", "val2");

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));
  }

  private Column[] createColumns() {
    Column col1 = Column.of(ICEBERG_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(ICEBERG_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(ICEBERG_COL_NAME3, Types.StringType.get(), "col_3_comment");
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
    Column col4 = Column.of(ICEBERG_COL_NAME4, structType, "col_4_comment");
    return new Column[] {col1, col2, col3, col4};
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  @Test
  void testOperationIcebergSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    // list schema check.
    Set<String> schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(schemaName));

    List<org.apache.iceberg.catalog.Namespace> icebergNamespaces =
        icebergSupportsNamespaces.listNamespaces(IcebergCatalogWrapperHelper.getIcebergNamespace());
    schemaNames =
        icebergNamespaces.stream().map(ns -> ns.level(ns.length() - 1)).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(testSchemaName);
    schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap());

    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    icebergNamespaces =
        icebergSupportsNamespaces.listNamespaces(IcebergCatalogWrapperHelper.getIcebergNamespace());
    schemaNames =
        icebergNamespaces.stream().map(ns -> ns.level(ns.length() - 1)).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    // alert、load schema check.
    schemas.alterSchema(schemaIdent.name(), SchemaChange.setProperty("t1", "v1"));
    Schema schema = schemas.loadSchema(schemaIdent.name());
    String val = schema.properties().get("t1");
    Assertions.assertEquals("v1", val);

    Map<String, String> hiveCatalogProps =
        icebergSupportsNamespaces.loadNamespaceMetadata(
            IcebergCatalogWrapperHelper.getIcebergNamespace(schemaIdent.name()));
    Assertions.assertTrue(hiveCatalogProps.containsKey("t1"));

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent.name(), schema_comment, emptyMap));

    // drop schema check.
    schemas.dropSchema(schemaIdent.name(), false);
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));
    org.apache.iceberg.catalog.Namespace icebergNamespace =
        IcebergCatalogWrapperHelper.getIcebergNamespace(schemaIdent.name());
    Assertions.assertThrows(
        NoSuchNamespaceException.class,
        () -> {
          icebergSupportsNamespaces.loadNamespaceMetadata(icebergNamespace);
        });

    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
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
    Throwable excep =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> schemas.dropSchema(schemaIdent.name(), true));
    Assertions.assertTrue(
        excep.getMessage().contains("Iceberg does not support cascading delete operations."));

    Assertions.assertFalse(schemas.dropSchema(schemaIdent.name(), false));
    Assertions.assertFalse(tableCatalog.dropTable(table));
    icebergNamespaces =
        icebergSupportsNamespaces.listNamespaces(IcebergCatalogWrapperHelper.getIcebergNamespace());
    schemaNames =
        icebergNamespaces.stream().map(ns -> ns.level(ns.length() - 1)).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));
  }

  @Test
  void testCreateTableWithNullComment() {
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
  void testCreateTableWithNoneDistribution() {
    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(ICEBERG_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Transform[] partitioning = new Transform[] {Transforms.day(columns[1].name())};
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table tableWithPartitionAndSortorder =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            partitioning,
            distribution,
            sortOrders);
    Assertions.assertEquals(tableName, tableWithPartitionAndSortorder.name());
    Assertions.assertEquals(Distributions.RANGE, tableWithPartitionAndSortorder.distribution());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(Distributions.RANGE, loadTable.distribution());
    tableCatalog.dropTable(tableIdentifier);

    Table tableWithPartition =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            partitioning,
            distribution,
            new SortOrder[0]);
    Assertions.assertEquals(tableName, tableWithPartition.name());
    Assertions.assertEquals(Distributions.HASH, tableWithPartition.distribution());

    loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(Distributions.HASH, loadTable.distribution());
    tableCatalog.dropTable(tableIdentifier);

    Table tableWithoutPartitionAndSortOrder =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            new Transform[0],
            distribution,
            new SortOrder[0]);
    Assertions.assertEquals(tableName, tableWithoutPartitionAndSortOrder.name());
    Assertions.assertEquals(Distributions.NONE, tableWithoutPartitionAndSortOrder.distribution());

    loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(Distributions.NONE, loadTable.distribution());
  }

  @Test
  void testCreateAndLoadIcebergTable() {
    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(ICEBERG_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Transform[] partitioning = new Transform[] {Transforms.day(columns[1].name())};
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

    // TODO add partitioning and sort order check
    assertPartitioningAndSortOrder(partitioning, sortOrders, createdTable);

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

    assertPartitioningAndSortOrder(partitioning, sortOrders, loadTable);

    // catalog load check
    org.apache.iceberg.Table table =
        icebergCatalog.loadTable(
            IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdentifier));
    Assertions.assertEquals(tableName, table.name().substring(table.name().lastIndexOf(".") + 1));
    Assertions.assertEquals(
        table_comment, table.properties().get(IcebergTable.ICEBERG_COMMENT_FIELD_NAME));
    resultProp = table.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    org.apache.iceberg.Schema icebergSchema = table.schema();
    Assertions.assertEquals(icebergSchema.columns().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      Assertions.assertNotNull(icebergSchema.findField(columns[i].name()));
    }
    Assertions.assertEquals(partitioning.length, table.spec().fields().size());
    Assertions.assertEquals(partitioning.length, table.sortOrder().fields().size());

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

  private void assertPartitioningAndSortOrder(
      Transform[] expectedPartitioning, SortOrder[] expectedSortOrders, Table actualTable) {
    Assertions.assertArrayEquals(expectedPartitioning, actualTable.partitioning());
    Assertions.assertEquals(expectedSortOrders.length, actualTable.sortOrder().length);
    for (int i = 0; i < expectedSortOrders.length; i++) {
      SortOrder expected = expectedSortOrders[i];
      SortOrder actual = actualTable.sortOrder()[i];
      Assertions.assertEquals(expected.expression().toString(), actual.expression().toString());
      Assertions.assertEquals(expected.direction(), actual.direction());
      Assertions.assertEquals(expected.nullOrdering(), actual.nullOrdering());
    }
  }

  @Test
  void testTimestampTypeConversion() {

    Column col1 =
        Column.of("iceberg_column_1", Types.TimestampType.withTimeZone(), "col_1_comment");
    Column col2 =
        Column.of("iceberg_column_2", Types.TimestampType.withoutTimeZone(), "col_2_comment");

    Column[] columns = new Column[] {col1, col2};

    String timestampTableName = "timestamp_table";

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, timestampTableName);

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(tableIdentifier, columns, table_comment, properties);
    Assertions.assertEquals("iceberg_column_1", createdTable.columns()[0].name());
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(), createdTable.columns()[0].dataType());
    Assertions.assertEquals("col_1_comment", createdTable.columns()[0].comment());

    Assertions.assertEquals("iceberg_column_2", createdTable.columns()[1].name());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(), createdTable.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", createdTable.columns()[1].comment());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals("iceberg_column_1", loadTable.columns()[0].name());
    Assertions.assertEquals(Types.TimestampType.withTimeZone(6), loadTable.columns()[0].dataType());
    Assertions.assertEquals("col_1_comment", loadTable.columns()[0].comment());

    Assertions.assertEquals("iceberg_column_2", loadTable.columns()[1].name());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(6), loadTable.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", loadTable.columns()[1].comment());

    org.apache.iceberg.Table table =
        icebergCatalog.loadTable(
            IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdentifier));
    org.apache.iceberg.Schema icebergSchema = table.schema();
    Assertions.assertEquals("iceberg_column_1", icebergSchema.columns().get(0).name());
    Assertions.assertEquals(
        org.apache.iceberg.types.Types.TimestampType.withZone(),
        icebergSchema.columns().get(0).type());
    Assertions.assertEquals("col_1_comment", icebergSchema.columns().get(0).doc());

    Assertions.assertEquals("iceberg_column_2", icebergSchema.columns().get(1).name());
    Assertions.assertEquals(
        org.apache.iceberg.types.Types.TimestampType.withoutZone(),
        icebergSchema.columns().get(1).type());
    Assertions.assertEquals("col_2_comment", icebergSchema.columns().get(1).doc());
  }

  @Test
  void testListAndDropIcebergTable() {
    Column[] columns = createColumns();

    NameIdentifier table1 = NameIdentifier.of(schemaName, "table_1");

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

    List<TableIdentifier> tableIdentifiers =
        icebergCatalog.listTables(IcebergCatalogWrapperHelper.getIcebergNamespace(schemaName));
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0).name());

    NameIdentifier table2 = NameIdentifier.of(schemaName, "table_2");
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

    tableIdentifiers =
        icebergCatalog.listTables(IcebergCatalogWrapperHelper.getIcebergNamespace(schemaName));
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0).name());
    Assertions.assertEquals("table_2", tableIdentifiers.get(1).name());

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(table1));

    nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_2", nameIdentifiers[0].name());

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(table2));
    nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(0, nameIdentifiers.length);

    tableIdentifiers =
        icebergCatalog.listTables(IcebergCatalogWrapperHelper.getIcebergNamespace(schemaName));
    Assertions.assertEquals(0, tableIdentifiers.size());
  }

  @Test
  public void testAlterIcebergTable() {
    Column[] columns = createColumns();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
                columns,
                table_comment,
                createProperties(),
                new Transform[] {Transforms.identity(columns[0].name())});
    Assertions.assertNull(table.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());
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

    table =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(schemaName, tableName), TableChange.rename(alertTableName));
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());

    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.removeProperty("key1"),
            TableChange.setProperty("key2", "val2_new"),
            TableChange.addColumn(new String[] {"col_5_for_add"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {ICEBERG_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnComment(new String[] {ICEBERG_COL_NAME1}, "comment_new"),
            TableChange.updateColumnType(
                new String[] {ICEBERG_COL_NAME1}, Types.IntegerType.get()));

    table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, alertTableName));
    Assertions.assertEquals(alertTableName, table.name());
    Assertions.assertEquals("val2_new", table.properties().get("key2"));

    Assertions.assertEquals(ICEBERG_COL_NAME1, table.columns()[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), table.columns()[0].dataType());
    Assertions.assertEquals("comment_new", table.columns()[0].comment());

    Assertions.assertEquals("col_2_new", table.columns()[1].name());
    Assertions.assertEquals(Types.DateType.get(), table.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", table.columns()[1].comment());

    Assertions.assertEquals(ICEBERG_COL_NAME3, table.columns()[2].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[2].dataType());
    Assertions.assertEquals("col_3_comment", table.columns()[2].comment());

    Assertions.assertEquals(ICEBERG_COL_NAME4, table.columns()[3].name());
    Assertions.assertEquals(columns[3].dataType(), table.columns()[3].dataType());
    Assertions.assertEquals("col_4_comment", table.columns()[3].comment());

    Assertions.assertEquals("col_5_for_add", table.columns()[4].name());
    Assertions.assertEquals(Types.StringType.get(), table.columns()[4].dataType());
    Assertions.assertNull(table.columns()[4].comment());

    Assertions.assertEquals(1, table.partitioning().length);
    Assertions.assertEquals(
        columns[0].name(),
        ((Transform.SingleFieldTransform) table.partitioning()[0]).fieldName()[0]);

    // test add column with default value exception
    TableChange withDefaultValue =
        TableChange.addColumn(
            new String[] {"newColumn"}, Types.ByteType.get(), "comment", Literals.NULL);
    RuntimeException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .alterTable(NameIdentifier.of(schemaName, alertTableName), withDefaultValue));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Iceberg does not support column default value. Illegal column:"),
        "The exception message is: " + exception.getMessage());

    Column col1 = Column.of("name", Types.StringType.get(), "comment");
    Column col2 = Column.of("address", Types.StringType.get(), "comment");
    Column col3 = Column.of("date_of_birth", Types.DateType.get(), "comment");

    Column[] newColumns = new Column[] {col1, col2, col3};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("CatalogHiveIT_table"));
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
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class, () -> tableCatalog.alterTable(tableIdentifier, change));
    Assertions.assertTrue(illegalArgumentException.getMessage().contains("no_column"));

    TableChange change2 =
        TableChange.updateColumnDefaultValue(
            new String[] {col1.name()}, Literals.of("hello", Types.StringType.get()));
    illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> tableCatalog.alterTable(tableIdentifier, change2));
    Assertions.assertTrue(
        illegalArgumentException
            .getMessage()
            .contains("Iceberg does not support column default value. Illegal column: name"),
        "The exception is: " + illegalArgumentException.getMessage());

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
    catalog.asTableCatalog().dropTable(tableIdentifier);
  }

  @Test
  void testPartitionAndSortOrderIcebergTable() {
    Column[] columns = createColumns();
    String testTableName = GravitinoITUtils.genRandomName("test_table");
    SortOrder[] sortOrders = {
      SortOrders.ascending(NamedReference.field(columns[0].name())),
      SortOrders.descending(NamedReference.field(columns[2].name()))
    };

    Transform[] partitioning = {
      Transforms.day(columns[1].name()), Transforms.identity(columns[2].name())
    };

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, testTableName),
            columns,
            table_comment,
            createProperties(),
            partitioning,
            Distributions.NONE,
            sortOrders);

    TableIdentifier tableIdentifier = TableIdentifier.of(schemaName, testTableName);
    org.apache.iceberg.Table table = icebergCatalog.loadTable(tableIdentifier);
    PartitionSpec spec = table.spec();
    Map<Integer, String> idToName = table.schema().idToName();
    List<PartitionField> fields = spec.fields();
    Assertions.assertEquals(2, fields.size());
    Assertions.assertEquals(columns[1].name(), idToName.get(fields.get(0).sourceId()));
    Assertions.assertEquals(columns[2].name(), idToName.get(fields.get(1).sourceId()));
    Assertions.assertEquals("day", fields.get(0).transform().toString());
    Assertions.assertEquals("identity", fields.get(1).transform().toString());

    List<SortField> sortFields = table.sortOrder().fields();
    Assertions.assertEquals(2, sortFields.size());
    Assertions.assertEquals(columns[0].name(), idToName.get(sortFields.get(0).sourceId()));
    Assertions.assertEquals(columns[2].name(), idToName.get(sortFields.get(1).sourceId()));
    Assertions.assertEquals(org.apache.iceberg.SortDirection.ASC, sortFields.get(0).direction());
    Assertions.assertEquals(NullOrder.NULLS_FIRST, sortFields.get(0).nullOrder());
    Assertions.assertEquals(org.apache.iceberg.SortDirection.DESC, sortFields.get(1).direction());
    Assertions.assertEquals(NullOrder.NULLS_LAST, sortFields.get(1).nullOrder());
  }

  @Test
  void testOperationDataIcebergTable() {
    Column[] columns = createColumns();
    String testTableName = GravitinoITUtils.genRandomName("test_table");
    SortOrder[] sortOrders = {
      SortOrders.of(
          NamedReference.field(columns[0].name()),
          SortDirection.DESCENDING,
          NullOrdering.NULLS_FIRST),
      SortOrders.of(
          NamedReference.field(columns[2].name()),
          SortDirection.DESCENDING,
          NullOrdering.NULLS_FIRST),
    };
    Transform[] transforms = {
      Transforms.day(columns[1].name()), Transforms.identity(columns[2].name())
    };
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
    TableIdentifier tableIdentifier = TableIdentifier.of(schemaName, testTableName);
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
    // insert data
    String insertSQL =
        String.format(
            INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE, tableIdentifier, String.join(", ", values));
    spark.sql(insertSQL);

    // select data
    Dataset<Row> sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, tableIdentifier));
    Assertions.assertEquals(4, sql.count());
    Row[] result = (Row[]) sql.sort(ICEBERG_COL_NAME1).collect();
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
            "UPDATE iceberg.%s SET %s = 100 WHERE %s = 1",
            tableIdentifier, ICEBERG_COL_NAME1, ICEBERG_COL_NAME1));
    sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, tableIdentifier));
    Assertions.assertEquals(4, sql.count());
    result = (Row[]) sql.sort(ICEBERG_COL_NAME1).collect();
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
    spark.sql(
        String.format("DELETE FROM iceberg.%s WHERE %s = 100", tableIdentifier, ICEBERG_COL_NAME1));
    sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, tableIdentifier));
    Assertions.assertEquals(3, sql.count());
    result = (Row[]) sql.sort(ICEBERG_COL_NAME1).collect();
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
  public void testOperatorSchemeProperties() {
    NameIdentifier ident = NameIdentifier.of("testCreateSchemaCheck");
    Map<String, String> prop = Maps.newHashMap();
    prop.put(IcebergSchemaPropertiesMetadata.COMMENT, "val1");
    prop.put("key2", "val2");

    // create
    SupportsSchemas schemas = catalog.asSchemas();
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schemas.createSchema(ident.name(), schema_comment, prop));
    Assertions.assertTrue(
        illegalArgumentException.getMessage().contains(IcebergSchemaPropertiesMetadata.COMMENT));
    prop.remove(IcebergSchemaPropertiesMetadata.COMMENT);
    catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadSchema.auditInfo().creator());
    Assertions.assertNull(loadSchema.auditInfo().lastModifier());
    Assertions.assertFalse(
        loadSchema.properties().containsKey(IcebergSchemaPropertiesMetadata.COMMENT));
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));

    // alter
    SchemaChange change = SchemaChange.setProperty("comment", "v1");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> schemas.alterSchema(ident.name(), change));

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asSchemas()
                .alterSchema(ident.name(), SchemaChange.setProperty("comment-test", "v1")));
    Schema schema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().lastModifier());
    Assertions.assertEquals("v1", schema.properties().get("comment-test"));

    // drop
    Assertions.assertTrue(catalog.asSchemas().dropSchema(ident.name(), false));
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(ident.name()));
  }

  @Test
  public void testTableDistribution() {
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(ICEBERG_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Transform[] partitioning = new Transform[] {Transforms.day(columns[1].name())};

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    // Create a data table for Distributions.NONE
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        new Transform[0],
        distribution,
        new SortOrder[0]);

    Table loadTable = tableCatalog.loadTable(tableIdentifier);

    // check table
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        distribution,
        new SortOrder[0],
        new Transform[0],
        loadTable);

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));

    distribution = Distributions.HASH;
    // Create a data table for Distributions.hash
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        sortOrders);

    loadTable = tableCatalog.loadTable(tableIdentifier);
    // check table
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        distribution,
        sortOrders,
        partitioning,
        loadTable);
    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));

    // Create a data table for Distributions.NONE and set field name
    Distribution hash = Distributions.hash(0, NamedReference.field(ICEBERG_COL_NAME1));
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              tableCatalog.createTable(
                  tableIdentifier,
                  columns,
                  table_comment,
                  properties,
                  partitioning,
                  hash,
                  sortOrders);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Iceberg's Distribution Mode.HASH does not support set expressions."));

    distribution = Distributions.RANGE;
    // Create a data table for Distributions.hash
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        sortOrders);

    loadTable = tableCatalog.loadTable(tableIdentifier);
    // check table
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        distribution,
        sortOrders,
        partitioning,
        loadTable);

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));

    // Create a data table for Distributions.range and set field name
    Distribution of = Distributions.of(Strategy.RANGE, 0, NamedReference.field(ICEBERG_COL_NAME1));
    illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              tableCatalog.createTable(
                  tableIdentifier,
                  columns,
                  table_comment,
                  properties,
                  partitioning,
                  of,
                  sortOrders);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Iceberg's Distribution Mode.RANGE not support set expressions."));
  }

  @Test
  void testIcebergTablePropertiesWhenCreate() {
    String[] providers =
        new String[] {
          null,
          DEFAULT_ICEBERG_PROVIDER,
          ICEBERG_PARQUET_FILE_FORMAT,
          ICEBERG_ORC_FILE_FORMAT,
          ICEBERG_AVRO_FILE_FORMAT
        };

    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(ICEBERG_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Transform[] partitioning = new Transform[] {Transforms.day(columns[1].name())};
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Arrays.stream(providers)
        .forEach(
            provider -> {
              if (provider != null) {
                properties.put(PROP_PROVIDER, provider);
              }
              if (DEFAULT_ICEBERG_PROVIDER.equals(provider)) {
                provider = null;
              }
              checkIcebergTableFileFormat(
                  tableCatalog,
                  tableIdentifier,
                  columns,
                  table_comment,
                  properties,
                  partitioning,
                  distribution,
                  sortOrders,
                  provider);
              tableCatalog.dropTable(tableIdentifier);
            });

    properties.put(PROP_PROVIDER, "text");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                tableIdentifier,
                columns,
                table_comment,
                properties,
                partitioning,
                distribution,
                sortOrders));

    properties.put(PROP_PROVIDER, ICEBERG_PARQUET_FILE_FORMAT);
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        sortOrders);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.alterTable(
                tableIdentifier, TableChange.setProperty(PROP_PROVIDER, ICEBERG_ORC_FILE_FORMAT)));
  }

  private static void checkIcebergTableFileFormat(
      TableCatalog tableCatalog,
      NameIdentifier tableIdentifier,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      String expectedFileFormat) {
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier, columns, comment, properties, partitioning, distribution, sortOrders);
    Assertions.assertEquals(expectedFileFormat, createdTable.properties().get(DEFAULT_FILE_FORMAT));
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(expectedFileFormat, loadTable.properties().get(DEFAULT_FILE_FORMAT));
  }

  @Test
  public void testTableSortOrder() {
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.HASH;

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(ICEBERG_COL_NAME2),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST),
          SortOrders.of(
              FunctionExpression.of(
                  "bucket", Literals.integerLiteral(10), NamedReference.field(ICEBERG_COL_NAME1)),
              SortDirection.ASCENDING,
              NullOrdering.NULLS_LAST),
          SortOrders.of(
              FunctionExpression.of(
                  "truncate", Literals.integerLiteral(2), NamedReference.field(ICEBERG_COL_NAME3)),
              SortDirection.ASCENDING,
              NullOrdering.NULLS_LAST),
        };

    Transform[] partitioning = new Transform[] {Transforms.day(columns[1].name())};

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    // Create a data table for Distributions.NONE
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        sortOrders);

    Table loadTable = tableCatalog.loadTable(tableIdentifier);

    // check table
    assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        distribution,
        sortOrders,
        partitioning,
        loadTable);

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));
  }

  @Test
  void testTimeTypePrecision() {
    String tableName = GravitinoITUtils.genRandomName("test_time_precision");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    columns =
        ArrayUtils.addAll(
            columns,
            // time type - only support precision 6 (microsecond) or no precision
            Column.of("time_col", Types.TimeType.get()),
            Column.of("time_col_6", Types.TimeType.of(6)),
            // timestamptz type (with timezone) - only support precision 6 (microsecond) or no
            // precision
            Column.of("timestamptz_col", Types.TimestampType.withTimeZone()),
            Column.of("timestamptz_col_6", Types.TimestampType.withTimeZone(6)),
            // timestamp type (without timezone) - only support precision 6 (microsecond) or no
            // precision
            Column.of("timestamp_col", Types.TimestampType.withoutTimeZone()),
            Column.of("timestamp_col_6", Types.TimestampType.withoutTimeZone(6)));

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

    Assertions.assertEquals(2, timeColumns.length);
    for (Column column : timeColumns) {
      switch (column.name()) {
        case "time_col":
        case "time_col_6":
          Assertions.assertEquals(Types.TimeType.of(6), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected time column: " + column.name());
      }
    }

    // Verify timestamptz type precisions (with timezone)
    Column[] timestamptzColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("timestamptz_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(2, timestamptzColumns.length);
    for (Column column : timestamptzColumns) {
      switch (column.name()) {
        case "timestamptz_col":
        case "timestamptz_col_6":
          Assertions.assertEquals(Types.TimestampType.withTimeZone(6), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected timestamptz column: " + column.name());
      }
    }

    // Verify timestamp type precisions (without timezone)
    Column[] timestampColumns =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("timestamp_col"))
            .toArray(Column[]::new);

    Assertions.assertEquals(2, timestampColumns.length);
    for (Column column : timestampColumns) {
      switch (column.name()) {
        case "timestamp_col":
        case "timestamp_col_6":
          Assertions.assertEquals(Types.TimestampType.withoutTimeZone(6), column.dataType());
          break;
        default:
          Assertions.fail("Unexpected timestamp column: " + column.name());
      }
    }

    // Verify Iceberg schema type conversion
    org.apache.iceberg.Table icebergTable =
        icebergCatalog.loadTable(
            TableIdentifier.of(
                IcebergCatalogWrapperHelper.getIcebergNamespace(schemaName), tableName));
    org.apache.iceberg.Schema schema = icebergTable.schema();

    // Verify field types in Iceberg schema
    for (org.apache.iceberg.types.Types.NestedField field : schema.columns()) {
      String fieldName = field.name();
      org.apache.iceberg.types.Type fieldType = field.type();

      if (fieldName.startsWith("time_col")) {
        Assertions.assertInstanceOf(
            org.apache.iceberg.types.Types.TimeType.class,
            fieldType,
            String.format(
                "Field %s should be TimeType but was %s",
                fieldName, fieldType.getClass().getSimpleName()));
      } else {
        String format =
            String.format(
                "Field %s should be TimestampType but was %s",
                fieldName, fieldType.getClass().getSimpleName());
        if (fieldName.startsWith("timestamptz_col")) {
          Assertions.assertInstanceOf(
              org.apache.iceberg.types.Types.TimestampType.class, fieldType, format);
          org.apache.iceberg.types.Types.TimestampType tsType =
              (org.apache.iceberg.types.Types.TimestampType) fieldType;
          Assertions.assertTrue(tsType.shouldAdjustToUTC(), "Timestamptz should adjust to UTC");
        } else if (fieldName.startsWith("timestamp_col")) {
          Assertions.assertInstanceOf(
              org.apache.iceberg.types.Types.TimestampType.class, fieldType, format);
          org.apache.iceberg.types.Types.TimestampType tsType =
              (org.apache.iceberg.types.Types.TimestampType) fieldType;
          Assertions.assertFalse(tsType.shouldAdjustToUTC(), "Timestamp should not adjust to UTC");
        }
      }
    }

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(tableIdentifier));
  }

  @Test
  void testTimeTypePrecisionValidation() {
    String tableName = GravitinoITUtils.genRandomName("test_time_precision_validation");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();

    // Test unsupported time precision
    Column[] unsupportedTimeColumns = {
      Column.of("time_col_0", Types.TimeType.of(0)),
      Column.of("time_col_1", Types.TimeType.of(1)),
      Column.of("time_col_3", Types.TimeType.of(3)),
      Column.of("time_col_9", Types.TimeType.of(9))
    };

    // Test unsupported timestamptz precision (with timezone)
    Column[] unsupportedTimestamptzColumns = {
      Column.of("timestamptz_col_0", Types.TimestampType.withTimeZone(0)),
      Column.of("timestamptz_col_1", Types.TimestampType.withTimeZone(1)),
      Column.of("timestamptz_col_3", Types.TimestampType.withTimeZone(3)),
      Column.of("timestamptz_col_9", Types.TimestampType.withTimeZone(9))
    };

    // Test unsupported timestamp precision (without timezone)
    Column[] unsupportedTimestampColumns = {
      Column.of("timestamp_col_0", Types.TimestampType.withoutTimeZone(0)),
      Column.of("timestamp_col_1", Types.TimestampType.withoutTimeZone(1)),
      Column.of("timestamp_col_3", Types.TimestampType.withoutTimeZone(3)),
      Column.of("timestamp_col_9", Types.TimestampType.withoutTimeZone(9))
    };

    TableCatalog tableCatalog = catalog.asTableCatalog();

    // Test time precision validation
    for (Column column : unsupportedTimeColumns) {
      Column[] testColumns = ArrayUtils.addAll(columns, column);
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  tableCatalog.createTable(
                      tableIdentifier,
                      testColumns,
                      table_comment,
                      createProperties(),
                      Transforms.EMPTY_TRANSFORM,
                      Distributions.NONE,
                      new SortOrder[0]));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains("Iceberg only supports microsecond precision (6) for time type"));
    }

    // Test timestamptz precision validation (with timezone)
    for (Column column : unsupportedTimestamptzColumns) {
      Column[] testColumns = ArrayUtils.addAll(columns, column);
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  tableCatalog.createTable(
                      tableIdentifier,
                      testColumns,
                      table_comment,
                      createProperties(),
                      Transforms.EMPTY_TRANSFORM,
                      Distributions.NONE,
                      new SortOrder[0]));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains("Iceberg only supports microsecond precision (6) for timestamptz type"));
    }

    // Test timestamp precision validation (without timezone)
    for (Column column : unsupportedTimestampColumns) {
      Column[] testColumns = ArrayUtils.addAll(columns, column);
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  tableCatalog.createTable(
                      tableIdentifier,
                      testColumns,
                      table_comment,
                      createProperties(),
                      Transforms.EMPTY_TRANSFORM,
                      Distributions.NONE,
                      new SortOrder[0]));
      Assertions.assertTrue(
          exception
              .getMessage()
              .contains("Iceberg only supports microsecond precision (6) for timestamp type"));
    }
  }

  protected static void assertionsTableInfo(
      String tableName,
      String tableComment,
      List<Column> columns,
      Map<String, String> properties,
      Distribution distribution,
      SortOrder[] sortOrder,
      Transform[] partitioning,
      Table table) {
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(tableComment, table.comment());
    Assertions.assertEquals(columns.size(), table.columns().length);
    Assertions.assertEquals(distribution, table.distribution());
    Assertions.assertArrayEquals(sortOrder, table.sortOrder());
    Assertions.assertArrayEquals(partitioning, table.partitioning());
    for (int i = 0; i < columns.size(); i++) {
      Assertions.assertEquals(columns.get(i).name(), table.columns()[i].name());
      Assertions.assertEquals(columns.get(i).dataType(), table.columns()[i].dataType());
      Assertions.assertEquals(columns.get(i).nullable(), table.columns()[i].nullable());
      Assertions.assertEquals(columns.get(i).comment(), table.columns()[i].comment());
      Assertions.assertEquals(columns.get(i).autoIncrement(), table.columns()[i].autoIncrement());
    }

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertEquals(entry.getValue(), table.properties().get(entry.getKey()));
    }
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
