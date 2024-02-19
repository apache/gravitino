/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortField;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class CatalogIcebergIT extends AbstractIT {
  public static String metalakeName = GravitinoITUtils.genRandomName("iceberg_it_metalake");
  public static String catalogName = GravitinoITUtils.genRandomName("iceberg_it_catalog");
  public static String schemaName = GravitinoITUtils.genRandomName("iceberg_it_schema");
  public static String tableName = GravitinoITUtils.genRandomName("iceberg_it_table");
  public static String alertTableName = "alert_table_name";
  public static String table_comment = "table_comment";

  public static String schema_comment = "schema_comment";
  public static String ICEBERG_COL_NAME1 = "iceberg_col_name1";
  public static String ICEBERG_COL_NAME2 = "iceberg_col_name2";
  public static String ICEBERG_COL_NAME3 = "iceberg_col_name3";
  public static String ICEBERG_COL_NAME4 = "iceberg_col_name4";
  private static final String provider = "lakehouse-iceberg";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String WAREHOUSE;
  private static String HIVE_METASTORE_URIS;

  private static String SELECT_ALL_TEMPLATE = "SELECT * FROM iceberg.%s";
  private static String INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE =
      "INSERT INTO iceberg.%s VALUES %s";
  private static GravitinoMetaLake metalake;

  private static Catalog catalog;

  private static HiveCatalog hiveCatalog;

  private static SparkSession spark;

  @BeforeAll
  public static void startup() {
    containerSuite.startHiveContainer();
    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-iceberg/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);

    createMetalake();
    createCatalog();
    createSchema();
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Iceberg Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.uri", HIVE_METASTORE_URIS)
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
  }

  @AfterAll
  public static void stop() {
    clearTableAndSchema();
    client.dropMetalake(NameIdentifier.of(metalakeName));
    spark.close();
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
      catalog.asTableCatalog().dropTable(nameIdentifier);
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
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    catalogProperties.put(
        IcebergConfig.CATALOG_BACKEND.getKey(), IcebergCatalogBackend.HIVE.name());
    catalogProperties.put(IcebergConfig.CATALOG_URI.getKey(), HIVE_METASTORE_URIS);
    catalogProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), WAREHOUSE);

    Map<String, String> hiveProperties = Maps.newHashMap();
    hiveProperties.put(IcebergConfig.CATALOG_URI.getKey(), HIVE_METASTORE_URIS);
    hiveProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), WAREHOUSE);

    hiveCatalog = new HiveCatalog();
    hiveCatalog.setConf(new HdfsConfiguration());
    hiveCatalog.initialize("hive", hiveProperties);

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
    prop.put("key1", "val1");
    prop.put("key2", "val2");

    Schema createdSchema = catalog.asSchemas().createSchema(ident, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName(ICEBERG_COL_NAME1)
            .withDataType(Types.IntegerType.get())
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(ICEBERG_COL_NAME2)
            .withDataType(Types.DateType.get())
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(ICEBERG_COL_NAME3)
            .withDataType(Types.StringType.get())
            .withComment("col_3_comment")
            .build();
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
    ColumnDTO col4 =
        new ColumnDTO.Builder()
            .withName(ICEBERG_COL_NAME4)
            .withDataType(structType)
            .withComment("col_4_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3, col4};
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
    Namespace namespace = Namespace.of(metalakeName, catalogName);
    // list schema check.
    NameIdentifier[] nameIdentifiers = schemas.listSchemas(namespace);
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    List<org.apache.iceberg.catalog.Namespace> icebergNamespaces =
        hiveCatalog.listNamespaces(IcebergTableOpsHelper.getIcebergNamespace());
    schemaNames =
        icebergNamespaces.stream().map(ns -> ns.level(ns.length() - 1)).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap());
    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    Map<String, NameIdentifier> schemaMap =
        Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v -> v));
    Assertions.assertTrue(schemaMap.containsKey(testSchemaName));

    icebergNamespaces = hiveCatalog.listNamespaces(IcebergTableOpsHelper.getIcebergNamespace());
    schemaNames =
        icebergNamespaces.stream().map(ns -> ns.level(ns.length() - 1)).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(testSchemaName));

    // alert、load schema check.
    schemas.alterSchema(schemaIdent, SchemaChange.setProperty("t1", "v1"));
    Schema schema = schemas.loadSchema(schemaIdent);
    String val = schema.properties().get("t1");
    Assertions.assertEquals("v1", val);

    Map<String, String> hiveCatalogProps =
        hiveCatalog.loadNamespaceMetadata(
            IcebergTableOpsHelper.getIcebergNamespace(schemaIdent.name()));
    Assertions.assertTrue(hiveCatalogProps.containsKey("t1"));

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent, schema_comment, emptyMap));

    // drop schema check.
    schemas.dropSchema(schemaIdent, false);
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent));
    org.apache.iceberg.catalog.Namespace icebergNamespace =
        IcebergTableOpsHelper.getIcebergNamespace(schemaIdent.name());
    Assertions.assertThrows(
        NoSuchNamespaceException.class,
        () -> {
          hiveCatalog.loadNamespaceMetadata(icebergNamespace);
        });

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
    icebergNamespaces = hiveCatalog.listNamespaces(IcebergTableOpsHelper.getIcebergNamespace());
    schemaNames =
        icebergNamespaces.stream().map(ns -> ns.level(ns.length() - 1)).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));
  }

  @Test
  void testCreateTableWithNullComment() {
    ColumnDTO[] columns = createColumns();
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(tableIdentifier, columns, null, null, null, null, null);
    Assertions.assertNull(createdTable.comment());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertNull(loadTable.comment());
  }

  @Test
  void testCreateAndLoadIcebergTable() {
    // Create table from Gravitino API
    ColumnDTO[] columns = createColumns();

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
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
      Assertions.assertEquals(createdTable.columns()[i], columns[i]);
    }

    // TODO add partitioning and sort order check
    Assertions.assertEquals(partitioning.length, createdTable.partitioning().length);
    Assertions.assertEquals(sortOrders.length, createdTable.sortOrder().length);

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

    Assertions.assertEquals(partitioning.length, loadTable.partitioning().length);
    Assertions.assertEquals(sortOrders.length, loadTable.sortOrder().length);

    // catalog load check
    org.apache.iceberg.Table table =
        hiveCatalog.loadTable(IcebergTableOpsHelper.buildIcebergTableIdentifier(tableIdentifier));
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

  @Test
  void testListAndDropIcebergTable() {
    ColumnDTO[] columns = createColumns();

    NameIdentifier table1 = NameIdentifier.of(metalakeName, catalogName, schemaName, "table_1");

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
    NameIdentifier[] nameIdentifiers =
        tableCatalog.listTables(Namespace.of(metalakeName, catalogName, schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_1", nameIdentifiers[0].name());

    List<TableIdentifier> tableIdentifiers =
        hiveCatalog.listTables(IcebergTableOpsHelper.getIcebergNamespace(schemaName));
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0).name());

    NameIdentifier table2 = NameIdentifier.of(metalakeName, catalogName, schemaName, "table_2");
    tableCatalog.createTable(
        table2,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);
    nameIdentifiers = tableCatalog.listTables(Namespace.of(metalakeName, catalogName, schemaName));
    Assertions.assertEquals(2, nameIdentifiers.length);
    Assertions.assertEquals("table_1", nameIdentifiers[0].name());
    Assertions.assertEquals("table_2", nameIdentifiers[1].name());

    tableIdentifiers =
        hiveCatalog.listTables(IcebergTableOpsHelper.getIcebergNamespace(schemaName));
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0).name());
    Assertions.assertEquals("table_2", tableIdentifiers.get(1).name());

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(table1));

    nameIdentifiers = tableCatalog.listTables(Namespace.of(metalakeName, catalogName, schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_2", nameIdentifiers[0].name());

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(table2));
    Namespace schemaNamespace = Namespace.of(metalakeName, catalogName, schemaName);
    nameIdentifiers = tableCatalog.listTables(schemaNamespace);
    Assertions.assertEquals(0, nameIdentifiers.length);

    tableIdentifiers =
        hiveCatalog.listTables(IcebergTableOpsHelper.getIcebergNamespace(schemaName));
    Assertions.assertEquals(0, tableIdentifiers.size());
  }

  @Test
  public void testAlterIcebergTable() {
    ColumnDTO[] columns = createColumns();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
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
                  NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                  TableChange.rename(alertTableName),
                  TableChange.updateComment(table_comment + "_new"));
        });

    table =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                TableChange.rename(alertTableName));
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, table.auditInfo().creator());

    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.removeProperty("key1"),
            TableChange.setProperty("key2", "val2_new"),
            TableChange.addColumn(new String[] {"col_5_for_add"}, Types.StringType.get()),
            TableChange.renameColumn(new String[] {ICEBERG_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnComment(new String[] {ICEBERG_COL_NAME1}, "comment_new"),
            TableChange.updateColumnType(
                new String[] {ICEBERG_COL_NAME1}, Types.IntegerType.get()));

    table =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName));
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
        ((Partitioning.SingleFieldPartitioning) table.partitioning()[0]).fieldName()[0]);

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
            GravitinoITUtils.genRandomName("CatalogHiveIT_table"));
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
  }

  @Test
  void testPartitionAndSortOrderIcebergTable() {
    ColumnDTO[] columns = createColumns();
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
            NameIdentifier.of(metalakeName, catalogName, schemaName, testTableName),
            columns,
            table_comment,
            createProperties(),
            partitioning,
            Distributions.NONE,
            sortOrders);

    TableIdentifier tableIdentifier = TableIdentifier.of(schemaName, testTableName);
    org.apache.iceberg.Table table = hiveCatalog.loadTable(tableIdentifier);
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
    ColumnDTO[] columns = createColumns();
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
            NameIdentifier.of(metalakeName, catalogName, schemaName, testTableName),
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
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, "testCreateSchemaCheck");
    Map<String, String> prop = Maps.newHashMap();
    prop.put(IcebergSchemaPropertiesMetadata.COMMENT, "val1");
    prop.put("key2", "val2");

    // create
    SupportsSchemas schemas = catalog.asSchemas();
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schemas.createSchema(ident, schema_comment, prop));
    Assertions.assertTrue(
        illegalArgumentException.getMessage().contains(IcebergSchemaPropertiesMetadata.COMMENT));
    prop.remove(IcebergSchemaPropertiesMetadata.COMMENT);
    catalog.asSchemas().createSchema(ident, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadSchema.auditInfo().creator());
    Assertions.assertNull(loadSchema.auditInfo().lastModifier());
    Assertions.assertFalse(
        loadSchema.properties().containsKey(IcebergSchemaPropertiesMetadata.COMMENT));
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));

    // alter
    SchemaChange change = SchemaChange.setProperty("comment", "v1");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> schemas.alterSchema(ident, change));

    Assertions.assertDoesNotThrow(
        () ->
            catalog.asSchemas().alterSchema(ident, SchemaChange.setProperty("comment-test", "v1")));
    Schema schema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().lastModifier());
    Assertions.assertEquals("v1", schema.properties().get("comment-test"));

    // drop
    Assertions.assertTrue(catalog.asSchemas().dropSchema(ident, false));
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(ident));
  }

  @Test
  public void testTableDistribution() {
    ColumnDTO[] columns = createColumns();

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
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
    Assertions.assertEquals(DTOConverters.toDTO(distribution), table.distribution());
    Assertions.assertArrayEquals(DTOConverters.toDTOs(sortOrder), table.sortOrder());
    Assertions.assertArrayEquals(DTOConverters.toDTOs(partitioning), table.partitioning());
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
}
