/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.rel.transforms.Transforms.day;
import static com.datastrato.gravitino.rel.transforms.Transforms.identity;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOpsHelper;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Distribution;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SortOrder;
import com.datastrato.gravitino.rel.SortOrder.Direction;
import com.datastrato.gravitino.rel.SortOrder.NullOrdering;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.transforms.Transform;
import com.datastrato.gravitino.rel.transforms.Transforms;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortDirection;
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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
  private static final String provider = "lakehouse-iceberg";
  private static final String WAREHOUSE =
      "hdfs://127.0.0.1:9000/user/hive/warehouse-catalog-iceberg/";
  private static final String URI = "thrift://127.0.0.1:9083";

  private static String SELECT_ALL_TEMPLATE = "SELECT * FROM iceberg.%s";
  private static String INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE =
      "INSERT INTO iceberg.%s VALUES %s";
  private static GravitinoMetaLake metalake;

  private static Catalog catalog;

  private static HiveCatalog hiveCatalog;

  private static SparkSession spark;

  @BeforeAll
  public static void startup() {
    createMetalake();
    createCatalog();
    createSchema();
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Iceberg Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.uri", URI)
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
    catalogProperties.put(IcebergConfig.CATALOG_URI.getKey(), URI);
    catalogProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), WAREHOUSE);

    Map<String, String> hiveProperties = Maps.newHashMap();
    hiveProperties.put(IcebergConfig.CATALOG_URI.getKey(), URI);
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
            .withDataType(TypeCreator.NULLABLE.I32)
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(ICEBERG_COL_NAME2)
            .withDataType(TypeCreator.NULLABLE.DATE)
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(ICEBERG_COL_NAME3)
            .withDataType(TypeCreator.NULLABLE.STRING)
            .withComment("col_3_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3};
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

    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap()));

    // drop schema check.
    schemas.dropSchema(schemaIdent, false);
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent));
    Assertions.assertThrows(
        NoSuchNamespaceException.class,
        () ->
            hiveCatalog.loadNamespaceMetadata(
                IcebergTableOpsHelper.getIcebergNamespace(schemaIdent.name())));

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
                Distribution.NONE,
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
  void testCreateAndLoadIcebergTable() {
    // Create table from Gravitino API
    ColumnDTO[] columns = createColumns();

    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Distribution distribution = Distribution.NONE;

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrder.builder()
              .withNullOrdering(NullOrdering.FIRST)
              .withDirection(Direction.DESC)
              .withTransform(Transforms.field(new String[] {ICEBERG_COL_NAME2}))
              .build()
        };

    Transform[] partitioning =
        new Transform[] {
          day(new String[] {columns[1].name()}),
        };

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
                    new Transform[0],
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
        new Transform[0],
        Distribution.NONE,
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
        new Transform[0],
        Distribution.NONE,
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
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            columns,
            table_comment,
            createProperties(),
            new Transform[] {identity(columns[0])});
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
            TableChange.removeProperty("key1"),
            TableChange.setProperty("key2", "val2_new"),
            TableChange.addColumn(new String[] {"col_4"}, TypeCreator.NULLABLE.STRING),
            TableChange.renameColumn(new String[] {ICEBERG_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnComment(new String[] {ICEBERG_COL_NAME1}, "comment_new"),
            TableChange.updateColumnType(
                new String[] {ICEBERG_COL_NAME1}, TypeCreator.NULLABLE.I32));

    Table table =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName));
    Assertions.assertEquals(alertTableName, table.name());
    Assertions.assertEquals("val2_new", table.properties().get("key2"));

    Assertions.assertEquals(ICEBERG_COL_NAME1, table.columns()[0].name());
    Assertions.assertEquals(TypeCreator.NULLABLE.I32, table.columns()[0].dataType());
    Assertions.assertEquals("comment_new", table.columns()[0].comment());

    Assertions.assertEquals("col_2_new", table.columns()[1].name());
    Assertions.assertEquals(TypeCreator.NULLABLE.DATE, table.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", table.columns()[1].comment());

    Assertions.assertEquals(ICEBERG_COL_NAME3, table.columns()[2].name());
    Assertions.assertEquals(TypeCreator.NULLABLE.STRING, table.columns()[2].dataType());
    Assertions.assertEquals("col_3_comment", table.columns()[2].comment());

    Assertions.assertEquals("col_4", table.columns()[3].name());
    Assertions.assertEquals(TypeCreator.NULLABLE.STRING, table.columns()[3].dataType());
    Assertions.assertNull(table.columns()[3].comment());

    Assertions.assertEquals(1, table.partitioning().length);
    Assertions.assertEquals(
        columns[0].name(), ((Transforms.NamedReference) table.partitioning()[0]).value()[0]);

    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName("name")
            .withDataType(TypeCreator.NULLABLE.STRING)
            .withComment("comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName("address")
            .withDataType(TypeCreator.NULLABLE.STRING)
            .withComment("comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName("date_of_birth")
            .withDataType(TypeCreator.NULLABLE.DATE)
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
            new Transform[0],
            Distribution.NONE,
            new SortOrder[0]);

    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .alterTable(
                        tableIdentifier,
                        TableChange.updateColumnPosition(
                            new String[] {"no_column"}, TableChange.ColumnPosition.first())));
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
      SortOrder.fieldSortOrder(new String[] {columns[0].name()}, Direction.ASC, NullOrdering.FIRST),
      SortOrder.fieldSortOrder(new String[] {columns[2].name()}, Direction.DESC, NullOrdering.LAST)
    };
    Transform[] transforms = {
      day(new String[] {columns[1].name()}), Transforms.identity(columns[2])
    };
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, testTableName),
            columns,
            table_comment,
            createProperties(),
            transforms,
            Distribution.NONE,
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
    Assertions.assertEquals(SortDirection.ASC, sortFields.get(0).direction());
    Assertions.assertEquals(NullOrder.NULLS_FIRST, sortFields.get(0).nullOrder());
    Assertions.assertEquals(SortDirection.DESC, sortFields.get(1).direction());
    Assertions.assertEquals(NullOrder.NULLS_LAST, sortFields.get(1).nullOrder());
  }

  @Test
  void testOperationDataIcebergTable() {
    ColumnDTO[] columns = createColumns();
    String testTableName = GravitinoITUtils.genRandomName("test_table");
    SortOrder[] sortOrders = {
      SortOrder.fieldSortOrder(new String[] {columns[0].name()}, Direction.ASC, NullOrdering.FIRST),
      SortOrder.fieldSortOrder(new String[] {columns[2].name()}, Direction.DESC, NullOrdering.LAST)
    };
    Transform[] transforms = {
      day(new String[] {columns[1].name()}), Transforms.identity(columns[2])
    };
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, testTableName),
            columns,
            table_comment,
            createProperties(),
            transforms,
            Distribution.NONE,
            sortOrders);
    TableIdentifier tableIdentifier = TableIdentifier.of(schemaName, testTableName);
    List<String> values = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      values.add(
          String.format(
              "(%s, %s, %s)", i, "date_sub(current_date(), " + i + ")", "'data" + i + "'"));
    }
    // insert data
    String insertSQL =
        String.format(
            INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE, tableIdentifier, String.join(",", values));
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
          String.format("[%s,%s,data%s]", i + 1, previousDay.format(formatter), i + 1),
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
            String.format("[100,%s,data%s]", previousDay.format(formatter), 1),
            result[i].toString());
      } else {
        LocalDate previousDay = currentDate.minusDays(i + 2);
        Assertions.assertEquals(
            String.format("[%s,%s,data%s]", i + 2, previousDay.format(formatter), i + 2),
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
          String.format("[%s,%s,data%s]", i + 2, previousDay.format(formatter), i + 2),
          result[i].toString());
    }
  }

  @Test
  public void testOperatorSchemeProperties() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, "testCreatSchemaCheck");
    Map<String, String> prop = Maps.newHashMap();
    prop.put(IcebergSchemaPropertiesMetadata.COMMENT, "val1");
    prop.put("key2", "val2");

    // create
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> catalog.asSchemas().createSchema(ident, schema_comment, prop));
    Assertions.assertTrue(
        illegalArgumentException.getMessage().contains(IcebergSchemaPropertiesMetadata.COMMENT));
    prop.remove(IcebergSchemaPropertiesMetadata.COMMENT);
    catalog.asSchemas().createSchema(ident, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertFalse(
        loadSchema.properties().containsKey(IcebergSchemaPropertiesMetadata.COMMENT));
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));

    // alert
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> catalog.asSchemas().alterSchema(ident, SchemaChange.setProperty("comment", "v1")));

    Assertions.assertDoesNotThrow(
        () ->
            catalog.asSchemas().alterSchema(ident, SchemaChange.setProperty("comment-test", "v1")));
    Schema schema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals("v1", schema.properties().get("comment-test"));

    // drop
    Assertions.assertTrue(catalog.asSchemas().dropSchema(ident, false));
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> catalog.asSchemas().loadSchema(ident));
  }
}
