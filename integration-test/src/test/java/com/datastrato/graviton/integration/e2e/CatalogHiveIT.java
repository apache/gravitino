/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e;

import static com.datastrato.graviton.rel.transforms.Transforms.identity;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.catalog.hive.HiveClientPool;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.integration.util.AbstractIT;
import com.datastrato.graviton.integration.util.GravitonITUtils;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.Distribution.DistributionMethod;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrder;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.datastrato.graviton.rel.transforms.Transforms.NamedReference;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogHiveIT extends AbstractIT {
  public static String metalakeName = GravitonITUtils.genRandomName("CatalogHiveIT_metalake");
  public static String catalogName = GravitonITUtils.genRandomName("CatalogHiveIT_catalog");
  public static String schemaName = GravitonITUtils.genRandomName("CatalogHiveIT_schema");
  public static String tableName = GravitonITUtils.genRandomName("CatalogHiveIT_table");
  public static String alertTableName = "alert_table_name";
  public static String table_comment = "table_comment";
  public static String HIVE_COL_NAME1 = "hive_col_name1";
  public static String HIVE_COL_NAME2 = "hive_col_name2";
  public static String HIVE_COL_NAME3 = "hive_col_name3";

  static String HIVE_METASTORE_URIS = "thrift://localhost:9083";

  private static HiveClientPool hiveClientPool;

  private static GravitonMetaLake metalake;

  private static Catalog catalog;

  @BeforeAll
  public static void startup() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);
    hiveClientPool = new HiveClientPool(1, hiveConf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public static void stop() {
    client.dropMetalake(NameIdentifier.of(metalakeName));
    if (hiveClientPool != null) {
      hiveClientPool.close();
    }
  }

  @AfterEach
  private void resetSchema() throws TException, InterruptedException {
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), true);
    assertThrows(
        NoSuchObjectException.class,
        () -> hiveClientPool.run(client -> client.getDatabase(schemaName)));
    createSchema();
  }

  private static void createMetalake() {
    GravitonMetaLake[] gravitonMetaLakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitonMetaLakes.length);

    GravitonMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitonMetaLake loadMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("provider", "hive");
    properties.put(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);
    properties.put(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname, "30");
    properties.put(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES.varname, "30");
    properties.put(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname, "5");

    Catalog createdCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            "comment",
            properties);
    Catalog loadCatalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private static void createSchema() throws TException, InterruptedException {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> properties1 = Maps.newHashMap();
    properties1.put("key1", "val1");
    properties1.put("key2", "val2");
    String comment = "comment";

    Schema createdSchema = catalog.asSchemas().createSchema(ident, comment, properties1);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(createdSchema.name().toLowerCase(), loadSchema.name());

    // Directly get database from hive metastore to verify the schema creation
    Database database = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Assertions.assertEquals(schemaName.toLowerCase(), database.getName());
    Assertions.assertEquals(comment, database.getDescription());
    Assertions.assertEquals("val1", database.getParameters().get("key1"));
    Assertions.assertEquals("val2", database.getParameters().get("key2"));
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName(HIVE_COL_NAME1)
            .withDataType(TypeCreator.NULLABLE.I8)
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(HIVE_COL_NAME2)
            .withDataType(TypeCreator.NULLABLE.DATE)
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(HIVE_COL_NAME3)
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
  void testCreateHiveTableWithDistributionAndSortOrder() throws TException, InterruptedException {
    // Create table from Graviton API
    ColumnDTO[] columns = createColumns();

    NameIdentifier nameIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Distribution distribution =
        Distribution.builder()
            .withDistributionNumber(10)
            .withTransforms(new Transform[] {Transforms.field(new String[] {HIVE_COL_NAME1})})
            .withdistributionMethod(DistributionMethod.EVEN)
            .build();

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrder.builder()
              .withNullOrder(NullOrder.FIRST)
              .withDirection(Direction.DESC)
              .withTransform(Transforms.field(new String[] {HIVE_COL_NAME2}))
              .build()
        };

    Map<String, String> properties = createProperties();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                table_comment,
                properties,
                new Transform[0],
                distribution,
                sortOrders);

    // Directly get table from hive metastore to check if the table is created successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTab.getParameters().get(key)));
    assertTableEquals(createdTable, hiveTab);

    // test null partition
    resetSchema();
    Table createdTable1 =
        catalog
            .asTableCatalog()
            .createTable(nameIdentifier, columns, table_comment, properties, null);

    // Directly get table from hive metastore to check if the table is created successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTable1 =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
    properties
        .keySet()
        .forEach(
            key ->
                Assertions.assertEquals(properties.get(key), hiveTable1.getParameters().get(key)));
    assertTableEquals(createdTable1, hiveTable1);

    // Test bad request
    // Bad name in distribution
    final Distribution badDistribution =
        Distribution.builder()
            .withDistributionNumber(10)
            .withTransforms(
                new Transform[] {Transforms.field(new String[] {HIVE_COL_NAME1 + "bad_name"})})
            .withdistributionMethod(DistributionMethod.EVEN)
            .build();
    Assertions.assertThrows(
        Exception.class,
        () -> {
          catalog
              .asTableCatalog()
              .createTable(
                  nameIdentifier,
                  columns,
                  table_comment,
                  properties,
                  new Transform[0],
                  badDistribution,
                  sortOrders);
        });

    final SortOrder[] badSortOrders =
        new SortOrder[] {
          SortOrder.builder()
              .withNullOrder(NullOrder.FIRST)
              .withDirection(Direction.DESC)
              .withTransform(Transforms.field(new String[] {HIVE_COL_NAME2 + "bad_name"}))
              .build()
        };

    Assertions.assertThrows(
        Exception.class,
        () -> {
          catalog
              .asTableCatalog()
              .createTable(
                  nameIdentifier,
                  columns,
                  table_comment,
                  properties,
                  new Transform[0],
                  distribution,
                  badSortOrders);
        });
  }

  @Test
  public void testCreateHiveTable() throws TException, InterruptedException {
    // Create table from Graviton API
    ColumnDTO[] columns = createColumns();

    NameIdentifier nameIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Map<String, String> properties = createProperties();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(nameIdentifier, columns, table_comment, properties, new Transform[0]);

    // Directly get table from hive metastore to check if the table is created successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTab.getParameters().get(key)));
    assertTableEquals(createdTable, hiveTab);

    // test null partition
    resetSchema();
    Table createdTable1 =
        catalog
            .asTableCatalog()
            .createTable(nameIdentifier, columns, table_comment, properties, null);

    // Directly get table from hive metastore to check if the table is created successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTable1 =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
    properties
        .keySet()
        .forEach(
            key ->
                Assertions.assertEquals(properties.get(key), hiveTable1.getParameters().get(key)));
    assertTableEquals(createdTable1, hiveTable1);
  }

  @Test
  public void testCreatePartitionedHiveTable() throws TException, InterruptedException {
    // Create table from Graviton API
    ColumnDTO[] columns = createColumns();

    NameIdentifier nameIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Map<String, String> properties = createProperties();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                table_comment,
                properties,
                new Transform[] {identity(columns[0]), identity(columns[1])});

    // Directly get table from hive metastore to check if the table is created successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTab.getParameters().get(key)));
    assertTableEquals(createdTable, hiveTab);
  }

  private void assertTableEquals(
      Table createdTable, org.apache.hadoop.hive.metastore.api.Table hiveTab) {
    Distribution distribution = createdTable.distribution();
    SortOrder[] sortOrders = createdTable.sortOrder();
    Assertions.assertEquals(schemaName.toLowerCase(), hiveTab.getDbName());
    Assertions.assertEquals(tableName.toLowerCase(), hiveTab.getTableName());
    Assertions.assertEquals("MANAGED_TABLE", hiveTab.getTableType());
    Assertions.assertEquals(table_comment, hiveTab.getParameters().get("comment"));

    Assertions.assertEquals(HIVE_COL_NAME1, hiveTab.getSd().getCols().get(0).getName());
    Assertions.assertEquals("tinyint", hiveTab.getSd().getCols().get(0).getType());
    Assertions.assertEquals("col_1_comment", hiveTab.getSd().getCols().get(0).getComment());

    Assertions.assertEquals(HIVE_COL_NAME2, hiveTab.getSd().getCols().get(1).getName());
    Assertions.assertEquals("date", hiveTab.getSd().getCols().get(1).getType());
    Assertions.assertEquals("col_2_comment", hiveTab.getSd().getCols().get(1).getComment());

    Assertions.assertEquals(HIVE_COL_NAME3, hiveTab.getSd().getCols().get(2).getName());
    Assertions.assertEquals("string", hiveTab.getSd().getCols().get(2).getType());
    Assertions.assertEquals("col_3_comment", hiveTab.getSd().getCols().get(2).getComment());

    Assertions.assertEquals(
        distribution == null ? 0 : distribution.distributionNumber(),
        hiveTab.getSd().getNumBuckets());

    List<String> resultDistributionCols =
        distribution == null
            ? Collections.emptyList()
            : Arrays.stream(distribution.transforms())
                .map(t -> ((NamedReference) t).value()[0])
                .collect(Collectors.toList());
    Assertions.assertEquals(resultDistributionCols, hiveTab.getSd().getBucketCols());

    for (int i = 0; i < sortOrders.length; i++) {
      Assertions.assertEquals(
          sortOrders[i].getDirection() == Direction.ASC ? 0 : 1,
          hiveTab.getSd().getSortCols().get(i).getOrder());
      Assertions.assertEquals(
          ((NamedReference) sortOrders[i].getTransform()).value()[0],
          hiveTab.getSd().getSortCols().get(i).getCol());
    }
    Assertions.assertNotNull(createdTable.partitioning());
    Assertions.assertEquals(createdTable.partitioning().length, hiveTab.getPartitionKeys().size());
    List<String> partitionKeys =
        Arrays.stream(createdTable.partitioning())
            .map(p -> ((Transforms.NamedReference) p).value()[0])
            .collect(Collectors.toList());
    List<String> hivePartitionKeys =
        hiveTab.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    Assertions.assertEquals(partitionKeys, hivePartitionKeys);
  }

  @Test
  public void testAlterHiveTable() throws TException, InterruptedException {
    ColumnDTO[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            columns,
            table_comment,
            createProperties(),
            new Transform[] {identity(columns[0])});
    catalog
        .asTableCatalog()
        .alterTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            TableChange.rename(alertTableName),
            TableChange.updateComment(table_comment + "_new"),
            TableChange.removeProperty("key1"),
            TableChange.setProperty("key2", "val2_new"),
            TableChange.addColumn(new String[] {"col_4"}, TypeCreator.NULLABLE.STRING),
            TableChange.renameColumn(new String[] {HIVE_COL_NAME2}, "col_2_new"),
            TableChange.updateColumnComment(new String[] {HIVE_COL_NAME1}, "comment_new"),
            TableChange.updateColumnType(new String[] {HIVE_COL_NAME1}, TypeCreator.NULLABLE.I32));

    // Direct get table from hive metastore to check if the table is altered successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, alertTableName));
    Assertions.assertEquals(schemaName.toLowerCase(), hiveTab.getDbName());
    Assertions.assertEquals(alertTableName, hiveTab.getTableName());
    Assertions.assertEquals("val2_new", hiveTab.getParameters().get("key2"));

    Assertions.assertEquals(HIVE_COL_NAME1, hiveTab.getSd().getCols().get(0).getName());
    Assertions.assertEquals("int", hiveTab.getSd().getCols().get(0).getType());
    Assertions.assertEquals("comment_new", hiveTab.getSd().getCols().get(0).getComment());

    Assertions.assertEquals("col_2_new", hiveTab.getSd().getCols().get(1).getName());
    Assertions.assertEquals("date", hiveTab.getSd().getCols().get(1).getType());
    Assertions.assertEquals("col_2_comment", hiveTab.getSd().getCols().get(1).getComment());

    Assertions.assertEquals(HIVE_COL_NAME3, hiveTab.getSd().getCols().get(2).getName());
    Assertions.assertEquals("string", hiveTab.getSd().getCols().get(2).getType());
    Assertions.assertEquals("col_3_comment", hiveTab.getSd().getCols().get(2).getComment());

    Assertions.assertEquals("col_4", hiveTab.getSd().getCols().get(3).getName());
    Assertions.assertEquals("string", hiveTab.getSd().getCols().get(3).getType());
    Assertions.assertNull(hiveTab.getSd().getCols().get(3).getComment());

    Assertions.assertEquals(1, hiveTab.getPartitionKeys().size());
    Assertions.assertEquals(columns[0].name(), hiveTab.getPartitionKeys().get(0).getName());
  }

  @Test
  public void testDropHiveTable() {
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            createColumns(),
            table_comment,
            createProperties(),
            new Transform[0]);
    catalog
        .asTableCatalog()
        .dropTable(NameIdentifier.of(metalakeName, catalogName, schemaName, alertTableName));

    // Directly get table from hive metastore to check if the table is dropped successfully.
    assertThrows(
        NoSuchObjectException.class,
        () -> hiveClientPool.run(client -> client.getTable(schemaName, alertTableName)));
  }

  // TODO (xun) enable this test waiting for fixed [#316] [Bug report] alterSchema throw
  // NoSuchSchemaException
  //  @Test
  public void testAlterSchema() throws TException, InterruptedException {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    catalog
        .asSchemas()
        .alterSchema(
            ident,
            SchemaChange.removeProperty("key1"),
            SchemaChange.setProperty("key2", "val2-alter"));

    NameIdentifier[] nameIdentifiers = catalog.asSchemas().listSchemas(ident.namespace());

    Map<String, String> properties2 = catalog.asSchemas().loadSchema(ident).properties();
    Assertions.assertFalse(properties2.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties2.get("key2"));

    Database database = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Map<String, String> properties3 = database.getParameters();
    Assertions.assertFalse(properties3.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties3.get("key2"));
  }
}
