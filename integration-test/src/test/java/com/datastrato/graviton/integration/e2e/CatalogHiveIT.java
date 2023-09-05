/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.catalog.hive.HiveClientPool;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.integration.util.AbstractIT;
import com.datastrato.graviton.integration.util.GravitonITUtils;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
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
  static String HADOOP_USER_NAME = "hive";

  private static HiveClientPool hiveClientPool;

  @BeforeAll
  public static void startup() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);

    GravitonMetaLake[] gravitonMetaLakes = client.listMetalakes();

    hiveClientPool = new HiveClientPool(1, hiveConf);
    client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    createHiveTable();
  }

  @AfterAll
  public static void stop() {
    client.dropMetalake(NameIdentifier.of(metalakeName));
    if (hiveClientPool != null) {
      hiveClientPool.close();
    }
  }

  public static void createHiveTable() throws TException, InterruptedException {
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));

    // Create catalog from Graviton API
    Map<String, String> properties = Maps.newHashMap();
    properties.put("provider", "hive");
    properties.put(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);

    Catalog catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            "comment",
            properties);

    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> properties1 = Maps.newHashMap();
    properties1.put("key1", "val1");
    properties1.put("key2", "val2");
    String comment = "comment";

    catalog.asSchemas().createSchema(ident, comment, properties1);

    // Directly get database from hive metastore to verify the schema creation
    Database database = hiveClientPool.run(client -> client.getDatabase(schemaName));
    Assertions.assertEquals(schemaName.toLowerCase(), database.getName());
    Assertions.assertEquals(comment, database.getDescription());
    Assertions.assertEquals("val1", database.getParameters().get("key1"));
    Assertions.assertEquals("val2", database.getParameters().get("key2"));

    // Create table from Graviton API
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
    ColumnDTO[] columns = new ColumnDTO[] {col1, col2, col3};

    NameIdentifier nameIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, tableName);
    Map<String, String> properties2 = Maps.newHashMap();
    properties1.put("key2-1", "val1");
    properties1.put("key2-2", "val2");
    catalog.asTableCatalog().createTable(nameIdentifier, columns, table_comment, properties2);

    // Directly get table from hive metastore to check if the table is created successfully.
    org.apache.hadoop.hive.metastore.api.Table hiveTab =
        hiveClientPool.run(client -> client.getTable(schemaName, tableName));
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
  }

  @Order(1)
  @Test
  public void testAlterHiveTable() throws TException, InterruptedException {
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
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
    Assertions.assertEquals(null, hiveTab.getSd().getCols().get(3).getComment());
  }

  @Order(2)
  @Test
  public void testDropHiveTable() {
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
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
  //  @Order(3)
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

  @Order(4)
  @Test
  public void testDropHiveDB() {
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), true);

    assertThrows(
        NoSuchObjectException.class,
        () -> hiveClientPool.run(client -> client.getDatabase(schemaName)));
  }
}
