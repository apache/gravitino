/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.hive.HiveClientPool;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TrinoConnectorIT extends AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static HiveClientPool hiveClientPool;

  public static String metalakeName =
      GravitinoITUtils.genRandomName("TrinoIT_metalake").toLowerCase();
  public static String catalogName =
      GravitinoITUtils.genRandomName("TrinoIT_catalog").toLowerCase();
  public static String databaseName =
      GravitinoITUtils.genRandomName("TrinoIT_database").toLowerCase();
  public static String scenarioTab1Name =
      GravitinoITUtils.genRandomName("TrinoIT_table1").toLowerCase();
  public static String scenarioTab2Name =
      GravitinoITUtils.genRandomName("TrinoIT_table2").toLowerCase();
  public static String tab1Name = GravitinoITUtils.genRandomName("TrinoIT_table3").toLowerCase();
  private static GravitinoMetaLake metalake;
  private static Catalog catalog;

  @BeforeAll
  public static void startDockerContainer() throws IOException, TException, InterruptedException {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");

    containerSuite.startHiveContainer();

    // Initial hive client
    HiveConf hiveConf = new HiveConf();
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
    hiveClientPool = closer.register(new HiveClientPool(1, hiveConf));

    // Currently must first create metalake and catalog then start trino container
    createMetalake();
    createCatalog();

    containerSuite.startTrinoContainer(
        trinoConfDir,
        System.getenv("GRAVITINO_ROOT_DIR") + "/trino-connector/build/libs",
        new InetSocketAddress(AbstractIT.getPrimaryNICIp(), getGravitinoServerPort()),
        metalakeName);
    containerSuite.getTrinoContainer().checkSyncCatalogFromGravitino(5, metalakeName, catalogName);

    createSchema();
  }

  @AfterAll
  public static void stopDockerContainer() {
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public static void createSchema() throws TException, InterruptedException {
    String sql1 =
        String.format(
            "CREATE SCHEMA \"%s.%s\".%s WITH (\n"
                + "  location = 'hdfs://%s:%d/user/hive/warehouse/%s.db'\n"
                + ")",
            metalakeName,
            catalogName,
            databaseName,
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            databaseName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql1);
    NameIdentifier idSchema = NameIdentifier.of(metalakeName, catalogName, databaseName);
    Schema schema = catalog.asSchemas().loadSchema(idSchema);
    Assertions.assertEquals(schema.name(), databaseName);
    Database database = hiveClientPool.run(client -> client.getDatabase(databaseName));
    Assertions.assertEquals(databaseName, database.getName());
  }

  @Test
  @Order(1)
  public void testShowSchemas() {
    String sql =
        String.format(
            "SHOW SCHEMAS FROM \"%s.%s\" LIKE '%s'", metalakeName, catalogName, databaseName);
    ArrayList<ArrayList<String>> queryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql);
    Assertions.assertEquals(queryData.get(0).get(0), databaseName);
  }

  @Test
  @Order(2)
  public void testCreateTable() throws TException, InterruptedException {
    String sql3 =
        String.format(
            "CREATE TABLE \"%s.%s\".%s.%s (\n"
                + "  col1 varchar,\n"
                + "  col2 varchar,\n"
                + "  col3 varchar\n"
                + ")\n"
                + "WITH (\n"
                + "  format = 'TEXTFILE'\n"
                + ")",
            metalakeName, catalogName, databaseName, tab1Name);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql3);

    // Verify in Gravitino Server
    NameIdentifier idTable1 = NameIdentifier.of(metalakeName, catalogName, databaseName, tab1Name);
    Table table1 = catalog.asTableCatalog().loadTable(idTable1);
    Assertions.assertEquals(table1.name(), tab1Name);

    // Verify in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab1 =
        hiveClientPool.run(client -> client.getTable(databaseName, tab1Name));
    Assertions.assertEquals(databaseName, hiveTab1.getDbName());
    Assertions.assertEquals(tab1Name, hiveTab1.getTableName());
  }

  @Order(3)
  @Test
  public void testShowTable() {
    String sql =
        String.format(
            "SHOW TABLES FROM \"%s.%s\".%s LIKE '%s'",
            metalakeName, catalogName, databaseName, tab1Name);
    ArrayList<ArrayList<String>> queryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql);
    Assertions.assertEquals(queryData.get(0).get(0), tab1Name);
  }

  @Test
  @Order(4)
  public void testScenarioTable1() throws TException, InterruptedException {
    String sql3 =
        String.format(
            "CREATE TABLE \"%s.%s\".%s.%s (\n"
                + "  user_name varchar,\n"
                + "  gender varchar,\n"
                + "  age varchar,\n"
                + "  phone varchar,\n"
                + "  email varchar,\n"
                + "  address varchar,\n"
                + "  birthday varchar,\n"
                + "  create_time varchar,\n"
                + "  update_time varchar\n"
                + ")\n"
                + "WITH (\n"
                + "  format = 'TEXTFILE'\n"
                + ")",
            metalakeName, catalogName, databaseName, scenarioTab1Name);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql3);

    // Verify in Gravitino Server
    NameIdentifier idTable1 =
        NameIdentifier.of(metalakeName, catalogName, databaseName, scenarioTab1Name);
    Table table1 = catalog.asTableCatalog().loadTable(idTable1);
    Assertions.assertEquals(table1.name(), scenarioTab1Name);

    // Verify in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab1 =
        hiveClientPool.run(client -> client.getTable(databaseName, scenarioTab1Name));
    Assertions.assertEquals(databaseName, hiveTab1.getDbName());
    Assertions.assertEquals(scenarioTab1Name, hiveTab1.getTableName());

    // Insert data to table1
    ArrayList<ArrayList<String>> table1Data = new ArrayList<>();
    table1Data.add(new ArrayList<>(Arrays.asList("jake", "male", "30", "+1 6125047154")));
    table1Data.add(new ArrayList<>(Arrays.asList("jeff", "male", "25", "+1 2673800457")));
    table1Data.add(new ArrayList<>(Arrays.asList("rose", "female", "28", "+1 7073958726")));
    table1Data.add(new ArrayList<>(Arrays.asList("sam", "man", "18", "+1 8157809623")));
    StringBuilder sql5 =
        new StringBuilder(
            String.format(
                "INSERT INTO \"%s.%s\".%s.%s (user_name, gender, age, phone) VALUES",
                metalakeName, catalogName, databaseName, scenarioTab1Name));
    int index = 0;
    for (ArrayList<String> record : table1Data) {
      sql5.append(
          String.format(
              " ('%s', '%s', '%s', '%s'),",
              record.get(0), record.get(1), record.get(2), record.get(3)));
      if (index++ == table1Data.size() - 1) {
        // Delete the last comma
        sql5.deleteCharAt(sql5.length() - 1);
      }
    }
    containerSuite.getTrinoContainer().executeUpdateSQL(sql5.toString());

    // Select data from table1 and verify it
    String sql6 =
        String.format(
            "SELECT user_name, gender, age, phone FROM \"%s.%s\".%s.%s ORDER BY user_name",
            metalakeName, catalogName, databaseName, scenarioTab1Name);
    ArrayList<ArrayList<String>> table1QueryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql6);
    Assertions.assertEquals(table1Data, table1QueryData);
  }

  @Test
  @Order(5)
  public void testScenarioTable2() throws TException, InterruptedException {
    String sql4 =
        String.format(
            "CREATE TABLE \"%s.%s\".%s.%s (\n"
                + "  user_name varchar,\n"
                + "  consumer varchar,\n"
                + "  recharge varchar,\n"
                + "  event_time varchar,\n"
                + "  create_time varchar,\n"
                + "  update_time varchar\n"
                + ")\n"
                + "WITH (\n"
                + "  format = 'TEXTFILE'\n"
                + ")",
            metalakeName, catalogName, databaseName, scenarioTab2Name);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql4);

    // Verify in Gravitino Server
    NameIdentifier idTable2 =
        NameIdentifier.of(metalakeName, catalogName, databaseName, scenarioTab2Name);
    Table table2 = catalog.asTableCatalog().loadTable(idTable2);
    Assertions.assertEquals(table2.name(), scenarioTab2Name);

    // Verify in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab2 =
        hiveClientPool.run(client -> client.getTable(databaseName, scenarioTab2Name));
    Assertions.assertEquals(databaseName, hiveTab2.getDbName());
    Assertions.assertEquals(scenarioTab2Name, hiveTab2.getTableName());

    // Insert data to table2
    ArrayList<ArrayList<String>> table2Data = new ArrayList<>();
    table2Data.add(new ArrayList<>(Arrays.asList("jake", "", "$250", "22nd,July,2009")));
    table2Data.add(new ArrayList<>(Arrays.asList("jeff", "$40.25", "", "18nd,July,2023")));
    table2Data.add(new ArrayList<>(Arrays.asList("rose", "$28.45", "", "22nd,July,2023")));
    table2Data.add(new ArrayList<>(Arrays.asList("sam", "$27.03", "", "22nd,July,2023")));
    int index = 0;
    StringBuilder sql7 =
        new StringBuilder(
            String.format(
                "INSERT INTO \"%s.%s\".%s.%s (user_name, consumer, recharge, event_time) VALUES",
                metalakeName, catalogName, databaseName, scenarioTab2Name));
    for (ArrayList<String> record : table2Data) {
      sql7.append(
          String.format(
              " ('%s', '%s', '%s', '%s'),",
              record.get(0), record.get(1), record.get(2), record.get(3)));
      if (index++ == table2Data.size() - 1) {
        // Delete the last comma
        sql7.deleteCharAt(sql7.length() - 1);
      }
    }
    containerSuite.getTrinoContainer().executeUpdateSQL(sql7.toString());

    // Select data from table1 and verify it
    String sql8 =
        String.format(
            "SELECT user_name, consumer, recharge, event_time FROM \"%s.%s\".%s.%s ORDER BY user_name",
            metalakeName, catalogName, databaseName, scenarioTab2Name);
    ArrayList<ArrayList<String>> table2QueryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql8);
    Assertions.assertEquals(table2Data, table2QueryData);
  }

  @Test
  @Order(6)
  public void testScenarioJoinTwoTable() {
    String sql9 =
        String.format(
            "SELECT * FROM (SELECT t1.user_name as user_name, gender, age, phone, consumer, recharge, event_time FROM \"%1$s.%2$s\".%3$s.%4$s AS t1\n"
                + "JOIN\n"
                + "    (SELECT user_name, consumer, recharge, event_time FROM \"%1$s.%2$s\".%3$s.%5$s) AS t2\n"
                + "        ON t1.user_name = t2.user_name) ORDER BY user_name",
            metalakeName, catalogName, databaseName, scenarioTab1Name, scenarioTab2Name);
    ArrayList<ArrayList<String>> joinQueryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql9);
    ArrayList<ArrayList<String>> joinData = new ArrayList<>();
    joinData.add(
        new ArrayList<>(
            Arrays.asList("jake", "male", "30", "+1 6125047154", "", "$250", "22nd,July,2009")));
    joinData.add(
        new ArrayList<>(
            Arrays.asList("jeff", "male", "25", "+1 2673800457", "$40.25", "", "18nd,July,2023")));
    joinData.add(
        new ArrayList<>(
            Arrays.asList(
                "rose", "female", "28", "+1 7073958726", "$28.45", "", "22nd,July,2023")));
    joinData.add(
        new ArrayList<>(
            Arrays.asList("sam", "man", "18", "+1 8157809623", "$27.03", "", "22nd,July,2023")));
    Assertions.assertEquals(joinData, joinQueryData);
  }

  @Test
  void testHiveSchemaAndTableCreatedByTrino() {
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

    String createSchemaSql =
        String.format("CREATE SCHEMA \"%s.%s\".%s", metalakeName, catalogName, schemaName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createSchemaSql);

    String createTableSql =
        String.format(
            "CREATE TABLE \"%s.%s\".%s.%s (id int, name varchar)"
                + " with ( serde_name = '123455', location = 'hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table')",
            metalakeName, catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createTableSql);

    Table table =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Assertions.assertEquals("123455", table.properties().get("serde-name"));
    Assertions.assertEquals(
        "hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table",
        table.properties().get("location"));
  }

  @Test
  void testHiveSchemaAndTableCreatedByGravitino() throws InterruptedException {
    String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

    GravitinoMetaLake createdMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog =
        createdMetalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            "hive",
            "comment",
            ImmutableMap.<String, String>builder()
                .put(
                    "metastore.uris",
                    String.format(
                        "thrift://%s:%s",
                        containerSuite.getHiveContainer().getContainerIpAddress(),
                        HiveContainer.HIVE_METASTORE_PORT))
                .build());

    Schema schema =
        catalog
            .asSchemas()
            .createSchema(
                NameIdentifier.of(metalakeName, catalogName, schemaName),
                "Created by gravitino client",
                ImmutableMap.<String, String>builder().build());

    Assertions.assertNotNull(schema);

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            new ColumnDTO[] {
              new ColumnDTO.Builder<>()
                  .withComment("xx1")
                  .withName("id")
                  .withDataType(Types.IntegerType.get())
                  .build(),
              new ColumnDTO.Builder<>()
                  .withComment("xx2")
                  .withName("name")
                  .withDataType(Types.IntegerType.get())
                  .build()
            },
            "Created by gravitino client",
            ImmutableMap.<String, String>builder()
                .put("format", "ORC")
                .put(
                    "location",
                    "hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table")
                .put("serde-name", "yuqi11")
                .put("table-type", "EXTERNAL_TABLE")
                .build());
    LOG.info("create table \"{}.{}\".{}.{}", metalakeName, catalogName, schemaName, tableName);

    Table table =
        catalog
            .asTableCatalog()
            .loadTable(NameIdentifier.of(metalakeName, catalogName, schemaName, tableName));
    Assertions.assertNotNull(table);
    // Now we need to wait at least 3 seconds for trino to sync the metadata from gravitino
    Thread.sleep(6000);

    String sql =
        String.format(
            "show create table \"%s.%s\".%s.%s", metalakeName, catalogName, schemaName, tableName);

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(data.contains("serde_name = 'yuqi11'"));
    Assertions.assertTrue(data.contains("table_type = 'EXTERNAL_TABLE'"));
    Assertions.assertTrue(data.contains("serde_lib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"));
    Assertions.assertTrue(
        data.contains(
            "location = 'hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table'"));
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
    Map<String, String> properties = Maps.newHashMap();
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    LOG.debug("hiveMetastoreUris is {}", hiveMetastoreUris);
    properties.put(METASTORE_URIS, hiveMetastoreUris);

    Catalog createdCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            "hive",
            "comment",
            properties);
    Catalog loadCatalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }
}
