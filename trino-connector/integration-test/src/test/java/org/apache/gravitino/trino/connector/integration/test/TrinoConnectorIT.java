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
package org.apache.gravitino.trino.connector.integration.test;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.TrinoContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated */
@Disabled
@Deprecated
@Tag("gravitino-docker-test")
public class TrinoConnectorIT extends BaseIT {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

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
  private static GravitinoMetalake metalake;
  private static Catalog catalog;

  @BeforeAll
  public void startDockerContainer() throws TException, InterruptedException {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");

    containerSuite.startHiveContainer();

    // Initial Hive client
    HiveConf hiveConf = new HiveConf();
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);

    // Currently must first create metalake and catalog then start Trino container
    createMetalake();
    createCatalog();

    containerSuite.startTrinoContainer(
        trinoConfDir,
        System.getenv("GRAVITINO_ROOT_DIR") + "/trino-connector/build/libs",
        getGravitinoServerPort(),
        metalakeName);
    Assertions.assertTrue(
        containerSuite.getTrinoContainer().checkSyncCatalogFromGravitino(5, catalogName),
        "Can not synchronize catalogs from gravitino");

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
            "CREATE SCHEMA %s.%s WITH (\n"
                + "  location = 'hdfs://%s:%d/user/hive/warehouse/%s.db'\n"
                + ")",
            catalogName,
            databaseName,
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            databaseName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql1);
    Schema schema = catalog.asSchemas().loadSchema(databaseName);
    Assertions.assertEquals(schema.name(), databaseName);

    ArrayList<ArrayList<String>> r =
        containerSuite
            .getTrinoContainer()
            .executeQuerySQL(
                String.format("show schemas from %s like '%s'", catalogName, databaseName));
    Assertions.assertEquals(r.get(0).get(0), databaseName);
  }

  @Test
  public void testShowSchemas() {
    String sql = String.format("SHOW SCHEMAS FROM %s LIKE '%s'", catalogName, databaseName);
    ArrayList<ArrayList<String>> queryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql);
    Assertions.assertEquals(queryData.get(0).get(0), databaseName);
  }

  @Test
  public void testCreateTable() {
    String sql3 =
        String.format(
            "CREATE TABLE %s.%s.%s (\n"
                + "  col1 varchar,\n"
                + "  col2 varchar,\n"
                + "  col3 varchar\n"
                + ")\n"
                + "WITH (\n"
                + "  format = 'TEXTFILE'\n"
                + ")",
            catalogName, databaseName, tab1Name);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql3);

    // Verify in Gravitino Server
    NameIdentifier idTable1 = NameIdentifier.of(metalakeName, catalogName, databaseName, tab1Name);
    Table table1 = catalog.asTableCatalog().loadTable(idTable1);
    Assertions.assertEquals(table1.name(), tab1Name);

    verifySchemaAndTable(databaseName, tab1Name);

    testShowTable();
  }

  private void testShowTable() {
    String sql =
        String.format("SHOW TABLES FROM %s.%s LIKE '%s'", catalogName, databaseName, tab1Name);
    ArrayList<ArrayList<String>> queryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql);
    Assertions.assertEquals(queryData.get(0).get(0), tab1Name);
  }

  private void verifySchemaAndTable(String dbName, String tableName) {
    // Verify in Hive Server
    ArrayList<ArrayList<String>> r =
        containerSuite
            .getTrinoContainer()
            .executeQuerySQL(String.format("show schemas from %s like '%s'", catalogName, dbName));
    Assertions.assertEquals(r.get(0).get(0), dbName);

    // Compare table
    r =
        containerSuite
            .getTrinoContainer()
            .executeQuerySQL(
                String.format("show create table %s.%s.%s", catalogName, dbName, tableName));
    Assertions.assertTrue(r.get(0).get(0).contains(tableName));
  }

  public void testScenarioTable1() {
    String sql3 =
        String.format(
            "CREATE TABLE %s.%s.%s (\n"
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
            catalogName, databaseName, scenarioTab1Name);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql3);

    // Verify in Gravitino Server
    NameIdentifier idTable1 = NameIdentifier.of(databaseName, scenarioTab1Name);
    Table table1 = catalog.asTableCatalog().loadTable(idTable1);
    Assertions.assertEquals(table1.name(), scenarioTab1Name);

    verifySchemaAndTable(databaseName, scenarioTab1Name);

    // Insert data to table1
    ArrayList<ArrayList<String>> table1Data = new ArrayList<>();
    table1Data.add(new ArrayList<>(Arrays.asList("jake", "male", "30", "+1 6125047154")));
    table1Data.add(new ArrayList<>(Arrays.asList("jeff", "male", "25", "+1 2673800457")));
    table1Data.add(new ArrayList<>(Arrays.asList("rose", "female", "28", "+1 7073958726")));
    table1Data.add(new ArrayList<>(Arrays.asList("sam", "man", "18", "+1 8157809623")));
    StringBuilder sql5 =
        new StringBuilder(
            String.format(
                "INSERT INTO %s.%s.%s (user_name, gender, age, phone) VALUES",
                catalogName, databaseName, scenarioTab1Name));
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
            "SELECT user_name, gender, age, phone FROM %s.%s.%s ORDER BY user_name",
            catalogName, databaseName, scenarioTab1Name);
    ArrayList<ArrayList<String>> table1QueryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql6);
    Assertions.assertEquals(table1Data, table1QueryData);
  }

  public void testScenarioTable2() {
    String sql4 =
        String.format(
            "CREATE TABLE %s.%s.%s (\n"
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
            catalogName, databaseName, scenarioTab2Name);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql4);

    // Verify in Gravitino Server
    NameIdentifier idTable2 = NameIdentifier.of(databaseName, scenarioTab2Name);
    Table table2 = catalog.asTableCatalog().loadTable(idTable2);
    Assertions.assertEquals(table2.name(), scenarioTab2Name);

    verifySchemaAndTable(databaseName, scenarioTab2Name);

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
                "INSERT INTO %s.%s.%s (user_name, consumer, recharge, event_time) VALUES",
                catalogName, databaseName, scenarioTab2Name));
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
            "SELECT user_name, consumer, recharge, event_time FROM %s.%s.%s ORDER BY user_name",
            catalogName, databaseName, scenarioTab2Name);
    ArrayList<ArrayList<String>> table2QueryData =
        containerSuite.getTrinoContainer().executeQuerySQL(sql8);
    Assertions.assertEquals(table2Data, table2QueryData);
  }

  @Test
  public void testScenarioJoinTwoTable() {
    testScenarioTable1();
    testScenarioTable2();

    String sql9 =
        String.format(
            "SELECT * FROM (SELECT t1.user_name as user_name, gender, age, phone, consumer, recharge, event_time FROM %1s.%2$s.%3$s AS t1\n"
                + "JOIN\n"
                + "    (SELECT user_name, consumer, recharge, event_time FROM %1$s.%2$s.%4$s) AS t2\n"
                + "        ON t1.user_name = t2.user_name) ORDER BY user_name",
            catalogName, databaseName, scenarioTab1Name, scenarioTab2Name);
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
  void testHiveSchemaCreatedByTrino() {
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();

    String createSchemaSql =
        String.format(
            "CREATE SCHEMA %s.%s with( location = 'hdfs://localhost:9000/user/hive/warehouse/hive_schema_1123123')",
            catalogName, schemaName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createSchemaSql);

    Schema schema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(
        "hdfs://localhost:9000/user/hive/warehouse/hive_schema_1123123",
        schema.properties().get("location"));
  }

  @Test
  void testHiveTableCreatedByTrino() {
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

    String createSchemaSql = String.format("CREATE SCHEMA %s.%s", catalogName, schemaName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createSchemaSql);

    String createTableSql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int, name varchar)"
                + " with ( serde_name = '123455', location = 'hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table'"
                + ", partitioned_by = ARRAY['name'], bucketed_by = ARRAY['id'], bucket_count = 50, sorted_by = ARRAY['name']"
                + ")",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createTableSql);

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertEquals("123455", table.properties().get("serde-name"));
    Assertions.assertEquals(
        "hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table",
        table.properties().get("location"));
    Assertions.assertEquals("MANAGED_TABLE", table.properties().get("table-type"));
    Assertions.assertEquals(
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", table.properties().get("serde-lib"));
    Assertions.assertEquals(
        "org.apache.hadoop.mapred.TextInputFormat", table.properties().get("input-format"));
    Assertions.assertEquals(
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        table.properties().get("output-format"));

    Distribution distribution = table.distribution();
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(50, distribution.number());
    Assertions.assertEquals(
        "id", ((NamedReference.FieldReference) distribution.expressions()[0]).fieldName()[0]);

    Assertions.assertEquals(1, table.partitioning().length);
    Transform transform = table.partitioning()[0];
    Assertions.assertEquals(Transforms.NAME_OF_IDENTITY, transform.name());
    Assertions.assertInstanceOf(Transforms.IdentityTransform.class, transform);
    Assertions.assertEquals("name", ((Transforms.IdentityTransform) transform).fieldName()[0]);

    Assertions.assertEquals(1, table.sortOrder().length);
    SortOrder sortOrder = table.sortOrder()[0];
    Assertions.assertEquals(
        "name", ((NamedReference.FieldReference) sortOrder.expression()).fieldName()[0]);
  }

  @Test
  void testHiveSchemaCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();

    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    Catalog catalog =
        createdMetalake.createCatalog(
            catalogName,
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

    catalog
        .asSchemas()
        .createSchema(
            schemaName,
            "Created by gravitino client",
            ImmutableMap.<String, String>builder()
                .put("location", "hdfs://localhost:9000/user/hive/warehouse/hive_schema_1223445.db")
                .build());

    String sql = String.format("show create schema %s.%s", catalogName, schemaName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load schema created by gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(
        data.contains(
            "location = 'hdfs://localhost:9000/user/hive/warehouse/hive_schema_1223445.db'"));
  }

  private static boolean checkTrinoHasRemoved(String sql, long maxWaitTimeSec) {
    await()
        .atMost(maxWaitTimeSec, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              try {
                ArrayList<ArrayList<String>> lists =
                    containerSuite.getTrinoContainer().executeQuerySQL(sql);
                if (lists.isEmpty()) {
                  return true;
                }

                LOG.info(
                    "Catalog has not synchronized yet, wait 200ms and retry. The SQL is '{}'", sql);
              } catch (Exception e) {
                LOG.warn("Failed to execute sql: {}", sql, e);
              }
              return false;
            });

    return true;
  }

  private static boolean checkTrinoHasLoaded(String sql, long maxWaitTimeSec) {
    await()
        .atMost(maxWaitTimeSec, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              try {
                ArrayList<ArrayList<String>> lists =
                    containerSuite.getTrinoContainer().executeQuerySQL(sql);
                if (!lists.isEmpty()) {
                  return true;
                }

                LOG.info(
                    "Trino has not load the data yet, wait 200ms and retry. The SQL is '{}'", sql);
              } catch (Exception e) {
                LOG.warn("Failed to execute sql: {}", sql, e);
              }
              return false;
            });

    return true;
  }

  private Column[] createHiveFullTypeColumns() {
    Column[] columns = createFullTypeColumns();
    Set<String> unsupportedType = Sets.newHashSet("FixedType", "StringType", "TimeType");
    // MySQL doesn't support timestamp time zone
    return Arrays.stream(columns)
        .filter(c -> !unsupportedType.contains(c.name()))
        .toArray(Column[]::new);
  }

  private Column[] createMySQLFullTypeColumns() {
    Column[] columns = createFullTypeColumns();
    Set<String> unsupportedType =
        Sets.newHashSet("FixedType", "StringType", "TimestampType", "BooleanType");
    // MySQL doesn't support timestamp time zone
    return Arrays.stream(columns)
        .filter(c -> !unsupportedType.contains(c.name()))
        .toArray(Column[]::new);
  }

  private Column[] createIcebergFullTypeColumns() {
    Column[] columns = createFullTypeColumns();

    Set<String> unsupportedType =
        Sets.newHashSet("ByteType", "ShortType", "VarCharType", "FixedCharType");
    return Arrays.stream(columns)
        .filter(c -> !unsupportedType.contains(c.name()))
        .toArray(Column[]::new);
  }

  private Column[] createFullTypeColumns() {
    // Generate all types of columns that in class Types
    return new Column[] {
      // Int type
      Column.of("BooleanType", Types.BooleanType.get()),
      Column.of("ByteType", Types.ByteType.get()),
      Column.of("ShortType", Types.ShortType.get()),
      Column.of("IntegerType", Types.IntegerType.get()),
      Column.of("LongType", Types.LongType.get()),

      // float type
      Column.of("FloatType", Types.FloatType.get()),
      Column.of("DoubleType", Types.DoubleType.get()),
      Column.of("DecimalType", Types.DecimalType.of(10, 3)),

      // Date Type
      Column.of("DateType", Types.DateType.get()),
      Column.of("TimeType", Types.TimeType.get()),
      Column.of("TimestampType", Types.TimestampType.withTimeZone()),

      // String Type
      Column.of("VarCharType", Types.VarCharType.of(100)),
      Column.of("FixedCharType", Types.FixedCharType.of(100)),
      Column.of("FixedType", Types.FixedType.of(1000)),

      // Binary Type
      Column.of("BinaryType", Types.BinaryType.get())
      // No Interval Type and complex type like map, struct, and list
    };
  }

  @Test
  void testColumnTypeNotNullByTrino() {
    String catalogName = GravitinoITUtils.genRandomName("mysql_catalog").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123", // username and password are referred from Hive dockerfile.
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };

    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    String hiveHost = containerSuite.getHiveContainer().getContainerIpAddress();

    createdMetalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "jdbc-mysql",
        "comment",
        ImmutableMap.<String, String>builder()
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-url", String.format("jdbc:mysql://%s:3306?useSSL=false", hiveHost))
            .build());

    String sql = String.format("show catalogs like '%s'", catalogName);
    Assertions.assertTrue(checkTrinoHasLoaded(sql, 30));

    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();
    String createSchemaSql = String.format("CREATE SCHEMA %s.%s", catalogName, schemaName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createSchemaSql);

    sql = String.format("show create schema %s.%s", catalogName, schemaName);
    Assertions.assertTrue(checkTrinoHasLoaded(sql, 30));

    String createTableSql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int not null, name varchar not null)",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createTableSql);

    String showCreateTableSql =
        String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);
    ArrayList<ArrayList<String>> rs =
        containerSuite.getTrinoContainer().executeQuerySQL(showCreateTableSql);
    Assertions.assertTrue(rs.get(0).get(0).toLowerCase(Locale.ENGLISH).contains("not null"));

    containerSuite
        .getTrinoContainer()
        .executeUpdateSQL(
            String.format(
                "insert into %s.%s.%s values(1, 'a')", catalogName, schemaName, tableName));
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            containerSuite
                .getTrinoContainer()
                .executeUpdateSQL(
                    String.format(
                        "insert into %s.%s.%s values(null, 'a')",
                        catalogName, schemaName, tableName)));
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            containerSuite
                .getTrinoContainer()
                .executeUpdateSQL(
                    String.format(
                        "insert into %s.%s.%s values(1, null)",
                        catalogName, schemaName, tableName)));
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            containerSuite
                .getTrinoContainer()
                .executeUpdateSQL(
                    String.format(
                        "insert into %s.%s.%s values(null, null)",
                        catalogName, schemaName, tableName)));

    catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
  }

  @Test
  void testHiveTableCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    Catalog catalog =
        createdMetalake.createCatalog(
            catalogName,
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
                schemaName,
                "Created by gravitino client",
                ImmutableMap.<String, String>builder().build());

    Assertions.assertNotNull(schema);

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createHiveFullTypeColumns(),
            "Created by gravitino client",
            ImmutableMap.<String, String>builder()
                .put("format", "ORC")
                .put(
                    "location",
                    "hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table")
                .put("serde-name", "mock11")
                .put("table-type", "EXTERNAL_TABLE")
                .build(),
            new Transform[] {Transforms.identity("BinaryType")},
            Distributions.of(Strategy.HASH, 4, NamedReference.field("BooleanType")),
            new SortOrder[] {
              SortOrders.of(
                  NamedReference.field("LongType"),
                  SortDirection.ASCENDING,
                  NullOrdering.NULLS_FIRST)
            });
    LOG.info("create table \"{}.{}\".{}.{}", metalakeName, catalogName, schemaName, tableName);

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertNotNull(table);

    String sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load table created by gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(data.contains("serde_name = 'mock11'"));
    Assertions.assertTrue(data.contains("table_type = 'EXTERNAL_TABLE'"));
    Assertions.assertTrue(data.contains("serde_lib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"));
    Assertions.assertTrue(
        data.contains(
            "location = 'hdfs://localhost:9000/user/hive/warehouse/hive_schema.db/hive_table'"));
    Assertions.assertTrue(
        data.contains("input_format = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'"));
    Assertions.assertTrue(data.contains("serde_lib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"));
    Assertions.assertTrue(data.contains("bucket_count = 4"));
    Assertions.assertTrue(data.contains("bucketed_by = ARRAY['booleantype']"));
    Assertions.assertTrue(data.contains("partitioned_by = ARRAY['binarytype']"));
    Assertions.assertTrue(data.contains("sorted_by = ARRAY['longtype']"));

    // Test table format issues.
    tableName = GravitinoITUtils.genRandomName("table_format1").toLowerCase();
    sql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int, name varchar) with (format = 'ORC')",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);
    Assertions.assertTrue(checkTrinoHasLoaded(sql, 30), "Trino fail to create table:" + tableName);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(
        data.contains("input_format = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'"));
    Assertions.assertTrue(
        data.contains("output_format = 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"));
    Assertions.assertTrue(data.contains("serde_lib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"));

    // Test input_format can overwrite format setting
    tableName = GravitinoITUtils.genRandomName("table_format2").toLowerCase();
    sql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int, name varchar) with (format = 'ORC', input_format = 'org.apache.hadoop.mapred.TextInputFormat')",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);
    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);
    Assertions.assertTrue(checkTrinoHasLoaded(sql, 30), "Trino fail to create table:" + tableName);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(
        data.contains("input_format = 'org.apache.hadoop.mapred.TextInputFormat'"));
    Assertions.assertTrue(
        data.contains("output_format = 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"));
    Assertions.assertTrue(data.contains("serde_lib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"));

    // Test output_format can overwrite format setting
    tableName = GravitinoITUtils.genRandomName("table_format3").toLowerCase();
    sql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int, name varchar) with (format = 'ORC', output_format = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat')",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);
    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);
    Assertions.assertTrue(checkTrinoHasLoaded(sql, 30), "Trino fail to create table:" + tableName);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(
        data.contains("input_format = 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'"));
    Assertions.assertTrue(
        data.contains(
            "output_format = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"));
    Assertions.assertTrue(data.contains("serde_lib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'"));
  }

  @Test
  void testHiveCatalogCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    createdMetalake.createCatalog(
        catalogName,
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
            .put("trino.bypass.hive.immutable-partitions", "true")
            .put("trino.bypass.hive.target-max-file-size", "1GB")
            .put("trino.bypass.hive.create-empty-bucket-files", "true")
            .put("trino.bypass.hive.validate-bucketing", "true")
            .build());
    Catalog catalog = createdMetalake.loadCatalog(catalogName);
    Assertions.assertEquals(
        "true", catalog.properties().get("trino.bypass.hive.immutable-partitions"));
    Assertions.assertEquals(
        "1GB", catalog.properties().get("trino.bypass.hive.target-max-file-size"));
    Assertions.assertEquals(
        "true", catalog.properties().get("trino.bypass.hive.create-empty-bucket-files"));
    Assertions.assertEquals(
        "true", catalog.properties().get("trino.bypass.hive.validate-bucketing"));

    String sql = String.format("show catalogs like '%s'", catalogName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql);
    }

    // Because we assign 'hive.target-max-file-size' a wrong value, Trino can't load the catalog
    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    Assertions.assertEquals(catalogName, data);
  }

  @Test
  void testWrongHiveCatalogProperty() {
    String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    createdMetalake.createCatalog(
        catalogName,
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
            .put("trino.bypass.hive.immutable-partitions", "true")
            // it should be like '1GB, 1MB', we make it wrong purposely.
            .put("trino.bypass.hive.target-max-file-size", "xxxx")
            .put("trino.bypass.hive.create-empty-bucket-files", "true")
            .put("trino.bypass.hive.validate-bucketing", "true")
            .build());
    Catalog catalog = createdMetalake.loadCatalog(catalogName);
    Assertions.assertEquals(
        "true", catalog.properties().get("trino.bypass.hive.immutable-partitions"));
    Assertions.assertEquals(
        "xxxx", catalog.properties().get("trino.bypass.hive.target-max-file-size"));
    Assertions.assertEquals(
        "true", catalog.properties().get("trino.bypass.hive.create-empty-bucket-files"));
    Assertions.assertEquals(
        "true", catalog.properties().get("trino.bypass.hive.validate-bucketing"));

    String sql = String.format("show catalogs like '%s'", catalogName);
    await().atLeast(6, TimeUnit.SECONDS);

    // Because we assign 'hive.target-max-file-size' a wrong value, Trino can't load the catalog
    Assertions.assertTrue(containerSuite.getTrinoContainer().executeQuerySQL(sql).isEmpty());
  }

  @Test
  void testIcebergTableAndSchemaCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String hiveContainerIp = containerSuite.getHiveContainer().getContainerIpAddress();
    Catalog catalog =
        createdMetalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            ImmutableMap.<String, String>builder()
                .put(
                    "uri",
                    String.format(
                        "thrift://%s:%s", hiveContainerIp, HiveContainer.HIVE_METASTORE_PORT))
                .put(
                    "warehouse",
                    String.format("hdfs://%s:9000/user/hive/warehouse", hiveContainerIp))
                .put("catalog-backend", "hive")
                .build());

    Schema schema =
        catalog
            .asSchemas()
            .createSchema(
                schemaName,
                "Created by gravitino client",
                ImmutableMap.<String, String>builder().build());

    Assertions.assertNotNull(schema);

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createIcebergFullTypeColumns(),
            "Created by gravitino client",
            ImmutableMap.<String, String>builder()
                .put("format-version", "1")
                .put("key1", "value1")
                .build(),
            new Transform[] {Transforms.identity("BinaryType")},
            Distributions.NONE,
            new SortOrder[] {
              SortOrders.of(
                  NamedReference.field("LongType"),
                  SortDirection.ASCENDING,
                  NullOrdering.NULLS_FIRST)
            });
    LOG.info("create table \"{}.{}\".{}.{}", metalakeName, catalogName, schemaName, tableName);

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertNotNull(table);

    String sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);

    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load table created by Gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    LOG.info("create Iceberg Hive table SQL is: {}", data);
    // Iceberg does not contain any properties;
    Assertions.assertFalse(data.contains("key1"));
    Assertions.assertTrue(data.contains("partitioning = ARRAY['BinaryType']"));
    Assertions.assertTrue(data.contains("sorted_by = ARRAY['LongType']"));

    String tableCreatedByTrino = GravitinoITUtils.genRandomName("table").toLowerCase();
    String createTableSql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int, name varchar) with (partitioning = ARRAY['name'], sorted_by = ARRAY['id'])",
            catalogName, schemaName, tableCreatedByTrino);
    containerSuite.getTrinoContainer().executeUpdateSQL(createTableSql);

    table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableCreatedByTrino));

    Arrays.stream(table.partitioning())
        .anyMatch(p -> ((Transform.SingleFieldTransform) p).fieldName()[0].equals("name"));
    Arrays.stream(table.sortOrder())
        .anyMatch(
            p -> ((NamedReference.FieldReference) p.expression()).fieldName()[0].equals("id"));
  }

  @Test
  void testIcebergTableAndSchemaCreatedByTrino() {
    String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

    String createSchemaSql = String.format("CREATE SCHEMA %s.%s", catalogName, schemaName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createSchemaSql);

    String createTableSql =
        String.format(
            "CREATE TABLE %s.%s.%s (id int, name varchar)", catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(createTableSql);

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertNotNull(table);
  }

  @Test
  void testIcebergCatalogCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("iceberg_catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("iceberg_catalog").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123", // username and password are referred from Hive dockerfile
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };

    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    String hiveHost = containerSuite.getHiveContainer().getContainerIpAddress();

    createdMetalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        "comment",
        ImmutableMap.<String, String>builder()
            .put(
                "uri",
                String.format(
                    "jdbc:mysql://%s:3306/metastore_db?createDatabaseIfNotExist=true", hiveHost))
            .put("warehouse", "file:///tmp/iceberg")
            .put("catalog-backend", "jdbc")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .build());
    Catalog catalog = createdMetalake.loadCatalog(catalogName);
    Assertions.assertEquals("root", catalog.properties().get("jdbc-user"));

    String sql = String.format("show catalogs like '%s'", catalogName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    Assertions.assertEquals(catalogName, data);

    catalog
        .asSchemas()
        .createSchema(
            schemaName,
            "Created by gravitino client",
            ImmutableMap.<String, String>builder().build());

    sql = String.format("show schemas in %s like '%s'", catalogName, schemaName);
    Assertions.assertTrue(checkTrinoHasLoaded(sql, 30));

    final String sql1 = String.format("drop schema %s.%s cascade", catalogName, schemaName);
    // Will fail because the Iceberg catalog does not support cascade drop
    TrinoContainer trinoContainer = containerSuite.getTrinoContainer();
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          trinoContainer.executeUpdateSQL(sql1);
        });

    final String sql2 = String.format("show schemas in %s like '%s'", catalogName, schemaName);
    success = checkTrinoHasLoaded(sql2, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql2);
    }

    // Do not support the cascade drop
    Throwable excep =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalog.asSchemas().dropSchema(schemaName, true));
    Assertions.assertTrue(
        excep.getMessage().contains("Iceberg does not support cascading delete operations."));
    final String sql3 = String.format("show schemas in %s like '%s'", catalogName, schemaName);
    success = checkTrinoHasLoaded(sql3, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql);
    }
  }

  @Test
  void testMySQLCatalogCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("mysql_catalog").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123", // username and password are referred from Hive dockerfile.
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };

    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    String hiveHost = containerSuite.getHiveContainer().getContainerIpAddress();

    createdMetalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "jdbc-mysql",
        "comment",
        ImmutableMap.<String, String>builder()
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-url", String.format("jdbc:mysql://%s:3306?useSSL=false", hiveHost))
            .build());
    Catalog catalog = createdMetalake.loadCatalog(catalogName);
    Assertions.assertEquals("root", catalog.properties().get("jdbc-user"));

    String sql = String.format("show catalogs like '%s'", catalogName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    Assertions.assertEquals(catalogName, data);
  }

  @Test
  void testMySQLTableCreatedByGravitino() {
    String catalogName = GravitinoITUtils.genRandomName("mysql_catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("mysql_schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("mysql_table").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123", // username and password are referred from Hive dockerfile.
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };

    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    String hiveHost = containerSuite.getHiveContainer().getContainerIpAddress();

    createdMetalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "jdbc-mysql",
        "comment",
        ImmutableMap.<String, String>builder()
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-url", String.format("jdbc:mysql://%s:3306?useSSL=false", hiveHost))
            .build());
    Catalog catalog = createdMetalake.loadCatalog(catalogName);
    Assertions.assertEquals("root", catalog.properties().get("jdbc-user"));

    String sql = String.format("show catalogs like '%s'", catalogName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    Assertions.assertEquals(catalogName, data);

    Schema schema =
        catalog
            .asSchemas()
            .createSchema(schemaName, null, ImmutableMap.<String, String>builder().build());

    Assertions.assertNotNull(schema);

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createMySQLFullTypeColumns(),
            "Created by gravitino client",
            ImmutableMap.<String, String>builder().build());
    LOG.info("create table \"{}.{}\".{}.{}", metalakeName, catalogName, schemaName, tableName);

    Table table = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));
    Assertions.assertNotNull(table);

    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);

    success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load table created by gravitino: " + sql);
    }

    // Create a table with primary key
    Column[] columns = createMySQLFullTypeColumns();
    columns =
        Arrays.stream(columns)
            .map(
                c -> {
                  if ("IntegerType".equals(c.name())) {
                    return Column.of(c.name(), c.dataType(), "", false, true, null);
                  }
                  return c;
                })
            .toArray(Column[]::new);

    tableName = GravitinoITUtils.genRandomName("mysql_table_with_primary").toLowerCase();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            "Created by gravitino client",
            ImmutableMap.<String, String>builder().build(),
            new Transform[0],
            Distributions.NONE,
            new SortOrder[0],
            new Index[] {
              Indexes.createMysqlPrimaryKey(new String[][] {new String[] {"IntegerType"}})
            });
    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);

    success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load table created by gravitino: " + sql);
    }
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);

    Assertions.assertTrue(data.contains("engine = 'InnoDB'"));
    Assertions.assertTrue(
        data.contains("integertype integer NOT NULL WITH ( auto_increment = true )"));
  }

  @Test
  void testMySQLTableCreatedByTrino() {
    String catalogName = GravitinoITUtils.genRandomName("mysql_catalog").toLowerCase();
    String schemaName = GravitinoITUtils.genRandomName("mysql_schema").toLowerCase();
    String tableName = GravitinoITUtils.genRandomName("mysql_table").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123", // username and password are referred from Hive dockerfile.
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };

    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    String hiveHost = containerSuite.getHiveContainer().getContainerIpAddress();

    createdMetalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "jdbc-mysql",
        "comment",
        ImmutableMap.<String, String>builder()
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-url", String.format("jdbc:mysql://%s:3306?useSSL=false", hiveHost))
            .build());
    Catalog catalog = createdMetalake.loadCatalog(catalogName);
    Assertions.assertEquals("root", catalog.properties().get("jdbc-user"));

    String sql = String.format("show catalogs like '%s'", catalogName);
    boolean success = checkTrinoHasLoaded(sql, 30);
    if (!success) {
      Assertions.fail("Trino fail to load catalogs created by gravitino: " + sql);
    }

    String data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    Assertions.assertEquals(catalogName, data);

    // Create schema
    sql = String.format("create schema %s.%s", catalogName, schemaName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    // create table
    sql =
        String.format(
            "create table %s.%s.%s (id int, name varchar)", catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    // Add a not null column
    sql =
        String.format(
            "alter table %s.%s.%s add column age int not null comment 'age of users'",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    sql =
        String.format(
            "alter table %s.%s.%s add column address varchar(20) not null comment 'address of users'",
            catalogName, schemaName, tableName);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, tableName));

    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName);

    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    Assertions.assertTrue(data.contains("age integer NOT NULL"));
    Assertions.assertTrue(data.contains("address varchar(20) NOT NULL"));

    // Test special characters in table name
    String tableName1 = "t112";
    sql =
        String.format(
            "create table %s.%s.%s (id int, t1name varchar)", catalogName, schemaName, tableName1);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    String tableName2 = "t212";
    sql =
        String.format(
            "create table %s.%s.%s (id int, t2name varchar)", catalogName, schemaName, tableName2);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    String tableName3 = "t_12";
    sql =
        String.format(
            "create table %s.%s.%s (id int, t3name varchar)", catalogName, schemaName, tableName3);

    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    String tableName4 = "_1__";
    sql =
        String.format(
            "create table %s.%s.%s (id int, t4name varchar)", catalogName, schemaName, tableName4);
    containerSuite.getTrinoContainer().executeUpdateSQL(sql);

    // Get table tableName1
    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName1);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    data.contains("t1name varchar");

    // Get table tableName2
    sql = String.format("show create table  %s.%s.%s", catalogName, schemaName, tableName2);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    data.contains("t2name varchar");

    // Get table tableName3
    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName3);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    data.contains("t3name varchar");

    // Get table tableName4
    sql = String.format("show create table %s.%s.%s", catalogName, schemaName, tableName4);
    data = containerSuite.getTrinoContainer().executeQuerySQL(sql).get(0).get(0);
    data.contains("t4name varchar");
  }

  @Test
  void testDropCatalogAndCreateAgain() {
    String catalogName = GravitinoITUtils.genRandomName("mysql_catalog").toLowerCase();
    GravitinoMetalake createdMetalake = client.loadMetalake(metalakeName);
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123", // username and password are referred from Hive dockerfile.
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };

    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    String hiveHost = containerSuite.getHiveContainer().getContainerIpAddress();

    // Create the catalog and drop it for 3 times to test the create/drop catalog function work
    // well.
    for (int i = 0; i < 3; i++) {
      createdMetalake.createCatalog(
          catalogName,
          Catalog.Type.RELATIONAL,
          "jdbc-mysql",
          "comment",
          ImmutableMap.<String, String>builder()
              .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
              .put("jdbc-user", "root")
              .put("jdbc-password", "ds123")
              .put("jdbc-url", String.format("jdbc:mysql://%s:3306?useSSL=false", hiveHost))
              .build());

      String sql = String.format("show catalogs like '%s'", catalogName);
      boolean success = checkTrinoHasLoaded(sql, 30);
      Assertions.assertTrue(success, "Trino should load the catalog: " + sql);

      createdMetalake.disableCatalog(catalogName);
      createdMetalake.dropCatalog(catalogName);
      // We need to test we can't load this catalog any more by Trino.
      success = checkTrinoHasRemoved(sql, 30);
      Assertions.assertTrue(success, "Trino should not load the catalog any more: " + sql);
    }
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    GravitinoMetalake createdMetalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
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
    properties.put("metastore.uris", hiveMetastoreUris);

    Catalog createdCatalog =
        metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, "hive", "comment", properties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }
}
