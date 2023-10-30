/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static java.lang.String.format;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.hive.HiveClientPool;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.CloseableGroup;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

@Tag("gravitino-trino-it")
public class TrinoConnectorIT extends AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorIT.class);
  private static final CloseableGroup closer = CloseableGroup.create();

  public static TrinoContainer trinoContainer;
  static Connection trinoJdbcConnection = null;

  public static HiveContainer hiveContainer;
  private static HiveClientPool hiveClientPool;

  public static String metalakeName =
      GravitinoITUtils.genRandomName("TrinoIT_metalake").toLowerCase();
  public static String catalogName =
      GravitinoITUtils.genRandomName("TrinoIT_catalog").toLowerCase();
  public static String databaseName =
      GravitinoITUtils.genRandomName("TrinoIT_database").toLowerCase();
  public static String table1Name = GravitinoITUtils.genRandomName("TrinoIT_table1").toLowerCase();
  public static String table2Name = GravitinoITUtils.genRandomName("TrinoIT_table2").toLowerCase();
  private static GravitinoMetaLake metalake;
  private static Catalog catalog;

  static final String TRINO_CONNECTOR_LIB_DIR =
      System.getenv("GRAVITINO_ROOT_DIR") + "/trino-connector/build/libs";

  @BeforeAll
  public static void startDockerContainer() throws IOException, TException, InterruptedException {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");

    // Let containers assign addresses in a fixed subnet to avoid `mac-docker-connector` needing to
    // refresh the configuration
    com.github.dockerjava.api.model.Network.Ipam.Config ipamConfig =
        new com.github.dockerjava.api.model.Network.Ipam.Config();
    ipamConfig
        .withSubnet("192.168.0.0/24")
        .withGateway("192.168.0.1")
        .withIpRange("192.168.0.100/28");

    Network network =
        closer.register(
            Network.builder()
                .createNetworkCmdModifier(
                    cmd ->
                        cmd.withIpam(
                            new com.github.dockerjava.api.model.Network.Ipam()
                                .withConfig(ipamConfig)))
                .build());

    // Start Hive container
    HiveContainer.Builder hiveBuilder =
        HiveContainer.builder()
            .withHostName("gravitino-ci-hive")
            .withEnvVars(
                ImmutableMap.<String, String>builder().put("HADOOP_USER_NAME", "root").build())
            .withNetwork(network);
    hiveContainer = closer.register(hiveBuilder.build());
    hiveContainer.start();

    // Check Hive container status
    checkHiveContainerStatus(5);

    // Initial hive client
    HiveConf hiveConf = new HiveConf();
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            getPrimaryNICIp(), hiveContainer.getMappedPort(HiveContainer.HIVE_METASTORE_PORT));
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
    hiveClientPool = closer.register(new HiveClientPool(1, hiveConf));

    // Currently must first create metalake and catalog then start trino container
    createMetalake();
    createCatalog();

    // Configure Trino config file
    updateTrinoConfigFile();

    // Start Trino container
    String hiveContainerIp = hiveContainer.getContainerIpAddress();
    TrinoContainer.Builder trinoBuilder =
        TrinoContainer.builder()
            .withEnvVars(
                ImmutableMap.<String, String>builder().put("HADOOP_USER_NAME", "root").build())
            .withNetwork(network)
            .withExtraHosts(
                ImmutableMap.<String, String>builder()
                    .put(hiveContainerIp, hiveContainerIp)
                    .build())
            .withFilesToMount(
                ImmutableMap.<String, String>builder()
                    .put(TrinoContainer.TRINO_CONTAINER_CONF_DIR, trinoConfDir)
                    .put(
                        TrinoContainer.TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR,
                        TRINO_CONNECTOR_LIB_DIR)
                    .build())
            .withExposePorts(ImmutableSet.of(TrinoContainer.TRINO_PORT));

    trinoContainer = closer.register(trinoBuilder.build());
    trinoContainer.start();

    // Test Trino JDBC connection
    testTrinoJdbcConnection();

    createSchemaInTrino();
  }

  @AfterAll
  public static void stopDockerContainer() {
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private static void checkHiveContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isHiveContainerReady = false;
    while (nRetry++ < retryLimit) {
      try {
        String[] commandAndArgs = new String[] {"bash", "/tmp/check-status.sh"};
        Container.ExecResult execResult = hiveContainer.executeInContainer(commandAndArgs);
        if (execResult.getExitCode() != 0) {
          String message =
              format(
                  "Command [%s] exited with %s",
                  String.join(" ", commandAndArgs), execResult.getExitCode());
          LOG.error("{}", message);
          LOG.error("stderr: {}", execResult.getStderr());
          LOG.error("stdout: {}", execResult.getStdout());
        } else {
          LOG.info("Hive container startup success!");
          isHiveContainerReady = true;
          break;
        }
        Thread.sleep(5000);
      } catch (RuntimeException e) {
        LOG.error(e.getMessage(), e);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    Assertions.assertTrue(isHiveContainerReady, "Hive container startup failed!");
  }

  static void testTrinoJdbcConnection() {
    int nRetry = 0;
    boolean isTrinoJdbcConnectionReady = false;
    int sleepTime = 5000;
    while (nRetry++ < 10 && !isTrinoJdbcConnectionReady) {
      isTrinoJdbcConnectionReady =
          testTrinoJdbcConnection(trinoContainer.getMappedPort(TrinoContainer.TRINO_PORT));
      try {
        Thread.sleep(sleepTime);
        LOG.warn("Waiting for trino server to be ready... ({}ms)", nRetry * sleepTime);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    Assertions.assertTrue(isTrinoJdbcConnectionReady, "Test Trino JDBC connection failed!");
  }

  public static void createSchemaInTrino() throws TException, InterruptedException {
    String sql1 =
        String.format(
            "CREATE SCHEMA \"%s.%s\".%s WITH (\n"
                + "  location = 'hdfs://%s:9000/user/hive/warehouse/%s.db'\n"
                + ")",
            metalakeName,
            catalogName,
            databaseName,
            hiveContainer.getContainerIpAddress(),
            databaseName);
    updateTrino(sql1);
    NameIdentifier idSchema = NameIdentifier.of(metalakeName, catalogName, databaseName);
    Schema schema = catalog.asSchemas().loadSchema(idSchema);
    Assertions.assertEquals(schema.name(), databaseName);
    Database database = hiveClientPool.run(client -> client.getDatabase(databaseName));
    Assertions.assertEquals(databaseName, database.getName());
  }

  @Test
  public void trinoTable1Test() throws TException, InterruptedException {
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
            metalakeName, catalogName, databaseName, table1Name);
    updateTrino(sql3);

    // Verify in Gravitino Server
    NameIdentifier idTable1 =
        NameIdentifier.of(metalakeName, catalogName, databaseName, table1Name);
    Table table1 = catalog.asTableCatalog().loadTable(idTable1);
    Assertions.assertEquals(table1.name(), table1Name);

    // Verify in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab1 =
        hiveClientPool.run(client -> client.getTable(databaseName, table1Name));
    Assertions.assertEquals(databaseName, hiveTab1.getDbName());
    Assertions.assertEquals(table1Name, hiveTab1.getTableName());

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
                metalakeName, catalogName, databaseName, table1Name));
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
    updateTrino(sql5.toString());

    // Select data from table1 and verify it
    String sql6 =
        String.format(
            "SELECT user_name, gender, age, phone FROM \"%s.%s\".%s.%s ORDER BY user_name",
            metalakeName, catalogName, databaseName, table1Name);
    ArrayList<ArrayList<String>> table1QueryData = queryTrino(sql6);
    Assertions.assertEquals(table1Data, table1QueryData);
  }

  @Test
  public void trinoTable2Test() throws TException, InterruptedException {
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
            metalakeName, catalogName, databaseName, table2Name);
    updateTrino(sql4);

    // Verify in Gravitino Server
    NameIdentifier idTable2 =
        NameIdentifier.of(metalakeName, catalogName, databaseName, table2Name);
    Table table2 = catalog.asTableCatalog().loadTable(idTable2);
    Assertions.assertEquals(table2.name(), table2Name);

    // Verify in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab2 =
        hiveClientPool.run(client -> client.getTable(databaseName, table2Name));
    Assertions.assertEquals(databaseName, hiveTab2.getDbName());
    Assertions.assertEquals(table2Name, hiveTab2.getTableName());

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
                metalakeName, catalogName, databaseName, table2Name));
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
    updateTrino(sql7.toString());

    // Select data from table1 and verify it
    String sql8 =
        String.format(
            "SELECT user_name, consumer, recharge, event_time FROM \"%s.%s\".%s.%s ORDER BY user_name",
            metalakeName, catalogName, databaseName, table2Name);
    ArrayList<ArrayList<String>> table2QueryData = queryTrino(sql8);
    Assertions.assertEquals(table2Data, table2QueryData);
  }

  @Order(99)
  @Test
  public void joinTwoTableTest() {
    String sql9 =
        String.format(
            "SELECT * FROM (SELECT t1.user_name as user_name, gender, age, phone, consumer, recharge, event_time FROM \"%1$s.%2$s\".%3$s.%4$s AS t1\n"
                + "JOIN\n"
                + "    (SELECT user_name, consumer, recharge, event_time FROM \"%1$s.%2$s\".%3$s.%5$s) AS t2\n"
                + "        ON t1.user_name = t2.user_name) ORDER BY user_name",
            metalakeName, catalogName, databaseName, table1Name, table2Name);
    ArrayList<ArrayList<String>> joinQueryData = queryTrino(sql9);
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

  private static boolean testTrinoJdbcConnection(int trinoPort) {
    final String dbUrl = String.format("jdbc:trino://127.0.0.1:%d", trinoPort);
    Statement stmt = null;
    try {
      trinoJdbcConnection = DriverManager.getConnection(dbUrl, "admin", "");
      closer.register(trinoJdbcConnection);
      stmt = trinoJdbcConnection.createStatement();
      ResultSet rs = stmt.executeQuery("select 1");
      while (rs.next()) {
        int one = rs.getInt(1);
        Assertions.assertEquals(1, one);
      }
      rs.close();
      stmt.close();
    } catch (SQLException se) {
      LOG.error(se.getMessage(), se);
      if (se.getMessage().contains("Trino server is still initializing")) {
        return false;
      }
      Assertions.fail(se.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      Assertions.fail(e.getMessage());
    } finally {
      try {
        if (stmt != null) stmt.close();
      } catch (SQLException se) {
      } // do nothing
    } // end try
    return true;
  }

  private static ArrayList<ArrayList<String>> queryTrino(String sql) {
    LOG.info("queryTrino() SQL: {}", sql);
    ArrayList<ArrayList<String>> queryData = new ArrayList<>();
    try {
      Statement stmt = trinoJdbcConnection.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      while (rs.next()) {
        ArrayList<String> record = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          String columnValue = rs.getString(i);
          record.add(columnValue);
        }
        queryData.add(record);
      }
      rs.close();
      stmt.close();
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
      Assertions.fail(e.getMessage());
    }
    return queryData;
  }

  private static void updateTrino(String sql) {
    LOG.info("updateTrino() SQL: {}", sql);
    try {
      Statement stmt = trinoJdbcConnection.createStatement();
      stmt.executeUpdate(sql);
      stmt.close();
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
      Assertions.fail(e.getMessage());
    }
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
            hiveContainer.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);
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

    catalog = loadCatalog;
  }

  private static void updateTrinoConfigFile(String filePath, Map<String, String> configs) {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
      StringBuilder content = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append("\n");
      }

      String updatedContent = "";
      for (Map.Entry<String, String> entry : configs.entrySet()) {
        updatedContent = content.toString().replaceAll(entry.getKey(), entry.getValue());
      }
      writer.write(updatedContent);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private static void updateTrinoConfigFile() throws IOException {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");
    ITUtils.rewriteConfigFile(
        trinoConfDir + "/catalog/gravitino.properties.template",
        trinoConfDir + "/catalog/gravitino.properties",
        ImmutableMap.<String, String>builder()
            .put(
                TrinoContainer.TRINO_CONF_GRAVITINO_URI,
                String.format("http://%s:%d", getPrimaryNICIp(), getGravitinoServerPort()))
            .put(TrinoContainer.TRINO_CONF_GRAVITINO_METALAKE, metalakeName)
            .build());
    ITUtils.rewriteConfigFile(
        trinoConfDir + "/catalog/hive.properties.template",
        trinoConfDir + "/catalog/hive.properties",
        ImmutableMap.of(
            TrinoContainer.TRINO_CONF_HIVE_METASTORE_URI,
            String.format(
                "thrift://%s:%d",
                hiveContainer.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT)));
  }

  // Get host IP from primary NIC
  private static String getPrimaryNICIp() {
    String hostIP = "127.0.0.1";
    try {
      NetworkInterface networkInterface = NetworkInterface.getByName("en0"); // macOS
      if (networkInterface == null) {
        networkInterface = NetworkInterface.getByName("eth0"); // Linux and Windows
      }
      if (networkInterface != null) {
        Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          InetAddress address = addresses.nextElement();
          if (!address.isLoopbackAddress() && address.getHostAddress().indexOf(':') == -1) {
            hostIP = address.getHostAddress().replace("/", ""); // remove the first char '/'
            break;
          }
        }
      } else {
        InetAddress ip = InetAddress.getLocalHost();
        hostIP = ip.getHostAddress();
      }
    } catch (SocketException | UnknownHostException e) {
      LOG.error(e.getMessage(), e);
    }
    return hostIP;
  }
}
