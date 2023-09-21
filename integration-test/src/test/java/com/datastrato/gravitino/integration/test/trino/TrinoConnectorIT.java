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
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class TrinoConnectorIT extends AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorIT.class);

  public static TrinoContainer trinoContainer;
  static Connection trinoJdbcConnection = null;
  static final int TRINO_PORT = 8080;

  public static HiveContainer hiveContainer;
  static final int HIVE_METASTORE_PORT = 9083;
  private static HiveClientPool hiveClientPool;

  private static final AutoCloseableCloser closer = AutoCloseableCloser.create();

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

  // Replace the placeholder in trino config file
  static final String TRION_CONF_GRAVITINO_METALAKE_NAME = "GRAVITINO_METALAKE_NAME";
  static final String TRION_CONF_GRAVITINO_HOST_IP = "GRAVITINO_HOST_IP";
  static final String TRION_CONF_GRAVITINO_HOST_PORT = "GRAVITINO_HOST_PORT";
  static final String HIVE_HOST_IP_PLACEHOLDER = "HIVE_HOST_IP";

  static final String TRION_CONTAINER_CONF_DIR = "/etc/trino";

  static final String TRION_CONTAINER_PLUGIN_GRAVITINO_DIR = "/usr/lib/trino/plugin/gravitino";
  static final String TRION_CONNECTOR_LIB_DIR =
      System.getenv("GRAVITINO_ROOT_DIR") + "/trino-connector/build/libs";

  @BeforeAll
  public static void startDockerContainer() {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");

    // Let containers assign addresses in a fixed subnet to avoid `mac-docker-connector` needing to
    // refresh the configuration
    com.github.dockerjava.api.model.Network.Ipam.Config ipamConfig =
        new com.github.dockerjava.api.model.Network.Ipam.Config();
    // Set container fixed subnet avoid `mac-docker-connector` needing to refresh the configuration
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
    closer.register(network::close);

    HiveContainer.Builder hiveBuilder =
        HiveContainer.builder()
            .withHostName("gravitino-ci-hive")
            .withEnvVars(
                ImmutableMap.<String, String>builder().put("HADOOP_USER_NAME", "root").build())
            .withNetwork(network);
    hiveContainer = closer.register(hiveBuilder.build());
    closer.register(hiveContainer::close);
    hiveContainer.start();

    if (checkHiveContainerStatus()) {
      LOG.info("Hive container startup success!");
    } else {
      LOG.error("Hive container startup failed!");
      Assertions.fail();
    }

    HiveConf hiveConf = new HiveConf();
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d", getLocalHostIp(), hiveContainer.getMappedPort(HIVE_METASTORE_PORT));
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
    hiveClientPool = closer.register(new HiveClientPool(1, hiveConf));
    closer.register(hiveClientPool::close);

    // Currently must first create metalake and catalog then start trino container
    createMetalake();
    createCatalog();

    // Configure Trino config file
    updateConfigFile();

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
                    .put(TRION_CONTAINER_CONF_DIR, trinoConfDir)
                    .put(TRION_CONTAINER_PLUGIN_GRAVITINO_DIR, TRION_CONNECTOR_LIB_DIR)
                    .build())
            .withExposePorts(ImmutableSet.of(TRINO_PORT, 5005));

    trinoContainer = closer.register(trinoBuilder.build());
    closer.register(trinoContainer::close);
    trinoContainer.start();
  }

  @AfterAll
  public static void stopDockerContainer() {
    try {
      if (trinoJdbcConnection != null) trinoJdbcConnection.close();
    } catch (SQLException se) {
      LOG.error(se.getMessage(), se);
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private static boolean checkHiveContainerStatus() {
    int nRetry = 0;
    while (nRetry++ < 5) {
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
          return true;
        }
        Thread.sleep(5000);
      } catch (RuntimeException e) {
        LOG.error(e.getMessage(), e);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  @Test
  public void joinTwoHiveTableTest() throws TException, InterruptedException {
    int nRetry = 0;
    int sleepTime = 5000;
    while (nRetry++ < 10 && !testTrinoJdbcConnection(trinoContainer.getMappedPort(TRINO_PORT))) {
      try {
        Thread.sleep(sleepTime);
        LOG.warn("Waiting for trino server to be ready... ({}ms)", nRetry * sleepTime);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    // Check tha Trino has synchronized the catalog from Gravitino
    boolean catalogFoundInTrino = false;
    nRetry = 0;
    while (!catalogFoundInTrino && nRetry++ < 10) {
      ArrayList<ArrayList<String>> queryData = queryTrino("SHOW CATALOGS");
      for (ArrayList<String> record : queryData) {
        String columnValue = record.get(0);
        if (columnValue.equals(String.format("%s.%s", metalakeName, catalogName))) {
          catalogFoundInTrino = true;
          break;
        }
      }
      try {
        Thread.sleep(sleepTime);
        LOG.warn(
            "Waiting for trino synchronized the catalog from Gravitino... ({}ms)",
            nRetry * sleepTime);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    Assertions.assertTrue(catalogFoundInTrino);

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

    String sql2 =
        String.format("SHOW CREATE SCHEMA \"%s.%s\".%s", metalakeName, catalogName, databaseName);
    queryTrino(sql2);

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
    // Check in Gravitino Server
    NameIdentifier idTable1 =
        NameIdentifier.of(metalakeName, catalogName, databaseName, table1Name);
    Table table1 = catalog.asTableCatalog().loadTable(idTable1);
    Assertions.assertEquals(table1.name(), table1Name);
    // Check in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab1 =
        hiveClientPool.run(client -> client.getTable(databaseName, table1Name));
    Assertions.assertEquals(databaseName, hiveTab1.getDbName());
    Assertions.assertEquals(table1Name, hiveTab1.getTableName());

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
    // Check in Gravitino Server
    NameIdentifier idTable2 =
        NameIdentifier.of(metalakeName, catalogName, databaseName, table2Name);
    Table table2 = catalog.asTableCatalog().loadTable(idTable2);
    Assertions.assertEquals(table2.name(), table2Name);
    // Check in Hive Server
    org.apache.hadoop.hive.metastore.api.Table hiveTab2 =
        hiveClientPool.run(client -> client.getTable(databaseName, table2Name));
    Assertions.assertEquals(databaseName, hiveTab2.getDbName());
    Assertions.assertEquals(table2Name, hiveTab2.getTableName());

    ArrayList<ArrayList<String>> table1Data = new ArrayList<>();
    table1Data.add(new ArrayList<>(Arrays.asList("jake", "male", "30", "+1 6125047154")));
    table1Data.add(new ArrayList<>(Arrays.asList("jeff", "male", "25", "+1 2673800457")));
    table1Data.add(new ArrayList<>(Arrays.asList("rose", "female", "28", "+1 7073958726")));
    table1Data.add(new ArrayList<>(Arrays.asList("sam", "man", "18", "+1 8157809623")));
    for (ArrayList<String> record : table1Data) {
      String sql6 =
          String.format(
              "INSERT INTO \"%s.%s\".%s.%s (user_name, gender, age, phone) VALUES ('%s', '%s', '%s', '%s')",
              metalakeName,
              catalogName,
              databaseName,
              table1Name,
              record.get(0),
              record.get(1),
              record.get(2),
              record.get(3));
      updateTrino(sql6);
    }
    String sql8 =
        String.format(
            "SELECT user_name, gender, age, phone FROM \"%s.%s\".%s.%s ORDER BY user_name",
            metalakeName, catalogName, databaseName, table1Name);
    ArrayList<ArrayList<String>> table1QueryData = queryTrino(sql8);
    Assertions.assertTrue(table1Data.equals(table1QueryData));

    ArrayList<ArrayList<String>> table2Data = new ArrayList<>();
    table2Data.add(new ArrayList<>(Arrays.asList("jake", "", "$250", "22nd,July,2009")));
    table2Data.add(new ArrayList<>(Arrays.asList("jeff", "$40.25", "", "18nd,July,2023")));
    table2Data.add(new ArrayList<>(Arrays.asList("rose", "$28.45", "", "22nd,July,2023")));
    table2Data.add(new ArrayList<>(Arrays.asList("sam", "$27.03", "", "22nd,July,2023")));
    for (ArrayList<String> record : table2Data) {
      String sql6 =
          String.format(
              "INSERT INTO \"%s.%s\".%s.%s (user_name, consumer, recharge, event_time) VALUES ('%s', '%s', '%s', '%s')",
              metalakeName,
              catalogName,
              databaseName,
              table2Name,
              record.get(0),
              record.get(1),
              record.get(2),
              record.get(3));
      updateTrino(sql6);
    }
    String sql9 =
        String.format(
            "SELECT user_name, consumer, recharge, event_time FROM \"%s.%s\".%s.%s ORDER BY user_name",
            metalakeName, catalogName, databaseName, table2Name);
    ArrayList<ArrayList<String>> table2QueryData = queryTrino(sql9);
    Assertions.assertTrue(table2Data.equals(table2QueryData));

    String sql10 =
        String.format(
            "SELECT * FROM (SELECT t1.user_name as user_name, gender, age, phone, consumer, recharge, event_time FROM \"%s.%s\".%s.%s AS t1\n"
                + "JOIN\n"
                + "    (SELECT user_name, consumer, recharge, event_time FROM \"%s.%s\".%s.%s) AS t2\n"
                + "        ON t1.user_name = t2.user_name) ORDER BY user_name",
            metalakeName,
            catalogName,
            databaseName,
            table1Name,
            metalakeName,
            catalogName,
            databaseName,
            table2Name);
    ArrayList<ArrayList<String>> joinQueryData = queryTrino(sql10);
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
    Assertions.assertTrue(joinData.equals(joinQueryData));
  }

  private static boolean testTrinoJdbcConnection(int trinoPort) {
    final String DB_URL = String.format("jdbc:trino://127.0.0.1:%d", trinoPort);
    Statement stmt = null;
    try {
      trinoJdbcConnection = DriverManager.getConnection(DB_URL, "admin", "");
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
        String.format("thrift://%s:%d", hiveContainer.getContainerIpAddress(), HIVE_METASTORE_PORT);
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

  private static void updateConfigFile(String fileName, Map<String, String> configs) {
    try {
      File file = new File(fileName);
      FileReader fileReader = new FileReader(file);
      BufferedReader reader = new BufferedReader(fileReader);
      StringBuilder content = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append("\n");
      }
      reader.close();

      final String[] originalContent = {content.toString()};

      configs.forEach(
          (k, v) -> {
            String newContent = originalContent[0].replace(k, v);
            originalContent[0] = newContent;
          });

      FileWriter fileWriter = new FileWriter(file);
      fileWriter.write(originalContent[0]);
      fileWriter.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private static void updateConfigFile() {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");
    updateConfigFile(
        trinoConfDir + "/catalog/gravitino.properties",
        ImmutableMap.<String, String>builder()
            .put(TRION_CONF_GRAVITINO_METALAKE_NAME, metalakeName)
            .put(TRION_CONF_GRAVITINO_HOST_IP, getLocalHostIp())
            .put(TRION_CONF_GRAVITINO_HOST_PORT, String.valueOf(getGravitinoServerPort()))
            .build());

    updateConfigFile(
        trinoConfDir + "/catalog/hive.properties",
        ImmutableMap.of(HIVE_HOST_IP_PLACEHOLDER, hiveContainer.getContainerIpAddress()));
  }

  private static String getLocalHostIp() {
    String hostIP = "127.0.0.1";
    try {
      NetworkInterface networkInterface = NetworkInterface.getByName("en0"); // macOS
      if (networkInterface == null) {
        networkInterface = NetworkInterface.getByName("eth0"); // Linux
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
