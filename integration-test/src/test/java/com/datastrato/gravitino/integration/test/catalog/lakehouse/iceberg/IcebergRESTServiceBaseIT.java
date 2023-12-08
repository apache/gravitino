/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.aux.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.datastrato.gravitino.utils.MapUtils;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * <p>Referred from spark/v3.4/spark/src/test/java/org/apache/iceberg/spark/SparkTestBase.java
 */

@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTServiceBaseIT extends AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServiceBaseIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private SparkSession sparkSession;
  protected IcebergCatalogBackend catalogType = IcebergCatalogBackend.MEMORY;

  @BeforeAll
  void initIcebergTestEnv() throws Exception {
    containerSuite.startHiveContainer();
    registerIcebergCatalogConfig();
    AbstractIT.startIntegrationTest();
    initSparkEnv();
    LOG.info("gravitino and spark env started,{}", catalogType);
  }

  @AfterAll
  void stopIcebergTestEnv() throws Exception {
    stopSparkEnv();
    AbstractIT.stopIntegrationTest();
    LOG.info("gravitino and spark env stopped,{}", catalogType);
  }

  // AbstractIT#startIntegrationTest() is static, so we couldn't inject catalog info
  // if startIntegrationTest() is auto invoked by Junit. so here we override
  // startIntegrationTest() to disable the auto invoke by junit.
  @BeforeAll
  public static void startIntegrationTest() {}

  @AfterAll
  public static void stopIntegrationTest() {}

  boolean catalogTypeNotMemory() {
    return !catalogType.equals(IcebergCatalogBackend.MEMORY);
  }

  private void registerIcebergCatalogConfig() {
    Map<String, String> icebergConfigs;

    switch (catalogType) {
      case HIVE:
        icebergConfigs = getIcebergHiveCatalogConfigs();
        break;
      case JDBC:
        icebergConfigs = getIcebergJdbcCatalogConfigs();
        break;
      case MEMORY:
        icebergConfigs = getIcebergMemoryCatalogConfigs();
        break;
      default:
        throw new RuntimeException("Not support Iceberg catalog type:" + catalogType);
    }

    AbstractIT.registerCustomConfigs(icebergConfigs);
    LOG.info("Iceberg REST service config registered," + StringUtils.join(icebergConfigs));
  }

  private static Map<String, String> getIcebergMemoryCatalogConfigs() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.MEMORY.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        "/tmp/");
    return configMap;
  }

  private static Map<String, String> getIcebergJdbcCatalogConfigs() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.JDBC.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        "jdbc:sqlite::memory:");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_USER.getKey(),
        "iceberg");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_PASSWORD.getKey(),
        "iceberg");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_INIT_TABLES.getKey(),
        "true");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-jdbc-sqlite",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));

    return configMap;
  }

  private static Map<String, String> getIcebergHiveCatalogConfigs() {
    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT));

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-hive",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));
    return customConfigs;
  }

  private static IcebergConfig buildIcebergConfig(Config config) {
    Map<String, String> m =
        config.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX);
    m = MapUtils.getPrefixMap(m, IcebergRESTService.SERVICE_NAME + ".");
    return new IcebergConfig(m);
  }

  private void initSparkEnv() {
    IcebergConfig icebergConfig = buildIcebergConfig(serverConfig);
    int port = icebergConfig.get(JettyServerConfig.WEBSERVER_HTTP_PORT);
    LOG.info("Iceberg REST server port:{}", port);
    String IcebergRESTUri = String.format("http://127.0.0.1:%d/iceberg/", port);
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.rest.type", "rest")
            .config("spark.sql.catalog.rest.uri", IcebergRESTUri)
            // drop Iceberg table purge may hang in spark local mode
            .config("spark.locality.wait.node", "0")
            .getOrCreate();
  }

  private void stopSparkEnv() {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
  }

  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = sparkSession.sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }
              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              }
              return value;
            })
        .toArray(Object[]::new);
  }

  /** check whether all child map content is in parent map */
  protected void checkMapContains(Map<String, String> child, Map<String, String> parent) {
    child.forEach(
        (k, v) -> {
          Assertions.assertTrue(parent.containsKey(k));
          Assertions.assertEquals(v, parent.get(k));
        });
  }

  /** mainly used to debug */
  protected void printObjects(List<Object[]> objects) {
    objects.stream()
        .forEach(
            row -> {
              String oneRow =
                  Arrays.stream(row).map(o -> String.valueOf(o)).collect(Collectors.joining(","));
              LOG.warn(oneRow);
            });
  }

  protected Map<String, String> getTableInfo(String tableName) {
    return convertToStringMap(sql("desc table extended " + tableName));
  }

  protected List<String> getTableColumns(String tableName) {
    List<Object[]> objects = sql("desc table extended " + tableName);
    List<String> columns = new ArrayList<>();
    objects.stream()
        .anyMatch(
            row -> {
              String columName = (String) row[0];
              if (StringUtils.isNoneBlank(columName)) {
                columns.add(columName);
                return false;
              }
              return true;
            });
    return columns;
  }

  protected Set<String> convertToStringSet(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toSet());
  }

  protected List<String> convertToStringList(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toList());
  }

  protected Map<String, String> convertToStringMap(List<Object[]> objects) {
    return objects.stream()
        .collect(
            Collectors.toMap(
                row -> String.valueOf(row[0]),
                row -> String.valueOf(row[1]),
                (oldValue, newValue) -> oldValue));
  }
}
