/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.aux.AuxiliaryServiceManager;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.graviton.catalog.lakehouse.iceberg.utils.IcebergCatalogUtil;
import com.datastrato.graviton.catalog.lakehouse.iceberg.utils.IcebergCatalogUtil.CatalogType;
import com.datastrato.graviton.integration.test.util.AbstractIT;
import com.datastrato.graviton.server.web.JettyServerConfig;
import com.datastrato.graviton.utils.MapUtils;
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
  private SparkSession sparkSession;
  protected CatalogType catalogType = IcebergCatalogUtil.CatalogType.MEMORY;

  @BeforeAll
  void initIcebergTestEnv() throws Exception {
    registerIcebergCatalogConfig();
    AbstractIT.startIntegrationTest();
    initSparkEnv();
    LOG.info("graviton and spark env started,{}", catalogType);
  }

  @AfterAll
  void stopIcebergTestEnv() throws Exception {
    stopSparkEnv();
    AbstractIT.stopIntegrationTest();
    LOG.info("graviton and spark env stopped,{}", catalogType);
  }

  // the purpose of `startIntegrationTest` and `stopIntegrationTest` is to
  // stop the corresponding func in AbstractIT auto run by junit
  @BeforeAll
  public static void startIntegrationTest() {}

  @AfterAll
  public static void stopIntegrationTest() {}

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
        AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_TYPE.getKey(),
        "memory");

    configMap.put(
        AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + "warehouse",
        "/tmp/");
    return configMap;
  }

  private static Map<String, String> getIcebergJdbcCatalogConfigs() {
    Map<String, String> configMap = new HashMap<>();
    return configMap;
  }

  private static Map<String, String> getIcebergHiveCatalogConfigs() {
    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put(
        AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_TYPE.getKey(),
        "hive");

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + "uri",
        "thrift://127.0.0.1:9083");

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + "warehouse",
        "file:///tmp/user/hive/warehouse-hive/");
    return customConfigs;
  }

  private static IcebergConfig buildIcebergConfig(Config config) {
    Map<String, String> m =
        config.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX);
    m = MapUtils.getPrefixMap(m, IcebergRESTService.SERVICE_NAME + ".");
    IcebergConfig icebergConfig = new IcebergConfig();
    icebergConfig.loadFromMap(m, k -> true);
    return icebergConfig;
  }

  private void initSparkEnv() {
    IcebergConfig icebergConfig = buildIcebergConfig(AbstractIT.getServerConfig());
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
