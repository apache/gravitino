/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.aux.AuxiliaryServiceManager;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergRESTConfig;
import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.graviton.integration.test.util.AbstractIT;
import com.datastrato.graviton.utils.MapUtils;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;

/*
 * <p>Referred from spark/v3.4/spark/src/test/java/org/apache/iceberg/spark/SparkTestBase.java
 */
public class TestIcebergRESTServiceBaseIT extends AbstractIT {
  private SparkSession sparkSession;

  public TestIcebergRESTServiceBaseIT() {
    initSparkEnv();
  }

  private static IcebergRESTConfig buildIcebergRESTConfig(Config config) {
    Map<String, String> m =
        config.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITON_AUX_SERVICE_PREFIX);
    m = MapUtils.getPrefixMap(m, IcebergRESTService.SERVICE_NAME + ".");
    IcebergRESTConfig icebergRESTConfig = new IcebergRESTConfig();
    icebergRESTConfig.loadFromMap(m, k -> true);
    return icebergRESTConfig;
  }

  private void initSparkEnv() {
    IcebergRESTConfig icebergRESTConfig = buildIcebergRESTConfig(serverConfig);
    int port = icebergRESTConfig.get(IcebergRESTConfig.ICEBERG_REST_SERVER_HTTP_PORT);
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

    sparkSession.sparkContext().setLogLevel("WARN");
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

  protected Map<String, String> convertToStringMap(List<Object[]> objects) {
    return objects.stream()
        .collect(
            Collectors.toMap(
                row -> String.valueOf(row[0]),
                row -> String.valueOf(row[1]),
                (oldValue, newValue) -> oldValue));
  }
}
