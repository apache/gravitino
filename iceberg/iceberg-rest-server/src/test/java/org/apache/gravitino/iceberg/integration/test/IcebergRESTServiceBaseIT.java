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
package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache;
import org.apache.gravitino.iceberg.integration.test.util.IcebergRESTServerManager;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * <p>Referred from spark/v3.4/spark/src/test/java/org/apache/iceberg/spark/SparkTestBase.java
 */

@SuppressWarnings("FormatStringAnnotation")
public abstract class IcebergRESTServiceBaseIT {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServiceBaseIT.class);
  private SparkSession sparkSession;
  protected IcebergCatalogBackend catalogType = IcebergCatalogBackend.MEMORY;
  private IcebergRESTServerManager icebergRESTServerManager;

  @BeforeAll
  void initIcebergTestEnv() throws Exception {
    // Start Hive HDFS if necessary
    initEnv();
    // Start Iceberg REST server
    this.icebergRESTServerManager = IcebergRESTServerManager.create();
    // Inject the catalog specific config to iceberg-rest-server.conf
    registerIcebergCatalogConfig();
    icebergRESTServerManager.startIcebergRESTServer();
    // Start Spark session
    initSparkEnv();
    LOG.info("Gravitino and Spark env started,{}", catalogType);
  }

  @AfterAll
  void stopIcebergTestEnv() throws Exception {
    stopSparkEnv();
    icebergRESTServerManager.stopIcebergRESTServer();
    LOG.info("Gravitino and Spark env stopped,{}", catalogType);
  }

  boolean catalogTypeNotMemory() {
    return !catalogType.equals(IcebergCatalogBackend.MEMORY);
  }

  boolean isSupportsViewCatalog() {
    return !catalogType.equals(IcebergCatalogBackend.HIVE);
  }

  abstract void initEnv();

  abstract Map<String, String> getCatalogConfig();

  protected boolean supportsCredentialVending() {
    return false;
  }

  private void copyBundleJar(String bundleName) {
    String bundleFileName = ITUtils.getBundleJarName(bundleName);

    String rootDir = System.getenv("GRAVITINO_ROOT_DIR");
    String sourceFile =
        String.format("%s/bundles/gcp-bundle/build/libs/%s", rootDir, bundleFileName);
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    String targetFile = String.format("%s/%s", targetDir, bundleFileName);
    LOG.info("Source file: {}, target directory: {}", sourceFile, targetDir);
    try {
      File target = new File(targetFile);
      if (!target.exists()) {
        LOG.info("Copy source file: {} to target directory: {}", sourceFile, targetDir);
        FileUtils.copyFileToDirectory(new File(sourceFile), new File(targetDir));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void registerIcebergCatalogConfig() {
    Map<String, String> icebergConfigs = getCatalogConfig();
    icebergConfigs.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.TABLE_METADATA_CACHE_IMPL.getKey(),
        LocalTableMetadataCache.class.getName());
    icebergRESTServerManager.registerCustomConfigs(icebergConfigs);
    LOG.info("Iceberg REST service config registered, {}", StringUtils.join(icebergConfigs));
  }

  private int getServerPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            icebergRESTServerManager.getServerConfig(), IcebergConfig.ICEBERG_CONFIG_PREFIX);
    return jettyServerConfig.getHttpPort();
  }

  private void initSparkEnv() {
    int port = getServerPort();
    LOG.info("Iceberg REST server port:{}", port);
    String icebergRESTUri = String.format("http://127.0.0.1:%d/iceberg/", port);
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.rest.type", "rest")
            .set("spark.sql.catalog.rest.uri", icebergRESTUri)
            // drop Iceberg table purge may hang in spark local mode
            .set("spark.locality.wait.node", "0");

    if (supportsCredentialVending()) {
      sparkConf.set(
          "spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation", "vended-credentials");
    }

    sparkSession = SparkSession.builder().master("local[1]").config(sparkConf).getOrCreate();
  }

  private void stopSparkEnv() {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
  }

  @FormatMethod
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

  protected Map<String, String> getViewInfo(String viewName) {
    return convertToStringMap(sql("desc extended " + viewName));
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
