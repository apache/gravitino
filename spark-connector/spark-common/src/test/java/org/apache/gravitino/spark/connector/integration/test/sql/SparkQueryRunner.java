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
package org.apache.gravitino.spark.connector.integration.test.sql;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

/** Run and check the correctness of the SparkSQLs */
public class SparkQueryRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkSQLRegressionTest.class);
  private static final String HIVE_CATALOG_NAME = "hive";
  private static final String ICEBERG_CATALOG_NAME = "iceberg";

  private SparkSession sparkSession;
  private String gravitinoUri;
  private String metalakeName;
  private String warehouse;
  private boolean regenerateGoldenFiles;
  // catalogType -> catalogName
  private Map<CatalogType, String> catalogs = new HashMap<>();
  private boolean isGravitinoEnvSetup;
  private String dataDir;
  private BaseIT baseIT;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  public SparkQueryRunner(SparkTestConfig sparkTestConfig) {
    this.regenerateGoldenFiles = sparkTestConfig.generateGoldenFiles();
    this.isGravitinoEnvSetup = sparkTestConfig.isGravitinoEnvSetUp();
    String baseDir = sparkTestConfig.getBaseDir();
    // translate to file:///xx/xx
    this.dataDir = Paths.get(baseDir, "data").toUri().toString();
    this.metalakeName = sparkTestConfig.getMetalakeName();
    if (isGravitinoEnvSetup) {
      try {
        setupGravitinoEnv();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      this.gravitinoUri = sparkTestConfig.getGravitinoUri();
      this.warehouse = sparkTestConfig.getWarehouseLocation();
    }
    initSparkEnv();

    baseIT = new BaseIT();
    catalogs.put(CatalogType.HIVE, HIVE_CATALOG_NAME);
    catalogs.put(CatalogType.ICEBERG, ICEBERG_CATALOG_NAME);
    catalogs.put(CatalogType.UNKNOWN, HIVE_CATALOG_NAME);
  }

  public void runQuery(TestCaseGroup sqlTestCaseGroup) throws IOException {
    useCatalog(sqlTestCaseGroup.getCatalogType());
    try {
      if (sqlTestCaseGroup.prepareFile != null) {
        runQuery(sqlTestCaseGroup.prepareFile);
      }
      sqlTestCaseGroup.testCases.forEach(
          testCase -> {
            try {
              runTestCase(testCase);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      if (sqlTestCaseGroup.cleanupFile != null) {
        runQuery(sqlTestCaseGroup.cleanupFile);
      }
    }
  }

  public void close() throws Exception {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
    if (isGravitinoEnvSetup) {
      closeGravitinoEnv();
    }
  }

  private void setupGravitinoEnv() throws Exception {
    // Start Hive and Hdfs
    containerSuite.startHiveContainer();
    String hiveMetastoreUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    this.warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);

    // Start Gravitino server
    baseIT.stopIntegrationTest();
    int gravitinoPort = baseIT.getGravitinoServerPort();
    this.gravitinoUri = String.format("http://127.0.0.1:%d", gravitinoPort);

    // Init metalake and catalog
    GravitinoAdminClient client = baseIT.getGravitinoClient();
    client.createMetalake(metalakeName, "", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    metalake.createCatalog(
        HIVE_CATALOG_NAME,
        Catalog.Type.RELATIONAL,
        "hive",
        "",
        getHiveCatalogConfigs(hiveMetastoreUri));
    metalake.createCatalog(
        ICEBERG_CATALOG_NAME,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        "",
        getIcebergCatalogConfigs(hiveMetastoreUri));

    client.close();
  }

  private Map<String, String> getHiveCatalogConfigs(String hiveMetastoreUri) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI, hiveMetastoreUri);
    return catalogProperties;
  }

  private Map<String, String> getIcebergCatalogConfigs(String hiveMetastoreUri) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, hiveMetastoreUri);
    return catalogProperties;
  }

  private void closeGravitinoEnv() throws Exception {
    baseIT.stopIntegrationTest();
  }

  private void writeQueryOutput(Path outputFile, List<QueryOutput> queryOutputs)
      throws IOException {
    String goldenOutput =
        "-- Automatically generated by Gravitino Spark SQL test\n"
            + queryOutputs.stream().map(QueryOutput::toString).collect(Collectors.joining("\n\n\n"))
            + "\n";
    stringToFile(outputFile, goldenOutput);
  }

  private List<String> getQueriesFromFile(Path file) throws IOException {
    String input = fileToString(file);
    Pair<List<String>, List<String>> pair = splitCommentsAndCodes(input);
    List<String> queries = pair.getRight();
    queries = splitWithSemicolon(queries);
    queries = cleanAndFilterQueries(queries);
    return queries;
  }

  private void runTestCase(TestCase testCase) throws IOException {
    LOG.info("Run test case:{}", testCase.toString());
    List<String> queries = getQueriesFromFile(testCase.getTestFile());
    List<QueryOutput> queryOutputs = runTestQueries(queries, true);
    if (regenerateGoldenFiles) {
      writeQueryOutput(testCase.getTestOutputFile(), queryOutputs);
    }
    List<QueryOutput> expectedOutputs = getExpectedOutputs(testCase.getTestOutputFile());

    Assertions.assertEquals(
        expectedOutputs.size(), queryOutputs.size(), "Query size not match for test: " + testCase);

    for (int i = 0; i < expectedOutputs.size(); i++) {
      QueryOutput queryOutput = queryOutputs.get(i);
      QueryOutput expectedOutput = expectedOutputs.get(i);
      Assertions.assertEquals(
          expectedOutput.getSql(), queryOutput.getSql(), "SQL not match for test: " + testCase);
      Assertions.assertEquals(
          expectedOutput.getSchema(),
          queryOutput.getSchema(),
          "SQL schema not match for test: " + testCase);
      Assertions.assertEquals(
          expectedOutput.getOutput(),
          queryOutput.getOutput(),
          "SQL output not match for test: " + testCase + ", sql: " + expectedOutput.getSql());
    }
  }

  private List<QueryOutput> runTestQueries(List<String> queries, boolean catchException) {
    SparkSession localSparkSession = sparkSession;
    return queries.stream()
        .map(
            query -> {
              Pair<String, List<String>> pair;
              if (catchException) {
                pair =
                    SQLQueryTestHelper.handleExceptions(
                        () -> SQLQueryTestHelper.getNormalizedResult(localSparkSession, query));
              } else {
                pair = SQLQueryTestHelper.getNormalizedResult(localSparkSession, query);
              }
              String schema = pair.getLeft();
              String output =
                  pair.getRight().stream()
                      .collect(Collectors.joining("\n"))
                      .replaceAll("\\s+$", "");
              return new QueryOutput(query, schema, output);
            })
        .collect(Collectors.toList());
  }

  private List<QueryOutput> getExpectedOutputs(Path outputPath) throws IOException {
    String goldenOutput = fileToString(outputPath);
    String[] segments = goldenOutput.split("-- !query.*\\n");

    List<QueryOutput> expectedQueryOutputs = new ArrayList<>();
    // the first segment is comment, skip it
    for (int i = 0; i * 3 + 3 < segments.length; i++) {
      QueryOutput queryOutput =
          new QueryOutput(
              segments[i * 3 + 1].trim(),
              segments[i * 3 + 2].trim(),
              segments[i * 3 + 3].trim().replaceAll("\\s+$", ""));
      expectedQueryOutputs.add(queryOutput);
    }
    return expectedQueryOutputs;
  }

  private static void stringToFile(Path path, String str) throws IOException {
    FileUtils.writeStringToFile(path.toFile(), str, StandardCharsets.UTF_8);
  }

  private String fileToString(Path filePath) throws IOException {
    return FileUtils.readFileToString(filePath.toFile(), StandardCharsets.UTF_8);
  }

  private static Pair<List<String>, List<String>> splitCommentsAndCodes(String input) {
    String[] lines = input.split("\n");
    List<String> comments = new ArrayList<>();
    List<String> codes = new ArrayList<>();
    for (String line : lines) {
      String trimmedLine = line.trim();
      if (trimmedLine.startsWith("--") && !trimmedLine.startsWith("--QUERY-DELIMITER")) {
        comments.add(line);
      } else {
        codes.add(line);
      }
    }
    return Pair.of(comments, codes);
  }

  private static List<String> splitWithSemicolon(List<String> sequences) {
    return Arrays.asList(String.join("\n", sequences).split("(?<!\\\\);"));
  }

  private static List<String> cleanAndFilterQueries(List<String> tempQueries) {
    return tempQueries.stream()
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(
            s ->
                Arrays.stream(s.split("\n"))
                    .filter(line -> !line.trim().startsWith("--"))
                    .collect(Collectors.joining("\n")))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private void sql(String sql) {
    sparkSession.sql(sql);
  }

  private void useCatalog(CatalogType catalogType) {
    String catalogName = catalogs.get(catalogType);
    sql("USE " + catalogName);
  }

  private void initSparkEnv() {
    this.sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark sql query test")
            .config("spark.plugins", GravitinoSparkPlugin.class.getName())
            .config(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .config(GravitinoSparkConfig.GRAVITINO_METALAKE, metalakeName)
            .config(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, "true")
            .config("spark.gravitino.test.data.dir", dataDir)
            .config(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            // produce old parquet format
            .config("spark.sql.parquet.writeLegacyFormat", "true")
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.locality.wait.node", "0")
            .enableHiveSupport()
            .getOrCreate();
  }

  private void runQuery(Path file) throws IOException {
    LOG.info("Run query: {}", file.toString());
    List<String> queries = getQueriesFromFile(file);
    runTestQueries(queries, false);
  }
}
