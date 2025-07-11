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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.util.Strings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entrypoint to run SparkSQL regression test, you could run it with specific Spark version:
 * ./gradlew :spark-connector:spark-3.4:test --tests
 * "org.apache.gravitino.spark.connector.integration.test.sql.SparkSQLRegressionTest34"
 * -PenableSparkSQLITs
 */
@Tag("gravitino-docker-test")
public abstract class SparkSQLRegressionTest {
  private static final Logger LOG = LoggerFactory.getLogger(SparkSQLRegressionTest.class);
  private static final String SPARK_TEST_CONFIG_FILE = "configFile";
  private SparkQueryRunner sparkQueryRunner;
  private SparkTestConfig sparkTestConfig;

  @Test
  public void run() throws IOException {
    String baseDir = sparkTestConfig.getBaseDir();
    List<String> testSQLs = sparkTestConfig.getTestSQLs();
    List<TestCaseGroup> sqlTestCaseGroups =
        findSQLTests(Paths.get(baseDir), Paths.get(baseDir), testSQLs, CatalogType.UNKNOWN);
    runSQLTests(sqlTestCaseGroups);
  }

  @BeforeAll
  void init() throws IOException {
    String configFile = System.getProperty(SPARK_TEST_CONFIG_FILE);
    this.sparkTestConfig = loadSparkTestConfig(configFile);
    this.sparkQueryRunner = new SparkQueryRunner(sparkTestConfig);
  }

  @AfterAll
  void close() throws Exception {
    if (sparkQueryRunner != null) {
      sparkQueryRunner.close();
    }
  }

  private List<TestCaseGroup> findSQLTests(
      Path basePath, Path path, List<String> testSQLs, CatalogType parentCatalogType)
      throws IOException {
    List<TestCaseGroup> sqlTestCaseGroups = new ArrayList<>();
    Path prepareSql = null;
    Path cleanupSql = null;
    List<TestCase> testCases = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path p : stream) {
        if (Files.isDirectory(p)) {
          CatalogType catalogType = CatalogType.fromString(p.getFileName().toString());
          sqlTestCaseGroups.addAll(
              findSQLTests(
                  basePath, p, testSQLs, CatalogType.merge(parentCatalogType, catalogType)));
        } else {
          if (p.toString().endsWith(".sql")) {
            if (p.getFileName().toString().equals("prepare.sql")) {
              prepareSql = p;
            } else if (p.getFileName().toString().equals("cleanup.sql")) {
              cleanupSql = p;
            } else {
              if (testSQLs.isEmpty()) {
                testCases.add(new TestCase(p));
              } else {
                // just run specific SQL
                Path remaindingPath = basePath.relativize(p);
                for (String whiteSQL : testSQLs) {
                  if (remaindingPath.startsWith(whiteSQL)) {
                    testCases.add(new TestCase(p));
                    break;
                  }
                }
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (!testCases.isEmpty()) {
      sqlTestCaseGroups.add(
          new TestCaseGroup(testCases, prepareSql, cleanupSql, parentCatalogType));
    }

    return sqlTestCaseGroups;
  }

  private void runSQLTests(List<TestCaseGroup> sqlTestCaseGroups) {
    sqlTestCaseGroups.forEach(
        sqlTestCaseGroup -> {
          try {
            sparkQueryRunner.runQuery(sqlTestCaseGroup);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private SparkTestConfig loadSparkTestConfig(String configFile) throws IOException {
    if (Strings.isBlank(configFile)) {
      String projectDir = System.getenv("GRAVITINO_ROOT_DIR");
      configFile =
          Paths.get(
                  projectDir,
                  "spark-connector",
                  "spark-common",
                  "src",
                  "test",
                  "resources",
                  "spark-test.conf")
              .toString();
    }
    LOG.info("config file: {}", configFile);

    SparkTestConfig testConfig = new SparkTestConfig();
    Properties properties = testConfig.loadPropertiesFromFile(new File(configFile));
    testConfig.loadFromProperties(properties);
    return testConfig;
  }
}
