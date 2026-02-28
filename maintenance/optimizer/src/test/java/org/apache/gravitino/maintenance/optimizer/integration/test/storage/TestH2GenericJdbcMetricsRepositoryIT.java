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

package org.apache.gravitino.maintenance.optimizer.integration.test.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.BaseGenericJdbcMetricsRepositoryTest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecord;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricRecordImpl;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.TableMetricWriteRequest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestH2GenericJdbcMetricsRepositoryIT extends BaseGenericJdbcMetricsRepositoryTest {
  private static final String H2_USER = "sa";
  private static final String H2_PASSWORD = "";
  private static final String H2_DRIVER = "org.h2.Driver";

  private String jdbcUrl;

  @BeforeAll
  void setUp() throws IOException {
    Path testDir = Files.createTempDirectory("optimizer-h2-metrics");
    jdbcUrl = "jdbc:h2:file:" + testDir.resolve("metrics.db") + ";MODE=MYSQL";
    initializeSchema(jdbcUrl);
    storage = new GenericJdbcMetricsRepository();
    storage.initialize(createJdbcConfigs(jdbcUrl));
    storage.cleanupTableMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.cleanupJobMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
  }

  @AfterAll
  void tearDown() {
    storage.cleanupTableMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.cleanupJobMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.close();
  }

  @Test
  void testInitializeTwiceFails() {
    GenericJdbcMetricsRepository repository = new GenericJdbcMetricsRepository();
    String initTwiceJdbcUrl = withSuffix(jdbcUrl, "_init_twice");
    initializeSchema(initTwiceJdbcUrl);
    repository.initialize(createJdbcConfigs(initTwiceJdbcUrl));
    try {
      IllegalStateException e =
          Assertions.assertThrows(
              IllegalStateException.class,
              () -> repository.initialize(createJdbcConfigs(initTwiceJdbcUrl)));
      Assertions.assertTrue(e.getMessage().contains("already been initialized"));
    } finally {
      cleanupAndClose(repository);
    }
  }

  @Test
  void testInitializeWithoutSchemaAutoCreatesForH2() {
    String autoCreateSchemaJdbcUrl = withSuffix(jdbcUrl, "_auto_create_schema");
    GenericJdbcMetricsRepository repository = createInitializedRepository(autoCreateSchemaJdbcUrl);
    try {
      NameIdentifier tableId = NameIdentifier.of("catalog", "db", "auto_create_table");
      long now = currentEpochSeconds();
      repository.storeTableMetrics(
          List.of(
              new TableMetricWriteRequest(
                  tableId, "row_count", Optional.empty(), new MetricRecordImpl(now, "12"))));

      Map<String, List<MetricRecord>> metrics =
          repository.getTableMetrics(tableId, now - 1, now + 1);
      Assertions.assertTrue(metrics.containsKey("row_count"));
      Assertions.assertEquals(List.of("12"), getMetricValues(metrics.get("row_count")));
    } finally {
      cleanupAndClose(repository);
    }
  }

  private Map<String, String> createJdbcConfigs(String jdbcUrl) {
    return Map.of(
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_URL,
        jdbcUrl,
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_USER,
        H2_USER,
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_PASSWORD,
        H2_PASSWORD,
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_DRIVER,
        H2_DRIVER);
  }

  private void initializeSchema(String jdbcUrl) {
    JdbcMetricsRepositoryITUtils.initializeSchema(jdbcUrl, H2_USER, H2_PASSWORD, "h2");
  }

  private String withSuffix(String baseJdbcUrl, String suffix) {
    int optionsSeparator = baseJdbcUrl.indexOf(';');
    if (optionsSeparator < 0) {
      return baseJdbcUrl + suffix;
    }

    return baseJdbcUrl.substring(0, optionsSeparator)
        + suffix
        + baseJdbcUrl.substring(optionsSeparator);
  }

  private GenericJdbcMetricsRepository createInitializedRepository(String jdbcUrl) {
    GenericJdbcMetricsRepository repository = new GenericJdbcMetricsRepository();
    repository.initialize(createJdbcConfigs(jdbcUrl));
    return repository;
  }

  private void cleanupAndClose(GenericJdbcMetricsRepository repository) {
    repository.cleanupTableMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    repository.cleanupJobMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    repository.close();
  }
}
