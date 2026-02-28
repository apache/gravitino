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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.utils.jdbc.JdbcSqlScriptUtils;

final class JdbcMetricsRepositoryITUtils {

  private JdbcMetricsRepositoryITUtils() {}

  static void initializeSchema(
      String jdbcUrl, String username, String password, String databaseType) {
    String schemaSql = loadSchemaSql(databaseType);
    try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
      JdbcSqlScriptUtils.executeSqlScript(connection, schemaSql);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize " + databaseType + " schema for metrics", e);
    }
  }

  static String loadSchemaSql(String databaseType) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (StringUtils.isBlank(gravitinoHome)) {
      throw new IllegalStateException("GRAVITINO_HOME environment variable must be set for ITs");
    }
    Path root = Path.of(gravitinoHome);
    Path scriptPath =
        root.resolve(
            Path.of(
                "scripts",
                databaseType,
                "schema-" + ConfigConstants.CURRENT_SCRIPT_VERSION + "-" + databaseType + ".sql"));
    try {
      return Files.readString(scriptPath, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load schema script: " + scriptPath, e);
    }
  }

  static Map<String, String> createJdbcMetricsConfigs(
      String jdbcUrl, String username, String password, String driverClassName) {
    return Map.of(
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_URL,
        jdbcUrl,
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_USER,
        username,
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_PASSWORD,
        password,
        OptimizerConfig.OPTIMIZER_PREFIX
            + GenericJdbcMetricsRepository.JDBC_METRICS_PREFIX
            + GenericJdbcMetricsRepository.JDBC_DRIVER,
        driverClassName);
  }
}
