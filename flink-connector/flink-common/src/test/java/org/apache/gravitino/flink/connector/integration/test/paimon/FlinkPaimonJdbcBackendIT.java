/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.flink.connector.integration.test.paimon;

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

@Tag("gravitino-docker-test")
public abstract class FlinkPaimonJdbcBackendIT extends FlinkPaimonCatalogIT {

  private static final String DEFAULT_PAIMON_CATALOG = "test_flink_paimon_jdbc_catalog";

  // Paimon's JdbcCatalog bootstraps a paimon_table_properties system table whose 4-column
  // composite VARCHAR(255) primary key exceeds MySQL InnoDB's 3072-byte index length limit under
  // the utf8mb4 charset (4 bytes/char): 4 * 255 * 4 = 4080 bytes. utf8mb3 (3 bytes/char) fits:
  // 4 * 255 * 3 = 3060 bytes. Use a dedicated database with that charset instead of the shared
  // MYSQL_CATALOG_MYSQL_IT one (which defaults to utf8mb4) to avoid touching shared test
  // infrastructure.
  private static final String PAIMON_JDBC_BACKEND_DATABASE = "paimon_jdbc_backend_it";

  @TempDir private static Path warehouseDir;

  private String mysqlUsername;

  private String mysqlPassword;

  private String databaseUrl;

  private String mysqlDriver;

  @Override
  protected void createGravitinoCatalogByFlinkSql(String catalogName) {
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-paimon', "
                + "'warehouse'='%s',"
                + "'metastore'='jdbc',"
                + "'uri'='%s',"
                + "'jdbc.user'='%s',"
                + "'jdbc.password'='%s'"
                + ")",
            catalogName, warehouseDir, databaseUrl, mysqlUsername, mysqlPassword));
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    // Only used to ensure the MySQL container itself is up; this test uses its own database
    // (see PAIMON_JDBC_BACKEND_DATABASE) rather than the shared MYSQL_CATALOG_MYSQL_IT one.
    containerSuite.startMySQLContainer(MYSQL_CATALOG_MYSQL_IT);
    String mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver = containerSuite.getMySQLContainer().getDriverClassName(MYSQL_CATALOG_MYSQL_IT);

    Class.forName(mysqlDriver);
    try (Connection connection =
            DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci;",
              PAIMON_JDBC_BACKEND_DATABASE));
    }
    databaseUrl = mysqlUrl + "/" + PAIMON_JDBC_BACKEND_DATABASE;
  }

  @Override
  protected String getPaimonCatalogName() {
    return DEFAULT_PAIMON_CATALOG;
  }

  @Override
  protected Map<String, String> getPaimonCatalogOptions() {
    return ImmutableMap.of(
        PaimonConstants.CATALOG_BACKEND,
        "jdbc",
        "warehouse",
        warehouseDir.toString(),
        "uri",
        databaseUrl,
        "jdbc-user",
        mysqlUsername,
        "jdbc-password",
        mysqlPassword,
        "jdbc-driver",
        mysqlDriver);
  }

  @Override
  protected String getWarehouse() {
    return warehouseDir.toString();
  }

  @Override
  protected boolean supportViewOperation() {
    // Paimon JDBC metastore backend does not support view operations.
    return false;
  }
}
