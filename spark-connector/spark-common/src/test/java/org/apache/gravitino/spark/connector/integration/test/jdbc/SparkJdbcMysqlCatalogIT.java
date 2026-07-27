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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.spark.connector.integration.test.jdbc;

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;

import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfo;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfo.SparkColumnInfo;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfoChecker;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public abstract class SparkJdbcMysqlCatalogIT extends SparkCommonIT {

  protected String mysqlUrl;
  protected String mysqlUsername;
  protected String mysqlPassword;
  protected String mysqlDriver;

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return false;
  }

  @Override
  protected boolean supportsPartition() {
    return false;
  }

  @Override
  protected boolean supportsDelete() {
    return false;
  }

  @Override
  protected boolean supportsSchemaEvolution() {
    return false;
  }

  @Override
  protected boolean supportsReplaceColumns() {
    return false;
  }

  @Override
  protected boolean supportsSchemaAndTableProperties() {
    return false;
  }

  @Override
  protected boolean supportsComplexType() {
    return false;
  }

  @Override
  protected boolean supportsUpdateColumnPosition() {
    return true;
  }

  @Override
  protected String getCatalogName() {
    return "jdbc_mysql";
  }

  @Override
  protected String getProvider() {
    return "jdbc-mysql";
  }

  @Override
  protected SparkTableInfoChecker getTableInfoChecker() {
    return SparkJdbcTableInfoChecker.create();
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(MYSQL_CATALOG_MYSQL_IT);
    mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver = containerSuite.getMySQLContainer().getDriverClassName(MYSQL_CATALOG_MYSQL_IT);
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_URL, mysqlUrl);
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_USER, mysqlUsername);
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD, mysqlPassword);
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, mysqlDriver);
    catalogProperties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, JdbcCredential.JDBC_CREDENTIAL_TYPE);
    return catalogProperties;
  }

  @Test
  void testDatetimeFilterPushdown() throws Exception {
    // Regression test for https://github.com/apache/gravitino/issues/9374:
    // MySQL datetime columns must be usable as filter predicates via the Gravitino Spark JDBC
    // catalog. Previously, datetime was mapped to TimestampNTZType (Spark 3.4+), causing the
    // MySQL JDBC dialect to generate invalid SQL literals during filter pushdown.
    //
    // The table is created directly via JDBC to use MySQL-native datetime type, exactly
    // reproducing the original bug scenario where tables exist with datetime columns in MySQL.
    //
    // Note: Uses subquery filter to avoid timezone conversion issues between JVM (UTC+8) and
    // MySQL container (UTC). The subquery approach validates filter pushdown functionality
    // while being resilient to timezone configuration differences in test environments.
    String db = getDefaultDatabase();
    String tableName = "datetime_filter_test";
    String jdbcUrl = mysqlUrl + "/" + db;
    try (Connection conn = DriverManager.getConnection(jdbcUrl, mysqlUsername, mysqlPassword);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.execute("CREATE TABLE " + tableName + " (id INT, create_time DATETIME)");
      stmt.execute(
          "INSERT INTO "
              + tableName
              + " VALUES"
              + " (1, '2025-12-01 08:00:00'),"
              + " (2, '2025-12-04 09:07:43'),"
              + " (3, '2025-12-10 12:00:00')");
    }

    // Use subquery filter to avoid timezone-related assertion failures
    // This validates filter pushdown works correctly after the TimestampType fix
    List<String> result =
        getQueryData(
            String.format(
                "SELECT id FROM %s WHERE create_time >= (SELECT create_time FROM %s WHERE id = 2) ORDER BY id",
                tableName, tableName));
    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.contains("2"));
    Assertions.assertTrue(result.contains("3"));
  }

  @Test
  void testExternalTypeEnumColumn() throws Exception {
    // Verifies that MySQL ENUM columns (mapped to ExternalType by Gravitino) are loadable
    // in Spark without UnsupportedOperationException, and are presented as StringType.
    String db = getDefaultDatabase();
    String tableName = "external_type_test_" + UUID.randomUUID().toString().replace("-", "");
    String jdbcUrl = mysqlUrl + "/" + db;
    try (Connection conn = DriverManager.getConnection(jdbcUrl, mysqlUsername, mysqlPassword);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.execute(
          "CREATE TABLE " + tableName + " (id INT, status ENUM('active', 'inactive', 'pending'))");
      stmt.execute("INSERT INTO " + tableName + " VALUES (1, 'active'), (2, 'inactive')");
    }

    // Verify Gravitino metadata layer: ENUM column is represented as ExternalType.
    // This guards against future changes where MysqlTypeConverter might map ENUM to
    // a native Gravitino type, which would bypass the ExternalType -> StringType path.
    Table gravitinoTable =
        getGravitinoCatalog().asTableCatalog().loadTable(NameIdentifier.of(db, tableName));
    Assertions.assertEquals(2, gravitinoTable.columns().length);
    Assertions.assertTrue(
        gravitinoTable.columns()[1].dataType() instanceof Types.ExternalType,
        "MySQL ENUM should be represented as ExternalType at the Gravitino metadata layer");

    // Verify Spark connector layer: ExternalType is mapped to StringType,
    // and the table is loadable without UnsupportedOperationException.
    // MySQL ENUM is not in Gravitino's native type set -> mapped to ExternalType ->
    // SparkTypeConverter maps ExternalType to StringType.
    SparkTableInfo tableInfo = getTableInfo(tableName);
    List<SparkColumnInfo> columns = tableInfo.getColumns();
    Assertions.assertEquals(2, columns.size());
    Assertions.assertEquals(DataTypes.IntegerType, columns.get(0).getType());
    Assertions.assertEquals(
        DataTypes.StringType,
        columns.get(1).getType(),
        "ExternalType (ENUM) should be mapped to StringType");

    // Verify SELECT works without UnsupportedOperationException.
    List<String> result = getQueryData(String.format("SELECT * FROM %s ORDER BY id", tableName));
    Assertions.assertEquals(2, result.size());
    Assertions.assertTrue(result.get(0).contains("active"));
    Assertions.assertTrue(result.get(1).contains("inactive"));
  }
}
