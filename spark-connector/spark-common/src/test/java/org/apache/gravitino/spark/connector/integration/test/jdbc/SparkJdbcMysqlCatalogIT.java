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
package org.apache.gravitino.spark.connector.integration.test.jdbc;

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfoChecker;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants;
import org.apache.spark.SparkException;
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
    return catalogProperties;
  }

  @Test
  void testMySqlDatetimeFilterWithTimestampNtz() {
    String tableName = "t_mysql_datetime_ntz_filter";
    String fullTableName = String.join(".", getCatalogName(), getDefaultDatabase(), tableName);
    String originalTimestampType =
        getSparkSession().conf().get("spark.sql.timestampType", "TIMESTAMP_LTZ");
    getSparkSession().conf().set("spark.sql.timestampType", "TIMESTAMP_NTZ");
    dropTableIfExists(fullTableName);

    try {
      sql(
          String.format(
              "CREATE TABLE %s (id INT, name STRING, update_time TIMESTAMP)", fullTableName));
      sql(
          String.format(
              "INSERT INTO %s VALUES "
                  + "(1, 'xu', TIMESTAMP '2025-12-04 09:07:43'), "
                  + "(2, 'zhangsan', TIMESTAMP '2025-12-05 09:07:43')",
              fullTableName));

      String querySql =
          String.format(
              "SELECT * FROM %s WHERE update_time >= '2025-12-04 09:07:43'", fullTableName);

      SparkException sparkException =
          Assertions.assertThrows(SparkException.class, () -> getQueryData(querySql));
      Assertions.assertTrue(
          sparkException.getMessage().contains("SQLSyntaxErrorException"),
          () -> "Expected MySQL syntax error when filtering DATETIME with TIMESTAMP_NTZ literals");
    } finally {
      dropTableIfExists(fullTableName);
      getSparkSession().conf().set("spark.sql.timestampType", originalTimestampType);
    }
  }
}
