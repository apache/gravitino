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

import static org.apache.gravitino.integration.test.util.TestDatabaseName.PG_CATALOG_PG_IT;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfoChecker;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConstants;
import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-test")
public class SparkJdbcPostgreSqlCatalogIT extends SparkCommonIT {

  protected String pgUrl;
  protected String pgUsername;
  protected String pgPassword;
  protected String pgDriver;

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
    return false;
  }

  @Override
  protected boolean supportsCreateTableWithComment() {
    return false;
  }

  @Override
  protected String getCatalogName() {
    return "jdbc_postgresql";
  }

  @Override
  protected String getProvider() {
    return "jdbc-postgresql";
  }

  @Override
  protected SparkTableInfoChecker getTableInfoChecker() {
    return SparkJdbcTableInfoChecker.create();
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startPostgreSQLContainer(PG_CATALOG_PG_IT);
    this.pgUrl = containerSuite.getPostgreSQLContainer().getJdbcUrl();
    this.pgUsername = containerSuite.getPostgreSQLContainer().getUsername();
    this.pgPassword = containerSuite.getPostgreSQLContainer().getPassword();
    this.pgDriver = containerSuite.getPostgreSQLContainer().getDriverClassName(PG_CATALOG_PG_IT);
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_URL, this.pgUrl);
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_USER, this.pgUsername);
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD, this.pgPassword);
    catalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, this.pgDriver);
    catalogProperties.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE, PG_CATALOG_PG_IT.toString());
    return catalogProperties;
  }
}
