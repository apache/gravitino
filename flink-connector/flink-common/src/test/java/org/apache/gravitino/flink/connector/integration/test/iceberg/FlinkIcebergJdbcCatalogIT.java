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

package org.apache.gravitino.flink.connector.integration.test.iceberg;

import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Tag;

/** Iceberg catalog with a JDBC (MySQL) metadata backend. */
@Tag("gravitino-docker-test")
public abstract class FlinkIcebergJdbcCatalogIT extends FlinkIcebergCatalogIT {

  private static final TestDatabaseName TEST_DB_NAME =
      TestDatabaseName.FLINK_ICEBERG_JDBC_CATALOG_IT;

  private static MySQLContainer mySQLContainer;

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC);
    catalogProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, getUri());
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_USER, mySQLContainer.getUsername());
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, mySQLContainer.getPassword());
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, getDriverClassName());
    catalogProperties.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    // Align catalog_name between server-side and Flink-side JdbcCatalog so tables created through
    // Gravitino are visible to the Flink-side reads/writes.
    catalogProperties.put(IcebergConstants.CATALOG_BACKEND_NAME, DEFAULT_ICEBERG_CATALOG);
    // `credential-providers` is intentionally omitted: the lakehouse-iceberg catalog auto-enables
    // `jdbc-user-password` for JDBC backends, so GravitinoIcebergCatalog.open() injects the
    // credentials via credential vending. This exercises the real auto-vending path.
    // `jdbc.user`/`jdbc.password` are only needed for CREATE CATALOG, where Flink opens the
    // catalog before it is persisted in Gravitino and vending is not yet available.
    catalogProperties.put("jdbc.user", mySQLContainer.getUsername());
    catalogProperties.put("jdbc.password", mySQLContainer.getPassword());
    return catalogProperties;
  }

  @Override
  protected String buildCreateCatalogSql(String catalogName) {
    return String.format(
        "create catalog %s with ("
            + "'type'='%s', "
            + "'catalog-backend'='%s',"
            + "'uri'='%s',"
            + "'warehouse'='%s',"
            + "'jdbc-user'='%s',"
            + "'jdbc-password'='%s',"
            + "'jdbc-driver'='%s',"
            + "'jdbc.user'='%s',"
            + "'jdbc.password'='%s'"
            + ")",
        catalogName,
        GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
        getCatalogBackend(),
        getUri(),
        warehouse,
        mySQLContainer.getUsername(),
        mySQLContainer.getPassword(),
        getDriverClassName(),
        mySQLContainer.getUsername(),
        mySQLContainer.getPassword());
  }

  @Override
  protected String getCatalogBackend() {
    return IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC;
  }

  @Override
  protected String getUri() {
    return mySQLContainer.getJdbcUrl(TEST_DB_NAME);
  }

  private String getDriverClassName() {
    try {
      return mySQLContainer.getDriverClassName(TEST_DB_NAME);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to get MySQL driver class name", e);
    }
  }
}
