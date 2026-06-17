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
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
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
    // Align the catalog name that the server-side and Flink-side native Iceberg JdbcCatalog use for
    // the `catalog_name` column in the JDBC backend tables. The Flink-side native catalog is named
    // after the Flink catalog (the Gravitino catalog name), while the server-side defaults to the
    // `catalog-backend` value (`jdbc`). Without this, tables created through Gravitino are
    // invisible
    // to the Flink-side reads/writes. The table read/write tests run on the default catalog.
    catalogProperties.put(IcebergConstants.CATALOG_BACKEND_NAME, DEFAULT_ICEBERG_CATALOG);
    // Gravitino hides `jdbc-user`/`jdbc-password`, so they never reach the Flink-side native
    // Iceberg
    // catalog through the loaded properties. Enable credential vending so the server hands the JDBC
    // user/password to the client, which GravitinoIcebergCatalog.open() injects into the native
    // catalog via CredentialPropertyUtils.applyIcebergCredentials. This covers all access to an
    // already-created catalog.
    catalogProperties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, JdbcCredential.JDBC_CREDENTIAL_TYPE);
    // Vending cannot help at CREATE time: Flink opens the catalog (and Iceberg's JdbcCatalog
    // eagerly
    // connects to the database) before the catalog is persisted in Gravitino, so getCredentials()
    // finds nothing to vend. The descriptor-based create path therefore needs the native
    // `jdbc.user`/`jdbc.password` directly. See testCreateGravitinoIcebergCatalog.
    catalogProperties.put("jdbc.user", mySQLContainer.getUsername());
    catalogProperties.put("jdbc.password", mySQLContainer.getPassword());
    return catalogProperties;
  }

  /**
   * The base WITH clause only carries backend/uri/warehouse, but a JDBC backend also needs the jdbc
   * driver, credential vending, and native jdbc credentials (the latter for the create-time eager
   * connect, since vending is not available until the catalog is persisted in Gravitino).
   */
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
            + "'%s'='%s',"
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
        CredentialConstants.CREDENTIAL_PROVIDERS,
        JdbcCredential.JDBC_CREDENTIAL_TYPE,
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
