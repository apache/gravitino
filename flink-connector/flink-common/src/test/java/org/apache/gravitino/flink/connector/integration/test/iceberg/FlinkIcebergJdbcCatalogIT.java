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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableResult;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
   * The base test creates the catalog via SQL with only backend/uri/warehouse, but a JDBC backend
   * also requires the jdbc credentials and credential vending, so the WITH clause is extended here.
   */
  @Override
  @Test
  public void testCreateGravitinoIcebergUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    String catalogName = "gravitino_iceberg_using_sql";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='%s', "
                + "'catalog-backend'='%s',"
                + "'uri'='%s',"
                + "'warehouse'='%s',"
                + "'jdbc-user'='%s',"
                + "'jdbc-password'='%s',"
                + "'jdbc-driver'='%s',"
                + "'%s'='%s',"
                // Native jdbc credentials are needed for the create-time eager connect (vending is
                // not available until the catalog is persisted in Gravitino).
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
            mySQLContainer.getPassword()));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(getUri(), properties.get(IcebergConstants.URI));

    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, catalog.get());

    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    TableResult result = tableEnv.executeSql("show catalogs");
    Assertions.assertEquals(
        numCatalogs + 1, Lists.newArrayList(result.collect()).size(), "Should have 2 catalogs");

    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the one that is created just now.");

    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<org.apache.flink.table.catalog.Catalog> droppedCatalog =
        tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
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
