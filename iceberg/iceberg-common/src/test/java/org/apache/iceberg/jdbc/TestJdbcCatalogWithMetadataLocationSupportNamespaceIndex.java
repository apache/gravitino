/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.jdbc;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link JdbcCatalogWithMetadataLocationSupport} creates the {@code
 * iceberg_tables}-namespace supporting index against a real PostgreSQL backend, and that it can be
 * disabled via {@link IcebergConstants#ICEBERG_JDBC_CREATE_NAMESPACE_INDEX}.
 *
 * <p>Runs against SQLite too (via {@link
 * TestJdbcCatalogWithMetadataLocationSupport#testLoadFields()} and friends, in the sibling
 * non-docker test class) to confirm the feature is a no-op for non-PostgreSQL backends; that
 * coverage isn't duplicated here.
 */
@Tag("gravitino-docker-test")
public class TestJdbcCatalogWithMetadataLocationSupportNamespaceIndex {

  private static final String INDEX_NAME = "gravitino_iceberg_tables_namespace_pattern";

  private static PostgreSQLContainer postgreSQLContainer;

  @BeforeAll
  public static void startPostgres() {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_ICEBERG_NAMESPACE_INDEX_IT);
    postgreSQLContainer = containerSuite.getPostgreSQLContainer();
  }

  @Test
  void testIndexCreatedByDefault() throws Exception {
    String jdbcUrl = postgreSQLContainer.getJdbcUrl(TestDatabaseName.PG_ICEBERG_NAMESPACE_INDEX_IT);

    JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(true);
    catalog.initialize("test_index_default", newProperties(jdbcUrl));

    Assertions.assertTrue(
        indexExists(jdbcUrl), "Expected " + INDEX_NAME + " to be created by default");
  }

  @Test
  void testIndexCreationCanBeDisabled() throws Exception {
    String jdbcUrl = postgreSQLContainer.getJdbcUrl(TestDatabaseName.PG_ICEBERG_NAMESPACE_INDEX_IT);

    Map<String, String> properties = newProperties(jdbcUrl);
    properties.put(IcebergConstants.ICEBERG_JDBC_CREATE_NAMESPACE_INDEX, "false");

    JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(true);
    catalog.initialize("test_index_disabled", properties);

    Assertions.assertFalse(
        indexExists(jdbcUrl),
        "Expected " + INDEX_NAME + " not to be created when explicitly disabled");
  }

  private boolean indexExists(String jdbcUrl) throws Exception {
    try (Connection conn =
            DriverManager.getConnection(
                jdbcUrl, postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery("SELECT 1 FROM pg_indexes WHERE indexname = '" + INDEX_NAME + "'")) {
      return rs.next();
    }
  }

  private Map<String, String> newProperties(String jdbcUrl) throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, jdbcUrl);
    properties.put(
        CatalogProperties.WAREHOUSE_LOCATION,
        Files.createTempDirectory("jdbc-namespace-index-it").toString());
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, postgreSQLContainer.getUsername());
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, postgreSQLContainer.getPassword());
    return properties;
  }
}
