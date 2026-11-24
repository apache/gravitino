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
package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonJdbcIT extends CatalogPaimonBaseIT {

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "jdbc";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-paimon/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
    URI = mySQLContainer.getJdbcUrl(TEST_DB_NAME);
    jdbcUser = mySQLContainer.getUsername();
    jdbcPassword = mySQLContainer.getPassword();

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.URI, URI);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER, jdbcUser);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD, jdbcPassword);
    catalogProperties.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver");

    return catalogProperties;
  }

  @Test
  void testTestConnection() {
    Map<String, String> props = initPaimonCatalogProperties();
    props.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD, "wrong_password");

    Exception exception =
        Assertions.assertThrows(
            ConnectionFailedException.class,
            () ->
                metalake.testConnection(
                    GravitinoITUtils.genRandomName("paimon_it_catalog"),
                    Catalog.Type.RELATIONAL,
                    provider,
                    null,
                    props));
    Assertions.assertTrue(exception.getMessage().contains("Access denied for user"));
  }

  @Override
  protected void initSparkEnv() {
    // Point Spark at the same JDBC metastore, so it sees the views created through Gravitino. Do
    // not
    // let it create the `default` database there: the MySQL database is shared with other ITs.
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Paimon Catalog integration test")
            .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.metastore", "jdbc")
            .config("spark.sql.catalog.paimon.uri", URI)
            .config("spark.sql.catalog.paimon.jdbc.user", jdbcUser)
            .config("spark.sql.catalog.paimon.jdbc.password", jdbcPassword)
            .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE)
            .config("spark.sql.catalog.paimon.cache-enabled", "false")
            .config("spark.sql.catalog.paimon.disable-create-table-in-default-db", "true")
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
  }

  // The Paimon JDBC catalog supports views since Paimon 2.0.
  @Test
  @Override
  void testSparkCreateViewAndLoadByGravitino() {
    NameIdentifier baseTableIdentifier =
        createSimplePaimonTableForViewInterop("spark_create_view_source_table");
    String viewName = GravitinoITUtils.genRandomName("spark_create_view");
    spark.sql(
        String.format(
            "CREATE VIEW paimon.%s.%s AS SELECT id, name FROM paimon.%s.%s",
            schemaName, viewName, schemaName, baseTableIdentifier.name()));

    View loadedView = catalog.asViewCatalog().loadView(NameIdentifier.of(schemaName, viewName));
    Assertions.assertEquals(viewName, loadedView.name());
    Assertions.assertEquals(2, loadedView.columns().length);
    Assertions.assertEquals("id", loadedView.columns()[0].name());
    Assertions.assertEquals("name", loadedView.columns()[1].name());
    Assertions.assertTrue(loadedView.representations().length > 0);
  }

  @Test
  @Override
  void testGravitinoCreateViewAndReadBySpark() {
    NameIdentifier baseTableIdentifier =
        createSimplePaimonTableForViewInterop("gravitino_create_view_source_table");
    String viewName = GravitinoITUtils.genRandomName("gravitino_create_view");
    String query =
        String.format("SELECT id, name FROM paimon.%s.%s", schemaName, baseTableIdentifier.name());
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    viewCatalog.createView(
        NameIdentifier.of(schemaName, viewName),
        "view_for_spark_read",
        new Column[] {
          Column.of("id", Types.IntegerType.get(), "id column"),
          Column.of("name", Types.StringType.get(), "name column")
        },
        new Representation[] {
          SQLRepresentation.builder().withDialect("query").withSql(query).build(),
          SQLRepresentation.builder().withDialect("spark").withSql(query).build()
        },
        "paimon",
        schemaName,
        Collections.emptyMap());

    List<Row> results =
        spark
            .sql(String.format("SELECT * FROM paimon.%s.%s ORDER BY id", schemaName, viewName))
            .collectAsList();
    Assertions.assertEquals(2, results.size());
    Assertions.assertEquals(1, results.get(0).getInt(0));
    Assertions.assertEquals("name_1", results.get(0).getString(1));
    Assertions.assertEquals(2, results.get(1).getInt(0));
    Assertions.assertEquals("name_2", results.get(1).getString(1));
  }

  @Test
  void testTablePropertiesTableOnMySqlUtf8mb4() throws Exception {
    try (Connection connection = DriverManager.getConnection(URI, jdbcUser, jdbcPassword);
        Statement statement = connection.createStatement()) {
      // The test database uses utf8mb4, where Paimon's own table definition does not fit.
      try (ResultSet rs =
          statement.executeQuery(
              "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA"
                  + " WHERE SCHEMA_NAME = DATABASE()")) {
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("utf8mb4", rs.getString(1));
      }

      // Created by Gravitino with a key that fits, so the catalog started.
      Map<String, Integer> keyColumns = new HashMap<>();
      try (ResultSet rs =
          statement.executeQuery(
              "SELECT k.COLUMN_NAME, c.CHARACTER_MAXIMUM_LENGTH"
                  + " FROM information_schema.KEY_COLUMN_USAGE k"
                  + " JOIN information_schema.COLUMNS c ON c.TABLE_SCHEMA = k.TABLE_SCHEMA"
                  + " AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME"
                  + " WHERE k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = 'paimon_table_properties'"
                  + " AND k.CONSTRAINT_NAME = 'PRIMARY'")) {
        while (rs.next()) {
          keyColumns.put(rs.getString(1), rs.getInt(2));
        }
      }
      Assertions.assertEquals(
          ImmutableMap.of(
              "catalog_key", 64, "database_name", 255, "table_name", 255, "property_key", 194),
          keyColumns);

      // Paimon writes the table options into it when a table is created.
      String tableName = GravitinoITUtils.genRandomName("paimon_table_properties_sync");
      catalog
          .asTableCatalog()
          .createTable(
              NameIdentifier.of(schemaName, tableName),
              new Column[] {Column.of("id", Types.IntegerType.get(), "id")},
              null,
              ImmutableMap.of("file.format", "parquet"));
      try (PreparedStatement ps =
          connection.prepareStatement(
              "SELECT property_value FROM paimon_table_properties"
                  + " WHERE database_name = ? AND table_name = ? AND property_key = ?")) {
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, "file.format");
        try (ResultSet rs = ps.executeQuery()) {
          Assertions.assertTrue(rs.next());
          Assertions.assertEquals("parquet", rs.getString(1));
        }
      }
    }
  }
}
