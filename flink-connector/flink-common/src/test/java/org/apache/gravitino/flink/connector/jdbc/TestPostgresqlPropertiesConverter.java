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

package org.apache.gravitino.flink.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.flink.connector.jdbc.postgresql.PostgresqlPropertiesConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgresqlPropertiesConverter extends AbstractJdbcPropertiesConverterTestSuite {

  private static final String FLINK_BYPASS_DEFAULT_DATABASE = "flink.bypass.default-database";

  @Override
  protected JdbcPropertiesConverter getConverter(Map<String, String> catalogOptions) {
    return PostgresqlPropertiesConverter.INSTANCE;
  }

  @Test
  public void testToFlinkTableProperties() {
    String jdbcDatabase = "gravitino";
    String schema = "public";
    String tableName = "table_meta";
    // No 'flink.bypass.default-database' is set, so the connection database must fall back to
    // the catalog's jdbc-database.
    Map<String, String> catalogPropertiesWithDatabase = new HashMap<>(catalogProperties);
    catalogPropertiesWithDatabase.remove(FLINK_BYPASS_DEFAULT_DATABASE);
    catalogPropertiesWithDatabase.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_URL, gravitinoUrlWithDomain);
    catalogPropertiesWithDatabase.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE, jdbcDatabase);

    // Mirrors the production call in BaseCatalog#toFlinkTable: the first argument is the Flink
    // catalog properties (as produced by toFlinkCatalogProperties), the second is the Gravitino
    // table's own properties, which do not carry catalog-level properties like jdbc-database.
    Map<String, String> flinkCatalogProperties =
        getConverter(catalogPropertiesWithDatabase)
            .toFlinkCatalogProperties(catalogPropertiesWithDatabase);
    Map<String, String> tableProperties =
        getConverter(catalogPropertiesWithDatabase)
            .toFlinkTableProperties(
                flinkCatalogProperties, ImmutableMap.of(), new ObjectPath(schema, tableName));

    // The connection URL must target the PostgreSQL database (jdbc-database), not the schema.
    Assertions.assertEquals(
        flinkUrlWithDomain + jdbcDatabase,
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL));
    // The schema must be carried via the schema-qualified table name instead.
    Assertions.assertEquals(
        schema + "." + tableName,
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_NAME));
  }

  @Test
  public void testToFlinkTablePropertiesWithExplicitDefaultDatabase() {
    // 'flink.bypass.default-database' names the Flink current database (a PostgreSQL schema), so
    // the connection must still target jdbc-database.
    String schema = "public";
    Map<String, String> catalogPropertiesWithBoth = new HashMap<>(catalogProperties);
    catalogPropertiesWithBoth.put(FLINK_BYPASS_DEFAULT_DATABASE, schema);
    catalogPropertiesWithBoth.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_URL, gravitinoUrlWithDomain);
    catalogPropertiesWithBoth.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE, "gravitino");

    Map<String, String> flinkCatalogProperties =
        getConverter(catalogPropertiesWithBoth).toFlinkCatalogProperties(catalogPropertiesWithBoth);
    Map<String, String> tableProperties =
        getConverter(catalogPropertiesWithBoth)
            .toFlinkTableProperties(
                flinkCatalogProperties, ImmutableMap.of(), new ObjectPath(schema, "t"));

    Assertions.assertEquals(
        schema, flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
    Assertions.assertEquals(
        flinkUrlWithDomain + "gravitino",
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL));
    Assertions.assertEquals(
        schema + ".t", tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_NAME));
  }

  @Test
  public void testToFlinkTablePropertiesWithDatabaseFromJdbcUrl() {
    // Without jdbc-database, the connection database comes from jdbc-url.
    Map<String, String> catalogPropertiesWithUrlDatabase = new HashMap<>(catalogProperties);
    catalogPropertiesWithUrlDatabase.put(FLINK_BYPASS_DEFAULT_DATABASE, "public");
    catalogPropertiesWithUrlDatabase.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
        "jdbc:postgresql://db.example.com:5432/myDatabase?sslmode=require");

    Map<String, String> flinkCatalogProperties =
        getConverter(catalogPropertiesWithUrlDatabase)
            .toFlinkCatalogProperties(catalogPropertiesWithUrlDatabase);
    Map<String, String> tableProperties =
        getConverter(catalogPropertiesWithUrlDatabase)
            .toFlinkTableProperties(
                flinkCatalogProperties, ImmutableMap.of(), new ObjectPath("public", "t"));

    Assertions.assertEquals(
        "jdbc:postgresql://db.example.com:5432/myDatabase",
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL));
  }

  @Test
  public void testConnectionDatabaseFromJdbcUrl() {
    // Without jdbc-database, the database in jdbc-url is the connection database, but not the
    // Flink default database, which names a schema.
    Map<String, String> catalogPropertiesWithUrlDatabase = new HashMap<>(catalogProperties);
    catalogPropertiesWithUrlDatabase.remove(FLINK_BYPASS_DEFAULT_DATABASE);
    catalogPropertiesWithUrlDatabase.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
        "jdbc:postgresql://db.example.com:5432/myDatabase?sslmode=require");

    Map<String, String> flinkCatalogProperties =
        getConverter(catalogPropertiesWithUrlDatabase)
            .toFlinkCatalogProperties(catalogPropertiesWithUrlDatabase);

    Assertions.assertNull(
        flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
    Assertions.assertEquals(
        "myDatabase",
        flinkCatalogProperties.get(JdbcPropertiesConstants.GRAVITINO_CONNECTION_DATABASE));
  }

  @Test
  public void testToFlinkTablePropertiesWithBypassBaseUrl() {
    // Flink JDBC accepts a base-url with or without the trailing slash, and 4.x also one with the
    // database and query parameters.
    Map<String, String> expectedTableUrls =
        ImmutableMap.of(
            "jdbc:postgresql://127.0.0.1:15432/", "jdbc:postgresql://127.0.0.1:15432/gravitino",
            "jdbc:postgresql://127.0.0.1:15432", "jdbc:postgresql://127.0.0.1:15432/gravitino",
            "jdbc:postgresql://127.0.0.1:15432/gravitino",
                "jdbc:postgresql://127.0.0.1:15432/gravitino",
            "jdbc:postgresql://127.0.0.1:15432/gravitino?sslmode=require",
                "jdbc:postgresql://127.0.0.1:15432/gravitino?sslmode=require");
    for (Map.Entry<String, String> entry : expectedTableUrls.entrySet()) {
      String bypassBaseUrl = entry.getKey();
      Map<String, String> catalogPropertiesWithBypassUrl = new HashMap<>(catalogProperties);
      catalogPropertiesWithBypassUrl.put(FLINK_BYPASS_DEFAULT_DATABASE, "public");
      catalogPropertiesWithBypassUrl.put(
          JdbcPropertiesConstants.GRAVITINO_JDBC_URL, gravitinoUrlWithDomain);
      catalogPropertiesWithBypassUrl.put(
          JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE, "gravitino");
      catalogPropertiesWithBypassUrl.put("flink.bypass.base-url", bypassBaseUrl);

      Map<String, String> flinkCatalogProperties =
          getConverter(catalogPropertiesWithBypassUrl)
              .toFlinkCatalogProperties(catalogPropertiesWithBypassUrl);
      Map<String, String> tableProperties =
          getConverter(catalogPropertiesWithBypassUrl)
              .toFlinkTableProperties(
                  flinkCatalogProperties, ImmutableMap.of(), new ObjectPath("public", "t"));

      Assertions.assertEquals(
          entry.getValue(),
          tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL),
          bypassBaseUrl);
    }
  }

  @Test
  public void testToFlinkTablePropertiesWithoutJdbcDatabase() {
    Map<String, String> catalogPropertiesWithoutDatabase = new HashMap<>(catalogProperties);
    catalogPropertiesWithoutDatabase.remove(FLINK_BYPASS_DEFAULT_DATABASE);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          Map<String, String> flinkCatalogProperties =
              getConverter(catalogPropertiesWithoutDatabase)
                  .toFlinkCatalogProperties(catalogPropertiesWithoutDatabase);
          getConverter(catalogPropertiesWithoutDatabase)
              .toFlinkTableProperties(
                  flinkCatalogProperties, ImmutableMap.of(), new ObjectPath("public", "t"));
        });
  }

  @Test
  public void testToFlinkTablePropertiesWithEmptyJdbcDatabase() {
    Map<String, String> catalogPropertiesWithEmptyDatabase = new HashMap<>(catalogProperties);
    catalogPropertiesWithEmptyDatabase.remove(FLINK_BYPASS_DEFAULT_DATABASE);
    catalogPropertiesWithEmptyDatabase.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE, "");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          Map<String, String> flinkCatalogProperties =
              getConverter(catalogPropertiesWithEmptyDatabase)
                  .toFlinkCatalogProperties(catalogPropertiesWithEmptyDatabase);
          getConverter(catalogPropertiesWithEmptyDatabase)
              .toFlinkTableProperties(
                  flinkCatalogProperties, ImmutableMap.of(), new ObjectPath("public", "t"));
        });
  }
}
