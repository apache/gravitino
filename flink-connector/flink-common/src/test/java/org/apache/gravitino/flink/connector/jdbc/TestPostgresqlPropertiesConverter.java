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
        flinkUrl + jdbcDatabase,
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL));
    // The schema must be carried via the schema-qualified table name instead.
    Assertions.assertEquals(
        schema + "." + tableName,
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_NAME));
  }

  @Test
  public void testToFlinkTablePropertiesPrefersExplicitDefaultDatabase() {
    // When 'flink.bypass.default-database' is explicitly set, it takes precedence over
    // jdbc-database.
    String explicitDefaultDatabase = "explicit_db";
    Map<String, String> catalogPropertiesWithBoth = new HashMap<>(catalogProperties);
    catalogPropertiesWithBoth.put(FLINK_BYPASS_DEFAULT_DATABASE, explicitDefaultDatabase);
    catalogPropertiesWithBoth.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE, "gravitino");

    Map<String, String> flinkCatalogProperties =
        getConverter(catalogPropertiesWithBoth).toFlinkCatalogProperties(catalogPropertiesWithBoth);
    Map<String, String> tableProperties =
        getConverter(catalogPropertiesWithBoth)
            .toFlinkTableProperties(
                flinkCatalogProperties, ImmutableMap.of(), new ObjectPath("public", "t"));

    Assertions.assertEquals(
        flinkUrl + explicitDefaultDatabase,
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL));
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
