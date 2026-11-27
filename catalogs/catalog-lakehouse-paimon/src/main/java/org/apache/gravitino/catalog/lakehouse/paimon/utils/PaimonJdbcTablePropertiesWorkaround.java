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
package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.paimon.jdbc.JdbcCatalog;
import org.apache.paimon.options.CatalogOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the {@code paimon_table_properties} table of a Paimon JDBC catalog on MySQL before Paimon
 * does.
 *
 * <p>Since Paimon 1.4, the JDBC catalog creates this table at startup with a primary key of four
 * {@code VARCHAR(255)} columns. On MySQL with {@code utf8mb4} the key takes 4 x 255 x 4 = 4080
 * bytes, above InnoDB's 3072-byte limit, so the catalog fails to start (apache/paimon#11096).
 * Paimon only creates the table when it is missing, so this class creates it first with the same
 * columns and a key that fits: the database and table names keep 255 characters, as in {@code
 * paimon_tables}, and {@code catalog_key} and {@code property_key} are shortened to 64 and 194
 * characters, which adds up to exactly 3072 bytes.
 *
 * <p>TODO: Remove this class after upgrading to a Paimon release that fixes apache/paimon#11096.
 */
public class PaimonJdbcTablePropertiesWorkaround {

  private static final Logger LOG =
      LoggerFactory.getLogger(PaimonJdbcTablePropertiesWorkaround.class);

  @VisibleForTesting static final String TABLE_PROPERTIES_TABLE_NAME = "paimon_table_properties";

  @VisibleForTesting
  static final String CREATE_TABLE_PROPERTIES_TABLE =
      "CREATE TABLE "
          + TABLE_PROPERTIES_TABLE_NAME
          + "("
          + "catalog_key VARCHAR(64) NOT NULL,"
          + "database_name VARCHAR(255) NOT NULL,"
          + "table_name VARCHAR(255) NOT NULL,"
          + "property_key VARCHAR(194) NOT NULL,"
          + "property_value VARCHAR(1000),"
          + "PRIMARY KEY (catalog_key, database_name, table_name, property_key)"
          + ")";

  private PaimonJdbcTablePropertiesWorkaround() {}

  /**
   * Creates the table if the catalog uses the JDBC backend on MySQL, syncs table properties and the
   * table does not exist yet. Otherwise, it does nothing and leaves the table to Paimon.
   *
   * @param paimonConfig The Paimon configuration of the catalog.
   */
  public static void createTablePropertiesTableIfNeeded(PaimonConfig paimonConfig) {
    Map<String, String> config = paimonConfig.getAllConfig();
    if (!isNeeded(config)) {
      return;
    }

    String uri = config.get(CatalogOptions.URI.key());
    Properties properties = new Properties();
    config.forEach(
        (key, value) -> {
          if (key.startsWith(JdbcCatalog.PROPERTY_PREFIX)) {
            properties.put(key.substring(JdbcCatalog.PROPERTY_PREFIX.length()), value);
          }
        });
    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      createTableIfMissing(connection);
    } catch (SQLException e) {
      // Leave it to Paimon, which creates the table itself and reports its own error.
      LOG.warn(
          "Failed to create {} before the Paimon JDBC catalog starts",
          TABLE_PROPERTIES_TABLE_NAME,
          e);
    }
  }

  @VisibleForTesting
  static boolean isNeeded(Map<String, String> config) {
    String backend = config.get(PaimonConfig.CATALOG_BACKEND.getKey());
    if (!PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(backend)) {
      return false;
    }
    String uri = config.get(CatalogOptions.URI.key());
    if (StringUtils.isBlank(uri) || !uri.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql:")) {
      return false;
    }
    String sync = config.get(CatalogOptions.SYNC_ALL_PROPERTIES.key());
    return sync == null
        ? CatalogOptions.SYNC_ALL_PROPERTIES.defaultValue()
        : Boolean.parseBoolean(sync);
  }

  @VisibleForTesting
  static void createTableIfMissing(Connection connection) throws SQLException {
    try (ResultSet tables =
        connection
            .getMetaData()
            .getTables(connection.getCatalog(), null, TABLE_PROPERTIES_TABLE_NAME, null)) {
      if (tables.next()) {
        return;
      }
    }
    try (Statement statement = connection.createStatement()) {
      statement.execute(CREATE_TABLE_PROPERTIES_TABLE);
      LOG.info("Created {} for the Paimon JDBC catalog", TABLE_PROPERTIES_TABLE_NAME);
    }
  }
}
