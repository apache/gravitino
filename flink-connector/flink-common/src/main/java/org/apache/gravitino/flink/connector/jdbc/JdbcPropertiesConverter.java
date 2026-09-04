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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;

public abstract class JdbcPropertiesConverter
    implements CatalogPropertiesConverter, SchemaAndTablePropertiesConverter {

  private static final Pattern jdbcUrlPattern = Pattern.compile("(jdbc:\\w+://[^:/]+(?::\\d+)?)");

  @Override
  public Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoCatalogProperties =
        CatalogPropertiesConverter.super.toGravitinoCatalogProperties(flinkConf);
    if (!gravitinoCatalogProperties.containsKey(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER)) {
      gravitinoCatalogProperties.put(
          JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, defaultDriverName());
    }
    return gravitinoCatalogProperties;
  }

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkCatalogProperties =
        CatalogPropertiesConverter.super.toFlinkCatalogProperties(gravitinoProperties);
    String gravitinoJdbcUrl = gravitinoProperties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL);
    Preconditions.checkArgument(
        gravitinoJdbcUrl != null,
        "Cannot create catalog properties: missing '"
            + JdbcPropertiesConstants.GRAVITINO_JDBC_URL
            + "'.");

    // The URL in FlinkJdbcCatalog does not support database and other parameters.
    flinkCatalogProperties.put(
        JdbcPropertiesConstants.FLINK_JDBC_URL, getBaseUrlFromJdbcUrl(gravitinoJdbcUrl));

    // Default the Flink catalog's 'default-database' option from the Gravitino jdbc-database
    // property when it was not already supplied (e.g. via flink.bypass.default-database). This
    // makes the value available when building per-table scan properties, since only the Flink
    // catalog properties (not the Gravitino table properties) reach toFlinkTableProperties, and
    // 'default-database' -- unlike 'jdbc-database' -- is an option Flink's JdbcCatalogFactory
    // already recognizes, so it survives that factory's strict option validation.
    String gravitinoJdbcDatabase =
        gravitinoProperties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE);
    if (gravitinoJdbcDatabase != null) {
      flinkCatalogProperties.putIfAbsent(
          JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE, gravitinoJdbcDatabase);
    }
    return flinkCatalogProperties;
  }

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return JdbcPropertiesConstants.flinkToGravitinoMap.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    return JdbcPropertiesConstants.gravitinoToFlinkMap.get(configKey);
  }

  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    String jdbcUser = flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_USER);
    String jdbcPassword = flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD);
    String jdbcBaseUrl = flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_URL);
    Preconditions.checkArgument(
        jdbcUser != null, JdbcPropertiesConstants.FLINK_JDBC_USER + " should not be null.");
    Preconditions.checkArgument(
        jdbcPassword != null, JdbcPropertiesConstants.FLINK_JDBC_PASSWORD + " should not be null.");
    Preconditions.checkArgument(
        jdbcBaseUrl != null, JdbcPropertiesConstants.FLINK_JDBC_URL + " should not be null.");
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put(
        JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL,
        jdbcBaseUrl + getConnectionDatabase(flinkCatalogProperties, tablePath));
    tableOptions.put(JdbcPropertiesConstants.FLINK_JDBC_TABLE_NAME, getTableName(tablePath));
    tableOptions.put(JdbcPropertiesConstants.FLINK_JDBC_USER, jdbcUser);
    tableOptions.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, jdbcPassword);
    return tableOptions;
  }

  /**
   * Returns the database name used to build the JDBC connection URL for a table scan. Defaults to
   * the Flink "database", which corresponds to the Gravitino schema name.
   *
   * @param flinkCatalogProperties the Flink catalog properties produced by {@link
   *     #toFlinkCatalogProperties(Map)}.
   * @param tablePath the Flink table path being scanned.
   * @return the database name to use when building the JDBC connection URL.
   */
  protected String getConnectionDatabase(
      Map<String, String> flinkCatalogProperties, ObjectPath tablePath) {
    return tablePath.getDatabaseName();
  }

  /** Returns the table name used in the JDBC connector's {@code table-name} option. */
  protected String getTableName(ObjectPath tablePath) {
    return tablePath.getObjectName();
  }

  protected abstract String defaultDriverName();

  private static String getBaseUrlFromJdbcUrl(String jdbcUrl) {
    Matcher matcher = jdbcUrlPattern.matcher(jdbcUrl);
    if (matcher.find()) {
      return matcher.group(1) + "/";
    } else {
      throw new IllegalArgumentException("Invalid JDBC URL format.");
    }
  }
}
