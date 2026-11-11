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

package org.apache.gravitino.flink.connector.jdbc.postgresql;

import com.google.common.base.Strings;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.flink.connector.jdbc.GravitinoJdbcCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.flink.connector.jdbc.JdbcPropertiesConverter;

public class PostgresqlPropertiesConverter extends JdbcPropertiesConverter {

  private static final Pattern JDBC_URL_DATABASE_PATTERN =
      Pattern.compile("jdbc:\\w+://[^/]+/([^/?;]+)");

  private PostgresqlPropertiesConverter() {}

  public static final PostgresqlPropertiesConverter INSTANCE = new PostgresqlPropertiesConverter();

  @Override
  protected String defaultDriverName() {
    return "org.postgresql.Driver";
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoJdbcCatalogFactoryOptions.POSTGRESQL_IDENTIFIER;
  }

  /**
   * For PostgreSQL, a Flink "database" is a Gravitino schema, so 'default-database' names the
   * schema Flink starts in, while JDBC connections always target the catalog's PostgreSQL database.
   * That database is resolved from jdbc-database, or from jdbc-url when jdbc-database is not set.
   */
  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkCatalogProperties =
        super.toFlinkCatalogProperties(gravitinoProperties);
    String connectionDatabase =
        gravitinoProperties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE);
    if (StringUtils.isBlank(connectionDatabase)) {
      connectionDatabase =
          getDatabaseFromJdbcUrl(
              gravitinoProperties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
    }
    if (StringUtils.isNotBlank(connectionDatabase)) {
      flinkCatalogProperties.put(
          JdbcPropertiesConstants.GRAVITINO_CONNECTION_DATABASE, connectionDatabase);
    }
    return flinkCatalogProperties;
  }

  @Override
  protected String getConnectionDatabase(
      Map<String, String> flinkCatalogProperties, ObjectPath tablePath) {
    // Fall back to 'default-database' when no connection database can be resolved, which is
    // also what the inner Flink JDBC catalog connects to in that case.
    String database =
        flinkCatalogProperties.getOrDefault(
            JdbcPropertiesConstants.GRAVITINO_CONNECTION_DATABASE,
            flinkCatalogProperties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(database),
        "Cannot resolve the PostgreSQL database: set '"
            + JdbcPropertiesConstants.GRAVITINO_JDBC_DATABASE
            + "' or include the database in '"
            + JdbcPropertiesConstants.GRAVITINO_JDBC_URL
            + "'.");
    return database;
  }

  @Override
  protected String getTableName(ObjectPath tablePath) {
    return tablePath.getDatabaseName() + "." + tablePath.getObjectName();
  }

  private static String getDatabaseFromJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
      return null;
    }
    Matcher matcher = JDBC_URL_DATABASE_PATTERN.matcher(jdbcUrl);
    return matcher.find() ? matcher.group(1) : null;
  }
}
