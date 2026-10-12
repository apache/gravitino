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
package org.apache.gravitino.catalog.jdbc.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates the canonical {@link JdbcConfig#CONNECT_TIMEOUT_MS connect-timeout-ms} into the
 * driver-family connection properties that actually bound a JDBC connection attempt.
 *
 * <p>DBCP has no portable TCP-connect timeout; the real bound lives in the driver, and both the
 * parameter name and its unit differ per driver family:
 *
 * <ul>
 *   <li><b>MySQL Connector/J</b> ({@code com.mysql.cj.jdbc.Driver}, the legacy {@code
 *       com.mysql.jdbc.Driver} — also used by Doris and StarRocks — and the MySQL-compatible
 *       OceanBase Connector/J {@code com.oceanbase.jdbc.Driver}): {@code connectTimeout} and {@code
 *       socketTimeout} in <b>milliseconds</b>.
 *   <li><b>PostgreSQL</b> ({@code org.postgresql.Driver}, also used by the PostgreSQL-compatible
 *       Hologres catalog): {@code connectTimeout} and {@code socketTimeout} in <b>seconds</b>.
 *   <li><b>ClickHouse</b> ({@code com.clickhouse.jdbc.ClickHouseDriver}): {@code connect_timeout}
 *       and {@code socket_timeout} in <b>milliseconds</b> (note the underscore names).
 * </ul>
 *
 * <p>An unknown driver family yields no properties (logged at debug), so adding a new JDBC provider
 * never breaks; that provider opts in by registering its family here.
 *
 * <p><b>Defaults, never overrides.</b> For each property a family would emit, an operator value is
 * preserved when it is already present either as a DBCP connection property key in the catalog
 * configuration ({@link JdbcConfig#getAllConfig()}) or as a query parameter in the {@code
 * jdbc-url}. In that case the property is omitted from the returned map so the operator's value
 * wins. The URL-parameter check is case-sensitive and anchored at a parameter boundary ({@code ?}
 * or {@code &}), mirroring {@code MaxComputeTimeoutUtils.hasParameter}, so a differently-cased or
 * substring variant does not suppress injection of the canonical key.
 *
 * <p>The returned properties are applied as DBCP <em>connection</em> properties (via {@code
 * BasicDataSource.addConnectionProperty}), not raw pool keys, so they neither collide with the
 * unsafe pool properties nor make the pool eager; they take effect on the first real connection,
 * i.e. during create-time verification.
 */
public final class JdbcConnectionTimeouts {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionTimeouts.class);

  /** The MySQL Connector/J driver class (modern). */
  static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

  /** The MySQL Connector/J driver class (legacy, pre-8.0). */
  static final String MYSQL_LEGACY_DRIVER = "com.mysql.jdbc.Driver";

  /** The OceanBase Connector/J driver class (MySQL-compatible, milliseconds). */
  static final String OCEANBASE_DRIVER = "com.oceanbase.jdbc.Driver";

  /** The PostgreSQL driver class (also used by the Hologres catalog). */
  static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

  /** The ClickHouse JDBC driver class. */
  static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

  /** The connect/socket parameter names used by the MySQL and PostgreSQL families (camelCase). */
  static final String CONNECT_TIMEOUT_PARAM = "connectTimeout";

  static final String SOCKET_TIMEOUT_PARAM = "socketTimeout";

  /** The connect/socket parameter names used by ClickHouse (snake_case). */
  static final String CLICKHOUSE_CONNECT_TIMEOUT_PARAM = "connect_timeout";

  static final String CLICKHOUSE_SOCKET_TIMEOUT_PARAM = "socket_timeout";

  private JdbcConnectionTimeouts() {
    // Utility class, prevent instantiation.
  }

  /**
   * Returns the driver-family connection properties that bound a single connection to the canonical
   * {@link JdbcConfig#getConnectTimeoutMs()} value, omitting any the operator has already set.
   *
   * @param config the catalog JDBC configuration; must not be null
   * @param jdbcUrl the JDBC URL whose query parameters are inspected for operator-set timeouts; may
   *     be null or blank
   * @return an ordered map of connection property name to value; empty when the driver family is
   *     unknown or the operator has already set every relevant property
   */
  public static Map<String, String> connectionProperties(JdbcConfig config, String jdbcUrl) {
    Map<String, String> properties = new LinkedHashMap<>();
    String driver = config.getJdbcDriver();
    long millis = config.getConnectTimeoutMs();

    String connectParam;
    String socketParam;
    boolean seconds;
    if (MYSQL_DRIVER.equals(driver)
        || MYSQL_LEGACY_DRIVER.equals(driver)
        || OCEANBASE_DRIVER.equals(driver)) {
      // MySQL Connector/J and the MySQL-compatible OceanBase driver expect milliseconds.
      connectParam = CONNECT_TIMEOUT_PARAM;
      socketParam = SOCKET_TIMEOUT_PARAM;
      seconds = false;
    } else if (POSTGRESQL_DRIVER.equals(driver)) {
      // PostgreSQL (and Hologres via the PostgreSQL driver) expect seconds.
      connectParam = CONNECT_TIMEOUT_PARAM;
      socketParam = SOCKET_TIMEOUT_PARAM;
      seconds = true;
    } else if (CLICKHOUSE_DRIVER.equals(driver)) {
      // ClickHouse uses snake_case parameter names and expects milliseconds.
      connectParam = CLICKHOUSE_CONNECT_TIMEOUT_PARAM;
      socketParam = CLICKHOUSE_SOCKET_TIMEOUT_PARAM;
      seconds = false;
    } else {
      LOG.debug(
          "JDBC driver '{}' has no known connect-timeout translation; leaving connection timeouts "
              + "unbounded by Gravitino. Set them in the jdbc-url if needed.",
          driver);
      return properties;
    }

    String value = Long.toString(seconds ? millisToSeconds(millis) : millis);
    addIfAbsent(properties, connectParam, value, config, jdbcUrl);
    addIfAbsent(properties, socketParam, value, config, jdbcUrl);
    return properties;
  }

  /**
   * Converts milliseconds to seconds, rounded up with a minimum of 1 so a sub-second bound never
   * collapses to 0 ("no timeout").
   *
   * @param millis the timeout in milliseconds
   * @return the timeout in whole seconds, at least 1
   */
  private static long millisToSeconds(long millis) {
    return Math.max(1L, (millis + 999L) / 1000L);
  }

  /**
   * Adds a connection property unless the operator has already set it (as a config key or a {@code
   * jdbc-url} query parameter), preserving the operator's value in that case.
   *
   * @param properties the map being built
   * @param name the connection property name
   * @param value the Gravitino default value
   * @param config the catalog configuration
   * @param jdbcUrl the JDBC URL
   */
  private static void addIfAbsent(
      Map<String, String> properties,
      String name,
      String value,
      JdbcConfig config,
      String jdbcUrl) {
    if (config.getAllConfig().containsKey(name) || hasUrlParameter(jdbcUrl, name)) {
      return;
    }
    properties.put(name, value);
  }

  /**
   * Determines whether the URL already carries the given query parameter.
   *
   * <p>The match is case-sensitive and anchored at a parameter boundary ({@code ?} or {@code &}),
   * so a differently-cased variant is treated as a different parameter and {@code socketTimeout} is
   * not considered present merely because {@code tunnelSocketTimeout} is.
   *
   * @param url the JDBC URL; may be null or blank
   * @param parameter the exact query parameter name to look for
   * @return true if the parameter is already present with exactly this key
   */
  private static boolean hasUrlParameter(String url, String parameter) {
    if (StringUtils.isBlank(url)) {
      return false;
    }
    String needle = parameter + "=";
    int index = url.indexOf(needle);
    while (index >= 0) {
      char preceding = index == 0 ? '\0' : url.charAt(index - 1);
      if (preceding == '?' || preceding == '&') {
        return true;
      }
      index = url.indexOf(needle, index + 1);
    }
    return false;
  }
}
