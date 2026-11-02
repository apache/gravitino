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
 * driver-family connection properties that bound a JDBC connection, so an unreachable or silent
 * backend fails fast instead of hanging.
 *
 * <p>Two translations are provided, because a pooled connection and the create-time verification
 * probe need different bounds:
 *
 * <ul>
 *   <li>{@link #connectionProperties} — used for the <b>pooled</b> connections of normal
 *       operations. It bounds only the <em>connection-establishment</em> timeout, deliberately not
 *       the socket-read timeout: a read timeout on the pool would also abort legitimate
 *       long-running operations on a healthy backend (a large {@code ALTER TABLE}, a slow metadata
 *       query).
 *   <li>{@link #probeConnectionProperties} — used for the one-off <b>create-time verification
 *       probe</b>. It bounds the connect timeout <em>and</em> the socket-read (and, for PostgreSQL,
 *       login) timeout, because that probe runs under the metalake write lock: a backend that
 *       accepts the TCP connection but never speaks the database protocol (a load balancer with a
 *       dead backend, a wrong port, a proxy that accepts and never forwards) would otherwise make
 *       the connect-only bound wait on the driver's default read timeout, which is "wait forever",
 *       and hold the lock for the whole metalake.
 * </ul>
 *
 * <p>DBCP has no portable timeout; the real bound lives in the driver, and both the parameter name
 * and its unit differ per driver family:
 *
 * <ul>
 *   <li><b>MySQL Connector/J</b> ({@code com.mysql.cj.jdbc.Driver}, the legacy {@code
 *       com.mysql.jdbc.Driver} — also used by Doris and StarRocks — and the MySQL-compatible
 *       OceanBase Connector/J {@code com.oceanbase.jdbc.Driver}): {@code connectTimeout} and {@code
 *       socketTimeout} in <b>milliseconds</b>.
 *   <li><b>PostgreSQL</b> ({@code org.postgresql.Driver}, also used by the PostgreSQL-compatible
 *       Hologres catalog): {@code connectTimeout}, {@code socketTimeout} and {@code loginTimeout}
 *       in <b>seconds</b>.
 *   <li><b>ClickHouse</b> ({@code com.clickhouse.jdbc.ClickHouseDriver}): {@code connect_timeout}
 *       and {@code socket_timeout} in <b>milliseconds</b> (note the underscore names).
 * </ul>
 *
 * <p>An unknown driver family yields no properties (logged at debug), so adding a new JDBC provider
 * never breaks; that provider opts in by registering its family here.
 *
 * <p><b>Default, never override.</b> A property is omitted when the operator has already set it in
 * one of the two places that actually reach the driver: a query parameter in the {@code jdbc-url},
 * or inside the DBCP {@code connectionProperties} value (a {@code name=value;...} string), so the
 * operator's value always wins. Both checks are case-sensitive and anchored at a boundary ({@code
 * ?} or {@code &} for the URL, {@code ;} for connectionProperties), so a differently-cased or
 * substring variant does not suppress injection of the canonical key. A bare top-level config key
 * of the same name is deliberately not treated as operator-set, because DBCP does not honor such a
 * key.
 *
 * <p>The pooled properties are applied as DBCP <em>connection</em> properties (via {@code
 * BasicDataSource.addConnectionProperty}), not raw pool keys, so they neither collide with the
 * unsafe pool properties nor make the pool eager.
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

  /** The connect-timeout parameter name used by the MySQL and PostgreSQL families (camelCase). */
  static final String CONNECT_TIMEOUT_PARAM = "connectTimeout";

  /** The socket-read-timeout parameter name used by the MySQL and PostgreSQL families. */
  static final String SOCKET_TIMEOUT_PARAM = "socketTimeout";

  /** The login-timeout parameter name used by the PostgreSQL family. */
  static final String LOGIN_TIMEOUT_PARAM = "loginTimeout";

  /** The connect-timeout parameter name used by ClickHouse (snake_case). */
  static final String CLICKHOUSE_CONNECT_TIMEOUT_PARAM = "connect_timeout";

  /** The socket-read-timeout parameter name used by ClickHouse (snake_case). */
  static final String CLICKHOUSE_SOCKET_TIMEOUT_PARAM = "socket_timeout";

  private JdbcConnectionTimeouts() {
    // Utility class, prevent instantiation.
  }

  /**
   * Returns the driver-family <b>connect-timeout</b> connection property that bounds a pooled
   * connection to the canonical {@link JdbcConfig#getConnectTimeoutMs()} value, or an empty map
   * when the driver family is unknown or the operator has already set the connect timeout. Only the
   * connect timeout is returned here; see {@link #probeConnectionProperties} for the create-time
   * probe, which also bounds the read timeout.
   *
   * @param config the catalog JDBC configuration; must not be null
   * @param jdbcUrl the JDBC URL whose query parameters are inspected for operator-set timeouts; may
   *     be null or blank
   * @return a map of connect-timeout property name to value, or an empty map
   */
  public static Map<String, String> connectionProperties(JdbcConfig config, String jdbcUrl) {
    return timeoutProperties(config, jdbcUrl, false /* includeReadTimeout */);
  }

  /**
   * Returns the driver-family <b>connect and read (and, for PostgreSQL, login)</b> timeout
   * connection properties that bound the one-off create-time verification probe to the canonical
   * {@link JdbcConfig#getConnectTimeoutMs()} value, or an empty map when the driver family is
   * unknown or the operator has already set the properties. Bounding the read timeout here (but not
   * on the pool, see {@link #connectionProperties}) ensures a backend that accepts the socket but
   * never speaks the protocol fails the probe instead of blocking the metalake write lock forever.
   *
   * @param config the catalog JDBC configuration; must not be null
   * @param jdbcUrl the JDBC URL whose query parameters are inspected for operator-set timeouts; may
   *     be null or blank
   * @return a map of timeout property name to value, or an empty map
   */
  public static Map<String, String> probeConnectionProperties(JdbcConfig config, String jdbcUrl) {
    return timeoutProperties(config, jdbcUrl, true /* includeReadTimeout */);
  }

  private static Map<String, String> timeoutProperties(
      JdbcConfig config, String jdbcUrl, boolean includeReadTimeout) {
    Map<String, String> properties = new LinkedHashMap<>();
    String driver = config.getJdbcDriver();
    long millis = config.getConnectTimeoutMs();

    String connectParam;
    String readParam;
    String loginParam;
    boolean seconds;
    if (MYSQL_DRIVER.equals(driver)
        || MYSQL_LEGACY_DRIVER.equals(driver)
        || OCEANBASE_DRIVER.equals(driver)) {
      // MySQL Connector/J and the MySQL-compatible OceanBase driver expect milliseconds.
      connectParam = CONNECT_TIMEOUT_PARAM;
      readParam = SOCKET_TIMEOUT_PARAM;
      loginParam = null;
      seconds = false;
    } else if (POSTGRESQL_DRIVER.equals(driver)) {
      // PostgreSQL (and Hologres via the PostgreSQL driver) expect seconds. loginTimeout bounds the
      // authentication handshake, which socketTimeout alone does not fully cover.
      connectParam = CONNECT_TIMEOUT_PARAM;
      readParam = SOCKET_TIMEOUT_PARAM;
      loginParam = LOGIN_TIMEOUT_PARAM;
      seconds = true;
    } else if (CLICKHOUSE_DRIVER.equals(driver)) {
      // ClickHouse uses snake_case parameter names and expects milliseconds.
      connectParam = CLICKHOUSE_CONNECT_TIMEOUT_PARAM;
      readParam = CLICKHOUSE_SOCKET_TIMEOUT_PARAM;
      loginParam = null;
      seconds = false;
    } else {
      LOG.debug(
          "JDBC driver '{}' has no known timeout translation; leaving the connection timeout "
              + "unbounded by Gravitino. Set it in the jdbc-url if needed.",
          driver);
      return properties;
    }

    String value = Long.toString(seconds ? millisToSeconds(millis) : millis);
    addIfAbsent(properties, connectParam, value, config, jdbcUrl);
    if (includeReadTimeout) {
      addIfAbsent(properties, readParam, value, config, jdbcUrl);
      if (loginParam != null) {
        addIfAbsent(properties, loginParam, value, config, jdbcUrl);
      }
    }
    return properties;
  }

  /**
   * Converts milliseconds to seconds, rounded up with a minimum of 1 so a sub-second bound never
   * collapses to 0 ("no timeout"). The arithmetic is overflow-safe for the whole positive {@code
   * long} range (the input is validated positive and {@code <= Integer.MAX_VALUE} by {@link
   * JdbcConfig}).
   *
   * @param millis the timeout in milliseconds (positive)
   * @return the timeout in whole seconds, at least 1
   */
  private static long millisToSeconds(long millis) {
    // (millis - 1) / 1000 + 1 is ceil(millis/1000) for positive millis without the overflow that
    // (millis + 999) would hit near Long.MAX_VALUE.
    return (millis - 1L) / 1000L + 1L;
  }

  /**
   * The DBCP2 pool key whose value is a {@code name=value;name2=value2} string of connection
   * properties. {@code DataSourceUtils} forwards it to {@code BasicDataSourceFactory}, which parses
   * it into the pool's connection properties, and the create-time probe reads the same catalog
   * configuration. An operator-set timeout lives inside this value, not as a top-level config key.
   */
  static final String DBCP_CONNECTION_PROPERTIES_KEY = "connectionProperties";

  /**
   * Adds a timeout property unless the operator has already set it, preserving the operator's value
   * in that case. The operator can set it in two places that actually reach the driver: a {@code
   * jdbc-url} query parameter, or inside the DBCP {@code connectionProperties} value (a {@code
   * name=value;...} string). A bare top-level config key of the same name is deliberately NOT
   * treated as operator-set, because DBCP does not recognize such a key — honoring it would
   * suppress the bound while the driver still saw no timeout.
   *
   * @param properties the map being built
   * @param name the timeout property name
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
    if (hasUrlParameter(jdbcUrl, name)
        || hasConnectionProperty(config.getAllConfig().get(DBCP_CONNECTION_PROPERTIES_KEY), name)) {
      return;
    }
    properties.put(name, value);
  }

  /**
   * Determines whether the DBCP {@code connectionProperties} value already defines the given
   * property. The value is a {@code name=value} list separated by {@code ;} (the format DBCP's
   * {@code BasicDataSourceFactory} parses), so the name is matched case-sensitively at a {@code ;}
   * boundary, mirroring the URL check; a differently-cased or substring variant does not count.
   *
   * @param connectionProperties the DBCP connectionProperties string; may be null or blank
   * @param name the exact property name to look for
   * @return true if the property is present with exactly this name
   */
  private static boolean hasConnectionProperty(String connectionProperties, String name) {
    if (StringUtils.isBlank(connectionProperties)) {
      return false;
    }
    for (String pair : connectionProperties.split(";")) {
      int eq = pair.indexOf('=');
      String key = (eq >= 0 ? pair.substring(0, eq) : pair).trim();
      if (key.equals(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines whether the URL already carries the given query parameter.
   *
   * <p>The match is case-sensitive and anchored at a parameter boundary ({@code ?} or {@code &}),
   * so a differently-cased variant is treated as a different parameter and {@code connectTimeout}
   * is not considered present merely because {@code tunnelConnectTimeout} is.
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
