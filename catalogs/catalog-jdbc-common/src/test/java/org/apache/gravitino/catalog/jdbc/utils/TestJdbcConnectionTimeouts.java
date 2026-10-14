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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcConnectionTimeouts {

  private static final String DRIVER_KEY = "jdbc-driver";
  private static final String URL_KEY = "jdbc-url";
  private static final String CONNECT_TIMEOUT_MS_KEY = "jdbc.pool.connect-timeout-ms";
  private static final String CONNECT_TIMEOUT = "connectTimeout";
  private static final String SOCKET_TIMEOUT = "socketTimeout";
  private static final String CLICKHOUSE_CONNECT_TIMEOUT = "connect_timeout";
  private static final String CLICKHOUSE_SOCKET_TIMEOUT = "socket_timeout";

  private static JdbcConfig config(Map<String, String> props) {
    return new JdbcConfig(props);
  }

  @Test
  void testMysqlFamilyEmitsConnectTimeoutMillisOnly() {
    for (String driver : new String[] {"com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver"}) {
      Map<String, String> props = Maps.newHashMap();
      props.put(DRIVER_KEY, driver);
      props.put(URL_KEY, "jdbc:mysql://host:3306/db");

      Map<String, String> result =
          JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

      // Only the connect timeout is emitted (never a socket-read timeout), as milliseconds.
      Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT), driver);
      Assertions.assertFalse(result.containsKey(SOCKET_TIMEOUT), driver);
      Assertions.assertEquals(1, result.size(), driver);
    }
  }

  @Test
  void testOceanBaseDriverEmitsConnectTimeoutMillis() {
    // OceanBase Connector/J is MySQL-compatible and uses the same camelCase param in milliseconds.
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.oceanbase.jdbc.Driver");
    props.put(URL_KEY, "jdbc:oceanbase://host:2881/db");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT));
    Assertions.assertFalse(result.containsKey(SOCKET_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testClickHouseDriverEmitsSnakeCaseConnectTimeoutMillis() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.clickhouse.jdbc.ClickHouseDriver");
    props.put(URL_KEY, "jdbc:clickhouse://host:8443/default");
    props.put(CONNECT_TIMEOUT_MS_KEY, "7500");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    // ClickHouse uses the snake_case name and milliseconds; the camelCase name must NOT be emitted,
    // and no socket-read timeout is emitted.
    Assertions.assertEquals("7500", result.get(CLICKHOUSE_CONNECT_TIMEOUT));
    Assertions.assertFalse(result.containsKey(CLICKHOUSE_SOCKET_TIMEOUT));
    Assertions.assertFalse(result.containsKey(CONNECT_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testClickHouseOperatorConnectTimeoutInJdbcUrlIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.clickhouse.jdbc.ClickHouseDriver");
    String url = "jdbc:clickhouse://host:8443/default?connect_timeout=5000";
    props.put(URL_KEY, url);

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), url);

    // The operator's connect_timeout wins, so nothing is injected.
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testHologresUsesPostgresqlDriverSeconds() {
    // Hologres connects through the PostgreSQL driver, so it inherits the seconds translation.
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.postgresql.Driver");
    props.put(URL_KEY, "jdbc:postgresql://host:80/db");
    props.put(CONNECT_TIMEOUT_MS_KEY, "5000");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals("5", result.get(CONNECT_TIMEOUT));
    Assertions.assertFalse(result.containsKey(SOCKET_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testPostgresRoundsUpToSecondsWithMinimumOne() {
    assertPostgresConnectSeconds("30000", "30");
    assertPostgresConnectSeconds("1", "1");
    assertPostgresConnectSeconds("999", "1");
    assertPostgresConnectSeconds("1000", "1");
    assertPostgresConnectSeconds("1001", "2");
    assertPostgresConnectSeconds("1999", "2");
  }

  @Test
  void testPostgresSecondsConversionAtMaxAllowedValue() {
    // The config caps the value at Integer.MAX_VALUE ms; the ceiling conversion stays correct and
    // within int range at that bound (and is overflow-safe for the whole positive long range).
    long millis = Integer.MAX_VALUE;
    long expectedSeconds = (millis - 1L) / 1000L + 1L;
    assertPostgresConnectSeconds(Long.toString(millis), Long.toString(expectedSeconds));
    Assertions.assertTrue(expectedSeconds > 1L, "conversion must not collapse to 1 second");
    Assertions.assertTrue(
        expectedSeconds <= Integer.MAX_VALUE, "converted seconds must fit the driver int range");
  }

  private static void assertPostgresConnectSeconds(String millis, String expectedSeconds) {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.postgresql.Driver");
    props.put(URL_KEY, "jdbc:postgresql://host:5432/db");
    props.put(CONNECT_TIMEOUT_MS_KEY, millis);

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals(expectedSeconds, result.get(CONNECT_TIMEOUT), millis);
    Assertions.assertFalse(result.containsKey(SOCKET_TIMEOUT), millis);
  }

  @Test
  void testUnknownDriverEmitsNothing() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.sqlite.JDBC");
    props.put(URL_KEY, "jdbc:sqlite:/tmp/test.db");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testOperatorConnectTimeoutInConnectionPropertiesIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    props.put(URL_KEY, "jdbc:mysql://host:3306/db");
    // The operator set connectTimeout inside the DBCP connectionProperties value (name=value;...).
    props.put("connectionProperties", "connectTimeout=5000;useSSL=false");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    // The operator's connectTimeout wins, so nothing is injected (no overwrite).
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testBareTopLevelConnectTimeoutKeyDoesNotSuppressInjection() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    props.put(URL_KEY, "jdbc:mysql://host:3306/db");
    // A bare top-level connectTimeout key is NOT a DBCP connection property (DBCP ignores it), so
    // it must NOT suppress injection — otherwise the connection would end up with no timeout.
    props.put(CONNECT_TIMEOUT, "5000");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testOperatorConnectTimeoutInJdbcUrlIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    String url = "jdbc:mysql://host:3306/db?connectTimeout=5000";
    props.put(URL_KEY, url);

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), url);

    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testUrlParameterMatchIsCaseSensitiveAndBoundaryAnchored() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    // "ConnectTimeout" (wrong case) and "tunnelConnectTimeout" (substring) must NOT suppress the
    // canonical key, so the canonical connect timeout is still injected.
    String url = "jdbc:mysql://host:3306/db?ConnectTimeout=5000&tunnelConnectTimeout=6000";
    props.put(URL_KEY, url);

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), url);

    Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testCustomConnectTimeoutPropagates() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    props.put(URL_KEY, "jdbc:mysql://host:3306/db");
    props.put(CONNECT_TIMEOUT_MS_KEY, "7500");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals("7500", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testNullJdbcUrlIsSafe() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.postgresql.Driver");

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), null);

    Assertions.assertEquals("30", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals(1, result.size());
  }

  @Test
  void testMysqlProbeEmitsConnectAndSocketTimeoutMillis() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    props.put(URL_KEY, "jdbc:mysql://host:3306/db");
    props.put(CONNECT_TIMEOUT_MS_KEY, "2000");

    Map<String, String> result =
        JdbcConnectionTimeouts.probeConnectionProperties(config(props), props.get(URL_KEY));

    // The probe bounds both the connect and the read timeout (milliseconds for MySQL).
    Assertions.assertEquals("2000", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals("2000", result.get(SOCKET_TIMEOUT));
    Assertions.assertEquals(2, result.size());
  }

  @Test
  void testPostgresProbeEmitsConnectSocketAndLoginTimeoutSeconds() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.postgresql.Driver");
    props.put(URL_KEY, "jdbc:postgresql://host:5432/db");
    props.put(CONNECT_TIMEOUT_MS_KEY, "2000");

    Map<String, String> result =
        JdbcConnectionTimeouts.probeConnectionProperties(config(props), props.get(URL_KEY));

    // The probe bounds connect, socket and login timeouts, converted to seconds.
    Assertions.assertEquals("2", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals("2", result.get(SOCKET_TIMEOUT));
    Assertions.assertEquals("2", result.get("loginTimeout"));
    Assertions.assertEquals(3, result.size());
  }

  @Test
  void testClickHouseProbeEmitsSnakeCaseConnectAndSocketTimeoutMillis() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.clickhouse.jdbc.ClickHouseDriver");
    props.put(URL_KEY, "jdbc:clickhouse://host:8443/default");
    props.put(CONNECT_TIMEOUT_MS_KEY, "2000");

    Map<String, String> result =
        JdbcConnectionTimeouts.probeConnectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals("2000", result.get(CLICKHOUSE_CONNECT_TIMEOUT));
    Assertions.assertEquals("2000", result.get(CLICKHOUSE_SOCKET_TIMEOUT));
    Assertions.assertFalse(result.containsKey(CONNECT_TIMEOUT));
    Assertions.assertEquals(2, result.size());
  }

  @Test
  void testProbeUnknownDriverEmitsNothing() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.sqlite.JDBC");
    props.put(URL_KEY, "jdbc:sqlite::memory:");

    Map<String, String> result =
        JdbcConnectionTimeouts.probeConnectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testProbeOperatorSocketTimeoutIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    String url = "jdbc:mysql://host:3306/db?socketTimeout=9000";
    props.put(URL_KEY, url);

    Map<String, String> result =
        JdbcConnectionTimeouts.probeConnectionProperties(config(props), url);

    // The operator's socketTimeout wins; only the connect timeout is injected.
    Assertions.assertFalse(result.containsKey(SOCKET_TIMEOUT));
    Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT));
  }
}
