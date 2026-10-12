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
  void testMysqlFamilyEmitsMilliseconds() {
    for (String driver : new String[] {"com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver"}) {
      Map<String, String> props = Maps.newHashMap();
      props.put(DRIVER_KEY, driver);
      props.put(URL_KEY, "jdbc:mysql://host:3306/db");

      Map<String, String> result =
          JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

      // Default canonical value is 30000 ms, emitted as-is for MySQL Connector/J.
      Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT), driver);
      Assertions.assertEquals("30000", result.get(SOCKET_TIMEOUT), driver);
      Assertions.assertEquals(2, result.size(), driver);
    }
  }

  @Test
  void testOceanBaseDriverEmitsMilliseconds() {
    // OceanBase Connector/J is MySQL-compatible and uses the same camelCase params in milliseconds.
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.oceanbase.jdbc.Driver");
    props.put(URL_KEY, "jdbc:oceanbase://host:2881/db");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals("30000", result.get(SOCKET_TIMEOUT));
    Assertions.assertEquals(2, result.size());
  }

  @Test
  void testClickHouseDriverEmitsSnakeCaseMilliseconds() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.clickhouse.jdbc.ClickHouseDriver");
    props.put(URL_KEY, "jdbc:clickhouse://host:8443/default");
    props.put(CONNECT_TIMEOUT_MS_KEY, "7500");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    // ClickHouse uses snake_case names and milliseconds; the camelCase names must NOT be emitted.
    Assertions.assertEquals("7500", result.get(CLICKHOUSE_CONNECT_TIMEOUT));
    Assertions.assertEquals("7500", result.get(CLICKHOUSE_SOCKET_TIMEOUT));
    Assertions.assertFalse(result.containsKey(CONNECT_TIMEOUT));
    Assertions.assertFalse(result.containsKey(SOCKET_TIMEOUT));
    Assertions.assertEquals(2, result.size());
  }

  @Test
  void testClickHouseOperatorValueInJdbcUrlIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.clickhouse.jdbc.ClickHouseDriver");
    String url = "jdbc:clickhouse://host:8443/default?socket_timeout=60000";
    props.put(URL_KEY, url);

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), url);

    // socket_timeout is left to the operator; connect_timeout is still defaulted.
    Assertions.assertFalse(result.containsKey(CLICKHOUSE_SOCKET_TIMEOUT));
    Assertions.assertEquals("30000", result.get(CLICKHOUSE_CONNECT_TIMEOUT));
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
    Assertions.assertEquals("5", result.get(SOCKET_TIMEOUT));
  }

  @Test
  void testPostgresRoundsUpToSecondsWithMinimumOne() {
    assertPostgresValue("30000", "30");
    assertPostgresValue("1", "1");
    assertPostgresValue("999", "1");
    assertPostgresValue("1000", "1");
    assertPostgresValue("1001", "2");
    assertPostgresValue("1999", "2");
  }

  private static void assertPostgresValue(String millis, String expectedSeconds) {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.postgresql.Driver");
    props.put(URL_KEY, "jdbc:postgresql://host:5432/db");
    props.put(CONNECT_TIMEOUT_MS_KEY, millis);

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    Assertions.assertEquals(expectedSeconds, result.get(CONNECT_TIMEOUT), millis);
    Assertions.assertEquals(expectedSeconds, result.get(SOCKET_TIMEOUT), millis);
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
  void testOperatorValueAsConnectionPropertyIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    props.put(URL_KEY, "jdbc:mysql://host:3306/db");
    // The operator set connectTimeout as a DBCP connection property directly.
    props.put(CONNECT_TIMEOUT, "5000");

    Map<String, String> result =
        JdbcConnectionTimeouts.connectionProperties(config(props), props.get(URL_KEY));

    // connectTimeout is left to the operator; socketTimeout is still defaulted.
    Assertions.assertFalse(result.containsKey(CONNECT_TIMEOUT));
    Assertions.assertEquals("30000", result.get(SOCKET_TIMEOUT));
  }

  @Test
  void testOperatorValueInJdbcUrlIsPreserved() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    String url = "jdbc:mysql://host:3306/db?connectTimeout=5000&socketTimeout=6000";
    props.put(URL_KEY, url);

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), url);

    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  void testUrlParameterMatchIsCaseSensitiveAndBoundaryAnchored() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "com.mysql.cj.jdbc.Driver");
    // "ConnectTimeout" (wrong case) and "tunnelSocketTimeout" (substring) must NOT suppress the
    // canonical keys, so both canonical timeouts are still injected.
    String url = "jdbc:mysql://host:3306/db?ConnectTimeout=5000&tunnelSocketTimeout=6000";
    props.put(URL_KEY, url);

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), url);

    Assertions.assertEquals("30000", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals("30000", result.get(SOCKET_TIMEOUT));
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
    Assertions.assertEquals("7500", result.get(SOCKET_TIMEOUT));
  }

  @Test
  void testNullJdbcUrlIsSafe() {
    Map<String, String> props = Maps.newHashMap();
    props.put(DRIVER_KEY, "org.postgresql.Driver");

    Map<String, String> result = JdbcConnectionTimeouts.connectionProperties(config(props), null);

    Assertions.assertEquals("30", result.get(CONNECT_TIMEOUT));
    Assertions.assertEquals("30", result.get(SOCKET_TIMEOUT));
  }
}
