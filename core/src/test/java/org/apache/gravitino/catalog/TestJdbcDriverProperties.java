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
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests catalog JDBC driver defaults. */
public class TestJdbcDriverProperties {

  @Test
  void testProviderDefaults() {
    Map<String, String> drivers =
        ImmutableMap.<String, String>builder()
            .put("jdbc-clickhouse", "com.clickhouse.jdbc.ClickHouseDriver")
            .put("jdbc-doris", "com.mysql.cj.jdbc.Driver")
            .put("jdbc-hologres", "org.postgresql.Driver")
            .put("jdbc-mysql", "com.mysql.cj.jdbc.Driver")
            .put("jdbc-postgresql", "org.postgresql.Driver")
            .put("jdbc-starrocks", "com.mysql.cj.jdbc.Driver")
            .build();
    drivers.forEach(
        (provider, driver) -> {
          Map<String, String> input = ImmutableMap.of("key", "value");
          Map<String, String> result = JdbcDriverProperties.withDefaultDriver(provider, input);
          Assertions.assertEquals(driver, result.get("jdbc-driver"));
          Assertions.assertEquals("value", result.get("key"));
          Assertions.assertFalse(input.containsKey("jdbc-driver"));
        });
  }

  @Test
  void testIcebergJdbcBackendDefaults() {
    Map<String, String> postgres =
        ImmutableMap.of("catalog-backend", "jdbc", "uri", "jdbc:postgresql://host/db");
    Assertions.assertEquals(
        "org.postgresql.Driver",
        JdbcDriverProperties.withDefaultDriver("lakehouse-iceberg", postgres).get("jdbc-driver"));

    Map<String, String> mysql =
        ImmutableMap.of("catalog-backend", "jdbc", "uri", "jdbc:mysql://host/db");
    Assertions.assertEquals(
        "com.mysql.cj.jdbc.Driver",
        JdbcDriverProperties.withDefaultDriver("lakehouse-iceberg", mysql).get("jdbc-driver"));
  }

  @Test
  void testOceanBaseDriverFollowsJdbcUrl() {
    Map<String, String> nativeUrl =
        ImmutableMap.of("jdbc-url", "jdbc:oceanbase://localhost:2881/test");
    Assertions.assertEquals(
        "com.oceanbase.jdbc.Driver",
        JdbcDriverProperties.withDefaultDriver("jdbc-oceanbase", nativeUrl).get("jdbc-driver"));

    Map<String, String> mysqlUrl = ImmutableMap.of("jdbc-url", "jdbc:mysql://localhost:2881/test");
    Assertions.assertEquals(
        "com.mysql.cj.jdbc.Driver",
        JdbcDriverProperties.withDefaultDriver("jdbc-oceanbase", mysqlUrl).get("jdbc-driver"));

    Map<String, String> unknownUrl = ImmutableMap.of("jdbc-url", "jdbc:unknown://localhost/test");
    Assertions.assertSame(
        unknownUrl, JdbcDriverProperties.withDefaultDriver("jdbc-oceanbase", unknownUrl));
  }

  @Test
  void testExplicitAndUnknownDrivers() {
    Map<String, String> explicit = ImmutableMap.of("jdbc-driver", "custom.Driver");
    Assertions.assertSame(explicit, JdbcDriverProperties.withDefaultDriver("jdbc-mysql", explicit));
    Map<String, String> unknown =
        ImmutableMap.of("catalog-backend", "jdbc", "uri", "jdbc:sqlite::memory:");
    Assertions.assertSame(
        unknown, JdbcDriverProperties.withDefaultDriver("lakehouse-iceberg", unknown));
    Assertions.assertSame(unknown, JdbcDriverProperties.withDefaultDriver("hive", unknown));
    Assertions.assertSame(
        unknown, JdbcDriverProperties.withDefaultDriver("jdbc-sqlserver", unknown));
    Assertions.assertNull(JdbcDriverProperties.withDefaultDriver(null, null));
    Assertions.assertEquals(
        "com.mysql.cj.jdbc.Driver",
        JdbcDriverProperties.withDefaultDriver("JDBC-MySQL", null).get("jdbc-driver"));
  }
}
