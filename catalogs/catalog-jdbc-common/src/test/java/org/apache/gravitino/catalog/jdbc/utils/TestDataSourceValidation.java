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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests JDBC configuration validation before any backend connection. */
public class TestDataSourceValidation {

  @Test
  void testInvalidConfigurationFailsBeforeConnection() {
    for (Map<String, String> invalid :
        List.of(
            Map.of("jdbc-driver", "java.lang.String"),
            Map.of("jdbc-driver", "missing.Driver"),
            Map.of("jdbc-url", "jdbc:postgresql://localhost/db"),
            Map.of("verify-on-create", "yes"),
            Map.of("jdbc.pool.test-on-borrow", "maybe"),
            Map.of("jdbc.pool.min-size", "0"),
            Map.of("jdbc.pool.max-size", "-1"),
            Map.of("jdbc.pool.min-size", "5", "jdbc.pool.max-size", "2"),
            Map.of("jdbc.pool.max-wait-ms", "0"),
            Map.of("jdbc.pool.connect-timeout-ms", "2147483648"))) {
      IllegalArgumentException exception =
          Assertions.assertThrows(IllegalArgumentException.class, () -> createDataSource(invalid));
      Assertions.assertTrue(
          exception.getMessage().toLowerCase(Locale.ROOT).contains("jdbc")
              || exception.getMessage().contains("verify-on-create"));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "connectionFactoryClassName",
        "evictionPolicyClassName",
        "driverClassName",
        "url",
        "username",
        "password",
        "initialSize",
        "INITIALSIZE"
      })
  void testUnsafeNativePoolPropertiesAreRejected(String key) {
    GravitinoRuntimeException error =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> createDataSource(Map.of(key, "1")));
    Assertions.assertTrue(error.getMessage().contains(key));
  }

  private static BasicDataSource createDataSource(Map<String, String> overrides) {
    Map<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "1");
    properties.put(JdbcConfig.POOL_MIN_SIZE.getKey(), "1");
    properties.putAll(overrides);
    return (BasicDataSource) DataSourceUtils.createDataSource(properties);
  }
}
