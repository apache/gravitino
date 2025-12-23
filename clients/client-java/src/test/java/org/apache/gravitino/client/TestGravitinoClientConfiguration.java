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
package org.apache.gravitino.client;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoClientConfiguration {
  @Test
  void testValidConfig() {
    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.connectionTimeoutMs", String.valueOf(20),
            "gravitino.client.socketTimeoutMs", String.valueOf(10));
    GravitinoClientConfiguration clientConfiguration =
        GravitinoClientConfiguration.buildFromProperties(properties);
    Assertions.assertEquals(clientConfiguration.getClientConnectionTimeoutMs(), 20L);
    Assertions.assertEquals(clientConfiguration.getClientSocketTimeoutMs(), 10);
  }

  @Test
  void testInValidConfig() {

    Map<String, String> properties =
        ImmutableMap.of(
            "gravitino.client.xxxx", String.valueOf(10),
            "gravitino.client.socketTimeoutMs", String.valueOf(20));
    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientConfiguration.buildFromProperties(properties));
    Assertions.assertEquals(
        "Invalid property for client: gravitino.client.xxxx", throwable.getMessage());
  }

  @Test
  void testConnectionPoolDefaults() {
    // Test default values when properties are not provided
    Map<String, String> properties = ImmutableMap.of();
    GravitinoClientConfiguration clientConfiguration =
        GravitinoClientConfiguration.buildFromProperties(properties);

    Assertions.assertEquals(
        GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS_DEFAULT,
        clientConfiguration.getClientMaxConnections());
    Assertions.assertEquals(
        GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS_PER_ROUTE_DEFAULT,
        clientConfiguration.getClientMaxConnectionsPerRoute());
  }

  @Test
  void testConnectionPoolCustomValues() {
    // Test custom connection pool values
    Map<String, String> properties =
        ImmutableMap.of(
            GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS, "400",
            GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS_PER_ROUTE, "400");
    GravitinoClientConfiguration clientConfiguration =
        GravitinoClientConfiguration.buildFromProperties(properties);

    Assertions.assertEquals(400, clientConfiguration.getClientMaxConnections());
    Assertions.assertEquals(400, clientConfiguration.getClientMaxConnectionsPerRoute());
  }

  @Test
  void testConnectionPoolInvalidNegativeValues() {
    // Test that negative values are rejected
    Map<String, String> properties =
        ImmutableMap.of(GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS, "-1");

    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              GravitinoClientConfiguration config =
                  GravitinoClientConfiguration.buildFromProperties(properties);
              config.getClientMaxConnections();
            });
    Assertions.assertTrue(throwable.getMessage().contains("The value must be a positive number"));
  }

  @Test
  void testConnectionPoolInvalidZeroValues() {
    // Test that zero values are rejected
    Map<String, String> properties =
        ImmutableMap.of(GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS_PER_ROUTE, "0");

    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              GravitinoClientConfiguration config =
                  GravitinoClientConfiguration.buildFromProperties(properties);
              config.getClientMaxConnectionsPerRoute();
            });
    Assertions.assertTrue(throwable.getMessage().contains("The value must be a positive number"));
  }

  @Test
  void testConnectionPoolValidation_InvalidConfiguration() {
    // Test invalid configuration: maxConnections < maxConnectionsPerRoute
    Map<String, String> properties =
        ImmutableMap.of(
            GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS, "50",
            GravitinoClientConfiguration.CLIENT_MAX_CONNECTIONS_PER_ROUTE, "100");

    Throwable throwable =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> GravitinoClientConfiguration.buildFromProperties(properties));

    Assertions.assertTrue(
        throwable
            .getMessage()
            .contains(
                "gravitino.client.maxConnections (50) must be greater than or equal to gravitino.client.maxConnectionsPerRoute (100)"));
    Assertions.assertTrue(
        throwable
            .getMessage()
            .contains(
                "The total connection pool size cannot be smaller than the per-route connection limit"));
  }
}
