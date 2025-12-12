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
package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoLanceNamespaceWrapper {

  @Test
  public void testClientPropertiesExtraction() {
    // Test that client properties are correctly extracted from LanceConfig
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "400")
            .put("gravitino.client.maxConnectionsPerRoute", "400")
            .put("gravitino.client.connectionTimeoutMs", "5000")
            .put("gravitino.client.socketTimeoutMs", "30000")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties (same logic as in initialize())
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Verify all client properties are extracted
    Assertions.assertEquals(4, clientProperties.size());
    Assertions.assertEquals("400", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("400", clientProperties.get("gravitino.client.maxConnectionsPerRoute"));
    Assertions.assertEquals("5000", clientProperties.get("gravitino.client.connectionTimeoutMs"));
    Assertions.assertEquals("30000", clientProperties.get("gravitino.client.socketTimeoutMs"));

    // Verify non-client properties are not included
    Assertions.assertFalse(
        clientProperties.containsKey(LanceConfig.NAMESPACE_BACKEND_URI.getKey()));
    Assertions.assertFalse(clientProperties.containsKey(LanceConfig.METALAKE_NAME.getKey()));
  }

  @Test
  public void testClientPropertiesWithDefaults() {
    // Test that initialization works without client properties
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties (same logic as in initialize())
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Should be empty (no client properties provided)
    Assertions.assertEquals(0, clientProperties.size());
  }

  @Test
  public void testClientPropertiesPartialConfig() {
    // Test with only some client properties
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "200")
            // Only maxConnections, not maxConnectionsPerRoute
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Should only have the one property
    Assertions.assertEquals(1, clientProperties.size());
    Assertions.assertEquals("200", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertNull(clientProperties.get("gravitino.client.maxConnectionsPerRoute"));
  }

  @Test
  public void testClientPropertiesFiltering() {
    // Test that properties with similar names but different prefixes are not included
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "400")
            .put("gravitino.server.maxThreads", "800") // Should NOT be included
            .put("gravitino.cache.maxEntries", "10000") // Should NOT be included
            .put("gravitino.client.connectionTimeoutMs", "5000")
            .put("some.other.client.property", "value") // Should NOT be included
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Only properties with "gravitino.client." prefix should be included
    Assertions.assertEquals(2, clientProperties.size());
    Assertions.assertEquals("400", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("5000", clientProperties.get("gravitino.client.connectionTimeoutMs"));

    // Verify others are excluded
    Assertions.assertFalse(clientProperties.containsKey("gravitino.server.maxThreads"));
    Assertions.assertFalse(clientProperties.containsKey("gravitino.cache.maxEntries"));
    Assertions.assertFalse(clientProperties.containsKey("some.other.client.property"));
  }

  @Test
  public void testHighThroughputConfiguration() {
    // Test high-throughput scenario (like Lance REST server with 800 threads)
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://gravitino-prod:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "production")
            .put("gravitino.client.maxConnections", "800")
            .put("gravitino.client.maxConnectionsPerRoute", "800")
            .put("gravitino.client.connectionTimeoutMs", "5000")
            .put("gravitino.client.socketTimeoutMs", "30000")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Verify Lance-specific config
    Assertions.assertEquals("http://gravitino-prod:8090", lanceConfig.getNamespaceBackendUri());
    Assertions.assertEquals("production", lanceConfig.getGravitinoMetalake());

    // Extract and verify client properties
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Verify high-throughput settings
    Assertions.assertEquals(4, clientProperties.size());
    Assertions.assertEquals("800", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("800", clientProperties.get("gravitino.client.maxConnectionsPerRoute"));

    // Verify single-route pattern (maxConnections == maxConnectionsPerRoute)
    Assertions.assertEquals(
        clientProperties.get("gravitino.client.maxConnections"),
        clientProperties.get("gravitino.client.maxConnectionsPerRoute"));
  }

  @Test
  public void testLanceConfigWithAllClientProperties() {
    // Test complete client configuration
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "400")
            .put("gravitino.client.maxConnectionsPerRoute", "400")
            .put("gravitino.client.connectionTimeoutMs", "10000")
            .put("gravitino.client.socketTimeoutMs", "60000")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Verify all properties are accessible
    Map<String, String> allConfig = lanceConfig.getAllConfig();
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.maxConnections"));
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.maxConnectionsPerRoute"));
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.connectionTimeoutMs"));
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.socketTimeoutMs"));

    // Verify values
    Assertions.assertEquals("400", allConfig.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("400", allConfig.get("gravitino.client.maxConnectionsPerRoute"));
    Assertions.assertEquals("10000", allConfig.get("gravitino.client.connectionTimeoutMs"));
    Assertions.assertEquals("60000", allConfig.get("gravitino.client.socketTimeoutMs"));
  }
}
