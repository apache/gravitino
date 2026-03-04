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

package org.apache.gravitino.lance.common.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLanceConfig {
  @Test
  public void testLanceHttpPort() {
    Map<String, String> properties = ImmutableMap.of();
    LanceConfig lanceConfig = new LanceConfig(properties);
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(lanceConfig);
    Assertions.assertEquals(
        LanceConfig.DEFAULT_LANCE_REST_SERVICE_HTTP_PORT, jettyServerConfig.getHttpPort());
    Assertions.assertEquals(
        LanceConfig.DEFAULT_LANCE_REST_SERVICE_HTTPS_PORT, jettyServerConfig.getHttpsPort());

    properties =
        ImmutableMap.of(
            JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            "9101",
            JettyServerConfig.WEBSERVER_HTTPS_PORT.getKey(),
            "9533");
    lanceConfig = new LanceConfig(properties);
    jettyServerConfig = JettyServerConfig.fromConfig(lanceConfig);
    Assertions.assertEquals(9101, jettyServerConfig.getHttpPort());
    Assertions.assertEquals(9533, jettyServerConfig.getHttpsPort());
  }

  @Test
  public void testGravitinoUriAndMetalake() {
    // Test default values
    Map<String, String> properties = ImmutableMap.of();
    LanceConfig lanceConfig = new LanceConfig(properties);
    Assertions.assertEquals("http://localhost:8090", lanceConfig.getNamespaceBackendUri());
    Assertions.assertNull(lanceConfig.getGravitinoMetalake()); // No default, must be configured

    // Test custom values
    properties =
        ImmutableMap.of(
            LanceConfig.NAMESPACE_BACKEND_URI.getKey(),
            "http://gravitino-server:8090",
            LanceConfig.METALAKE_NAME.getKey(),
            "production");
    lanceConfig = new LanceConfig(properties);
    Assertions.assertEquals("http://gravitino-server:8090", lanceConfig.getNamespaceBackendUri());
    Assertions.assertEquals("production", lanceConfig.getGravitinoMetalake());
  }

  @Test
  public void testCompleteConfiguration() {
    // Test all configurations together for auxiliary mode
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://gravitino-prod:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "production")
            .put(LanceConfig.NAMESPACE_BACKEND.getKey(), "gravitino")
            .put(JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(), "9101")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Verify all config values
    Assertions.assertEquals("http://gravitino-prod:8090", lanceConfig.getNamespaceBackendUri());
    Assertions.assertEquals("production", lanceConfig.getGravitinoMetalake());

    JettyServerConfig jettyConfig = JettyServerConfig.fromConfig(lanceConfig);
    Assertions.assertEquals(9101, jettyConfig.getHttpPort());
  }
}
