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
  public void testLoadLanceConfig() {
    Map<String, String> properties =
        ImmutableMap.of("gravitino.lance-rest.catalog-name", "test_catalog");

    LanceConfig lanceConfig = new LanceConfig();
    lanceConfig.loadFromMap(properties, k -> k.startsWith("gravitino.lance-rest."));
    Assertions.assertEquals("test_catalog", lanceConfig.getCatalogName());

    LanceConfig lanceConfig2 = new LanceConfig(properties);
    Assertions.assertEquals("test_catalog", lanceConfig2.getCatalogName());
  }

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
    Assertions.assertEquals("http://localhost:8090", lanceConfig.getGravitinoUri());
    Assertions.assertEquals("metalake", lanceConfig.getGravitinoMetalake());

    // Test custom values
    properties =
        ImmutableMap.of(
            LanceConfig.GRAVITINO_URI.getKey(),
            "http://gravitino-server:8090",
            LanceConfig.GRAVITINO_METALAKE.getKey(),
            "production");
    lanceConfig = new LanceConfig(properties);
    Assertions.assertEquals("http://gravitino-server:8090", lanceConfig.getGravitinoUri());
    Assertions.assertEquals("production", lanceConfig.getGravitinoMetalake());
  }
}
