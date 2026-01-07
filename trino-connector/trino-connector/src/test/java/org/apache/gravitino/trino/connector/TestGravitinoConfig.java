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
package org.apache.gravitino.trino.connector;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;

public class TestGravitinoConfig {

  @BeforeClass
  public static void startup() throws Exception {}

  @AfterClass
  public static void shutdown() throws Exception {}

  @Test
  public void testGravitinoConfig() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);

    GravitinoConfig config = new GravitinoConfig(configMap);

    assertEquals(gravitinoUrl, config.getURI());
    assertEquals(metalake, config.getMetalake());
  }

  @Test
  public void testMissingConfig() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    ImmutableMap<String, String> configMap = ImmutableMap.of("gravitino.uri", gravitinoUrl);
    try {
      GravitinoConfig config = new GravitinoConfig(configMap);
      assertEquals(gravitinoUrl, config.getURI());
    } catch (TrinoException e) {
      if (!GRAVITINO_MISSING_CONFIG.toErrorCode().equals(e.getErrorCode())) {
        throw e;
      }
    }
  }

  @Test
  public void testGravitinoConfigWithSkipTrinoVersionValidation() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertEquals(config.isSkipTrinoVersionValidation(), false);

    ImmutableMap<String, String> configMapWithSkipValidation =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.trino.skip-version-validation",
            "true");
    GravitinoConfig configWithSkipValidation = new GravitinoConfig(configMapWithSkipValidation);

    assertEquals(configWithSkipValidation.isSkipTrinoVersionValidation(), true);
  }

  @Test
  public void testGravitinoConfigWithClientConfig() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertTrue(config.getClientConfig().isEmpty());

    ImmutableMap<String, String> configMapWithClientConfig =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.client.socketTimeoutMs",
            "10000",
            "gravitino.client.connectionTimeoutMs",
            "20000");
    GravitinoConfig configWithClientConfig = new GravitinoConfig(configMapWithClientConfig);
    Map<String, String> clientConfig = configWithClientConfig.getClientConfig();
    assertEquals(clientConfig.get("gravitino.client.socketTimeoutMs"), "10000");
    assertEquals(clientConfig.get("gravitino.client.connectionTimeoutMs"), "20000");
  }

  @Test
  public void testGravitinoConfigWithSkipCatalogPatterns() {
    String gravitinoUrl = "http://127.0.0.1:8000";
    String metalake = "user_001";
    ImmutableMap<String, String> configMap =
        ImmutableMap.of("gravitino.uri", gravitinoUrl, "gravitino.metalake", metalake);
    GravitinoConfig config = new GravitinoConfig(configMap);

    assertFalse(skipCatalog("test_catalog", config));

    ImmutableMap<String, String> configMapWithSkipCatalogList =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.trino.skip-catalog-patterns",
            "test_.*, test1\\.c.*");
    GravitinoConfig configWithSkipCatalogPatterns =
        new GravitinoConfig(configMapWithSkipCatalogList);
    assertTrue(skipCatalog("test_catalog", configWithSkipCatalogPatterns));
    assertTrue(skipCatalog("test1.catalog", configWithSkipCatalogPatterns));
    assertFalse(skipCatalog("test1_catalog", configWithSkipCatalogPatterns));
    assertFalse(skipCatalog("test2_catalog", configWithSkipCatalogPatterns));

    ImmutableMap<String, String> configMapWithInvalidSkipCatalogList =
        ImmutableMap.of(
            "gravitino.uri",
            gravitinoUrl,
            "gravitino.metalake",
            metalake,
            "gravitino.trino.skip-catalog-patterns",
            "test_.*, (abc");
    assertThrowsExactly(
        TrinoException.class,
        () -> new GravitinoConfig(configMapWithInvalidSkipCatalogList),
        "Config `gravitino.trino.skip-catalog-patterns` is invalid because it contains an illegal regular expression");
  }

  private static boolean skipCatalog(String catalogName, GravitinoConfig config) {
    for (Pattern pattern : config.getSkipCatalogPatterns()) {
      if (pattern.matcher(catalogName).matches()) {
        return true;
      }
    }
    return false;
  }
}
