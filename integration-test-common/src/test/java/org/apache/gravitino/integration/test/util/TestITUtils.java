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
package org.apache.gravitino.integration.test.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.server.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestITUtils {
  @TempDir private Path tempDir;

  @Test
  public void testIsCiEnvironmentByCI() {
    Assertions.assertTrue(ITUtils.isCiEnvironment(Map.of("CI", "true")));
  }

  @Test
  public void testIsCiEnvironmentByGitHubActions() {
    Assertions.assertTrue(ITUtils.isCiEnvironment(Map.of("GITHUB_ACTIONS", "true")));
  }

  @Test
  public void testIsCiEnvironmentCaseInsensitive() {
    Assertions.assertTrue(ITUtils.isCiEnvironment(Map.of("CI", "TRUE")));
  }

  @Test
  public void testIsCiEnvironmentFalse() {
    Assertions.assertFalse(ITUtils.isCiEnvironment(Map.of()));
    Assertions.assertFalse(ITUtils.isCiEnvironment(Map.of("CI", "false")));
  }

  @Test
  void testOverwriteConfigFileRoundTripsProperties() throws IOException {
    Properties expected = configProperties();
    Path configFile = tempDir.resolve("gravitino.conf");
    ServerConfig config = new ServerConfig(false);
    Properties properties = expected;
    for (int rewrite = 0; rewrite < 3; rewrite++) {
      ITUtils.overwriteConfigFile(configFile.toString(), properties);
      properties = config.loadPropertiesFromFile(configFile.toFile());
      Assertions.assertEquals(expected, properties);
    }
  }

  @Test
  void testRewriteConfigFileRoundTripsProperties() throws IOException {
    Properties template = configProperties();
    Path templateFile = tempDir.resolve("gravitino.conf.template");
    try (OutputStream output = Files.newOutputStream(templateFile)) {
      template.store(output, null);
    }
    Map<String, String> overrides =
        Map.of("jdbc.url", "jdbc:h2:file:/tmp/\u5143\u6570\u636e", "added", "caf\u00e9");
    Properties expected = configProperties();
    expected.putAll(overrides);
    Path configFile = tempDir.resolve("gravitino.conf");

    ITUtils.rewriteConfigFile(templateFile.toString(), configFile.toString(), overrides);

    Properties properties = new ServerConfig(false).loadPropertiesFromFile(configFile.toFile());
    Assertions.assertEquals(expected, properties);
  }

  @Test
  void testOverwriteConfigFilePreservesInheritedDefaults() throws IOException {
    Properties defaults = new Properties();
    defaults.setProperty("inherited", "caf\u00e9");
    defaults.setProperty("overridden", "default");
    Properties properties = new Properties(defaults);
    properties.setProperty("overridden", "explicit");
    Path configFile = tempDir.resolve("gravitino.conf");

    ITUtils.overwriteConfigFile(configFile.toString(), properties);

    Properties loaded = new ServerConfig(false).loadPropertiesFromFile(configFile.toFile());
    Assertions.assertEquals("caf\u00e9", loaded.getProperty("inherited"));
    Assertions.assertEquals("explicit", loaded.getProperty("overridden"));
    Assertions.assertEquals(2, loaded.size());
  }

  @Test
  void testCheckServerPortIsFreeAcceptsAnUnusedPort() throws IOException {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    Assertions.assertDoesNotThrow(() -> ITUtils.checkServerPortIsFree("localhost", port));
  }

  @Test
  void testCheckServerPortIsFreeRejectsAPortSomethingElseHolds() throws IOException {
    // A server left behind by an earlier run keeps answering on this port. Starting a suite against
    // it runs the tests against a stranger's configuration, so refuse before the launch rather than
    // after a readiness probe the leftover satisfies.
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();

      IllegalStateException e =
          Assertions.assertThrows(
              IllegalStateException.class, () -> ITUtils.checkServerPortIsFree("localhost", port));

      Assertions.assertTrue(
          e.getMessage().contains(String.valueOf(port)),
          "the message has to name the port so the cause is actionable: " + e.getMessage());
    }
  }

  private Properties configProperties() {
    Properties properties = new Properties();
    properties.setProperty("jdbc.url", "jdbc:h2:file:/tmp/caf\u00e9;WRITE_DELAY=0");
    properties.setProperty("windows.path", "C:\\caf\u00e9\\metadata\\u0041");
    properties.setProperty(" key:=#!", "  leading\tline1\nline2\rline3\\u0041");
    properties.setProperty("\u5143\u6570\u636e", "\uD83D\uDE80");
    return properties;
  }
}
