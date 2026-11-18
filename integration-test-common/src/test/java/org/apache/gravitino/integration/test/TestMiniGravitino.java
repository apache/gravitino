/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.RESTClient;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.storage.relational.database.H2Database;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class TestMiniGravitino {

  @TempDir private Path mockConfDir;

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "jdbc:h2")
  void testDefaultH2UsesSynchronousWrites(@Nullable String templateUrl) throws Exception {
    Properties properties = rewriteConfig(templateUrl, Collections.emptyMap());
    ServerConfig config = new ServerConfig();
    config.loadFromProperties(properties);
    String jdbcUrl = H2Database.getConnectionUrl(config);
    assertTrue(jdbcUrl.startsWith("jdbc:h2:file:" + mockConfDir.resolve("metadata") + ";"));
    assertTrue(Class.forName("org.h2.mvstore.RandomAccessStore").desiredAssertionStatus());

    try (Connection connection = DriverManager.getConnection(jdbcUrl, "sa", "");
        Statement statement = connection.createStatement()) {
      try (ResultSet settings =
          statement.executeQuery(
              "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'WRITE_DELAY'")) {
        assertTrue(settings.next());
        do {
          assertEquals("0", settings.getString(1));
        } while (settings.next());
      } finally {
        statement.execute("SHUTDOWN");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:h2:file:/tmp/custom-metadata",
        "jdbc:h2:file:/tmp/custom-metadata;WRITE_DELAY=100",
        "jdbc:mysql://localhost/gravitino",
        "jdbc:postgresql://localhost/gravitino"
      })
  void testPreservesExplicitJdbcUrls(String jdbcUrl) throws Exception {
    Properties properties =
        rewriteConfig("jdbc:h2", Map.of(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY, jdbcUrl));
    assertEquals(jdbcUrl, properties.getProperty(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY));
  }

  @Test
  void testPreservesJdbcUrlFromTemplate() throws Exception {
    String jdbcUrl = "jdbc:h2:file:/tmp/template-metadata;WRITE_DELAY=100";
    Properties properties = rewriteConfig(jdbcUrl, Collections.emptyMap());
    assertEquals(jdbcUrl, properties.getProperty(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:h2:file:/tmp/jvm-metadata;WRITE_DELAY=100",
        "jdbc:mysql://localhost/gravitino",
        "jdbc:postgresql://localhost/gravitino"
      })
  void testPreservesJdbcUrlFromJvm(String jdbcUrl) throws Throwable {
    withSystemProperty(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY,
        jdbcUrl,
        () -> {
          Properties properties = rewriteConfig(null, Collections.emptyMap());
          ServerConfig config = new ServerConfig();
          config.loadFromProperties(properties);
          assertEquals(jdbcUrl, config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL));
        });
  }

  @ParameterizedTest
  @ValueSource(strings = {"jvm-metadata", "caf\u00e9", "\u5143\u6570\u636e"})
  void testUsesStoragePathFromJvm(String directory) throws Throwable {
    String storagePath = mockConfDir.resolve(directory).toString();
    withSystemProperty(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(),
        storagePath,
        () -> {
          Properties properties = rewriteConfig("jdbc:h2", null, Collections.emptyMap());
          // Startup may write the configuration twice more when disabling auxiliary services.
          for (int rewrite = 0; rewrite < 3; rewrite++) {
            ServerConfig config = new ServerConfig();
            config.loadFromProperties(properties);
            String jdbcUrl = H2Database.getConnectionUrl(config);
            assertTrue(jdbcUrl.startsWith("jdbc:h2:file:" + storagePath + ";"), jdbcUrl);
            assertTrue(jdbcUrl.contains(";WRITE_DELAY=0"));
            if (rewrite < 2) {
              Path configFile = mockConfDir.resolve("gravitino.conf");
              ITUtils.overwriteConfigFile(configFile.toString(), properties);
              properties = config.loadPropertiesFromFile(configFile.toFile());
            }
          }
        });
  }

  @Test
  void testTemplateOverridesJvmJdbcUrl() throws Throwable {
    withSystemProperty(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY,
        "jdbc:postgresql://localhost/gravitino",
        () -> {
          Properties properties = rewriteConfig("jdbc:h2", Collections.emptyMap());
          ServerConfig config = new ServerConfig();
          config.loadFromProperties(properties);
          String jdbcUrl = H2Database.getConnectionUrl(config);
          assertTrue(jdbcUrl.startsWith("jdbc:h2:file:" + mockConfDir.resolve("metadata") + ";"));
          assertTrue(jdbcUrl.contains(";WRITE_DELAY=0"));
        });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testFileStoragePathOverridesJvm(boolean useOverride) throws Throwable {
    String templatePath = mockConfDir.resolve("caf\u00e9").toString();
    String storagePath =
        useOverride ? mockConfDir.resolve("\u5143\u6570\u636e").toString() : templatePath;
    withSystemProperty(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(),
        mockConfDir.resolve("jvm-metadata").toString(),
        () -> {
          Map<String, String> overrides =
              useOverride
                  ? Map.of(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(), storagePath)
                  : Collections.emptyMap();
          Properties properties = rewriteConfig("jdbc:h2", templatePath, overrides);
          ServerConfig config = new ServerConfig();
          config.loadFromProperties(properties);
          String jdbcUrl = H2Database.getConnectionUrl(config);
          assertTrue(jdbcUrl.startsWith("jdbc:h2:file:" + storagePath + ";"), jdbcUrl);
          assertTrue(jdbcUrl.contains(";WRITE_DELAY=0"));
        });
  }

  @Test
  void testContextLaunchesTheOpenSourceServerByDefault() {
    // Existing callers pass no launcher and must keep getting the open source server.
    MiniGravitinoContext context = new MiniGravitinoContext(Collections.emptyMap(), false, false);

    assertSame(MiniGravitinoContext.DEFAULT_SERVER_LAUNCHER, context.serverLauncher());
  }

  @Test
  void testContextCarriesACustomServerLauncher() throws Exception {
    // A distribution that ships its own entry point initializes components the open source server
    // knows nothing about, so embedded mode has to be able to start that server instead.
    List<String[]> launched = new ArrayList<>();
    MiniGravitinoContext context =
        new MiniGravitinoContext(Collections.emptyMap(), false, false, launched::add);

    context.serverLauncher().launch(new String[] {"gravitino.conf"});

    assertEquals(1, launched.size());
    assertEquals("gravitino.conf", launched.get(0)[0]);
  }

  @Test
  void testStopCleansResourcesWhenServerTaskDoesNotTerminate() throws Exception {
    ExecutorService executor = mock(ExecutorService.class);
    RESTClient restClient = mock(RESTClient.class);
    MiniGravitino miniGravitino = createMiniGravitino(executor, restClient);
    when(executor.awaitTermination(3, TimeUnit.MINUTES)).thenReturn(false);

    RuntimeException exception = assertThrows(RuntimeException.class, miniGravitino::stop);

    assertEquals("Can not terminate MiniGravitino server task", exception.getMessage());
    verify(restClient).close();
    assertFalse(Files.exists(mockConfDir));
  }

  @Test
  void testStopPreservesInterruptionWhenResourceCleanupFails() throws Exception {
    ExecutorService executor = mock(ExecutorService.class);
    RESTClient restClient = mock(RESTClient.class);
    MiniGravitino miniGravitino = createMiniGravitino(executor, restClient);
    InterruptedException interruption = new InterruptedException("interrupted");
    IOException closeFailure = new IOException("close failed");
    when(executor.awaitTermination(3, TimeUnit.MINUTES)).thenThrow(interruption);
    doThrow(closeFailure).when(restClient).close();

    InterruptedException exception = assertThrows(InterruptedException.class, miniGravitino::stop);

    assertSame(interruption, exception);
    assertEquals(1, exception.getSuppressed().length);
    assertSame(closeFailure, exception.getSuppressed()[0]);
    assertFalse(Files.exists(mockConfDir));
  }

  private Properties rewriteConfig(@Nullable String templateUrl, Map<String, String> overrides)
      throws IOException {
    return rewriteConfig(templateUrl, mockConfDir.resolve("metadata").toString(), overrides);
  }

  private Properties rewriteConfig(
      @Nullable String templateUrl, @Nullable String storagePath, Map<String, String> overrides)
      throws IOException {
    Properties template = new Properties();
    if (storagePath != null) {
      template.setProperty(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(), storagePath);
    }
    if (templateUrl != null) {
      template.setProperty(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY, templateUrl);
    }
    Path templateFile = mockConfDir.resolve("gravitino.conf.template");
    try (OutputStream output = Files.newOutputStream(templateFile)) {
      template.store(output, null);
    }
    Path configFile = mockConfDir.resolve("gravitino.conf");
    MiniGravitino miniGravitino =
        new MiniGravitino(
            new MiniGravitinoContext(overrides, true, true),
            mockConfDir.toFile(),
            mock(ExecutorService.class),
            null);
    miniGravitino.customizeConfigFile(templateFile.toString(), configFile.toString());
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(configFile)) {
      properties.load(input);
    }
    return properties;
  }

  private void withSystemProperty(String key, String value, Executable test) throws Throwable {
    String previous = System.setProperty(key, value);
    try {
      test.execute();
    } finally {
      if (previous == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, previous);
      }
    }
  }

  private MiniGravitino createMiniGravitino(ExecutorService executor, RESTClient restClient)
      throws IOException {
    Files.writeString(mockConfDir.resolve("gravitino.conf"), "test");
    return new MiniGravitino(
        new MiniGravitinoContext(Collections.emptyMap(), true, true),
        mockConfDir.toFile(),
        executor,
        restClient);
  }
}
