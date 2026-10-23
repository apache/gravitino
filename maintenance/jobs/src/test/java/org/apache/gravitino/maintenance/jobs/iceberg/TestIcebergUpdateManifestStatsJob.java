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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJobTemplateProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.statistics.GravitinoStatisticsUpdater;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Test;

/** Tests the standalone manifest statistics template and argument validation. */
public class TestIcebergUpdateManifestStatsJob {
  /** Verifies manifest statistics use an independent built-in template. */
  @Test
  public void testIndependentTemplateRegistration() {
    Map<String, JobTemplate> templates =
        new BuiltInJobTemplateProvider()
            .jobTemplates().stream()
                .collect(Collectors.toMap(JobTemplate::name, template -> template));
    SparkJobTemplate template =
        (SparkJobTemplate) templates.get("builtin-iceberg-update-manifest-stats");
    assertNotNull(template);
    assertEquals(IcebergUpdateManifestStatsJob.class.getName(), template.className());
    assertEquals(10, template.arguments().size());
    assertFalse(template.arguments().contains("--spec-id"));
    assertTrue(template.arguments().contains("{{spark_conf:-}}"));
    assertTrue(template.arguments().contains("{{coordination_directory:-}}"));
    assertFalse(template.arguments().contains("--update-mode"));
    assertEquals("v1", template.customFields().get("version"));
    assertTrue(template.configs().containsKey("spark.sql.extensions"));
    SparkJobTemplate existing = (SparkJobTemplate) templates.get("builtin-iceberg-update-stats");
    assertNotNull(existing);
    assertFalse(existing.arguments().contains("--spec-id"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseUpdateMode("manifests"));
  }

  /** Verifies catalog and table identifiers are validated and escaped. */
  @Test
  public void testTableIdentifierValidationAndEscaping() {
    assertEquals(
        "`cat``alog`.`db`.`tbl``name`",
        IcebergUpdateManifestStatsJob.buildTableIdentifier("cat`alog", "db.tbl`name"));
    for (String invalid : Arrays.asList("table", "db.", ".table", "db.table.extra", " ")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> IcebergUpdateManifestStatsJob.buildTableIdentifier("catalog", invalid));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateManifestStatsJob.main(new String[] {"--catalog", "catalog"}));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateManifestStatsJob.main(new String[] {"--table", "db.table"}));
  }

  /** Verifies short auth options reach HTTP requests made by the real statistics updater. */
  @Test
  public void testUpdaterSendsBasicAuthFromAliases() throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          exchange.sendResponseHeaders(401, -1);
          exchange.close();
        });
    server.start();
    try (GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater()) {
      Map<String, String> options = new HashMap<>();
      options.put("gravitino_uri", "http://127.0.0.1:" + server.getAddress().getPort());
      options.put("metalake", "test");
      options.put("auth_type", "basic");
      options.put("username", "alice");
      options.put("password", "test-password");
      updater.initialize(
          new OptimizerEnv(
              new OptimizerConfig(
                  IcebergUpdateManifestStatsJob.buildOptimizerProperties(options))));
      // Stop at the HTTP boundary after capturing the credentials sent by the updater.
      assertThrows(
          RuntimeException.class,
          () ->
              updater.updateTableStatistics(
                  NameIdentifier.of("catalog", "db", "table"),
                  Collections.singletonList(
                      new StatisticEntryImpl<>("custom-count", StatisticValues.longValue(1)))));
      assertEquals(
          "Basic "
              + Base64.getEncoder()
                  .encodeToString("alice:test-password".getBytes(StandardCharsets.UTF_8)),
          authorization.get());
    } finally {
      server.stop(0);
    }
  }

  /** Verifies auth aliases are promoted without overriding canonical settings. */
  @Test
  public void testAuthAliases() {
    Map<String, String> options = new HashMap<>();
    options.put("gravitino_uri", "http://localhost:8090");
    options.put("metalake", "test");
    options.put("auth_type", "oauth");
    options.put("oauth_server_uri", "http://localhost:9000");
    options.put("oauth_path", "token");
    options.put("oauth_credential", "client:secret");
    options.put("oauth_scope", "catalog");
    Map<String, String> properties =
        IcebergUpdateManifestStatsJob.buildOptimizerProperties(options);
    assertEquals("oauth", properties.get(OptimizerConfig.AUTH_TYPE));
    assertEquals("http://localhost:9000", properties.get(OptimizerConfig.AUTH_OAUTH_SERVER_URI));
    assertEquals("token", properties.get(OptimizerConfig.AUTH_OAUTH_PATH));
    assertEquals("client:secret", properties.get(OptimizerConfig.AUTH_OAUTH_CREDENTIAL));
    assertEquals("catalog", properties.get(OptimizerConfig.AUTH_OAUTH_SCOPE));
    assertFalse(options.containsKey(OptimizerConfig.AUTH_TYPE));
    options.put(OptimizerConfig.AUTH_TYPE, "none");
    assertEquals(
        "none",
        IcebergUpdateManifestStatsJob.buildOptimizerProperties(options)
            .get(OptimizerConfig.AUTH_TYPE));
  }

  /** Verifies updater configuration aliases and required properties. */
  @Test
  public void testUpdaterConfiguration() {
    Map<String, String> options = new HashMap<>();
    options.put("gravitino_uri", " http://localhost:8090 ");
    options.put("metalake", " test ");
    options.put(GravitinoClientConfiguration.CLIENT_RETRY_TIMES, "2");
    Map<String, String> properties =
        IcebergUpdateManifestStatsJob.buildOptimizerProperties(options);
    assertEquals("0", properties.get(GravitinoClientConfiguration.CLIENT_RETRY_TIMES));
    assertEquals("http://localhost:8090", properties.get(OptimizerConfig.GRAVITINO_URI));
    assertEquals("test", properties.get(OptimizerConfig.GRAVITINO_METALAKE));
    assertEquals(" test ", options.get("metalake"));
    properties.remove("metalake");
    properties.remove("gravitino_uri");
    assertEquals(properties, IcebergUpdateManifestStatsJob.buildOptimizerProperties(properties));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateManifestStatsJob.buildOptimizerProperties(Collections.emptyMap()));
    options.put("metalake", " ");
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateManifestStatsJob.buildOptimizerProperties(options));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergUpdateManifestStatsJob.main(
                new String[] {"--catalog", "catalog", "--table", "db.table", "--spec-id", "-1"}));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergUpdateManifestStatsJob.main(
                new String[] {"--catalog", "catalog", "--table", "db.table"}));
  }
}
