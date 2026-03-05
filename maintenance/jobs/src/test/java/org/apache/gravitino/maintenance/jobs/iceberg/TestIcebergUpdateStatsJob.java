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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.Test;

public class TestIcebergUpdateStatsJob {

  @Test
  public void testJobTemplateHasCorrectNameAndVersion() {
    IcebergUpdateStatsAndMetricsJob job = new IcebergUpdateStatsAndMetricsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template);
    assertEquals("builtin-iceberg-update-stats", template.name());
    assertTrue(template.name().matches(JobTemplateProvider.BUILTIN_NAME_PATTERN));
    assertEquals("v1", template.customFields().get(JobTemplateProvider.PROPERTY_VERSION_KEY));
  }

  @Test
  public void testJobTemplateArguments() {
    IcebergUpdateStatsAndMetricsJob job = new IcebergUpdateStatsAndMetricsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.arguments());
    assertEquals(12, template.arguments().size());
    assertTrue(template.arguments().contains("--catalog"));
    assertTrue(template.arguments().contains("{{catalog_name}}"));
    assertTrue(template.arguments().contains("--table"));
    assertTrue(template.arguments().contains("{{table_identifier}}"));
    assertTrue(template.arguments().contains("--update-mode"));
    assertTrue(template.arguments().contains("{{update_mode}}"));
    assertTrue(template.arguments().contains("--target-file-size-bytes"));
    assertTrue(template.arguments().contains("{{target_file_size_bytes}}"));
    assertTrue(template.arguments().contains("--updater-options"));
    assertTrue(template.arguments().contains("{{updater_options}}"));
    assertTrue(template.arguments().contains("--spark-conf"));
    assertTrue(template.arguments().contains("{{spark_conf}}"));
  }

  @Test
  public void testParseArguments() {
    String[] args = {
      "--catalog", "cat",
      "--table", "db.tbl",
      "--update-mode", "metrics",
      "--target-file-size-bytes", "2048",
      "--updater-options", "{\"metalake\":\"ml\",\"gravitino_uri\":\"http://localhost:8090\"}",
      "--spark-conf", "{\"spark.master\":\"local[2]\"}"
    };

    Map<String, String> parsed = IcebergUpdateStatsAndMetricsJob.parseArguments(args);
    assertEquals("cat", parsed.get("catalog"));
    assertEquals("db.tbl", parsed.get("table"));
    assertEquals("metrics", parsed.get("update-mode"));
    assertEquals("2048", parsed.get("target-file-size-bytes"));
    assertEquals(
        "{\"metalake\":\"ml\",\"gravitino_uri\":\"http://localhost:8090\"}",
        parsed.get("updater-options"));
    assertEquals("{\"spark.master\":\"local[2]\"}", parsed.get("spark-conf"));
  }

  @Test
  public void testBuildStatsSql() {
    String tableSql = IcebergUpdateStatsAndMetricsJob.buildTableStatsSql("cat", "db.tbl", 100000L);
    String partitionSql =
        IcebergUpdateStatsAndMetricsJob.buildPartitionStatsSql("cat", "db.tbl", 100000L);

    assertTrue(tableSql.contains("FROM cat.db.tbl.files"));
    assertTrue(tableSql.contains("AS datafile_mse"));
    assertTrue(partitionSql.contains("FROM cat.db.tbl.files"));
    assertTrue(partitionSql.contains("GROUP BY partition"));
    assertTrue(partitionSql.startsWith("SELECT partition"));
  }

  @Test
  public void testParseTargetFileSize() {
    assertEquals(100000L, IcebergUpdateStatsAndMetricsJob.parseTargetFileSize(null));
    assertEquals(100000L, IcebergUpdateStatsAndMetricsJob.parseTargetFileSize(""));
    assertEquals(2048L, IcebergUpdateStatsAndMetricsJob.parseTargetFileSize("2048"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseTargetFileSize("-1"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseTargetFileSize("abc"));
  }

  @Test
  public void testParseUpdateMode() {
    assertEquals(
        IcebergUpdateStatsAndMetricsJob.UpdateMode.ALL,
        IcebergUpdateStatsAndMetricsJob.parseUpdateMode(null));
    assertEquals(
        IcebergUpdateStatsAndMetricsJob.UpdateMode.ALL,
        IcebergUpdateStatsAndMetricsJob.parseUpdateMode(""));
    assertEquals(
        IcebergUpdateStatsAndMetricsJob.UpdateMode.STATS,
        IcebergUpdateStatsAndMetricsJob.parseUpdateMode("stats"));
    assertEquals(
        IcebergUpdateStatsAndMetricsJob.UpdateMode.METRICS,
        IcebergUpdateStatsAndMetricsJob.parseUpdateMode("metrics"));
    assertEquals(
        IcebergUpdateStatsAndMetricsJob.UpdateMode.ALL,
        IcebergUpdateStatsAndMetricsJob.parseUpdateMode("all"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseUpdateMode("invalid"));
  }

  @Test
  public void testParseJsonOptions() {
    Map<String, String> parsed =
        IcebergUpdateStatsAndMetricsJob.parseJsonOptions(
            "{\"a\":\"b\",\"x\":1,\"flag\":true,\"nil\":null}");
    assertEquals("b", parsed.get("a"));
    assertEquals("1", parsed.get("x"));
    assertEquals("true", parsed.get("flag"));
    assertEquals("", parsed.get("nil"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseJsonOptions("{not_json}"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseJsonOptions("{\"nested\":{\"a\":1}}"));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.parseJsonOptions("{\"array\":[1,2,3]}"));
  }

  @Test
  public void testBuildOptimizerProperties() {
    Map<String, String> options =
        Map.of(
            "gravitino_uri", "http://localhost:8090",
            "metalake", "ml",
            "gravitino.optimizer.jdbcMetrics.jdbcUrl", "jdbc:mysql://localhost:3306/metrics");
    Map<String, String> optimizerProperties =
        IcebergUpdateStatsAndMetricsJob.buildOptimizerProperties(options);

    assertEquals("http://localhost:8090", optimizerProperties.get(OptimizerConfig.GRAVITINO_URI));
    assertEquals("ml", optimizerProperties.get(OptimizerConfig.GRAVITINO_METALAKE));
    assertEquals(
        "jdbc:mysql://localhost:3306/metrics",
        optimizerProperties.get("gravitino.optimizer.jdbcMetrics.jdbcUrl"));
  }

  @Test
  public void testRequireGravitinoConfig() {
    Map<String, String> optimizerProperties =
        Map.of(
            OptimizerConfig.GRAVITINO_URI, "http://localhost:8090",
            OptimizerConfig.GRAVITINO_METALAKE, "ml");
    assertEquals(
        optimizerProperties,
        IcebergUpdateStatsAndMetricsJob.requireGravitinoConfig(optimizerProperties));

    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergUpdateStatsAndMetricsJob.requireGravitinoConfig(Map.of()));
  }
}
