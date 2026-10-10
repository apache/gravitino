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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJobTemplateProvider;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
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
    assertTrue(
        template
            .arguments()
            .containsAll(Arrays.asList("--spec-id", "{{spec_id:-}}", "--updater-options")));
    assertTrue(template.arguments().contains("{{spark_conf:-}}"));
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

  /** Verifies optional spec IDs and invalid spec arguments. */
  @Test
  public void testSpecArgument() {
    assertNull(IcebergUpdateManifestStatsJob.parseSpecId(null));
    assertNull(IcebergUpdateManifestStatsJob.parseSpecId(""));
    assertEquals(0, IcebergUpdateManifestStatsJob.parseSpecId("0"));
    assertEquals(Integer.MAX_VALUE, IcebergUpdateManifestStatsJob.parseSpecId("2147483647"));
    for (String invalid : Arrays.asList("-1", "1.0", "abc", "2147483648", "1 OR 1=1")) {
      assertThrows(
          IllegalArgumentException.class, () -> IcebergUpdateManifestStatsJob.parseSpecId(invalid));
    }
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

  /** Verifies updater configuration aliases and required properties. */
  @Test
  public void testUpdaterConfiguration() {
    Map<String, String> options = new HashMap<>();
    options.put("gravitino_uri", " http://localhost:8090 ");
    options.put("metalake", " test ");
    Map<String, String> properties =
        IcebergUpdateManifestStatsJob.buildOptimizerProperties(options);
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
