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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IcebergMaintenanceJob} orchestration. */
public class TestIcebergMaintenanceJob {

  @Test
  public void testTemplatePlaceholders() {
    SparkJobTemplate template = new IcebergMaintenanceJob().jobTemplate();
    assertEquals(IcebergMaintenanceJob.NAME, template.name());
    List<String> args = template.arguments();
    assertTrue(args.contains("--tasks"));
    assertTrue(args.contains("{{tasks}}"));
    assertFalse(args.contains("{{tasks:-}}"));
    assertTrue(args.contains("--updater-options"));
    assertTrue(args.contains("--spark-conf"));
    assertFalse(args.contains("--maintenance-types"));
    assertFalse(args.contains("--policy-ids"));
    assertFalse(args.contains("--tables"));
    assertFalse(args.contains("--strategy"));
    // Spark settings come from --spark-conf only; template carries no Spark configs.
    assertTrue(template.configs().isEmpty());
  }

  @Test
  public void testRunRejectsInvalidArgs() {
    assertEquals(1, IcebergMaintenanceJob.run(new String[] {"--tasks", "[]"}));
    assertEquals(
        1,
        IcebergMaintenanceJob.run(
            new String[] {
              "--tasks",
              "[{\"table_id\":1,\"policy_type\":\"system_iceberg_snapshot_expiration\","
                  + "\"policy_id\":10,\"action_id\":100,\"params\":{}}]"
            }));
    assertEquals(
        1,
        IcebergMaintenanceJob.run(
            new String[] {
              "--tasks",
              "[{\"table_id\":1,\"policy_type\":\"system_row_filter\",\"policy_id\":10,"
                  + "\"action_id\":100,\"params\":{\"catalog_name\":\"c\","
                  + "\"table_identifier\":\"db.t\"}}]"
            }));
  }

  @Test
  public void testCompactionMinDataFileMseFromRules() {
    assertEquals(
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        IcebergMaintenanceJob.compactionMinDataFileMse(null));
    assertEquals(
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        IcebergMaintenanceJob.compactionMinDataFileMse(Collections.emptyMap()));
    assertEquals(
        10L,
        IcebergMaintenanceJob.compactionMinDataFileMse(
            Collections.singletonMap(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY, 10)));
  }

  @Test
  public void testWithCompactionDataMseRatioEnrichesBeforeAndAfterShape() throws Exception {
    Map<String, Object> inventory = new HashMap<>();
    inventory.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 20D);
    inventory.put(IcebergDataCompactionContent.FILE_NUMBER_METRIC, 3L);
    TableSnapshotMetrics sample = TableSnapshotMetrics.of(42L, inventory);

    TableSnapshotMetrics enriched =
        IcebergMaintenanceJob.withCompactionDataMseRatio(
            sample,
            Policy.BuiltInType.ICEBERG_COMPACTION,
            Collections.singletonMap(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY, 10));

    Map<String, Object> metrics =
        JsonUtils.anyFieldMapper()
            .readValue(enriched.metricsJson(), new TypeReference<Map<String, Object>>() {});
    assertEquals(42L, enriched.snapshotId());
    assertEquals(
        2D,
        ((Number) metrics.get(IcebergDataCompactionContent.DATA_MSE_RATIO_METRIC)).doubleValue());
    assertFalse(metrics.containsKey(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY));

    TableSnapshotMetrics skipped =
        IcebergMaintenanceJob.withCompactionDataMseRatio(
            sample, Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, null);
    assertEquals(sample.metricsJson(), skipped.metricsJson());
  }
}
