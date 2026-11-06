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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergMaintenanceJob.MaintenanceType;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IcebergMaintenanceJob}. */
public class TestIcebergMaintenanceJob {

  @Test
  public void testTemplatePlaceholders() {
    SparkJobTemplate template = new IcebergMaintenanceJob().jobTemplate();
    assertEquals(IcebergMaintenanceJob.NAME, template.name());
    List<String> args = template.arguments();
    assertTrue(args.contains("--maintenance-types"));
    assertTrue(args.contains("{{maintenance_types}}"));
    assertFalse(args.contains("{{maintenance_types:-}}"));
    assertTrue(args.contains("--policy-ids"));
    assertTrue(args.contains("{{policy_ids}}"));
    assertTrue(args.contains("--tables"));
    assertTrue(args.contains("{{tables}}"));
    assertTrue(args.contains("--rules"));
    assertTrue(args.contains("{{rules:-}}"));
    assertFalse(args.contains("--table-id"));
    assertFalse(args.contains("--gravitino-job-id"));
  }

  @Test
  public void testMaintenanceTypeFromWire() {
    assertEquals(MaintenanceType.EXPIRE_SNAPSHOTS, MaintenanceType.fromWire("expire_snapshots"));
    assertEquals(
        MaintenanceType.REWRITE_DATA_FILES, MaintenanceType.fromWire("REWRITE_DATA_FILES"));
    assertThrows(IllegalArgumentException.class, () -> MaintenanceType.fromWire("unknown"));
    assertThrows(IllegalArgumentException.class, () -> MaintenanceType.fromWire(null));
  }

  @Test
  public void testRunRejectsInvalidArgs() {
    assertEquals(
        1, IcebergMaintenanceJob.run(new String[] {"--maintenance-types", "expire_snapshots"}));
    assertEquals(
        1,
        IcebergMaintenanceJob.run(
            new String[] {
              "--maintenance-types",
              "expire_snapshots,rewrite_manifests",
              "--policy-ids",
              "10",
              "--tables",
              "[{\"table_id\":1,\"catalog_name\":\"c\",\"table_identifier\":\"db.t\"}]"
            }));
    assertEquals(
        1,
        IcebergMaintenanceJob.run(
            new String[] {
              "--maintenance-types", "expire_snapshots", "--policy-ids", "10", "--tables", "[]"
            }));
  }

  @Test
  public void testSnapshotDriftGateOnlyForCompactionAndManifest() {
    assertTrue(IcebergMaintenanceJob.usesSnapshotDriftGate(MaintenanceType.REWRITE_DATA_FILES));
    assertTrue(IcebergMaintenanceJob.usesSnapshotDriftGate(MaintenanceType.REWRITE_MANIFESTS));
    assertFalse(IcebergMaintenanceJob.usesSnapshotDriftGate(MaintenanceType.EXPIRE_SNAPSHOTS));
    assertFalse(IcebergMaintenanceJob.usesSnapshotDriftGate(MaintenanceType.REMOVE_ORPHAN_FILES));
  }

  @Test
  public void testCompactionMetricsSchema() throws Exception {
    Map<String, Object> metrics =
        IcebergMaintenanceJob.compactionMetrics(1.23e15, 42L, 126877696D, 9876543210L, 80L);
    Map<String, Object> parsed = parseJson(IcebergMaintenanceJob.toMetricsJson(metrics));
    assertEquals(5, parsed.size());
    assertEquals(
        1.23e15,
        ((Number) parsed.get(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC)).doubleValue());
    assertEquals(
        42L,
        ((Number) parsed.get(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC)).longValue());
    assertEquals(
        126877696D,
        ((Number) parsed.get(IcebergMaintenanceJob.AVG_FILE_SIZE_METRIC)).doubleValue());
    assertEquals(
        9876543210L,
        ((Number) parsed.get(IcebergMaintenanceJob.TOTAL_FILE_SIZE_METRIC)).longValue());
    assertEquals(80L, ((Number) parsed.get(IcebergMaintenanceJob.FILE_NUMBER_METRIC)).longValue());
    assertFalse(parsed.containsKey("maintenanceType"));
    assertFalse(parsed.containsKey("snapshotId"));
  }

  @Test
  public void testManifestBeforeAndAfterSchemas() throws Exception {
    Map<String, Object> before = IcebergMaintenanceJob.manifestBeforeMetrics(120L, 8388608D);
    Map<String, Object> beforeParsed = parseJson(IcebergMaintenanceJob.toMetricsJson(before));
    assertEquals(2, beforeParsed.size());
    assertEquals(
        120L,
        ((Number) beforeParsed.get(IcebergMaintenanceJob.MANIFEST_NUMBER_METRIC)).longValue());
    assertEquals(
        8388608D,
        ((Number) beforeParsed.get(IcebergMaintenanceJob.AVG_MANIFEST_SIZE_METRIC)).doubleValue());

    Map<String, Object> after = IcebergMaintenanceJob.manifestAfterMetrics(18750L);
    Map<String, Object> afterParsed = parseJson(IcebergMaintenanceJob.toMetricsJson(after));
    assertEquals(1, afterParsed.size());
    assertEquals(
        18750L,
        ((Number) afterParsed.get(IcebergMaintenanceJob.MANIFESTS_REMOVED_METRIC)).longValue());
    assertFalse(afterParsed.containsKey(IcebergMaintenanceJob.MANIFEST_NUMBER_METRIC));
  }

  @Test
  public void testExpireBeforeAndAfterSchemas() throws Exception {
    Map<String, Object> before = IcebergMaintenanceJob.expireBeforeMetrics(86L, 7800000000000L);
    Map<String, Object> beforeParsed = parseJson(IcebergMaintenanceJob.toMetricsJson(before));
    assertEquals(2, beforeParsed.size());
    assertEquals(
        86L, ((Number) beforeParsed.get(IcebergMaintenanceJob.SNAPSHOT_COUNT_METRIC)).longValue());
    assertEquals(
        7800000000000L,
        ((Number) beforeParsed.get(IcebergMaintenanceJob.TOTAL_FILE_SIZE_METRIC)).longValue());

    Map<String, Object> after = IcebergMaintenanceJob.expireAfterMetrics(62L, 7100000000000L);
    Map<String, Object> afterParsed = parseJson(IcebergMaintenanceJob.toMetricsJson(after));
    assertEquals(2, afterParsed.size());
    assertEquals(
        62L,
        ((Number) afterParsed.get(IcebergMaintenanceJob.SNAPSHOTS_CLEARED_METRIC)).longValue());
    assertEquals(
        7100000000000L,
        ((Number) afterParsed.get(IcebergMaintenanceJob.BYTES_RECLAIMED_METRIC)).longValue());
    assertFalse(afterParsed.containsKey(IcebergMaintenanceJob.SNAPSHOT_COUNT_METRIC));
  }

  @Test
  public void testExpireSetDifferenceIgnoresConcurrentAdds() {
    Set<Long> beforeSnapshots = new HashSet<>();
    beforeSnapshots.add(1L);
    beforeSnapshots.add(2L);
    beforeSnapshots.add(3L);
    Set<Long> afterSnapshots = new HashSet<>();
    afterSnapshots.add(2L);
    afterSnapshots.add(3L);
    afterSnapshots.add(99L); // concurrent commit
    assertEquals(1L, IcebergMaintenanceJob.snapshotsCleared(beforeSnapshots, afterSnapshots));

    Map<String, Long> beforeFiles = new HashMap<>();
    beforeFiles.put("a.parquet", 100L);
    beforeFiles.put("b.parquet", 200L);
    Map<String, Long> afterFiles = new HashMap<>();
    afterFiles.put("b.parquet", 200L);
    afterFiles.put("c.parquet", 500L); // concurrent write
    assertEquals(100L, IcebergMaintenanceJob.bytesReclaimed(beforeFiles, afterFiles));

    IcebergMaintenanceJob.ExpireInventory before =
        new IcebergMaintenanceJob.ExpireInventory(beforeSnapshots, beforeFiles);
    IcebergMaintenanceJob.ExpireInventory after =
        new IcebergMaintenanceJob.ExpireInventory(afterSnapshots, afterFiles);
    Map<String, Object> metrics = IcebergMaintenanceJob.expireAfterMetrics(before, after);
    assertEquals(
        1L, ((Number) metrics.get(IcebergMaintenanceJob.SNAPSHOTS_CLEARED_METRIC)).longValue());
    assertEquals(
        100L, ((Number) metrics.get(IcebergMaintenanceJob.BYTES_RECLAIMED_METRIC)).longValue());
  }

  @Test
  public void testBuildReachableContentFilesSql() {
    String sql = IcebergMaintenanceJob.buildReachableContentFilesSql("cat", "db.tbl");
    assertTrue(sql.contains("FROM `cat`.`db`.`tbl`.`all_files`"));
    assertTrue(sql.contains("GROUP BY file_path"));
    assertTrue(sql.contains("MAX(file_size_in_bytes)"));
    assertEquals(
        0L,
        IcebergMaintenanceJob.snapshotsCleared(Collections.emptySet(), Collections.singleton(1L)));
  }

  @Test
  public void testOrphanAfterSchema() throws Exception {
    Map<String, Object> after = IcebergMaintenanceJob.orphanAfterMetrics(288400L, 2500000000000L);
    Map<String, Object> parsed = parseJson(IcebergMaintenanceJob.toMetricsJson(after));
    assertEquals(2, parsed.size());
    assertEquals(
        288400L,
        ((Number) parsed.get(IcebergMaintenanceJob.ORPHAN_FILES_REMOVED_METRIC)).longValue());
    assertEquals(
        2500000000000L,
        ((Number) parsed.get(IcebergMaintenanceJob.ORPHAN_BYTES_RECLAIMED_METRIC)).longValue());
  }

  private static Map<String, Object> parseJson(String json) throws Exception {
    return JsonUtils.anyFieldMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
  }
}
