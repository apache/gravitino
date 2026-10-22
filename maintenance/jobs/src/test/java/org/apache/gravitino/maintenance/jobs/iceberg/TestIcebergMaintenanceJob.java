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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergMaintenanceJob.MaintenanceType;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IcebergMaintenanceJob}. */
public class TestIcebergMaintenanceJob {

  @Test
  public void testTemplateNameAndPlaceholders() {
    SparkJobTemplate template = new IcebergMaintenanceJob().jobTemplate();
    assertEquals(IcebergMaintenanceJob.NAME, template.name());
    assertEquals(IcebergMaintenanceJob.class.getName(), template.className());
    assertEquals("v1", template.customFields().get(JobTemplateProvider.PROPERTY_VERSION_KEY));
    List<String> args = template.arguments();
    assertFalse(args.contains("--maintenance-type"));
    assertFalse(args.contains("{{maintenance_type}}"));
    assertTrue(args.contains("--maintenance-types"));
    assertTrue(args.contains("{{maintenance_types}}"));
    assertTrue(args.contains("--updater-options"));
    assertTrue(args.contains("{{updater_options:-}}"));
    assertFalse(args.contains("--gravitino-job-id"));
    assertFalse(args.contains("{{gravitino_job_id}}"));
    assertFalse(args.contains("--table-id"));
    assertFalse(args.contains("{{table_id:-}}"));
    assertTrue(args.contains("--tables"));
    assertTrue(args.contains("{{tables}}"));
    assertFalse(args.contains("--policy-id"));
  }

  @Test
  public void testResolveTableTargetsSingleAndBatch() {
    Map<String, String> single = new HashMap<>();
    single.put(
        "tables",
        "[{\"table_id\":10,\"catalog_name\":\"iceberg\",\"table_identifier\":\"db.t1\"}]");
    List<IcebergMaintenanceJob.TableTarget> one = IcebergMaintenanceJob.resolveTableTargets(single);
    assertEquals(1, one.size());
    assertEquals(10L, one.get(0).tableId);
    assertEquals("iceberg", one.get(0).catalogName);
    assertEquals("db.t1", one.get(0).tableIdentifier);

    Map<String, String> batch = new HashMap<>();
    batch.put(
        "tables",
        "[{\"table_id\":10,\"catalog_name\":\"iceberg\",\"table_identifier\":\"db.t1\"},"
            + "{\"table_id\":11,\"catalog_name\":\"iceberg\",\"table_identifier\":\"db.t2\"}]");
    List<IcebergMaintenanceJob.TableTarget> many = IcebergMaintenanceJob.resolveTableTargets(batch);
    assertEquals(2, many.size());
    assertEquals(11L, many.get(1).tableId);
    assertEquals("db.t2", many.get(1).tableIdentifier);
  }

  @Test
  public void testResolveTableTargetsRequiresTables() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergMaintenanceJob.resolveTableTargets(
                Map.of("catalog", "iceberg", "table", "db.t1")));
  }

  @Test
  public void testMaintenanceTypeParsing() {
    assertEquals(MaintenanceType.EXPIRE_SNAPSHOTS, MaintenanceType.fromWire("expire_snapshots"));
    assertEquals(
        MaintenanceType.REWRITE_DATA_FILES, MaintenanceType.fromWire("REWRITE_DATA_FILES"));
    assertThrows(IllegalArgumentException.class, () -> MaintenanceType.fromWire("unknown"));
    assertThrows(IllegalArgumentException.class, () -> MaintenanceType.fromWire(null));
  }

  @Test
  public void testParseMaintenanceTypesChainAndSingle() {
    List<MaintenanceType> chain =
        IcebergMaintenanceJob.parseMaintenanceTypes(
            "rewrite_data_files, rewrite_manifests, expire_snapshots");
    assertEquals(3, chain.size());
    assertEquals(MaintenanceType.REWRITE_DATA_FILES, chain.get(0));
    assertEquals(MaintenanceType.REWRITE_MANIFESTS, chain.get(1));
    assertEquals(MaintenanceType.EXPIRE_SNAPSHOTS, chain.get(2));

    List<MaintenanceType> single = IcebergMaintenanceJob.parseMaintenanceTypes("expire_snapshots");
    assertEquals(List.of(MaintenanceType.EXPIRE_SNAPSHOTS), single);
  }

  @Test
  public void testTemplateRequiresMaintenanceTypes() {
    List<String> args = new IcebergMaintenanceJob().jobTemplate().arguments();
    assertTrue(args.contains("--maintenance-types"));
    assertTrue(args.contains("{{maintenance_types}}"));
    assertFalse(args.contains("{{maintenance_types:-}}"));
    assertTrue(args.contains("--policy-ids"));
    assertTrue(args.contains("{{policy_ids}}"));
    assertFalse(args.contains("{{policy_ids:-}}"));
  }

  @Test
  public void testParsePolicyIdsMatchesTypes() {
    assertEquals(List.of(10L, 20L), IcebergMaintenanceJob.parsePolicyIds("10, 20", 2));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergMaintenanceJob.parsePolicyIds("10", 2));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergMaintenanceJob.parsePolicyIds(null, 1));
  }

  @Test
  public void testStubMetricsJson() {
    String json = IcebergMaintenanceJob.stubMetricsJson("expire_snapshots", 42L);
    assertTrue(json.contains("expire_snapshots"));
    assertTrue(json.contains("42"));
  }

  @Test
  public void testResolveGravitinoJobIdFromEnv() {
    assertEquals(99L, IcebergMaintenanceJob.resolveGravitinoJobId("99", "/tmp/other"));
  }

  @Test
  public void testResolveGravitinoJobIdFromStagingDir() {
    assertEquals(
        42L,
        IcebergMaintenanceJob.resolveGravitinoJobId(
            null, "/tmp/gravitino/job-runs/" + JobHandle.JOB_ID_PREFIX + "42"));
  }

  @Test
  public void testResolveGravitinoJobIdMissing() {
    assertThrows(
        IllegalStateException.class,
        () -> IcebergMaintenanceJob.resolveGravitinoJobId(null, "/tmp/not-a-job-dir"));
  }

  @Test
  public void testRunRejectsMissingTablesJobConf() {
    assertEquals(
        1, IcebergMaintenanceJob.run(new String[] {"--maintenance-types", "expire_snapshots"}));
  }
}
