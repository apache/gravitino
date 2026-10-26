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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergMaintenanceJob.MaintenanceType;
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
  public void testResolveTablesAndTypes() {
    List<IcebergMaintenanceJob.TableTarget> targets =
        IcebergMaintenanceJob.resolveTableTargets(
            Map.of(
                "tables",
                "[{\"table_id\":10,\"catalog_name\":\"iceberg\",\"table_identifier\":\"db.t1\"},"
                    + "{\"table_id\":11,\"catalog_name\":\"iceberg\",\"table_identifier\":\"db.t2\"}]"));
    assertEquals(2, targets.size());
    assertEquals(11L, targets.get(1).tableId);

    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergMaintenanceJob.resolveTableTargets(Map.of("catalog", "iceberg")));

    List<MaintenanceType> chain =
        IcebergMaintenanceJob.parseMaintenanceTypes(
            "rewrite_data_files, rewrite_manifests, expire_snapshots");
    assertEquals(
        List.of(
            MaintenanceType.REWRITE_DATA_FILES,
            MaintenanceType.REWRITE_MANIFESTS,
            MaintenanceType.EXPIRE_SNAPSHOTS),
        chain);
    assertEquals(List.of(10L, 20L), IcebergMaintenanceJob.parsePolicyIds("10, 20", 2));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergMaintenanceJob.parsePolicyIds("10", 2));
  }

  @Test
  public void testResolveGravitinoJobIdAndMissingTables() {
    assertEquals(
        42L,
        IcebergMaintenanceJob.resolveGravitinoJobId(
            null, "/tmp/gravitino/job-runs/" + JobHandle.JOB_ID_PREFIX + "42"));
    assertThrows(
        IllegalStateException.class,
        () -> IcebergMaintenanceJob.resolveGravitinoJobId(null, "/tmp/not-a-job-dir"));
    assertEquals(
        1, IcebergMaintenanceJob.run(new String[] {"--maintenance-types", "expire_snapshots"}));
  }
}
