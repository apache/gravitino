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
}
