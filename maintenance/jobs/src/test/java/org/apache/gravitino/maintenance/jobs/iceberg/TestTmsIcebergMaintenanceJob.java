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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.iceberg.TmsIcebergMaintenanceJob.MaintenanceType;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TmsIcebergMaintenanceJob}. */
public class TestTmsIcebergMaintenanceJob {

  @Test
  public void testTemplateNameAndPlaceholders() {
    SparkJobTemplate template = new TmsIcebergMaintenanceJob().jobTemplate();
    assertEquals(TmsIcebergMaintenanceJob.NAME, template.name());
    assertEquals(TmsIcebergMaintenanceJob.class.getName(), template.className());
    assertEquals("v1", template.customFields().get(JobTemplateProvider.PROPERTY_VERSION_KEY));
    List<String> args = template.arguments();
    assertTrue(args.contains("--maintenance-type"));
    assertTrue(args.contains("{{maintenance_type}}"));
    assertTrue(args.contains("--gravitino-job-id"));
    assertTrue(args.contains("{{gravitino_job_id}}"));
    assertTrue(args.contains("--updater-options"));
    assertTrue(args.contains("{{updater_options:-}}"));
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
        TmsIcebergMaintenanceJob.parseMaintenanceTypes(
            "rewrite_data_files, rewrite_manifests, expire_snapshots", null);
    assertEquals(3, chain.size());
    assertEquals(MaintenanceType.REWRITE_DATA_FILES, chain.get(0));
    assertEquals(MaintenanceType.REWRITE_MANIFESTS, chain.get(1));
    assertEquals(MaintenanceType.EXPIRE_SNAPSHOTS, chain.get(2));

    List<MaintenanceType> single =
        TmsIcebergMaintenanceJob.parseMaintenanceTypes(null, "expire_snapshots");
    assertEquals(List.of(MaintenanceType.EXPIRE_SNAPSHOTS), single);
  }

  @Test
  public void testTemplateIncludesMaintenanceTypesPlaceholder() {
    List<String> args = new TmsIcebergMaintenanceJob().jobTemplate().arguments();
    assertTrue(args.contains("--maintenance-types"));
    assertTrue(args.contains("{{maintenance_types:-}}"));
  }

  @Test
  public void testStubMetricsJson() {
    String json = TmsIcebergMaintenanceJob.stubMetricsJson("expire_snapshots", 42L);
    assertTrue(json.contains("expire_snapshots"));
    assertTrue(json.contains("42"));
  }

  @Test
  public void testMissingRequiredArgsReturnsError() {
    assertEquals(1, TmsIcebergMaintenanceJob.run(new String[] {"--catalog", "c"}));
    assertEquals(
        1,
        TmsIcebergMaintenanceJob.run(
            new String[] {
              "--catalog", "c", "--table", "db.t", "--maintenance-type", "expire_snapshots"
            }));
  }
}
