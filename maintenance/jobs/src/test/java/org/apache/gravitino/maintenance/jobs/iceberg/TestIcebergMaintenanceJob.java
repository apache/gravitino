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

import java.util.List;
import org.apache.gravitino.job.SparkJobTemplate;
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
}
