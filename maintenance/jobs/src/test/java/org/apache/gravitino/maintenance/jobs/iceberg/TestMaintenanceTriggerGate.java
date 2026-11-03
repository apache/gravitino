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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergMaintenanceJob.MaintenanceType;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceTriggerGate}. */
public class TestMaintenanceTriggerGate {

  @Test
  public void testNonCompactionAlwaysMaintain() {
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            MaintenanceType.EXPIRE_SNAPSHOTS, new HashMap<>(), "{\"snapshotId\":1}"));
  }

  @Test
  public void testCompactionUsesEnqueueRules() {
    Map<String, String> args = new HashMap<>();
    args.put(
        MaintenanceTriggerGate.RULES_ARG,
        "{\"rewrite_data_files\":{"
            + "\"trigger-expr\":\"custom-data-file-mse >= minDataFileMse\","
            + "\"minDataFileMse\":10}}");

    String below =
        "{\""
            + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC
            + "\":5,\""
            + IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC
            + "\":0}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(MaintenanceType.REWRITE_DATA_FILES, args, below));

    String above =
        "{\""
            + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC
            + "\":11,\""
            + IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC
            + "\":0}";
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(MaintenanceType.REWRITE_DATA_FILES, args, above));
  }
}
