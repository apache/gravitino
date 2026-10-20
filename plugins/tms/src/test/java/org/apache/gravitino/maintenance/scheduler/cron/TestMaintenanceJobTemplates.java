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
package org.apache.gravitino.maintenance.scheduler.cron;

import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceJobTemplates}. */
class TestMaintenanceJobTemplates {

  @Test
  void testAllFourTypesMapToSingleTmsTemplate() {
    for (Policy.BuiltInType type :
        new Policy.BuiltInType[] {
          Policy.BuiltInType.ICEBERG_COMPACTION,
          Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
          Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
          Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL
        }) {
      Assertions.assertEquals(
          MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE,
          MaintenanceJobTemplates.templateName(type).orElseThrow());
      Assertions.assertTrue(MaintenanceJobTemplates.maintenanceType(type).isPresent());
    }
  }

  @Test
  void testMaintenanceTypeWireValues() {
    Assertions.assertEquals(
        "rewrite_data_files",
        MaintenanceJobTemplates.maintenanceType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .orElseThrow());
    Assertions.assertEquals(
        "expire_snapshots",
        MaintenanceJobTemplates.maintenanceType(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .orElseThrow());
  }
}
