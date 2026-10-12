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

package org.apache.gravitino.maintenance.policy;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableMaintenancePolicyContent {

  @Test
  void testFourBuiltInTaskTypes() {
    Assertions.assertEquals(4, TableMaintenanceTaskType.values().length);
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_COMPACTION, TableMaintenanceTaskType.COMPACTION.builtInType());
    Assertions.assertNull(TableMaintenanceTaskType.MANIFEST_REWRITE.builtInType());
    Assertions.assertNull(TableMaintenanceTaskType.SNAPSHOT_EXPIRY.builtInType());
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL,
        TableMaintenanceTaskType.ORPHAN_CLEANUP.builtInType());
    Assertions.assertTrue(TableMaintenanceTaskType.COMPACTION.onCommitAllowed());
    Assertions.assertFalse(TableMaintenanceTaskType.ORPHAN_CLEANUP.onCommitAllowed());
  }

  @Test
  void testFieldsValidateForCompactionAndOrphan() {
    TableMaintenancePolicyFields fields =
        new TableMaintenancePolicyFields(
            new TableMaintenanceSchedule(true, "0 2 * * *"),
            3_600_000L,
            ImmutableMap.of("uri", "http://irc:9001/iceberg"));

    Assertions.assertDoesNotThrow(() -> fields.validate(TableMaintenanceTaskType.COMPACTION));

    IllegalArgumentException orphanEx =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> fields.validate(TableMaintenanceTaskType.ORPHAN_CLEANUP));
    Assertions.assertTrue(orphanEx.getMessage().contains("onCommit"));
  }

  @Test
  void testRejectsEmptySchedule() {
    TableMaintenanceSchedule schedule = new TableMaintenanceSchedule(false, null);

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schedule.validate(TableMaintenanceTaskType.COMPACTION));
    Assertions.assertTrue(ex.getMessage().contains("schedule"));
  }

  @Test
  void testCompactionAllowsOnCommit() {
    TableMaintenanceSchedule schedule = new TableMaintenanceSchedule(true, null);
    Assertions.assertDoesNotThrow(() -> schedule.validate(TableMaintenanceTaskType.COMPACTION));
  }

  @Test
  void testOrphanRejectsOnCommit() {
    TableMaintenanceSchedule schedule = new TableMaintenanceSchedule(true, null);
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schedule.validate(TableMaintenanceTaskType.ORPHAN_CLEANUP));
    Assertions.assertTrue(ex.getMessage().contains("onCommit"));
  }

  @Test
  void testMergeJobOptionsNearestWins() {
    Map<String, String> farther = new HashMap<>();
    farther.put("spark.executor.memory", "4g");
    farther.put("uri", "http://old");
    Map<String, String> nearer = ImmutableMap.of("uri", "http://new");

    Map<String, String> merged = TableMaintenancePolicyFields.mergeJobOptions(farther, nearer);
    Assertions.assertEquals("4g", merged.get("spark.executor.memory"));
    Assertions.assertEquals("http://new", merged.get("uri"));
  }

  @Test
  void testTaskTypeForBuiltInPolicies() {
    Assertions.assertEquals(
        TableMaintenanceTaskType.COMPACTION,
        TableMaintenancePolicyContentSupport.taskTypeFor(Policy.BuiltInType.ICEBERG_COMPACTION));
    Assertions.assertEquals(
        TableMaintenanceTaskType.ORPHAN_CLEANUP,
        TableMaintenancePolicyContentSupport.taskTypeFor(
            Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL));
    Assertions.assertNull(
        TableMaintenancePolicyContentSupport.taskTypeFor(Policy.BuiltInType.CUSTOM));
    Assertions.assertNull(TableMaintenancePolicyContentSupport.maintenanceFields(null));
  }
}
