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
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableMaintenancePolicies {

  @Test
  void testFourBuiltInTaskTypes() {
    Assertions.assertEquals(4, TableMaintenanceTaskType.values().length);
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_COMPACTION, TableMaintenanceTaskType.COMPACTION.builtInType());
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
        TableMaintenanceTaskType.MANIFEST_REWRITE.builtInType());
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
        TableMaintenanceTaskType.SNAPSHOT_EXPIRY.builtInType());
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL,
        TableMaintenanceTaskType.ORPHAN_CLEANUP.builtInType());
    Assertions.assertTrue(TableMaintenanceTaskType.COMPACTION.onCommitAllowed());
    Assertions.assertFalse(TableMaintenanceTaskType.ORPHAN_CLEANUP.onCommitAllowed());
  }

  @Test
  void testMaintenanceFieldsIndependentOfTaskContent() {
    MaintenanceTrigger schedule = new MaintenanceTrigger(true, "0 0 2 * * ?");
    Long minIntervalMs = 3_600_000L;
    Map<String, String> jobOptions = ImmutableMap.of("spark.executor.memory", "4g");
    Map<String, String> updateOptions = ImmutableMap.of("metalake", "ml");

    PolicyContent content =
        PolicyContents.icebergDataCompaction(
            1L,
            1L,
            1L,
            1L,
            10L,
            "binpack",
            "",
            ImmutableMap.of(),
            schedule,
            minIntervalMs,
            jobOptions,
            updateOptions);
    Assertions.assertDoesNotThrow(content::validate);
    Assertions.assertDoesNotThrow(
        () -> TableMaintenancePolicies.validate(Policy.BuiltInType.ICEBERG_COMPACTION, content));
  }

  @Test
  void testValidateMaintenanceFieldsExternally() {
    Assertions.assertDoesNotThrow(
        () ->
            TableMaintenancePolicies.validate(
                Policy.BuiltInType.ICEBERG_COMPACTION,
                new MaintenanceTrigger(true, "0 0 2 * * ?"),
                100L,
                ImmutableMap.of("uri", "http://irc"),
                ImmutableMap.of("metalake", "ml")));

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TableMaintenancePolicies.validate(
                    Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL,
                    new MaintenanceTrigger(true, null),
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap()));
    Assertions.assertTrue(ex.getMessage().contains("onCommit"));
  }

  @Test
  void testCronExpressionGrammar() {
    Assertions.assertDoesNotThrow(() -> MaintenanceTrigger.validateCronExpression("0 0 2 * * ?"));
    Assertions.assertDoesNotThrow(
        () -> MaintenanceTrigger.validateCronExpression("0 0 2 ? * MON-FRI"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MaintenanceTrigger.validateCronExpression("0 2 * *"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MaintenanceTrigger.validateCronExpression("a a a a a a"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MaintenanceTrigger.validateCronExpression("0 0 2 * * foo"));
  }

  @Test
  void testMergeOptionsNearerWins() {
    Map<String, String> farther = ImmutableMap.of("a", "1", "b", "2");
    Map<String, String> nearer = ImmutableMap.of("b", "9");
    Map<String, String> merged = TableMaintenancePolicies.mergeOptions(farther, nearer);
    Assertions.assertEquals("1", merged.get("a"));
    Assertions.assertEquals("9", merged.get("b"));
  }
}
