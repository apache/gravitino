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
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableMaintenancePolicyContent {

  @Test
  void testCompactionPolicyAcceptsScheduleAndJobOptions() {
    TableMaintenanceSchedule schedule = new TableMaintenanceSchedule(true, "0 2 * * *");
    TableMaintenancePolicyFields fields =
        new TableMaintenancePolicyFields(
            schedule,
            3_600_000L,
            ImmutableMap.of("uri", "http://irc:9001/iceberg", "type", "rest"));
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, 1L, 100L, 50L, ImmutableMap.of(), fields);

    Assertions.assertDoesNotThrow(content::validate);
    TableMaintenancePolicyContentSupport.validateMaintenanceFields(
        Policy.BuiltInType.ICEBERG_COMPACTION, content);
    Assertions.assertEquals(fields, content.maintenanceFields());
  }

  @Test
  void testRejectsCredentialShapedJobOptions() {
    TableMaintenancePolicyFields fields =
        new TableMaintenancePolicyFields(
            new TableMaintenanceSchedule(false, "0 3 * * *"),
            null,
            ImmutableMap.of("oauthCredential", "id:secret"));

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> fields.validate(TableMaintenanceTaskType.COMPACTION));
    Assertions.assertTrue(ex.getMessage().contains("jobOptions"));
  }

  @Test
  void testRejectsOnCommitForOrphanCleanup() {
    TableMaintenanceSchedule schedule = new TableMaintenanceSchedule(true, null);

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schedule.validate(TableMaintenanceTaskType.ORPHAN_CLEANUP));
    Assertions.assertTrue(ex.getMessage().contains("onCommit"));
  }

  @Test
  void testMinIntervalMsResolutionOrder() {
    long fromPolicy =
        MinIntervalMsResolver.resolve(
            TableMaintenanceTaskType.COMPACTION, 120_000L, ImmutableMap.of());
    Assertions.assertEquals(120_000L, fromPolicy);

    long fromConf =
        MinIntervalMsResolver.resolve(
            TableMaintenanceTaskType.COMPACTION,
            null,
            ImmutableMap.of(
                MinIntervalMsResolver.configKey(TableMaintenanceTaskType.COMPACTION), "900000"));
    Assertions.assertEquals(900_000L, fromConf);

    long fromDefault =
        MinIntervalMsResolver.resolve(TableMaintenanceTaskType.COMPACTION, null, ImmutableMap.of());
    Assertions.assertEquals(
        TableMaintenanceTaskType.COMPACTION.defaultMinIntervalMs(), fromDefault);
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
  void testSensitiveRewriteOptionsRejected() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(1000L, 1L, ImmutableMap.of("password", "secret"));

    IllegalArgumentException ex =
        Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    Assertions.assertTrue(ex.getMessage().contains("rewriteOptions"));
  }
}
