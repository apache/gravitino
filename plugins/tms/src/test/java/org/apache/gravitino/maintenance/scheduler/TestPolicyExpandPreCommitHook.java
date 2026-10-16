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
package org.apache.gravitino.maintenance.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestPolicyExpandPreCommitHook {

  private static final String CRON = "0 0 2 * * ?";

  @Test
  void testEnabledMaintenancePolicyIsScheduled() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyExpandPreCommitHook hook = new PolicyExpandPreCommitHook(schedule);

    hook.afterWrite(policy(42L, true, compaction(CRON)));

    Assertions.assertEquals(List.of("sync 42 true " + CRON), schedule.operations);
  }

  @Test
  void testDisabledMaintenancePolicyRemovesTheRow() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyExpandPreCommitHook hook = new PolicyExpandPreCommitHook(schedule);

    hook.afterWrite(policy(7L, false, compaction(CRON)));

    Assertions.assertEquals(List.of("sync 7 false " + CRON), schedule.operations);
  }

  @Test
  void testNonMaintenancePolicyRemovesAnyStrayRow() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyExpandPreCommitHook hook = new PolicyExpandPreCommitHook(schedule);
    PolicyContent custom = PolicyContents.custom(Map.of(), Set.of(MetadataObject.Type.TABLE), null);

    hook.afterWrite(policy(1L, true, custom, Policy.BuiltInType.CUSTOM));

    Assertions.assertEquals(List.of("remove 1"), schedule.operations);
  }

  @Test
  void testDeleteRemovesByPolicyId() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyExpandPreCommitHook hook = new PolicyExpandPreCommitHook(schedule);

    hook.beforeDelete(11L);

    Assertions.assertEquals(List.of("remove 11"), schedule.operations);
  }

  private static PolicyEntity policy(long id, boolean enabled, PolicyContent content) {
    return policy(id, enabled, content, Policy.BuiltInType.ICEBERG_COMPACTION);
  }

  private static PolicyEntity policy(
      long id, boolean enabled, PolicyContent content, Policy.BuiltInType type) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("nightly")
        .withNamespace(NamespaceUtil.ofPolicy("ml"))
        .withPolicyType(type)
        .withContent(content)
        .withEnabled(enabled)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  private static PolicyContent compaction(String timeTrigger) {
    return PolicyContents.icebergDataCompaction(
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
        IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
        IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
        IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
        IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
        IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
        IcebergDataCompactionContent.DEFAULT_REWRITE_OPTIONS,
        timeTrigger,
        null,
        null,
        null,
        null);
  }

  private static final class RecordingSchedule implements PolicyExpandSchedule {
    private final List<String> operations = new ArrayList<>();

    @Override
    public void sync(long policyId, boolean enabled, String timeTrigger) {
      operations.add("sync " + policyId + " " + enabled + " " + timeTrigger);
    }

    @Override
    public void remove(long policyId) {
      operations.add("remove " + policyId);
    }
  }
}
