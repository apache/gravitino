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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.policy.AlterPolicyEvent;
import org.apache.gravitino.listener.api.event.policy.CreatePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.DisablePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.EnablePolicyEvent;
import org.apache.gravitino.listener.api.info.PolicyInfo;
import org.apache.gravitino.maintenance.scheduler.PolicyTimeTriggerEventListener.PolicySnapshot;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestPolicyTimeTriggerEventListener {

  private static final String CRON = "0 0 2 * * ?";

  @Test
  void testCreateEnabledMaintenancePolicySchedulesTimeTrigger() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyContent content = compaction(CRON);
    PolicyTimeTriggerEventListener listener =
        listener(schedule, new PolicySnapshot(42L, true, content));

    listener.onPostEvent(
        new CreatePolicyEvent(
            "user",
            policyId(),
            new PolicyInfo("p", "ICEBERG_COMPACTION", null, true, content, null)));

    Assertions.assertEquals(List.of("sync 42 true " + CRON), schedule.operations);
  }

  @Test
  void testCreateDisabledPolicyRemovesExpandRow() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyContent content = compaction(CRON);
    PolicyTimeTriggerEventListener listener =
        listener(schedule, new PolicySnapshot(7L, false, content));

    listener.onPostEvent(
        new CreatePolicyEvent(
            "user",
            policyId(),
            new PolicyInfo("p", "ICEBERG_COMPACTION", null, false, content, null)));

    Assertions.assertEquals(List.of("sync 7 false " + CRON), schedule.operations);
  }

  @Test
  void testAlterMovesTheExpandRowToTheNewCron() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyContent content = compaction("0 15 3 * * ?");
    PolicyTimeTriggerEventListener listener =
        listener(schedule, new PolicySnapshot(9L, true, content));

    listener.onPostEvent(
        new AlterPolicyEvent(
            "user",
            policyId(),
            new PolicyChange[0],
            new PolicyInfo("p", "ICEBERG_COMPACTION", null, true, content, null)));

    Assertions.assertEquals(List.of("sync 9 true 0 15 3 * * ?"), schedule.operations);
  }

  @Test
  void testEnableAndDisableFollowThePersistedPolicy() {
    RecordingSchedule schedule = new RecordingSchedule();
    AtomicReference<PolicySnapshot> stored =
        new AtomicReference<>(new PolicySnapshot(3L, false, compaction(CRON)));
    PolicyTimeTriggerEventListener listener =
        new PolicyTimeTriggerEventListener(
            () -> schedule, (metalake, name) -> Optional.of(stored.get()));

    listener.onPostEvent(new EnablePolicyEvent("user", policyId()));
    stored.set(new PolicySnapshot(3L, true, compaction(CRON)));
    listener.onPostEvent(new EnablePolicyEvent("user", policyId()));
    stored.set(new PolicySnapshot(3L, false, compaction(CRON)));
    listener.onPostEvent(new DisablePolicyEvent("user", policyId()));

    Assertions.assertEquals(
        List.of("sync 3 false " + CRON, "sync 3 true " + CRON, "sync 3 false " + CRON),
        schedule.operations);
  }

  @Test
  void testNonMaintenancePolicyIsIgnored() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyContent custom =
        PolicyContents.custom(
            Collections.emptyMap(),
            Collections.singleton(MetadataObject.Type.TABLE),
            Collections.emptyMap());
    PolicyTimeTriggerEventListener listener =
        listener(schedule, new PolicySnapshot(1L, true, custom));

    listener.onPostEvent(
        new CreatePolicyEvent(
            "user", policyId(), new PolicyInfo("p", "custom", null, true, custom, null)));

    Assertions.assertTrue(schedule.operations.isEmpty());
  }

  @Test
  void testDeleteRemovesTheExpandRowOnlyAfterSuccess() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyTimeTriggerEventListener listener =
        listener(schedule, new PolicySnapshot(11L, true, compaction(CRON)));
    NameIdentifier identifier = policyId();

    listener.onPreEvent(new DeletePolicyPreEvent("user", identifier));
    listener.onPostEvent(
        new DeletePolicyFailureEvent("user", identifier, new RuntimeException("boom")));
    Assertions.assertTrue(schedule.operations.isEmpty());

    listener.onPreEvent(new DeletePolicyPreEvent("user", identifier));
    listener.onPostEvent(new DeletePolicyEvent("user", identifier, true));
    Assertions.assertEquals(List.of("remove 11"), schedule.operations);
  }

  @Test
  void testDeleteOfMissingPolicyDoesNotRemove() {
    RecordingSchedule schedule = new RecordingSchedule();
    PolicyTimeTriggerEventListener listener =
        listener(schedule, new PolicySnapshot(11L, true, compaction(CRON)));

    listener.onPreEvent(new DeletePolicyPreEvent("user", policyId()));
    listener.onPostEvent(new DeletePolicyEvent("user", policyId(), false));

    Assertions.assertTrue(schedule.operations.isEmpty());
  }

  @Test
  void testStoppedSchedulerSkipsEvents() {
    PolicyTimeTriggerEventListener listener =
        new PolicyTimeTriggerEventListener(
            () -> null,
            (metalake, name) -> Optional.of(new PolicySnapshot(1L, true, compaction(CRON))));

    Assertions.assertDoesNotThrow(
        () ->
            listener.onPostEvent(
                new CreatePolicyEvent(
                    "user",
                    policyId(),
                    new PolicyInfo(
                        "p", "ICEBERG_COMPACTION", null, true, compaction(CRON), null))));
  }

  @Test
  void testQuartzCronHasAFutureExecution() {
    Instant next = PolicyExpandScheduler.nextExecution(CRON);
    Assertions.assertTrue(next.isAfter(Instant.now()));
  }

  @Test
  void testInvalidCronIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PolicyExpandScheduler.nextExecution("not-a-cron"));
  }

  private static PolicyTimeTriggerEventListener listener(
      RecordingSchedule schedule, PolicySnapshot snapshot) {
    return new PolicyTimeTriggerEventListener(
        () -> schedule, (metalake, name) -> Optional.of(snapshot));
  }

  private static NameIdentifier policyId() {
    return NameIdentifierUtil.ofPolicy("ml", "nightly");
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
