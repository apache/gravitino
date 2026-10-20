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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestPolicyExpandPipeline {

  private static final String CRON = "0 0 2 * * ?";

  @Test
  void testMissingPolicyRemovesExpandRow() {
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        pipeline(mockLookupEmpty(1L), mock(PolicyTableEnumerator.class), cronJobs, allowAll());

    Assertions.assertTrue(pipeline.expand(1L).isEmpty());
    verify(cronJobs, never()).enqueueDueNow(any(), anyLong(), any(), anyInt());
  }

  @Test
  void testDisabledPolicyRemovesExpandRow() {
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        pipeline(
            mockLookup(2L, policy(false, CRON, null)),
            mock(PolicyTableEnumerator.class),
            cronJobs,
            allowAll());

    Assertions.assertTrue(pipeline.expand(2L).isEmpty());
    verify(cronJobs, never()).enqueueDueNow(any(), anyLong(), any(), anyInt());
  }

  @Test
  void testBlankTimeTriggerRemovesExpandRow() {
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        pipeline(
            mockLookup(3L, policy(true, "  ", null)),
            mock(PolicyTableEnumerator.class),
            cronJobs,
            allowAll());

    Assertions.assertTrue(pipeline.expand(3L).isEmpty());
    verify(cronJobs, never()).enqueueDueNow(any(), anyLong(), any(), anyInt());
  }

  @Test
  void testInvalidTimeTriggerRemovesExpandRowWithoutEnqueue() {
    PolicyTableEnumerator tables = mock(PolicyTableEnumerator.class);
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        pipeline(mockLookup(4L, policy(true, "not-a-cron", null)), tables, cronJobs, allowAll());

    Assertions.assertTrue(pipeline.expand(4L).isEmpty());
    verify(tables, never()).listTableIds(any(), anyInt(), any(), anyLong());
    verify(cronJobs, never()).enqueueDueNow(any(), anyLong(), any(), anyInt());
  }

  @Test
  void testExpandEnqueuesCronJobsAndReturnsNextDue() {
    PolicyEntity policy = policy(true, CRON, null);
    ExpandOccupancyGate gate = allowAll();
    List<ExpandTableCandidate> candidates =
        List.of(
            new ExpandTableCandidate(10L, MetadataObject.Type.TABLE),
            new ExpandTableCandidate(11L, MetadataObject.Type.TABLE));
    PolicyTableEnumerator tables = mock(PolicyTableEnumerator.class);
    when(tables.listTableIds(
            eq(policy),
            eq(2),
            eq(gate),
            eq(MaintenanceMinIntervalConfigs.DEFAULT_HOURLY_MIN_INTERVAL_MS)))
        .thenReturn(candidates);
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    when(cronJobs.enqueueDueNow(
            eq(Policy.BuiltInType.ICEBERG_COMPACTION), eq(42L), eq(candidates), eq(2)))
        .thenReturn(2);
    PolicyExpandPipeline expandPipeline =
        pipeline(mockLookup(42L, policy), tables, cronJobs, gate, 2);

    Optional<Instant> next = expandPipeline.expand(42L);
    Assertions.assertTrue(next.isPresent());
    Assertions.assertTrue(next.get().isAfter(Instant.now().minusSeconds(1)));
    verify(cronJobs).enqueueDueNow(Policy.BuiltInType.ICEBERG_COMPACTION, 42L, candidates, 2);
  }

  @Test
  void testPassesResolvedMinIntervalToEnumerator() {
    PolicyEntity policy = policy(true, CRON, 60_000L);
    ExpandOccupancyGate gate = allowAll();
    List<ExpandTableCandidate> candidates =
        List.of(new ExpandTableCandidate(11L, MetadataObject.Type.TABLE));
    PolicyTableEnumerator tables = mock(PolicyTableEnumerator.class);
    when(tables.listTableIds(eq(policy), eq(10), eq(gate), eq(60_000L))).thenReturn(candidates);
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    when(cronJobs.enqueueDueNow(
            eq(Policy.BuiltInType.ICEBERG_COMPACTION), eq(42L), eq(candidates), eq(10)))
        .thenReturn(1);

    PolicyExpandPipeline expandPipeline =
        pipeline(mockLookup(42L, policy), tables, cronJobs, gate, 10);
    Assertions.assertTrue(expandPipeline.expand(42L).isPresent());
    verify(tables).listTableIds(eq(policy), eq(10), eq(gate), eq(60_000L));
    verify(cronJobs).enqueueDueNow(Policy.BuiltInType.ICEBERG_COMPACTION, 42L, candidates, 10);
  }

  @Test
  void testUsesServerDefaultMinIntervalWhenPolicyOmits() {
    PolicyEntity policy = policy(true, CRON, null);
    ExpandOccupancyGate gate = allowAll();
    List<ExpandTableCandidate> candidates =
        List.of(new ExpandTableCandidate(10L, MetadataObject.Type.TABLE));
    PolicyTableEnumerator tables = mock(PolicyTableEnumerator.class);
    when(tables.listTableIds(
            eq(policy),
            eq(10),
            eq(gate),
            eq(MaintenanceMinIntervalConfigs.DEFAULT_HOURLY_MIN_INTERVAL_MS)))
        .thenReturn(candidates);
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    when(cronJobs.enqueueDueNow(
            eq(Policy.BuiltInType.ICEBERG_COMPACTION), eq(42L), eq(candidates), eq(10)))
        .thenReturn(1);

    PolicyExpandPipeline expandPipeline =
        pipeline(mockLookup(42L, policy), tables, cronJobs, gate, 10);
    Assertions.assertTrue(expandPipeline.expand(42L).isPresent());
    verify(tables)
        .listTableIds(
            eq(policy),
            eq(10),
            eq(gate),
            eq(MaintenanceMinIntervalConfigs.DEFAULT_HOURLY_MIN_INTERVAL_MS));
  }

  private static PolicyExpandPipeline pipeline(
      PolicyLookup lookup,
      PolicyTableEnumerator tables,
      CronJobScheduler cronJobs,
      ExpandOccupancyGate gate) {
    return pipeline(lookup, tables, cronJobs, gate, 100);
  }

  private static PolicyExpandPipeline pipeline(
      PolicyLookup lookup,
      PolicyTableEnumerator tables,
      CronJobScheduler cronJobs,
      ExpandOccupancyGate gate,
      int batchSize) {
    return new PolicyExpandPipeline(
        lookup, tables, cronJobs, gate, new Config(false) {}, batchSize);
  }

  private static ExpandOccupancyGate allowAll() {
    return (tableId, policyId, minIntervalMs, nowMs) -> false;
  }

  private static PolicyLookup mockLookupEmpty(long policyId) {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(policyId)).thenReturn(Optional.empty());
    return lookup;
  }

  private static PolicyLookup mockLookup(long policyId, PolicyEntity policy) {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(policyId)).thenReturn(Optional.of(policy));
    return lookup;
  }

  private static PolicyEntity policy(boolean enabled, String timeTrigger, Long minIntervalMs) {
    Namespace ns = NamespaceUtil.ofPolicy("metalake");
    return PolicyEntity.builder()
        .withId(42L)
        .withName("compaction")
        .withNamespace(ns)
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
        .withEnabled(enabled)
        .withContent(
            PolicyContents.icebergDataCompaction(
                IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
                IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
                IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
                IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                null,
                timeTrigger,
                null,
                minIntervalMs,
                null,
                null))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
