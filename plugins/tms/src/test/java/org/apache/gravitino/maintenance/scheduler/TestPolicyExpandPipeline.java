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
import org.apache.gravitino.Namespace;
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
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.empty());
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        new PolicyExpandPipeline(lookup, mock(PolicyTableEnumerator.class), cronJobs, 100);

    Assertions.assertTrue(pipeline.expand(1L).isEmpty());
    verify(cronJobs, never()).enqueueDueNow(anyLong(), any(), anyInt());
  }

  @Test
  void testDisabledPolicyRemovesExpandRow() {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(2L)).thenReturn(Optional.of(policy(false, CRON)));
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        new PolicyExpandPipeline(lookup, mock(PolicyTableEnumerator.class), cronJobs, 100);

    Assertions.assertTrue(pipeline.expand(2L).isEmpty());
    verify(cronJobs, never()).enqueueDueNow(anyLong(), any(), anyInt());
  }

  @Test
  void testBlankTimeTriggerRemovesExpandRow() {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(3L)).thenReturn(Optional.of(policy(true, "  ")));
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline =
        new PolicyExpandPipeline(lookup, mock(PolicyTableEnumerator.class), cronJobs, 100);

    Assertions.assertTrue(pipeline.expand(3L).isEmpty());
    verify(cronJobs, never()).enqueueDueNow(anyLong(), any(), anyInt());
  }

  @Test
  void testInvalidTimeTriggerRemovesExpandRowWithoutEnqueue() {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(4L)).thenReturn(Optional.of(policy(true, "not-a-cron")));
    PolicyTableEnumerator tables = mock(PolicyTableEnumerator.class);
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    PolicyExpandPipeline pipeline = new PolicyExpandPipeline(lookup, tables, cronJobs, 100);

    Assertions.assertTrue(pipeline.expand(4L).isEmpty());
    verify(tables, never()).listTableIds(any(), anyInt());
    verify(cronJobs, never()).enqueueDueNow(anyLong(), any(), anyInt());
  }

  @Test
  void testExpandEnqueuesCronJobsAndReturnsNextDue() {
    PolicyEntity policy = policy(true, CRON);
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(42L)).thenReturn(Optional.of(policy));
    PolicyTableEnumerator tables = mock(PolicyTableEnumerator.class);
    when(tables.listTableIds(eq(policy), eq(2))).thenReturn(List.of(10L, 11L));
    CronJobScheduler cronJobs = mock(CronJobScheduler.class);
    when(cronJobs.enqueueDueNow(eq(42L), eq(List.of(10L, 11L)), eq(2))).thenReturn(2);
    PolicyExpandPipeline pipeline = new PolicyExpandPipeline(lookup, tables, cronJobs, 2);

    Optional<Instant> next = pipeline.expand(42L);
    Assertions.assertTrue(next.isPresent());
    Assertions.assertTrue(next.get().isAfter(Instant.now().minusSeconds(1)));
    verify(cronJobs).enqueueDueNow(42L, List.of(10L, 11L), 2);
  }

  private static PolicyEntity policy(boolean enabled, String timeTrigger) {
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
                null,
                null,
                null))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
