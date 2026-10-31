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

package org.apache.gravitino.maintenance.storage.service;

import static org.apache.gravitino.maintenance.storage.TableMaintenanceActionTestUtils.expireClaim;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.occupancy.OccupancyClaim;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceActionMetaService extends TestJDBCBackend {

  @TestTemplate
  void testInsertBindSampleFinishAndDeleteKeepsMetrics() {
    TableMaintenanceActionMetaService actionService =
        TableMaintenanceActionMetaService.getInstance();
    TableSnapshotMetricsMetaService metricsService = TableSnapshotMetricsMetaService.getInstance();

    long before = System.currentTimeMillis();
    OccupancyClaim claim =
        actionService
            .tryAcquireInFlight(
                10L,
                20L,
                TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10).claimTimeoutMs())
            .orElseThrow();
    long after = System.currentTimeMillis();
    Assertions.assertNull(actionService.getById(claim.actionId()).jobId());
    Assertions.assertEquals(0L, actionService.getById(claim.actionId()).finishedAt());
    // DB clock may skew slightly from the JVM; allow a small window.
    Assertions.assertTrue(claim.createdAt() >= before - 5_000L);
    Assertions.assertTrue(claim.createdAt() <= after + 5_000L);
    Assertions.assertEquals(claim.createdAt(), actionService.getById(claim.actionId()).createdAt());

    long jobId = 999L;
    long tableId = 10L;
    long policyId = 20L;
    actionService.bindJobId(claim, jobId);
    Assertions.assertEquals(jobId, actionService.getByJobIdAndTableId(jobId, tableId).jobId());
    Assertions.assertEquals(0L, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());

    long beforeMetricsId = metricsService.insertMetrics(tableId, policyId, 100L, "{\"mse\":1}");
    long actionId = claim.actionId();
    actionService.recordBeforeMetrics(actionId, beforeMetricsId);
    Assertions.assertEquals(
        beforeMetricsId, actionService.getByJobIdAndTableId(jobId, tableId).beforeMetricsId());
    actionService.recordBeforeMetrics(actionId, beforeMetricsId);
    Assertions.assertEquals(
        beforeMetricsId, actionService.getByJobIdAndTableId(jobId, tableId).beforeMetricsId());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> actionService.recordBeforeMetrics(actionId, beforeMetricsId + 1));

    long afterMetricsId = metricsService.insertMetrics(tableId, policyId, 200L, "{\"mse\":2}");
    actionService.recordAfterMetrics(actionId, afterMetricsId);
    actionService.markFinished(actionId, System.currentTimeMillis());
    // Idempotent finish when already closed.
    actionService.markFinished(actionId, System.currentTimeMillis() + 1);

    actionService.deleteByJobId(jobId);

    Assertions.assertTrue(actionService.listByJobId(jobId).isEmpty());
    Assertions.assertEquals(
        "{\"mse\":1}", metricsService.getMetrics(beforeMetricsId).metricsValue());
    Assertions.assertEquals(
        "{\"mse\":2}", metricsService.getMetrics(afterMetricsId).metricsValue());
  }

  @TestTemplate
  void testOneJobBindsMultipleTablesAndMarkFinishedClosesPerRow() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long jobId = 777L;
    OccupancyClaim claimA =
        actions.tryAcquireInFlight(101L, 201L, config.claimTimeoutMs()).orElseThrow();
    OccupancyClaim claimB =
        actions.tryAcquireInFlight(102L, 202L, config.claimTimeoutMs()).orElseThrow();
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);

    Assertions.assertEquals(2, actions.listByJobId(jobId).size());

    actions.recordBeforeMetrics(claimA.actionId(), 11L);
    actions.recordBeforeMetrics(claimB.actionId(), 22L);

    long finishedAtA = System.currentTimeMillis();
    actions.markFinished(claimA.actionId(), finishedAtA);
    Assertions.assertEquals(finishedAtA, actions.getByJobIdAndTableId(jobId, 101L).finishedAt());
    Assertions.assertEquals(0L, actions.getByJobIdAndTableId(jobId, 102L).finishedAt());

    long finishedAtB = finishedAtA + 10;
    actions.recordAfterMetrics(claimB.actionId(), 33L);
    actions.markFinished(claimB.actionId(), finishedAtB);
    Assertions.assertEquals(finishedAtB, actions.getByJobIdAndTableId(jobId, 102L).finishedAt());
    Assertions.assertEquals(33L, actions.getByJobIdAndTableId(jobId, 102L).afterMetricsId());
    // Idempotent finish for already closed A.
    actions.markFinished(claimA.actionId(), finishedAtA + 1);
  }

  @TestTemplate
  void testBindJobIdIdempotentWhenAlreadyBoundToSameJob() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 31L;
    long policyId = 41L;
    long jobId = 5100L;
    OccupancyClaim claim =
        actions.tryAcquireInFlight(tableId, policyId, config.claimTimeoutMs()).orElseThrow();
    actions.bindJobId(claim, jobId);
    // Spark lazy-bound first; submit-side bind must not fail.
    actions.bindJobId(claim, jobId);
    Assertions.assertEquals(jobId, actions.getByJobIdAndTableId(jobId, tableId).jobId());
  }

  @TestTemplate
  void testBindJobIdsIdempotentWhenAlreadyBoundToSameJob() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 37L;
    long policyA = 48L;
    long policyB = 49L;
    long jobId = 5700L;
    OccupancyClaim claimA =
        actions.tryAcquireInFlight(tableId, policyA, config.claimTimeoutMs()).orElseThrow();
    OccupancyClaim claimB =
        actions.tryAcquireInFlight(tableId, policyB, config.claimTimeoutMs()).orElseThrow();
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);
    // Spark may lazy-bind before commit-chain submit-side bindJobIds.
    actions.bindJobIds(List.of(claimA, claimB), jobId);
    Assertions.assertEquals(jobId, actions.getById(claimA.actionId()).jobId());
    Assertions.assertEquals(jobId, actions.getById(claimB.actionId()).jobId());
  }

  @TestTemplate
  void testBindJobIdFailsWhenBoundToDifferentJob() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    OccupancyClaim claim =
        actions.tryAcquireInFlight(32L, 42L, config.claimTimeoutMs()).orElseThrow();
    actions.bindJobId(claim, 5200L);
    Assertions.assertThrows(IllegalStateException.class, () -> actions.bindJobId(claim, 5201L));
  }

  @TestTemplate
  void testOneJobMultiplePoliciesOnSameTableRecordAndFinishPerPolicy() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 36L;
    long policyA = 46L;
    long policyB = 47L;
    long jobId = 5600L;
    OccupancyClaim claimA =
        actions.tryAcquireInFlight(tableId, policyA, config.claimTimeoutMs()).orElseThrow();
    OccupancyClaim claimB =
        actions.tryAcquireInFlight(tableId, policyB, config.claimTimeoutMs()).orElseThrow();
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);

    actions.recordBeforeMetrics(claimA.actionId(), 11L);
    actions.recordBeforeMetrics(claimB.actionId(), 22L);
    Assertions.assertEquals(
        11L, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyA).beforeMetricsId());
    Assertions.assertEquals(
        22L, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyB).beforeMetricsId());

    long finishedAt = System.currentTimeMillis();
    actions.markFinished(claimA.actionId(), finishedAt);
    Assertions.assertEquals(
        finishedAt, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyA).finishedAt());
    Assertions.assertEquals(
        0L, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyB).finishedAt());
  }

  @TestTemplate
  void testTryAcquireRejectsLiveUnboundAndReclaimsExpired() throws Exception {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 11L;
    long policyId = 21L;
    long timeoutMs = TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    Assertions.assertEquals(0L, actions.getById(first.get().actionId()).finishedAt());
    Assertions.assertTrue(actions.tryAcquireInFlight(tableId, policyId, timeoutMs).isEmpty());

    OccupancyClaim expired = expireClaim(first.get(), timeoutMs);
    // Let the DB clock advance so reclaim cannot reuse the original fencing token.
    Thread.sleep(2L);

    Optional<OccupancyClaim> reclaimed = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertNotEquals(first.get().actionId(), reclaimed.get().actionId());
    Assertions.assertTrue(actions.getById(first.get().actionId()).finishedAt() > 0);
    Assertions.assertTrue(reclaimed.get().createdAt() > expired.createdAt());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().actionId()).finishedAt());

    Assertions.assertThrows(IllegalStateException.class, () -> actions.bindJobId(expired, 1001L));
    actions.bindJobId(reclaimed.get(), 1002L);
    Assertions.assertEquals(1002L, actions.getById(reclaimed.get().actionId()).jobId());
  }

  @TestTemplate
  void testDeleteUnboundClaimFencesAgainstReclaim() throws Exception {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 12L;
    long policyId = 22L;
    long timeoutMs = TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    Assertions.assertTrue(actions.deleteUnboundClaim(first.get()));
    Assertions.assertNull(actions.getById(first.get().actionId()));

    Optional<OccupancyClaim> second = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(second.isPresent());
    OccupancyClaim expired = expireClaim(second.get(), timeoutMs);
    // Let the DB clock advance so reclaim cannot reuse the original fencing token.
    Thread.sleep(2L);
    Optional<OccupancyClaim> reclaimed = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertNotEquals(second.get().actionId(), reclaimed.get().actionId());
    Assertions.assertTrue(actions.getById(second.get().actionId()).finishedAt() > 0);
    Assertions.assertTrue(reclaimed.get().createdAt() > expired.createdAt());
    Assertions.assertNotEquals(second.get().createdAt(), reclaimed.get().createdAt());

    // Stale submitter cleanup must not delete the reclaimed live claim.
    Assertions.assertFalse(actions.deleteUnboundClaim(second.get()));
    Assertions.assertNotNull(actions.getById(reclaimed.get().actionId()));
    Assertions.assertNull(actions.getById(reclaimed.get().actionId()).jobId());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().actionId()).finishedAt());

    Assertions.assertTrue(actions.deleteUnboundClaim(reclaimed.get()));
    Assertions.assertNull(actions.getById(reclaimed.get().actionId()));
  }

  @TestTemplate
  void testDeleteClaimsFencesAgainstReclaim() throws Exception {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 38L;
    long policyId = 50L;
    long timeoutMs = TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    OccupancyClaim expired = expireClaim(first.get(), timeoutMs);
    // Let the DB clock advance so reclaim cannot reuse the original fencing token.
    Thread.sleep(2L);
    Optional<OccupancyClaim> reclaimed = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertNotEquals(first.get().actionId(), reclaimed.get().actionId());
    Assertions.assertTrue(actions.getById(first.get().actionId()).finishedAt() > 0);
    Assertions.assertTrue(reclaimed.get().createdAt() > expired.createdAt());

    // Failed submit cleanup uses deleteClaims; stale created_at must not remove live claim.
    actions.deleteClaims(List.of(first.get()));
    Assertions.assertNotNull(actions.getById(reclaimed.get().actionId()));
    Assertions.assertNull(actions.getById(reclaimed.get().actionId()).jobId());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().actionId()).finishedAt());

    actions.deleteClaims(List.of(reclaimed.get()));
    Assertions.assertNull(actions.getById(reclaimed.get().actionId()));
  }

  @TestTemplate
  void testTryAcquireBatchClaimsAllPoliciesOrNone() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 31L;
    long policyA = 41L;
    long policyB = 42L;
    long timeoutMs = 60_000L;

    Optional<List<OccupancyClaim>> batch =
        actions.tryAcquireInFlightBatch(tableId, List.of(policyA, policyB), timeoutMs);
    Assertions.assertTrue(batch.isPresent());
    Assertions.assertEquals(2, batch.get().size());
    Assertions.assertEquals(0L, actions.getById(batch.get().get(0).actionId()).finishedAt());
    Assertions.assertEquals(0L, actions.getById(batch.get().get(1).actionId()).finishedAt());

    // Partial hold blocks the whole chain.
    Assertions.assertTrue(
        actions.tryAcquireInFlightBatch(tableId, List.of(policyA, policyB), timeoutMs).isEmpty());
    Assertions.assertTrue(actions.tryAcquireInFlight(tableId, policyA, timeoutMs).isEmpty());

    long jobId = 3100L;
    actions.bindJobIds(batch.get(), jobId);
    Assertions.assertEquals(jobId, actions.getById(batch.get().get(0).actionId()).jobId());
    Assertions.assertEquals(jobId, actions.getById(batch.get().get(1).actionId()).jobId());

    long finishedAt = System.currentTimeMillis();
    actions.markFinished(batch.get().get(0).actionId(), finishedAt);
    actions.markFinished(batch.get().get(1).actionId(), finishedAt + 1);
    Assertions.assertTrue(actions.getById(batch.get().get(0).actionId()).finishedAt() > 0);
    Assertions.assertTrue(actions.getById(batch.get().get(1).actionId()).finishedAt() > 0);
  }
}
