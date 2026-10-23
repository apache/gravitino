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
package org.apache.gravitino.maintenance.occupancy;

import static org.apache.gravitino.maintenance.storage.TableMaintenanceActionTestUtils.expireClaim;

import java.io.IOException;
import java.util.function.Function;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.JobMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestOccupancyGate extends TestJDBCBackend {

  @TestTemplate
  void testInFlightReadsJobMetaServiceWithoutBackfillingFinishedAt() throws IOException {
    String metalakeName = "tms_occ_ml";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");
    JobTemplateEntity template =
        createAndInsertShellJobTemplateEntity("occ-template", "comment", metalakeName);

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(actions);
    long policyId = 22L;

    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));

    OccupancyClaim claim =
        actions.insertInFlight(
            table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));

    JobEntity started =
        JobEntity.builder()
            .withId(7001L)
            .withJobExecutionId("exec-7001")
            .withNamespace(NamespaceUtil.ofJob(metalakeName))
            .withJobTemplateName(template.name())
            .withStatus(JobHandle.Status.STARTED)
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(System.currentTimeMillis())
            .withFinishedAt(0L)
            .build();
    backend.insert(started, false);
    actions.bindJobId(claim, started.id());

    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.shouldSkip(table.id(), policyId, 3_600_000L, System.currentTimeMillis()));
    Assertions.assertEquals(0L, actions.getByJobId(started.id()).finishedAt());

    long finishedAt = 1_700_000_000_000L;
    JobMetaService.getInstance()
        .updateJob(
            NameIdentifierUtil.ofJob(metalakeName, started.name()),
            (Function<JobEntity, JobEntity>)
                current ->
                    JobEntity.builder()
                        .withId(current.id())
                        .withJobExecutionId(current.jobExecutionId())
                        .withNamespace(current.namespace())
                        .withJobTemplateName(current.jobTemplateName())
                        .withStatus(JobHandle.Status.SUCCEEDED)
                        .withAuditInfo(current.auditInfo())
                        .withStartedAt(current.startedAt())
                        .withFinishedAt(finishedAt)
                        .build());

    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertEquals(0L, actions.getByJobId(started.id()).finishedAt());
    Assertions.assertTrue(
        gate.withinMinInterval(table.id(), policyId, 3_600_000L, finishedAt + 1_000L));
    Assertions.assertFalse(
        gate.withinMinInterval(table.id(), policyId, 3_600_000L, finishedAt + 3_600_001L));
  }

  @TestTemplate
  void testMinIntervalUsesMarkedFinishedAt() throws IOException {
    String metalakeName = "tms_occ_ml2";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(actions);
    long policyId = 44L;
    long jobId = 7002L;
    long finishedAt = 1_700_000_100_000L;

    OccupancyClaim claim =
        actions.insertInFlight(
            table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    actions.bindJobId(claim, jobId);
    actions.markFinished(jobId, 200L, finishedAt);
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.withinMinInterval(table.id(), policyId, 3_600_000L, finishedAt + 1_000L));
  }

  @TestTemplate
  void testSoftSlotBlocksNewAcquireUntilMarkFinished() throws IOException {
    String metalakeName = "tms_occ_ml3";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");
    JobTemplateEntity template =
        createAndInsertShellJobTemplateEntity("occ-template-3", "comment", metalakeName);

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(actions);
    long policyId = 55L;
    long finishedAt = 1_700_000_200_000L;

    JobEntity succeeded =
        JobEntity.builder()
            .withId(7003L)
            .withJobExecutionId("exec-7003")
            .withNamespace(NamespaceUtil.ofJob(metalakeName))
            .withJobTemplateName(template.name())
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(finishedAt - 1_000L)
            .withFinishedAt(finishedAt)
            .build();
    backend.insert(succeeded, false);
    OccupancyClaim stale =
        actions.insertInFlight(
            table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    actions.bindJobId(stale, succeeded.id());
    Assertions.assertEquals(0L, actions.getByJobId(succeeded.id()).finishedAt());
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            actions.insertInFlight(
                table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS));

    actions.markFinished(succeeded.id(), 200L, finishedAt);
    Assertions.assertEquals(0, actions.listInFlight(table.id(), policyId).size());

    JobEntity started =
        JobEntity.builder()
            .withId(7004L)
            .withJobExecutionId("exec-7004")
            .withNamespace(NamespaceUtil.ofJob(metalakeName))
            .withJobTemplateName(template.name())
            .withStatus(JobHandle.Status.STARTED)
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(System.currentTimeMillis())
            .withFinishedAt(0L)
            .build();
    backend.insert(started, false);
    OccupancyClaim active =
        actions.insertInFlight(
            table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    actions.bindJobId(active, started.id());

    Assertions.assertEquals(1, actions.listInFlight(table.id(), policyId).size());
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.shouldSkip(table.id(), policyId, 3_600_000L, finishedAt + 3_600_001L));
  }

  @TestTemplate
  void testExpiredUnboundClaimIsNotInFlightOnDbClock() throws Exception {
    String metalakeName = "tms_occ_ml4";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long claimTimeoutMs = TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS;
    OccupancyGate gate = new OccupancyGate(actions, claimTimeoutMs);
    long policyId = 66L;

    OccupancyClaim claim = actions.insertInFlight(table.id(), policyId, claimTimeoutMs);
    Assertions.assertTrue(actions.hasLiveUnboundClaim(table.id(), policyId, claimTimeoutMs));
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));

    expireClaim(claim, claimTimeoutMs);

    // Unbound TTL is evaluated in the DAO against the database clock, so OccupancyGate agrees with
    // acquire/reclaim even when the JVM clock would disagree (multi-node skew).
    Assertions.assertFalse(actions.hasLiveUnboundClaim(table.id(), policyId, claimTimeoutMs));
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        actions.tryAcquireInFlight(table.id(), policyId, claimTimeoutMs).isPresent());
    Assertions.assertEquals(claim.id(), actions.getById(claim.id()).id());
  }
}
