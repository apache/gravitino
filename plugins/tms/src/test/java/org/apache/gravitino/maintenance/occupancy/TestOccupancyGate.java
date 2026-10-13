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

import java.io.IOException;
import java.util.function.Function;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobHandle;
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

    long actionId = actions.insertInFlight(table.id(), policyId);
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
    actions.bindJobId(actionId, started.id());

    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.shouldSkip(table.id(), policyId, 3_600_000L, System.currentTimeMillis()));
    Assertions.assertNull(actions.getByJobId(started.id()).finishedAt());

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
    Assertions.assertNull(actions.getByJobId(started.id()).finishedAt());
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

    long actionId = actions.insertInFlight(table.id(), policyId);
    actions.bindJobId(actionId, jobId);
    actions.markFinished(jobId, 200L, finishedAt);
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.withinMinInterval(table.id(), policyId, 3_600_000L, finishedAt + 1_000L));
  }

  @TestTemplate
  void testInFlightScansAllOpenRowsWhenStaleTerminalExists() throws IOException {
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
    long staleActionId = actions.insertInFlight(table.id(), policyId);
    actions.bindJobId(staleActionId, succeeded.id());
    Assertions.assertNull(actions.getByJobId(succeeded.id()).finishedAt());
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));

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
    long activeActionId = actions.insertInFlight(table.id(), policyId);
    actions.bindJobId(activeActionId, started.id());

    Assertions.assertEquals(2, actions.listInFlight(table.id(), policyId).size());
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.shouldSkip(table.id(), policyId, 3_600_000L, finishedAt + 3_600_001L));
  }

  @TestTemplate
  void testExpiredUnboundClaimIsNotInFlight() throws IOException {
    String metalakeName = "tms_occ_ml4";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long claimTimeoutMs = 60_000L;
    OccupancyGate gate = new OccupancyGate(actions, claimTimeoutMs);
    long policyId = 66L;
    long claimedAt = 1_700_000_400_000L;

    Assertions.assertTrue(
        actions.tryAcquireInFlight(table.id(), policyId, claimedAt, claimTimeoutMs).isPresent());
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId, claimedAt + 1_000L));
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId, claimedAt + claimTimeoutMs));
  }
}
