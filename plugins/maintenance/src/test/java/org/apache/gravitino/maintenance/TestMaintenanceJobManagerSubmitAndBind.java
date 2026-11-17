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
package org.apache.gravitino.maintenance;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.maintenance.model.OccupancyClaim;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Exercises the real {@link MaintenanceJobManager#submitAndBind} path (runJob + bind), including
 * the GROUP-owned metalake regression: submission uses {@code jobSubmitter}, not metalake owner.
 */
class TestMaintenanceJobManagerSubmitAndBind extends TestJDBCBackend {

  private static final String SUBMITTER = "tms";
  private static final long JOB_ID = 88001L;

  /**
   * Regression: GROUP-owned metalakes must still submit when authorization is on. TMS does not
   * require the metalake owner to be a USER; it {@code doAs} the configured jobSubmitter.
   */
  @TestTemplate
  void submitAndBindAsJobSubmitterWhenAuthorizationEnabled() throws IOException {
    String metalake = "maint_job_submit_ml";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "cat");
    createAndInsertSchema(metalake, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalake, "cat", "sch"), "tbl");
    long policyId = insertPolicy(metalake, "job-submit-policy").id();

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyClaim claim =
        actions
            .tryAcquireInFlight(
                table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS)
            .orElseThrow();

    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    JobEntity job = mock(JobEntity.class);
    when(job.id()).thenReturn(JOB_ID);
    AtomicReference<String> principalDuringRun = new AtomicReference<>();
    when(jobs.runJob(
            eq(metalake), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenAnswer(
            invocation -> {
              principalDuringRun.set(PrincipalUtils.getCurrentUserName());
              return job;
            });

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, true, SUBMITTER);
    Optional<Long> bound = manager.submitAndBind(metalake, new HashMap<>(), List.of(claim));

    Assertions.assertEquals(Optional.of(JOB_ID), bound);
    Assertions.assertEquals(SUBMITTER, principalDuringRun.get());
    Assertions.assertEquals(JOB_ID, actions.getById(claim.actionId()).jobId());
  }

  @TestTemplate
  void submitAndBindWithoutDoAsWhenAuthorizationDisabled() throws IOException {
    String metalake = "maint_job_submit_ml_noauth";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "cat");
    createAndInsertSchema(metalake, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalake, "cat", "sch"), "tbl");
    long policyId = insertPolicy(metalake, "job-submit-policy-noauth").id();

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyClaim claim =
        actions
            .tryAcquireInFlight(
                table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS)
            .orElseThrow();

    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    JobEntity job = mock(JobEntity.class);
    when(job.id()).thenReturn(JOB_ID + 1);
    AtomicReference<String> principalDuringRun = new AtomicReference<>();
    when(jobs.runJob(
            eq(metalake), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenAnswer(
            invocation -> {
              principalDuringRun.set(PrincipalUtils.getCurrentUserName());
              return job;
            });

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, false, null);
    Optional<Long> bound = manager.submitAndBind(metalake, new HashMap<>(), List.of(claim));

    Assertions.assertEquals(Optional.of(JOB_ID + 1), bound);
    // No doAs: principal stays the test thread default (anonymous).
    Assertions.assertEquals(PrincipalUtils.ANONYMOUS_PRINCIPAL.getName(), principalDuringRun.get());
    Assertions.assertEquals(JOB_ID + 1, actions.getById(claim.actionId()).jobId());
  }

  private PolicyEntity insertPolicy(String metalakeName, String policyName) throws IOException {
    return createAndInsertPolicyEntity(
        policyName,
        "comment",
        PolicyContents.custom(
            ImmutableMap.of("k", "v"), ImmutableSet.of(MetadataObject.Type.TABLE), null),
        metalakeName);
  }
}
