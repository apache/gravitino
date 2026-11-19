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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;

/**
 * Unit and JDBC-backed coverage for {@link MaintenanceJobManager}, including the GROUP-owned
 * metalake regression: with authorization on, submit uses {@code jobSubmitter}, not metalake owner.
 */
class TestMaintenanceJobManager extends TestJDBCBackend {

  private static final String METALAKE = "ml";
  private static final String SUBMITTER = "tms";
  private static final long JOB_ID = 88001L;

  @Test
  void constructorRequiresJobSubmitterWhenAuthorizationEnabled() {
    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new MaintenanceJobManager(jobs, true, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new MaintenanceJobManager(jobs, true, "  "));
    Assertions.assertDoesNotThrow(() -> new MaintenanceJobManager(jobs, false, null));
  }

  @Test
  void runJobWithoutDoAsWhenAuthorizationDisabled() {
    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    JobEntity job = mock(JobEntity.class);
    when(jobs.runJob(
            eq(METALAKE), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(job);

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, false, null);
    Map<String, String> conf = new HashMap<>();
    Assertions.assertSame(job, manager.runIcebergMaintenanceJob(METALAKE, conf));
    verify(jobs)
        .runJob(eq(METALAKE), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), eq(conf));
  }

  @Test
  void runJobAsConfiguredSubmitterWhenAuthorizationEnabled() {
    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    JobEntity job = mock(JobEntity.class);

    AtomicReference<String> principalDuringRun = new AtomicReference<>();
    when(jobs.runJob(
            eq(METALAKE), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenAnswer(
            invocation -> {
              principalDuringRun.set(PrincipalUtils.getCurrentUserName());
              return job;
            });

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, true, SUBMITTER);
    Map<String, String> conf = new HashMap<>();
    Assertions.assertSame(job, manager.runIcebergMaintenanceJob(METALAKE, conf));
    Assertions.assertEquals(SUBMITTER, principalDuringRun.get());
  }

  /**
   * Regression for GROUP-owned metalakes: {@code submitAndBind} must not require the metalake owner
   * to be a USER. Authorization-on submits as {@code jobSubmitter}.
   */
  @TestTemplate
  void submitAndBindAsJobSubmitterWhenAuthorizationEnabled() throws IOException {
    String metalake = "maint_job_submit_ml";
    OccupancyClaim claim = acquireClaim(metalake, "job-submit-policy");

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
    Assertions.assertEquals(
        JOB_ID, TableMaintenanceActionMetaService.getInstance().getById(claim.actionId()).jobId());
  }

  @TestTemplate
  void submitAndBindWithoutDoAsWhenAuthorizationDisabled() throws IOException {
    String metalake = "maint_job_submit_ml_noauth";
    OccupancyClaim claim = acquireClaim(metalake, "job-submit-policy-noauth");

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
    Assertions.assertEquals(PrincipalUtils.ANONYMOUS_PRINCIPAL.getName(), principalDuringRun.get());
    Assertions.assertEquals(
        JOB_ID + 1,
        TableMaintenanceActionMetaService.getInstance().getById(claim.actionId()).jobId());
  }

  @TestTemplate
  void submitAndBindReleasesClaimWhenRunJobFails() throws IOException {
    String metalake = "maint_job_submit_ml_fail";
    OccupancyClaim claim = acquireClaim(metalake, "job-submit-policy-fail");

    JobOperationDispatcher jobs = mock(JobOperationDispatcher.class);
    when(jobs.runJob(
            eq(metalake), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenThrow(new RuntimeException("submit failed"));

    MaintenanceJobManager manager = new MaintenanceJobManager(jobs, true, SUBMITTER);
    Optional<Long> bound = manager.submitAndBind(metalake, new HashMap<>(), List.of(claim));

    Assertions.assertEquals(Optional.empty(), bound);
    Assertions.assertNull(
        TableMaintenanceActionMetaService.getInstance().getById(claim.actionId()));
  }

  private OccupancyClaim acquireClaim(String metalake, String policyName) throws IOException {
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, "cat");
    createAndInsertSchema(metalake, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalake, "cat", "sch"), "tbl");
    long policyId = insertPolicy(metalake, policyName).id();
    return TableMaintenanceActionMetaService.getInstance()
        .tryAcquireInFlight(table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS)
        .orElseThrow();
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
