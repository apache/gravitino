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

import java.util.function.LongFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.maintenance.injob.InJobGateOutcome;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobPipeline;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobSampler;
import org.apache.gravitino.maintenance.injob.TypeDispatchingInJobRecommender;
import org.apache.gravitino.maintenance.storage.MetalakeNames;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Domain service for Spark sample and finish callbacks.
 *
 * <p>REST resources stay thin and delegate here, the same split {@code idp-basic} uses between
 * {@code IdpUserOperations} and {@code IdpUserGroupManager}.
 */
public class MaintenanceActionManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceActionManager.class);

  private final TableMaintenanceInJobPipeline pipeline;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final TableSnapshotMetricsMetaService metricsMetaService;
  private final LongFunction<String> metalakeLookup;

  /**
   * Production wiring with the default sampler and type-dispatching recommender.
   *
   * @return manager bound into the TMS Jersey feature
   */
  public static MaintenanceActionManager create() {
    return new MaintenanceActionManager(
        new TableMaintenanceInJobPipeline(
            new TableMaintenanceInJobSampler(), new TypeDispatchingInJobRecommender()),
        TableMaintenanceActionMetaService.getInstance(),
        TableSnapshotMetricsMetaService.getInstance(),
        MetalakeNames::forTableId);
  }

  /**
   * Creates a manager with explicit collaborators.
   *
   * @param pipeline in-job sample and gate pipeline
   * @param actionMetaService action rows
   * @param metricsMetaService snapshot metrics rows
   * @param metalakeLookup resolves a table id to its metalake name
   */
  public MaintenanceActionManager(
      TableMaintenanceInJobPipeline pipeline,
      TableMaintenanceActionMetaService actionMetaService,
      TableSnapshotMetricsMetaService metricsMetaService,
      LongFunction<String> metalakeLookup) {
    this.pipeline = pipeline;
    this.actionMetaService = actionMetaService;
    this.metricsMetaService = metricsMetaService;
    this.metalakeLookup = metalakeLookup;
  }

  /**
   * Persists S0 metrics and returns whether the job should continue.
   *
   * @param metalake metalake name from the request path
   * @param jobId Gravitino job run id bound to the action
   * @param request validated sample payload
   * @return gate outcome
   */
  public InJobGateOutcome sample(
      String metalake, long jobId, MaintenanceActionSampleRequest request) {
    TableMaintenanceActionPO action = loadInFlight(metalake, jobId);
    requireActionMatchesRequest(action, request.getTableId(), request.getPolicyId());
    InJobGateOutcome outcome =
        pipeline.gateCronJob(
            jobId,
            request.getTableId(),
            request.getPolicyId(),
            request.getSampledSnapshotId(),
            request.getMetricsJson(),
            request::getHeadSnapshotId);
    LOG.info(
        "TMS action sample for job {} metalake {}: continue={} stopReason={}",
        jobId,
        metalake,
        outcome.continueMaintenance(),
        outcome.stopReason());
    return outcome;
  }

  /**
   * Optionally persists S1 metrics, then marks the action finished.
   *
   * @param metalake metalake name from the request path
   * @param jobId Gravitino job run id bound to the action
   * @param request validated finish payload
   */
  public void finish(String metalake, long jobId, MaintenanceActionFinishRequest request) {
    TableMaintenanceActionPO action = loadInFlight(metalake, jobId);
    if (request.getAfterSnapshotId() != null && StringUtils.isNotBlank(request.getMetricsJson())) {
      metricsMetaService.insertMetrics(
          action.tableId(),
          action.policyId(),
          request.getAfterSnapshotId(),
          request.getMetricsJson());
    }
    actionMetaService.markFinished(jobId, request.getAfterSnapshotId(), System.currentTimeMillis());
    LOG.info(
        "TMS action finished for job {} metalake {} afterSnapshotId={}",
        jobId,
        metalake,
        request.getAfterSnapshotId());
  }

  private TableMaintenanceActionPO loadInFlight(String metalake, long jobId) {
    TableMaintenanceActionPO action = actionMetaService.getByJobId(jobId);
    if (action == null) {
      throw new NoSuchEntityException("no maintenance action for job_id=%s", jobId);
    }
    requireActionInFlight(action);
    requireActionInMetalake(action, metalake);
    return action;
  }

  private static void requireActionMatchesRequest(
      TableMaintenanceActionPO action, long tableId, long policyId) {
    if (!action.tableId().equals(tableId) || !action.policyId().equals(policyId)) {
      throw new IllegalArgumentException(
          String.format(
              "request tableId/policyId (%s/%s) do not match action for job_id=%s (%s/%s)",
              tableId, policyId, action.jobId(), action.tableId(), action.policyId()));
    }
  }

  /**
   * Soft-slot rows use {@code finished_at = 0} while in flight. Reject sample/finish on completed
   * rows so a late Spark retry cannot re-gate or re-run maintenance after reclaim/finish.
   */
  private static void requireActionInFlight(TableMaintenanceActionPO action) {
    if (action.finishedAt() == null || action.finishedAt() != 0L) {
      throw new NoSuchEntityException(
          "no in-flight maintenance action for job_id=%s", action.jobId());
    }
  }

  private void requireActionInMetalake(TableMaintenanceActionPO action, String metalake) {
    String actionMetalake = metalakeLookup.apply(action.tableId());
    if (actionMetalake == null) {
      throw new NoSuchEntityException(
          "no metalake for table_id=%s (job_id=%s)", action.tableId(), action.jobId());
    }
    if (!actionMetalake.equals(metalake)) {
      throw new NoSuchEntityException(
          "no maintenance action for job_id=%s under metalake %s", action.jobId(), metalake);
    }
  }
}
