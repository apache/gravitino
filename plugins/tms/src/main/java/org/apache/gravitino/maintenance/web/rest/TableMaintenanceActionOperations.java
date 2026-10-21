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
package org.apache.gravitino.maintenance.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.LongFunction;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.maintenance.injob.InJobGateOutcome;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobPipeline;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobSampler;
import org.apache.gravitino.maintenance.injob.TypeDispatchingInJobRecommender;
import org.apache.gravitino.maintenance.storage.MetalakeNames;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark callbacks for cron maintenance actions: sample {@code S0} / gate, then finish with optional
 * S1 metrics (§5.2.2).
 */
@Path("metalakes/{metalake}/maintenance/actions")
@Consumes({
  MediaType.APPLICATION_JSON,
  MediaType.APPLICATION_JSON + "; charset=UTF-8",
  "application/vnd.gravitino.v1+json",
  "application/vnd.gravitino.v1+json; charset=UTF-8"
})
@Produces({
  MediaType.APPLICATION_JSON + "; charset=UTF-8",
  "application/vnd.gravitino.v1+json; charset=UTF-8"
})
public class TableMaintenanceActionOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceActionOperations.class);

  private final TableMaintenanceInJobPipeline pipeline;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final TableSnapshotMetricsMetaService metricsMetaService;
  private final LongFunction<String> metalakeLookup;

  @Context private HttpServletRequest httpRequest;

  /** Production wiring with default sampler / type-dispatching recommender. */
  public TableMaintenanceActionOperations() {
    this(
        new TableMaintenanceInJobPipeline(
            new TableMaintenanceInJobSampler(), new TypeDispatchingInJobRecommender()),
        TableMaintenanceActionMetaService.getInstance(),
        TableSnapshotMetricsMetaService.getInstance(),
        MetalakeNames::forTableId);
  }

  @VisibleForTesting
  TableMaintenanceActionOperations(
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
   * Persists S0 metrics + {@code before_snapshot_id}, then returns continue vs stop (drift /
   * recommender).
   *
   * @param metalake metalake name from the request path (must own the action's table)
   * @param jobId Gravitino job run id bound to the action
   * @param request sample payload
   * @return gate outcome
   */
  @POST
  @Path("{jobId}/sample")
  @Timed(name = "tms-action-sample." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "tms-action-sample", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::RUN_JOB")
  public Response sample(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("jobId") long jobId,
      MaintenanceActionSampleRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (request == null) {
              throw new IllegalArgumentException("Request body cannot be null");
            }
            request.validate();
            TableMaintenanceActionPO action = actionMetaService.getByJobId(jobId);
            if (action == null) {
              throw new NoSuchEntityException("no maintenance action for job_id=%s", jobId);
            }
            requireActionInFlight(action);
            requireActionInMetalake(action, metalake);
            requireActionMatchesRequest(action, request.getTableId(), request.getPolicyId());
            InJobGateOutcome outcome =
                pipeline.gateCronJob(
                    jobId,
                    request.getTableId(),
                    request.getPolicyId(),
                    request.getSampledSnapshotId(),
                    request.getMetricsJson(),
                    request::getHeadSnapshotId);
            String stopReason = outcome.stopReason() == null ? null : outcome.stopReason().name();
            LOG.info(
                "TMS action sample for job {} metalake {}: continue={} stopReason={}",
                jobId,
                metalake,
                outcome.continueMaintenance(),
                stopReason);
            return Utils.ok(
                new MaintenanceActionSampleResponse(
                    outcome.continueMaintenance(),
                    stopReason,
                    outcome.sampledSnapshotId(),
                    outcome.headSnapshotId()));
          });
    } catch (Exception e) {
      return MaintenanceActionExceptionHandler.handle("sample", jobId, metalake, e);
    }
  }

  /**
   * Optionally persists S1 metrics, then marks {@code table_maintenance_action} finished with
   * {@code after_snapshot_id}.
   *
   * @param metalake metalake name from the request path (must own the action's table)
   * @param jobId Gravitino job run id bound to the action
   * @param request finish payload
   * @return empty success response
   */
  @POST
  @Path("{jobId}/finish")
  @Timed(name = "tms-action-finish." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "tms-action-finish", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::RUN_JOB")
  public Response finish(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("jobId") long jobId,
      MaintenanceActionFinishRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (request == null) {
              throw new IllegalArgumentException("Request body cannot be null");
            }
            request.validate();
            TableMaintenanceActionPO action = actionMetaService.getByJobId(jobId);
            if (action == null) {
              throw new NoSuchEntityException("no maintenance action for job_id=%s", jobId);
            }
            requireActionInFlight(action);
            requireActionInMetalake(action, metalake);
            if (request.getAfterSnapshotId() != null
                && StringUtils.isNotBlank(request.getMetricsJson())) {
              metricsMetaService.insertMetrics(
                  action.tableId(),
                  action.policyId(),
                  request.getAfterSnapshotId(),
                  request.getMetricsJson());
            }
            actionMetaService.markFinished(
                jobId, request.getAfterSnapshotId(), System.currentTimeMillis());
            LOG.info(
                "TMS action finished for job {} metalake {} afterSnapshotId={}",
                jobId,
                metalake,
                request.getAfterSnapshotId());
            return Utils.ok(new BaseResponse());
          });
    } catch (Exception e) {
      return MaintenanceActionExceptionHandler.handle("finish", jobId, metalake, e);
    }
  }

  @VisibleForTesting
  void setHttpRequest(HttpServletRequest httpRequest) {
    this.httpRequest = httpRequest;
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

  /**
   * Ensures the path metalake matches the table that owns the action (same scoping pattern as job
   * REST APIs under {@code /metalakes/{metalake}/jobs/...}).
   */
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
