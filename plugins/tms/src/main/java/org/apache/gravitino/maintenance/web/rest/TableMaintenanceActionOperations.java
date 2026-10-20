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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.maintenance.injob.InJobGateOutcome;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobPipeline;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobSampler;
import org.apache.gravitino.maintenance.injob.TypeDispatchingInJobRecommender;
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
public class TableMaintenanceActionOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceActionOperations.class);

  private final TableMaintenanceInJobPipeline pipeline;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final TableSnapshotMetricsMetaService metricsMetaService;

  /** Production wiring with default sampler / type-dispatching recommender. */
  public TableMaintenanceActionOperations() {
    this(
        new TableMaintenanceInJobPipeline(
            new TableMaintenanceInJobSampler(), new TypeDispatchingInJobRecommender()),
        TableMaintenanceActionMetaService.getInstance(),
        TableSnapshotMetricsMetaService.getInstance());
  }

  TableMaintenanceActionOperations(
      TableMaintenanceInJobPipeline pipeline,
      TableMaintenanceActionMetaService actionMetaService,
      TableSnapshotMetricsMetaService metricsMetaService) {
    this.pipeline = pipeline;
    this.actionMetaService = actionMetaService;
    this.metricsMetaService = metricsMetaService;
  }

  @Context private HttpServletRequest httpRequest;

  /**
   * Persists S0 metrics + {@code before_snapshot_id}, then returns continue vs stop (drift /
   * recommender).
   */
  @POST
  @Path("{jobId}/sample")
  @Produces("application/vnd.gravitino.v1+json")
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
            request.validate();
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
      LOG.warn("Failed TMS action sample for job {} metalake {}", jobId, metalake, e);
      return Utils.internalError(e.getMessage(), e);
    }
  }

  /**
   * Optionally persists S1 metrics, then marks {@code table_maintenance_action} finished with
   * {@code after_snapshot_id}.
   */
  @POST
  @Path("{jobId}/finish")
  @Produces("application/vnd.gravitino.v1+json")
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
            request.validate();
            TableMaintenanceActionPO action = actionMetaService.getByJobId(jobId);
            if (action == null) {
              throw new IllegalStateException("no maintenance action for job_id=" + jobId);
            }
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
      LOG.warn("Failed TMS action finish for job {} metalake {}", jobId, metalake, e);
      return Utils.internalError(e.getMessage(), e);
    }
  }
}
