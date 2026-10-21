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
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.maintenance.MaintenanceActionManager;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.maintenance.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.maintenance.injob.InJobGateOutcome;
import org.apache.gravitino.maintenance.web.MaintenanceActionOperation;
import org.apache.gravitino.maintenance.web.TableMaintenanceRESTUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;

/**
 * Spark callbacks for cron maintenance actions: sample {@code S0} / gate, then finish with optional
 * S1 metrics.
 *
 * <p>The resource validates the request and delegates to {@link MaintenanceActionManager}. Jersey
 * injects that manager from {@link TableMaintenanceBinder}.
 */
@Path("metalakes/{metalake}/maintenance/actions")
@Consumes({
  MediaType.APPLICATION_JSON,
  MediaType.APPLICATION_JSON + "; charset=UTF-8",
  "application/vnd.gravitino.v1+json",
  "application/vnd.gravitino.v1+json; charset=UTF-8"
})
@Produces("application/vnd.gravitino.v1+json")
public class TableMaintenanceActionOperations {

  private final MaintenanceActionManager manager;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the resource.
   *
   * @param manager sample and finish domain service
   */
  @Inject
  public TableMaintenanceActionOperations(MaintenanceActionManager manager) {
    this.manager = manager;
  }

  /**
   * Persists S0 metrics and returns continue vs stop.
   *
   * @param metalake metalake name from the request path
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
    return TableMaintenanceRESTUtils.doAs(
        httpRequest,
        () -> {
          if (request == null) {
            throw new IllegalArgumentException("Request body cannot be null");
          }
          request.validate();
          InJobGateOutcome outcome = manager.sample(metalake, jobId, request);
          String stopReason = outcome.stopReason() == null ? null : outcome.stopReason().name();
          return TableMaintenanceRESTUtils.ok(
              new MaintenanceActionSampleResponse(
                  outcome.continueMaintenance(),
                  stopReason,
                  outcome.sampledSnapshotId(),
                  outcome.headSnapshotId()));
        },
        MaintenanceActionOperation.SAMPLE,
        jobId,
        metalake);
  }

  /**
   * Optionally persists S1 metrics, then marks the action finished.
   *
   * @param metalake metalake name from the request path
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
    return TableMaintenanceRESTUtils.doAs(
        httpRequest,
        () -> {
          if (request == null) {
            throw new IllegalArgumentException("Request body cannot be null");
          }
          request.validate();
          manager.finish(metalake, jobId, request);
          return TableMaintenanceRESTUtils.ok(new BaseResponse());
        },
        MaintenanceActionOperation.FINISH,
        jobId,
        metalake);
  }

  /**
   * Installs the request used by {@link TableMaintenanceRESTUtils#doAs}.
   *
   * @param httpRequest request whose authenticated principal is adopted
   */
  @VisibleForTesting
  void setHttpRequest(HttpServletRequest httpRequest) {
    this.httpRequest = httpRequest;
  }
}
