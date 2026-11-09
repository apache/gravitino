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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.maintenance.MaintenanceMetricsManager;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.web.TableMaintenanceRESTUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;

/**
 * Spark callbacks for before/after snapshot metrics and soft-slot finish on an in-flight
 * maintenance action.
 *
 * <p>Continue / stop stays in the job. Soft-slot release is via {@code finish=true} query param on
 * metrics, or {@code POST .../finish} when no metrics are written.
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

  private final MaintenanceMetricsManager metricsManager;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the resource.
   *
   * @param metricsManager metrics report manager
   */
  @Inject
  public TableMaintenanceActionOperations(MaintenanceMetricsManager metricsManager) {
    this.metricsManager = metricsManager;
  }

  /**
   * Persists S0 metrics and {@code before_metrics_id} for {@code actionId}.
   *
   * @param metalake metalake name from the request path
   * @param actionId maintenance action id injected at submit
   * @param finish when {@code true}, close the soft slot after storing metrics
   * @param request metrics payload
   * @return empty success response
   */
  @POST
  @Path("{actionId}/metrics/before")
  @Timed(
      name = "maintenance-action-metrics-before." + MetricNames.HTTP_PROCESS_DURATION,
      absolute = true)
  @ResponseMetered(name = "maintenance-action-metrics-before", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::RUN_JOB")
  public Response reportBeforeMetrics(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("actionId") long actionId,
      @QueryParam("finish") @DefaultValue("false") boolean finish,
      MaintenanceActionMetricsRequest request) {
    return TableMaintenanceRESTUtils.doAs(
        httpRequest,
        () -> {
          requireBody(request);
          metricsManager.reportBeforeMetrics(metalake, actionId, finish, request);
          return TableMaintenanceRESTUtils.ok(new BaseResponse());
        },
        "report-before-metrics",
        actionId,
        metalake);
  }

  /**
   * Persists S1 metrics for {@code actionId}.
   *
   * @param metalake metalake name from the request path
   * @param actionId maintenance action id injected at submit
   * @param finish when {@code true}, close the soft slot after storing metrics
   * @param request metrics payload
   * @return empty success response
   */
  @POST
  @Path("{actionId}/metrics/after")
  @Timed(
      name = "maintenance-action-metrics-after." + MetricNames.HTTP_PROCESS_DURATION,
      absolute = true)
  @ResponseMetered(name = "maintenance-action-metrics-after", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::RUN_JOB")
  public Response reportAfterMetrics(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("actionId") long actionId,
      @QueryParam("finish") @DefaultValue("false") boolean finish,
      MaintenanceActionMetricsRequest request) {
    return TableMaintenanceRESTUtils.doAs(
        httpRequest,
        () -> {
          requireBody(request);
          metricsManager.reportAfterMetrics(metalake, actionId, finish, request);
          return TableMaintenanceRESTUtils.ok(new BaseResponse());
        },
        "report-after-metrics",
        actionId,
        metalake);
  }

  /**
   * Closes the soft slot for one bound action without writing metrics.
   *
   * @param metalake metalake name from the request path
   * @param actionId maintenance action id injected at submit
   * @param request finish payload
   * @return empty success response
   */
  @POST
  @Path("{actionId}/finish")
  @Timed(name = "maintenance-action-finish." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "maintenance-action-finish", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::RUN_JOB")
  public Response finishAction(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("actionId") long actionId,
      MaintenanceActionFinishRequest request) {
    return TableMaintenanceRESTUtils.doAs(
        httpRequest,
        () -> {
          requireBody(request);
          metricsManager.finishAction(metalake, actionId, request);
          return TableMaintenanceRESTUtils.ok(new BaseResponse());
        },
        "finish",
        actionId,
        metalake);
  }

  private static void requireBody(RESTRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("Request body cannot be null");
    }
    request.validate();
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
