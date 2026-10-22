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
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists snapshot metrics reported by Spark for an in-flight maintenance action.
 *
 * <p>Continue / stop decisions stay in the job. Occupancy is released when the Gravitino job
 * reaches a terminal state ({@code OccupancyGate#reclaimTerminalBound}).
 */
public class MaintenanceMetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceMetricsService.class);
  private static final TableMetaService TABLE_SERVICE = TableMetaService.getInstance();

  private final TableMaintenanceActionMetaService actionMetaService;
  private final TableSnapshotMetricsMetaService metricsMetaService;
  private final LongFunction<String> metalakeLookup;

  /**
   * Production wiring.
   *
   * @return service bound into the TMS Jersey feature
   */
  public static MaintenanceMetricsService create() {
    return new MaintenanceMetricsService(
        TableMaintenanceActionMetaService.getInstance(),
        TableSnapshotMetricsMetaService.getInstance(),
        TABLE_SERVICE::getMetalakeNameByTableId);
  }

  /**
   * Creates a service with explicit collaborators.
   *
   * @param actionMetaService action rows
   * @param metricsMetaService snapshot metrics rows
   * @param metalakeLookup resolves a table id to its metalake name
   */
  public MaintenanceMetricsService(
      TableMaintenanceActionMetaService actionMetaService,
      TableSnapshotMetricsMetaService metricsMetaService,
      LongFunction<String> metalakeLookup) {
    this.actionMetaService = actionMetaService;
    this.metricsMetaService = metricsMetaService;
    this.metalakeLookup = metalakeLookup;
  }

  /**
   * Idempotently stores metrics for the table/policy bound to {@code jobId}.
   *
   * @param metalake metalake name from the request path
   * @param jobId Gravitino job run id bound to the action
   * @param request validated metrics payload
   */
  public void reportMetrics(String metalake, long jobId, MaintenanceActionMetricsRequest request) {
    TableMaintenanceActionPO action = loadInFlight(metalake, jobId);
    metricsMetaService.insertMetrics(
        action.tableId(), action.policyId(), request.getSnapshotId(), request.getMetricsJson());
    LOG.info(
        "TMS metrics reported for job {} metalake {} table {} policy {} snapshot {}",
        jobId,
        metalake,
        action.tableId(),
        action.policyId(),
        request.getSnapshotId());
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

  /**
   * Soft-slot rows use {@code finished_at = 0} while in flight. Reject reports on completed rows so
   * a late Spark retry cannot write metrics after reclaim.
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
