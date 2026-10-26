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
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists before/after snapshot metrics and closes soft slots for in-flight maintenance actions.
 *
 * <p>Continue / stop stays in the job. Soft-slot release happens via the {@code finish} query
 * parameter on metrics reports, or via {@link #finishAction} when no metrics are needed. There is
 * no job-terminal reclaim path.
 *
 * <p>Callbacks that arrive before submit-side {@code bindJobId} lazy-bind the unbound claim for the
 * reported {@code (tableId, policyId)} onto the path {@code jobId}.
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
   * S0 report: stores metrics, records {@code before_snapshot_id}, optionally closes the soft slot
   * (for example snapshot drift / skip).
   *
   * @param metalake metalake name from the request path
   * @param jobId Gravitino job run id
   * @param finish when {@code true}, close the soft slot after storing metrics
   * @param request validated metrics payload
   */
  public void reportBeforeMetrics(
      String metalake, long jobId, boolean finish, MaintenanceActionMetricsRequest request) {
    TableMaintenanceActionPO action =
        loadInFlight(metalake, jobId, request.getTableId(), request.getPolicyId());
    metricsMetaService.insertMetrics(
        action.tableId(), action.policyId(), request.getSnapshotId(), request.getMetricsJson());
    actionMetaService.recordBeforeSnapshot(
        jobId, action.tableId(), action.policyId(), request.getSnapshotId());
    maybeFinish(
        jobId,
        action,
        /*afterSnapshotId=*/ null,
        finish,
        "before",
        metalake,
        request.getSnapshotId());
  }

  /**
   * S1 report: stores metrics and optionally closes the soft slot with {@code after_snapshot_id} =
   * {@code snapshotId}.
   *
   * @param metalake metalake name from the request path
   * @param jobId Gravitino job run id
   * @param finish when {@code true}, close the soft slot after storing metrics
   * @param request validated metrics payload
   */
  public void reportAfterMetrics(
      String metalake, long jobId, boolean finish, MaintenanceActionMetricsRequest request) {
    TableMaintenanceActionPO action =
        loadInFlight(metalake, jobId, request.getTableId(), request.getPolicyId());
    metricsMetaService.insertMetrics(
        action.tableId(), action.policyId(), request.getSnapshotId(), request.getMetricsJson());
    maybeFinish(
        jobId, action, request.getSnapshotId(), finish, "after", metalake, request.getSnapshotId());
  }

  /**
   * Closes the soft slot for one bound table/policy without writing metrics.
   *
   * @param metalake metalake name from the request path
   * @param jobId Gravitino job run id
   * @param request finish payload ({@code tableId}, {@code policyId}, optional {@code
   *     afterSnapshotId})
   */
  public void finishAction(String metalake, long jobId, MaintenanceActionFinishRequest request) {
    TableMaintenanceActionPO action =
        loadInFlight(metalake, jobId, request.getTableId(), request.getPolicyId());
    actionMetaService.markFinished(
        jobId,
        action.tableId(),
        action.policyId(),
        request.getAfterSnapshotId(),
        System.currentTimeMillis());
    LOG.info(
        "TMS action finished for job {} metalake {} table {} policy {} afterSnapshotId={}",
        jobId,
        metalake,
        action.tableId(),
        action.policyId(),
        request.getAfterSnapshotId());
  }

  private void maybeFinish(
      long jobId,
      TableMaintenanceActionPO action,
      Long afterSnapshotId,
      boolean finish,
      String phase,
      String metalake,
      long snapshotId) {
    if (!finish) {
      LOG.info(
          "TMS {} metrics reported for job {} metalake {} table {} policy {} snapshot {}",
          phase,
          jobId,
          metalake,
          action.tableId(),
          action.policyId(),
          snapshotId);
      return;
    }
    actionMetaService.markFinished(
        jobId, action.tableId(), action.policyId(), afterSnapshotId, System.currentTimeMillis());
    LOG.info(
        "TMS {} metrics reported and action finished for job {} metalake {} table {} policy {} snapshot {}",
        phase,
        jobId,
        metalake,
        action.tableId(),
        action.policyId(),
        snapshotId);
  }

  private TableMaintenanceActionPO loadInFlight(
      String metalake, long jobId, long tableId, long policyId) {
    TableMaintenanceActionPO action =
        actionMetaService.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId);
    if (action == null) {
      if (actionMetaService.bindJobIdIfUnbound(tableId, policyId, jobId)) {
        action = actionMetaService.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId);
      }
    }
    if (action == null) {
      throw new NoSuchEntityException(
          "no maintenance action for job_id=%s table_id=%s policy_id=%s", jobId, tableId, policyId);
    }
    requireActionInFlight(action);
    requireActionInMetalake(action, metalake);
    return action;
  }

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
