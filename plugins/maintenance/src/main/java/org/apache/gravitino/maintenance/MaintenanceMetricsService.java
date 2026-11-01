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

import java.util.Objects;
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
 * <p>Callbacks are keyed by {@code action_id} injected at submit time. Metrics writes require the
 * row to be bound ({@code job_id IS NOT NULL}).
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
   * @return service bound into the Jersey feature
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
   * S0 report: stores metrics, records {@code before_metrics_id}, optionally closes the soft slot
   * (for example snapshot drift / skip).
   *
   * @param metalake metalake name from the request path
   * @param actionId maintenance action id
   * @param finish when {@code true}, close the soft slot after storing metrics
   * @param request validated metrics payload
   */
  public void reportBeforeMetrics(
      String metalake, long actionId, boolean finish, MaintenanceActionMetricsRequest request) {
    TableMaintenanceActionPO action = loadInFlight(metalake, actionId, request);
    long metricsId =
        action.beforeMetricsId() != null
            ? action.beforeMetricsId()
            : metricsMetaService.insertMetrics(
                action.tableId(),
                action.policyId(),
                request.getSnapshotId(),
                request.getMetricsJson());
    actionMetaService.recordBeforeMetrics(actionId, metricsId);
    maybeFinish(actionId, action, finish, "before", metalake, metricsId);
  }

  /**
   * S1 report: stores metrics, records {@code after_metrics_id}, optionally closes the soft slot.
   *
   * @param metalake metalake name from the request path
   * @param actionId maintenance action id
   * @param finish when {@code true}, close the soft slot after storing metrics
   * @param request validated metrics payload
   */
  public void reportAfterMetrics(
      String metalake, long actionId, boolean finish, MaintenanceActionMetricsRequest request) {
    TableMaintenanceActionPO action = loadInFlight(metalake, actionId, request);
    long metricsId =
        action.afterMetricsId() != null
            ? action.afterMetricsId()
            : metricsMetaService.insertMetrics(
                action.tableId(),
                action.policyId(),
                request.getSnapshotId(),
                request.getMetricsJson());
    actionMetaService.recordAfterMetrics(actionId, metricsId);
    maybeFinish(actionId, action, finish, "after", metalake, metricsId);
  }

  /**
   * Closes the soft slot for one bound action without writing metrics.
   *
   * @param metalake metalake name from the request path
   * @param actionId maintenance action id
   * @param request finish payload ({@code tableId}, {@code policyId})
   */
  public void finishAction(String metalake, long actionId, MaintenanceActionFinishRequest request) {
    TableMaintenanceActionPO action =
        loadInFlight(
            metalake,
            actionId,
            request.getTableId(),
            request.getPolicyId(),
            /*requireBound=*/ true);
    actionMetaService.markFinished(actionId, System.currentTimeMillis());
    LOG.info(
        "action finished for action_id {} metalake {} table {} policy {}",
        actionId,
        metalake,
        action.tableId(),
        action.policyId());
  }

  private void maybeFinish(
      long actionId,
      TableMaintenanceActionPO action,
      boolean finish,
      String phase,
      String metalake,
      long metricsId) {
    if (!finish) {
      LOG.info(
          "{} metrics reported for action_id {} metalake {} table {} policy {} metricsId {}",
          phase,
          actionId,
          metalake,
          action.tableId(),
          action.policyId(),
          metricsId);
      return;
    }
    actionMetaService.markFinished(actionId, System.currentTimeMillis());
    LOG.info(
        "{} metrics reported and action finished for action_id {} metalake {} table {} policy {} metricsId {}",
        phase,
        actionId,
        metalake,
        action.tableId(),
        action.policyId(),
        metricsId);
  }

  private TableMaintenanceActionPO loadInFlight(
      String metalake, long actionId, MaintenanceActionMetricsRequest request) {
    return loadInFlight(
        metalake, actionId, request.getTableId(), request.getPolicyId(), /*requireBound=*/ true);
  }

  private TableMaintenanceActionPO loadInFlight(
      String metalake, long actionId, long tableId, long policyId, boolean requireBound) {
    TableMaintenanceActionPO action = actionMetaService.getById(actionId);
    if (action == null) {
      throw new NoSuchEntityException("no maintenance action for action_id=%s", actionId);
    }
    if (!Objects.equals(action.tableId(), tableId)
        || !Objects.equals(action.policyId(), policyId)) {
      throw new NoSuchEntityException(
          "action_id=%s does not match table_id=%s policy_id=%s", actionId, tableId, policyId);
    }
    requireActionInFlight(action);
    if (requireBound && action.jobId() == null) {
      throw new NoSuchEntityException(
          "maintenance action action_id=%s is not bound to a job yet", actionId);
    }
    requireActionInMetalake(action, metalake);
    return action;
  }

  private static void requireActionInFlight(TableMaintenanceActionPO action) {
    if (action.finishedAt() == null || action.finishedAt() != 0L) {
      throw new NoSuchEntityException(
          "no in-flight maintenance action for action_id=%s", action.actionId());
    }
  }

  private void requireActionInMetalake(TableMaintenanceActionPO action, String metalake) {
    String actionMetalake = metalakeLookup.apply(action.tableId());
    if (actionMetalake == null) {
      throw new NoSuchEntityException(
          "no metalake for table_id=%s (action_id=%s)", action.tableId(), action.actionId());
    }
    if (!actionMetalake.equals(metalake)) {
      throw new NoSuchEntityException(
          "no maintenance action for action_id=%s under metalake %s", action.actionId(), metalake);
    }
  }
}
