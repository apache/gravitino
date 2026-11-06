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
public class MaintenanceMetricsManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceMetricsManager.class);

  private static final TableMaintenanceActionMetaService ACTION_SERVICE =
      TableMaintenanceActionMetaService.getInstance();
  private static final TableSnapshotMetricsMetaService METRICS_SERVICE =
      TableSnapshotMetricsMetaService.getInstance();
  private static final TableMetaService TABLE_SERVICE = TableMetaService.getInstance();

  private static volatile MaintenanceMetricsManager instance;

  /**
   * Returns the shared TMS metrics manager for the server process.
   *
   * @return singleton manager
   */
  public static MaintenanceMetricsManager getInstance() {
    if (instance == null) {
      synchronized (MaintenanceMetricsManager.class) {
        if (instance == null) {
          instance = new MaintenanceMetricsManager();
        }
      }
    }
    return instance;
  }

  private MaintenanceMetricsManager() {}

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
    TableMaintenanceActionPO action =
        loadInFlight(metalake, actionId, request.getTableId(), request.getPolicyId());
    long metricsId =
        action.beforeMetricsId() != null
            ? action.beforeMetricsId()
            : METRICS_SERVICE.insertMetrics(
                action.tableId(),
                action.policyId(),
                request.getSnapshotId(),
                request.getMetricsJson());
    ACTION_SERVICE.recordBeforeMetrics(actionId, metricsId);
    if (finish) {
      ACTION_SERVICE.markFinished(actionId, System.currentTimeMillis());
      LOG.info(
          "before metrics reported and action finished for action_id {} metalake {} table {}"
              + " policy {} metricsId {}",
          actionId,
          metalake,
          action.tableId(),
          action.policyId(),
          metricsId);
    } else {
      LOG.info(
          "before metrics reported for action_id {} metalake {} table {} policy {} metricsId {}",
          actionId,
          metalake,
          action.tableId(),
          action.policyId(),
          metricsId);
    }
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
    TableMaintenanceActionPO action =
        loadInFlight(metalake, actionId, request.getTableId(), request.getPolicyId());
    long metricsId =
        action.afterMetricsId() != null
            ? action.afterMetricsId()
            : METRICS_SERVICE.insertMetrics(
                action.tableId(),
                action.policyId(),
                request.getSnapshotId(),
                request.getMetricsJson());
    ACTION_SERVICE.recordAfterMetrics(actionId, metricsId);
    if (finish) {
      ACTION_SERVICE.markFinished(actionId, System.currentTimeMillis());
      LOG.info(
          "after metrics reported and action finished for action_id {} metalake {} table {}"
              + " policy {} metricsId {}",
          actionId,
          metalake,
          action.tableId(),
          action.policyId(),
          metricsId);
    } else {
      LOG.info(
          "after metrics reported for action_id {} metalake {} table {} policy {} metricsId {}",
          actionId,
          metalake,
          action.tableId(),
          action.policyId(),
          metricsId);
    }
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
        loadInFlight(metalake, actionId, request.getTableId(), request.getPolicyId());
    ACTION_SERVICE.markFinished(actionId, System.currentTimeMillis());
    LOG.info(
        "action finished for action_id {} metalake {} table {} policy {}",
        actionId,
        metalake,
        action.tableId(),
        action.policyId());
  }

  private static TableMaintenanceActionPO loadInFlight(
      String metalake, long actionId, long tableId, long policyId) {
    TableMaintenanceActionPO action = ACTION_SERVICE.getById(actionId);
    if (action == null) {
      throw new NoSuchEntityException("no maintenance action for action_id=%s", actionId);
    }
    if (!Objects.equals(action.tableId(), tableId)
        || !Objects.equals(action.policyId(), policyId)) {
      throw new NoSuchEntityException(
          "action_id=%s does not match table_id=%s policy_id=%s", actionId, tableId, policyId);
    }
    if (action.finishedAt() == null || action.finishedAt() != 0L) {
      throw new NoSuchEntityException(
          "no in-flight maintenance action for action_id=%s", action.actionId());
    }
    if (action.jobId() == null) {
      throw new NoSuchEntityException(
          "maintenance action action_id=%s is not bound to a job yet", actionId);
    }
    String actionMetalake = TABLE_SERVICE.getMetalakeNameByTableId(action.tableId());
    if (actionMetalake == null) {
      throw new NoSuchEntityException(
          "no metalake for table_id=%s (action_id=%s)", action.tableId(), action.actionId());
    }
    if (!actionMetalake.equals(metalake)) {
      throw new NoSuchEntityException(
          "no maintenance action for action_id=%s under metalake %s", action.actionId(), metalake);
    }
    return action;
  }
}
