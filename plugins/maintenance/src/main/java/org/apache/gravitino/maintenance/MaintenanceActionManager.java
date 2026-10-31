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

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;

/**
 * Decides whether a maintenance action for one table and policy can start.
 *
 * <p>Reads {@code table_maintenance_action} through {@link TableMaintenanceActionMetaService}.
 * Unbound rows ({@code job_id} null, {@code finished_at = 0}) occupy the soft slot while still
 * within TTL on the database clock. Bound rows stay occupied until Spark closes them via
 * before/after metrics with {@code ?finish=true}. This manager does not reclaim on Gravitino job
 * terminal state, and it does not acquire or release the slot.
 */
public final class MaintenanceActionManager {

  private final TableMaintenanceActionMetaService actionMetaService;
  private final long claimTimeoutMs;

  /**
   * @param actionMetaService maintenance action store
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceActionManager(
      TableMaintenanceActionMetaService actionMetaService, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * @param actionMetaService maintenance action store
   */
  public MaintenanceActionManager(TableMaintenanceActionMetaService actionMetaService) {
    this(actionMetaService, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
  }

  /** Uses {@link TableMaintenanceActionMetaService#getInstance()} and the default claim timeout. */
  public MaintenanceActionManager() {
    this(TableMaintenanceActionMetaService.getInstance());
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when any open row is a live unbound claim or a still-bound soft-slot row
   */
  public boolean isOccupied(long tableId, long policyId) {
    if (actionMetaService.hasLiveUnboundClaim(tableId, policyId, claimTimeoutMs)) {
      return true;
    }
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis used only for cooldown (not unbound TTL)
   * @return true when the last known finish is still inside the cooldown window
   */
  public boolean isCoolingDown(long tableId, long policyId, long minIntervalMs, long nowMs) {
    if (minIntervalMs <= 0) {
      return false;
    }
    Long lastFinished = lastFinishedAt(tableId, policyId);
    return lastFinished != null && nowMs - lastFinished < minIntervalMs;
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis used only for cooldown (not unbound TTL)
   * @return true when the soft slot is free and the cooldown has elapsed
   */
  public boolean canStart(long tableId, long policyId, long minIntervalMs, long nowMs) {
    return !isOccupied(tableId, policyId)
        && !isCoolingDown(tableId, policyId, minIntervalMs, nowMs);
  }

  @Nullable
  private Long lastFinishedAt(long tableId, long policyId) {
    return actionMetaService.maxFinishedAt(tableId, policyId);
  }
}
