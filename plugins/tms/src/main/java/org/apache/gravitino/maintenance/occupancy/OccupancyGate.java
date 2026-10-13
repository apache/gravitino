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
package org.apache.gravitino.maintenance.occupancy;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.JobMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Expand / submit occupancy checks against {@code table_maintenance_action} (§5.2).
 *
 * <p>When {@code finished_at} is null, this gate treats a null {@code job_id} as occupied (row
 * reserved before {@code runJob}). Once {@code job_id} is bound, it loads the Gravitino job via
 * {@link JobMetaService#getJobByIdentifier} to decide whether it is still running. It does
 * <strong>not</strong> backfill {@code finished_at}, so multiple open rows may exist for the same
 * {@code (table_id, policy_id)}; the gate inspects all of them.
 */
public final class OccupancyGate {

  private static final Set<JobHandle.Status> ACTIVE =
      EnumSet.of(JobHandle.Status.QUEUED, JobHandle.Status.STARTED, JobHandle.Status.CANCELLING);

  private final TableMaintenanceActionMetaService actionMetaService;

  /**
   * @param actionMetaService maintenance action DAO
   */
  public OccupancyGate(TableMaintenanceActionMetaService actionMetaService) {
    this.actionMetaService = actionMetaService;
  }

  /** Uses {@link TableMaintenanceActionMetaService#getInstance()}. */
  public OccupancyGate() {
    this(TableMaintenanceActionMetaService.getInstance());
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when any open row is unbound or still maps to an active Gravitino job
   */
  public boolean isInFlight(long tableId, long policyId) {
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() == null) {
        return true;
      }
      JobEntity job = loadJob(open.jobId(), tableId);
      if (job != null && ACTIVE.contains(job.status())) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis
   * @return true when the last known finish is still inside the cooldown window
   */
  public boolean withinMinInterval(long tableId, long policyId, long minIntervalMs, long nowMs) {
    if (minIntervalMs <= 0) {
      return false;
    }
    Long lastFinished = lastFinishedAt(tableId, policyId);
    return lastFinished != null && nowMs - lastFinished < minIntervalMs;
  }

  /**
   * Expand / submit should skip when occupied or still cooling down.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis
   * @return true when enqueue / submit must be skipped
   */
  public boolean shouldSkip(long tableId, long policyId, long minIntervalMs, long nowMs) {
    return isInFlight(tableId, policyId)
        || withinMinInterval(tableId, policyId, minIntervalMs, nowMs);
  }

  /**
   * Prefer {@code table_maintenance_action.finished_at}; if that column is still null but a
   * Gravitino job is already terminal, use {@link JobEntity#finishedAt()} without writing back.
   */
  @Nullable
  private Long lastFinishedAt(long tableId, long policyId) {
    Long latest = actionMetaService.maxFinishedAt(tableId, policyId);
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() == null) {
        continue;
      }
      JobEntity job = loadJob(open.jobId(), tableId);
      if (job == null || ACTIVE.contains(job.status())) {
        continue;
      }
      Long fromJob = job.finishedAt() != null && job.finishedAt() > 0 ? job.finishedAt() : null;
      if (fromJob == null) {
        continue;
      }
      latest = latest == null ? fromJob : Math.max(latest, fromJob);
    }
    return latest;
  }

  @Nullable
  static JobEntity loadJob(long jobRunId, long tableId) {
    String metalake = metalakeNameForTable(tableId);
    if (metalake == null) {
      return null;
    }
    try {
      return JobMetaService.getInstance()
          .getJobByIdentifier(
              NameIdentifierUtil.ofJob(metalake, JobHandle.JOB_ID_PREFIX + jobRunId));
    } catch (NoSuchEntityException e) {
      return null;
    }
  }

  @Nullable
  private static String metalakeNameForTable(long tableId) {
    List<TablePO> tables =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.listTablePOsByTableIds(Collections.singletonList(tableId)));
    if (tables == null || tables.isEmpty() || tables.get(0).getMetalakeId() == null) {
      return null;
    }
    MetalakePO metalake =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.selectMetalakeMetaById(tables.get(0).getMetalakeId()));
    return metalake == null ? null : metalake.getMetalakeName();
  }
}
