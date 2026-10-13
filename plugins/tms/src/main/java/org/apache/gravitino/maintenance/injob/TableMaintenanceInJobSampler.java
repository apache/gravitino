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
package org.apache.gravitino.maintenance.injob;

import com.google.common.base.Preconditions;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;

/**
 * In-Spark sampling path for TMS cron jobs (§5.2.2): sample once at {@code S0}, persist metrics,
 * record {@code before_snapshot_id}, then gate on snapshot drift without re-sampling.
 */
public class TableMaintenanceInJobSampler {

  private final TableSnapshotMetricsMetaService metricsMetaService;
  private final TableMaintenanceActionMetaService actionMetaService;

  /** Creates a sampler using the default relational meta services. */
  public TableMaintenanceInJobSampler() {
    this(
        TableSnapshotMetricsMetaService.getInstance(),
        TableMaintenanceActionMetaService.getInstance());
  }

  /**
   * @param metricsMetaService metrics DAO
   * @param actionMetaService maintenance action DAO
   */
  public TableMaintenanceInJobSampler(
      TableSnapshotMetricsMetaService metricsMetaService,
      TableMaintenanceActionMetaService actionMetaService) {
    this.metricsMetaService = Preconditions.checkNotNull(metricsMetaService);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
  }

  /**
   * Persists metrics for {@code S0}, sets {@code before_snapshot_id} on the maintenance action row,
   * and returns whether the cron Spark Job should continue.
   *
   * <p>If HEAD has drifted after the sample, the caller must end the job early and must not
   * re-sample in a loop (§4.3).
   *
   * @param jobId Gravitino {@code job_run_meta.job_run_id} for the in-flight maintenance row
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param sampledSnapshotId Iceberg snapshot id at sample time ({@code S0})
   * @param metricsJson serialized metrics payload for {@code table_snapshot_metrics}
   * @param head supplies current HEAD after persistence
   * @return continue vs drift stop outcome
   */
  public InJobGateOutcome sampleAndGateCronJob(
      long jobId,
      long tableId,
      long policyId,
      long sampledSnapshotId,
      String metricsJson,
      SnapshotHead head) {
    Preconditions.checkNotNull(metricsJson, "metricsJson");
    Preconditions.checkNotNull(head, "head");

    metricsMetaService.insertMetrics(tableId, policyId, sampledSnapshotId, metricsJson);
    actionMetaService.recordBeforeSnapshot(jobId, sampledSnapshotId);

    long headSnapshotId = head.currentSnapshotId();
    if (sampledSnapshotId != headSnapshotId) {
      return InJobGateOutcome.stop(
          InJobGateOutcome.StopReason.SNAPSHOT_DRIFT, sampledSnapshotId, headSnapshotId);
    }
    return InJobGateOutcome.continueMaintenance(sampledSnapshotId);
  }

  /**
   * Evaluates drift for a commit-chain maintenance type after that type's sample at {@code S0}
   * (§5.2.3). When drift is detected the caller ends only the current type and continues the chain.
   *
   * @param sampledSnapshotId {@code S0} for the current type
   * @param head supplies current HEAD after persistence for this type
   * @return continue vs drift stop outcome
   */
  public InJobGateOutcome gateCommitChainType(long sampledSnapshotId, SnapshotHead head) {
    Preconditions.checkNotNull(head, "head");
    long headSnapshotId = head.currentSnapshotId();
    if (sampledSnapshotId != headSnapshotId) {
      return InJobGateOutcome.stop(
          InJobGateOutcome.StopReason.SNAPSHOT_DRIFT, sampledSnapshotId, headSnapshotId);
    }
    return InJobGateOutcome.continueMaintenance(sampledSnapshotId);
  }
}
