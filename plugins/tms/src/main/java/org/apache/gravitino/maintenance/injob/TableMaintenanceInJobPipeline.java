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

/**
 * Cron / commit-chain in-job gates: sample at {@code S0}, stop on snapshot drift, then apply the
 * recommend maintain? gate (§5.2.2). Does not execute the four maintenance types.
 */
public class TableMaintenanceInJobPipeline {

  private final TableMaintenanceInJobSampler sampler;
  private final InJobRecommender recommender;

  /**
   * @param sampler S0 sample + drift gate
   * @param recommender maintain vs skip after a clean sample
   */
  public TableMaintenanceInJobPipeline(
      TableMaintenanceInJobSampler sampler, InJobRecommender recommender) {
    this.sampler = Preconditions.checkNotNull(sampler);
    this.recommender = Preconditions.checkNotNull(recommender);
  }

  /**
   * Runs sample → drift → recommend for a cron Spark Job.
   *
   * @param jobId Gravitino {@code job_run_meta.job_run_id}
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param sampledSnapshotId {@code S0}
   * @param metricsJson metrics payload
   * @param head current HEAD supplier after persistence
   * @return continue only when HEAD matches {@code S0} and recommender says maintain
   */
  public InJobGateOutcome gateCronJob(
      long jobId,
      long tableId,
      long policyId,
      long sampledSnapshotId,
      String metricsJson,
      SnapshotHead head) {
    InJobGateOutcome afterSample =
        sampler.sampleAndGateCronJob(
            jobId, tableId, policyId, sampledSnapshotId, metricsJson, head);
    if (!afterSample.continueMaintenance()) {
      return afterSample;
    }

    RecommendDecision decision =
        recommender.recommend(tableId, policyId, sampledSnapshotId, metricsJson);
    if (decision == RecommendDecision.SKIP) {
      return InJobGateOutcome.stop(
          InJobGateOutcome.StopReason.RECOMMENDER_SKIP,
          sampledSnapshotId,
          afterSample.headSnapshotId());
    }
    return afterSample;
  }

  /**
   * Drift then recommend for one type inside a commit-chain job (§5.2.3). Caller ends only the
   * current type on stop; does not re-sample.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param sampledSnapshotId {@code S0} for this type
   * @param metricsJson metrics for this type's sample
   * @param head current HEAD after this type's sample persistence
   * @return continue vs stop for this type
   */
  public InJobGateOutcome gateCommitChainType(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson, SnapshotHead head) {
    InJobGateOutcome afterDrift = sampler.gateCommitChainType(sampledSnapshotId, head);
    if (!afterDrift.continueMaintenance()) {
      return afterDrift;
    }
    RecommendDecision decision =
        recommender.recommend(tableId, policyId, sampledSnapshotId, metricsJson);
    if (decision == RecommendDecision.SKIP) {
      return InJobGateOutcome.stop(
          InJobGateOutcome.StopReason.RECOMMENDER_SKIP,
          sampledSnapshotId,
          afterDrift.headSnapshotId());
    }
    return afterDrift;
  }
}
