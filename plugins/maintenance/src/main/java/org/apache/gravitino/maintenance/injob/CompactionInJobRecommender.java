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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionTriggerEvaluator;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-job recommender for Iceberg compaction. Evaluates {@code trigger-expr} via {@link
 * CompactionTriggerEvaluator} (same QL path as the optimizer strategy handler).
 */
public final class CompactionInJobRecommender implements InJobRecommender {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionInJobRecommender.class);
  private static final PolicyMetaService POLICY_SERVICE = PolicyMetaService.getInstance();

  private final Function<Long, Optional<PolicyEntity>> policyLoader;

  /** Loads policies through {@link PolicyMetaService}. */
  public CompactionInJobRecommender() {
    this(CompactionInJobRecommender::loadPolicy);
  }

  /**
   * @param policyLoader loads a policy by id when the caller did not supply a policy entity
   */
  CompactionInJobRecommender(Function<Long, Optional<PolicyEntity>> policyLoader) {
    this.policyLoader = Preconditions.checkNotNull(policyLoader);
  }

  @Override
  public RecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    return recommend(tableId, policyId, sampledSnapshotId, metricsJson, null);
  }

  @Override
  public RecommendDecision recommend(
      long tableId,
      long policyId,
      long sampledSnapshotId,
      String metricsJson,
      @Nullable PolicyEntity policy) {
    IcebergDataCompactionContent compaction = resolveCompactionContent(tableId, policyId, policy);
    if (compaction == null) {
      return RecommendDecision.SKIP;
    }
    Map<String, Object> metrics = MetricsJson.parseNumericValues(metricsJson);
    if (metrics.isEmpty()
        || !metrics.containsKey(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC)
        || !metrics.containsKey(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC)) {
      LOG.warn(
          "Compaction recommend skip: metrics JSON missing compaction fields for table {} policy {}",
          tableId,
          policyId);
      return RecommendDecision.SKIP;
    }
    boolean maintain = CompactionTriggerEvaluator.shouldMaintain(compaction, metrics);
    LOG.info(
        "Compaction recommend table={} policy={} snapshot={} metrics={} -> {}",
        tableId,
        policyId,
        sampledSnapshotId,
        metrics,
        maintain ? "MAINTAIN" : "SKIP");
    return maintain ? RecommendDecision.MAINTAIN : RecommendDecision.SKIP;
  }

  @Nullable
  private IcebergDataCompactionContent resolveCompactionContent(
      long tableId, long policyId, @Nullable PolicyEntity policy) {
    PolicyContent content;
    if (policy != null) {
      content = policy.content();
    } else {
      Optional<PolicyEntity> loaded = policyLoader.apply(policyId);
      if (loaded.isEmpty()) {
        LOG.warn("Compaction recommend skip: policy {} not found (table={})", policyId, tableId);
        return null;
      }
      content = loaded.get().content();
    }
    if (!(content instanceof IcebergDataCompactionContent)) {
      LOG.warn(
          "Compaction recommend skip: policy {} is not IcebergDataCompactionContent (table={})",
          policyId,
          tableId);
      return null;
    }
    return (IcebergDataCompactionContent) content;
  }

  private static Optional<PolicyEntity> loadPolicy(long policyId) {
    return Optional.ofNullable(POLICY_SERVICE.getPolicyById(policyId));
  }
}
