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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches in-job recommenders by policy type.
 *
 * <ul>
 *   <li>Compaction → threshold / {@code trigger-expr} evaluation
 *   <li>Orphan cleanup → policy {@code trigger-expr} ({@code true} by default)
 *   <li>Expire / rewrite-manifests placeholders → always maintain (cron already scheduled the work;
 *       content still ships {@code trigger-expr=false} for optimizer-only gating)
 * </ul>
 */
public final class TypeDispatchingInJobRecommender implements InJobRecommender {

  private static final Logger LOG = LoggerFactory.getLogger(TypeDispatchingInJobRecommender.class);
  private static final PolicyMetaService POLICY_SERVICE = PolicyMetaService.getInstance();

  private final Function<Long, Optional<PolicyEntity>> policyLoader;
  private final InJobRecommender compactionRecommender;
  private final InJobRecommender triggerExprRecommender;
  private final InJobRecommender placeholderRecommender;

  /** Production wiring. Loads policies through {@link PolicyMetaService}. */
  public TypeDispatchingInJobRecommender() {
    this(TypeDispatchingInJobRecommender::loadPolicy);
  }

  private TypeDispatchingInJobRecommender(Function<Long, Optional<PolicyEntity>> policyLoader) {
    this(
        policyLoader,
        new CompactionInJobRecommender(policyLoader),
        new TriggerExprInJobRecommender(policyLoader),
        AlwaysMaintainInJobRecommender.INSTANCE);
  }

  /**
   * @param policyLoader loads a policy by id
   * @param compactionRecommender compaction thresholds
   * @param triggerExprRecommender trigger-expr evaluation (orphan and similar)
   * @param placeholderRecommender expire / rewrite-manifests placeholders until real thresholds
   *     land
   */
  TypeDispatchingInJobRecommender(
      Function<Long, Optional<PolicyEntity>> policyLoader,
      InJobRecommender compactionRecommender,
      InJobRecommender triggerExprRecommender,
      InJobRecommender placeholderRecommender) {
    this.policyLoader = Preconditions.checkNotNull(policyLoader);
    this.compactionRecommender = Preconditions.checkNotNull(compactionRecommender);
    this.triggerExprRecommender = Preconditions.checkNotNull(triggerExprRecommender);
    this.placeholderRecommender = Preconditions.checkNotNull(placeholderRecommender);
  }

  /**
   * Test wiring that routes expire / rewrite-manifests placeholders to {@link
   * AlwaysMaintainInJobRecommender#INSTANCE}.
   *
   * @param policyLoader loads a policy by id
   * @param compactionRecommender compaction thresholds
   * @param triggerExprRecommender trigger-expr evaluation for orphan and similar
   */
  @VisibleForTesting
  TypeDispatchingInJobRecommender(
      Function<Long, Optional<PolicyEntity>> policyLoader,
      InJobRecommender compactionRecommender,
      InJobRecommender triggerExprRecommender) {
    this(
        policyLoader,
        compactionRecommender,
        triggerExprRecommender,
        AlwaysMaintainInJobRecommender.INSTANCE);
  }

  @Override
  public RecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    Optional<PolicyEntity> loaded = policyLoader.apply(policyId);
    if (loaded.isEmpty()) {
      LOG.warn("In-job recommend: policy {} missing; skip (table={})", policyId, tableId);
      return RecommendDecision.SKIP;
    }
    PolicyEntity policy = loaded.get();
    if (policy.policyType() == Policy.BuiltInType.ICEBERG_COMPACTION
        || policy.content() instanceof IcebergDataCompactionContent) {
      return compactionRecommender.recommend(
          tableId, policyId, sampledSnapshotId, metricsJson, policy);
    }
    if (isPlaceholderMaintenance(policy)) {
      return placeholderRecommender.recommend(
          tableId, policyId, sampledSnapshotId, metricsJson, policy);
    }
    return triggerExprRecommender.recommend(
        tableId, policyId, sampledSnapshotId, metricsJson, policy);
  }

  private static boolean isPlaceholderMaintenance(PolicyEntity policy) {
    Policy.BuiltInType type = policy.policyType();
    return type == Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION
        || type == Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS
        || policy.content() instanceof IcebergSnapshotExpirationContent
        || policy.content() instanceof IcebergRewriteManifestsContent;
  }

  private static Optional<PolicyEntity> loadPolicy(long policyId) {
    return Optional.ofNullable(POLICY_SERVICE.getPolicyById(policyId));
  }
}
