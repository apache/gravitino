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
import java.util.Optional;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.maintenance.scheduler.RelationalPolicyLookup;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches in-job recommenders by policy type. Compaction uses threshold evaluation; other
 * maintenance types keep {@link AlwaysMaintainInJobRecommender} until type-specific content lands.
 */
public final class TypeDispatchingInJobRecommender implements InJobRecommender {

  private static final Logger LOG = LoggerFactory.getLogger(TypeDispatchingInJobRecommender.class);

  private final PolicyLookup policyLookup;
  private final InJobRecommender compactionRecommender;
  private final InJobRecommender defaultRecommender;

  /** Production wiring. */
  public TypeDispatchingInJobRecommender() {
    this(
        new RelationalPolicyLookup(),
        new CompactionInJobRecommender(),
        AlwaysMaintainInJobRecommender.INSTANCE);
  }

  /**
   * @param policyLookup policy loader
   * @param compactionRecommender compaction thresholds
   * @param defaultRecommender fallback for non-compaction types
   */
  public TypeDispatchingInJobRecommender(
      PolicyLookup policyLookup,
      InJobRecommender compactionRecommender,
      InJobRecommender defaultRecommender) {
    this.policyLookup = Preconditions.checkNotNull(policyLookup);
    this.compactionRecommender = Preconditions.checkNotNull(compactionRecommender);
    this.defaultRecommender = Preconditions.checkNotNull(defaultRecommender);
  }

  @Override
  public RecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    Optional<PolicyEntity> loaded = policyLookup.findById(policyId);
    if (loaded.isEmpty()) {
      LOG.warn("In-job recommend: policy {} missing; skip (table={})", policyId, tableId);
      return RecommendDecision.SKIP;
    }
    PolicyEntity policy = loaded.get();
    if (policy.policyType() == Policy.BuiltInType.ICEBERG_COMPACTION
        || policy.content() instanceof IcebergDataCompactionContent) {
      return compactionRecommender.recommend(tableId, policyId, sampledSnapshotId, metricsJson);
    }
    return defaultRecommender.recommend(tableId, policyId, sampledSnapshotId, metricsJson);
  }
}
