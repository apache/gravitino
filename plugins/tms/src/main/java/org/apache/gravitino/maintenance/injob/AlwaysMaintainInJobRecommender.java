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

/**
 * In-job recommender that always returns {@link RecommendDecision#MAINTAIN}.
 *
 * <p>Production {@link TypeDispatchingInJobRecommender} uses this for expire / rewrite-manifests
 * placeholders: cron already scheduled the work, while policy content still ships {@code
 * trigger-expr=false} for optimizer-only gating. Also available for tests and injectable overrides.
 */
public final class AlwaysMaintainInJobRecommender implements InJobRecommender {

  /** Shared instance. */
  public static final AlwaysMaintainInJobRecommender INSTANCE =
      new AlwaysMaintainInJobRecommender();

  private AlwaysMaintainInJobRecommender() {}

  @Override
  public RecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    return RecommendDecision.MAINTAIN;
  }
}
