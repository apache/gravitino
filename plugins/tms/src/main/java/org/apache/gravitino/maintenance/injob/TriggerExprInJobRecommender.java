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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.maintenance.scheduler.RelationalPolicyLookup;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-job recommender that evaluates policy {@code trigger-expr} with the shared QL stack (numeric
 * rules + sampled metric bindings). Used for types whose content already exposes a real
 * trigger-expr (for example orphan cleanup {@code true}). Expire / rewrite-manifests placeholders
 * are dispatched to {@link AlwaysMaintainInJobRecommender} instead.
 */
public final class TriggerExprInJobRecommender implements InJobRecommender {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerExprInJobRecommender.class);

  /** Shared rule key on all Iceberg maintenance policy contents. */
  private static final String TRIGGER_EXPR_KEY = "trigger-expr";

  private final PolicyLookup policyLookup;
  private final QLExpressionEvaluator evaluator;

  /** Uses relational policy lookup. */
  public TriggerExprInJobRecommender() {
    this(new RelationalPolicyLookup());
  }

  /**
   * @param policyLookup loads policy content by id when the caller did not supply a policy entity
   */
  public TriggerExprInJobRecommender(PolicyLookup policyLookup) {
    this.policyLookup = Preconditions.checkNotNull(policyLookup);
    this.evaluator = new QLExpressionEvaluator();
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
    PolicyContent content = resolveContent(tableId, policyId, policy);
    if (content == null) {
      return RecommendDecision.SKIP;
    }
    Object trigger = content.rules().get(TRIGGER_EXPR_KEY);
    String expression =
        trigger == null || trigger.toString().trim().isEmpty()
            ? "false"
            : trigger.toString().trim();

    Map<String, Object> context = new HashMap<>();
    content
        .rules()
        .forEach(
            (key, value) -> {
              if (value == null) {
                return;
              }
              try {
                context.put(key, Long.parseLong(value.toString()));
              } catch (NumberFormatException ignored) {
                // Non-numeric rule values are not QL bindings.
              }
            });
    context.putAll(MetricsJson.parseNumericValues(metricsJson));

    boolean maintain = evaluator.evaluateBool(expression, context);
    LOG.info(
        "Trigger-expr recommend table={} policy={} snapshot={} expr={} -> {}",
        tableId,
        policyId,
        sampledSnapshotId,
        expression,
        maintain ? "MAINTAIN" : "SKIP");
    return maintain ? RecommendDecision.MAINTAIN : RecommendDecision.SKIP;
  }

  @Nullable
  private PolicyContent resolveContent(long tableId, long policyId, @Nullable PolicyEntity policy) {
    if (policy != null) {
      return policy.content();
    }
    Optional<PolicyEntity> loaded = policyLookup.findById(policyId);
    if (loaded.isEmpty()) {
      LOG.warn("Trigger-expr recommend skip: policy {} not found (table={})", policyId, tableId);
      return null;
    }
    return loaded.get().content();
  }
}
