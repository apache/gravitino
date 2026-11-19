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
package org.apache.gravitino.maintenance.optimizer.recommender.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared QL evaluation of policy {@code trigger-expr} against sampled metrics.
 *
 * <p>Used by compaction and rewrite-manifests facades ({@code CompactionTriggerEvaluator} / {@code
 * ManifestRewriteTriggerEvaluator}) so both keep type-specific {@code shouldMaintain} entry points
 * while sharing one expression stack.
 */
public final class TriggerExprEvaluator {

  /**
   * Rule key for the boolean trigger expression (same as policy content {@code TRIGGER_EXPR_KEY}).
   */
  public static final String TRIGGER_EXPR_KEY = "trigger-expr";

  private static final ExpressionEvaluator EVALUATOR = new QLExpressionEvaluator();

  private TriggerExprEvaluator() {}

  /**
   * Evaluates {@code trigger-expr} from a policy {@code rules()} map against sampled metrics.
   *
   * <p>Numeric rule values become QL bindings first; metric values overlay them. Missing or blank
   * {@code trigger-expr} evaluates as {@code false}.
   *
   * @param rules policy rules (thresholds + {@code trigger-expr})
   * @param metricValues sampled metric name → value
   * @return whether maintenance should run
   */
  public static boolean evaluate(Map<String, Object> rules, Map<String, Object> metricValues) {
    Preconditions.checkNotNull(rules, "rules");
    Preconditions.checkNotNull(metricValues, "metricValues");

    Map<String, Object> context = new HashMap<>();
    // Mirror BaseExpressionStrategyHandler#getRulesExpressionContext: numeric rules only.
    rules.forEach(
        (key, value) -> {
          if (value == null) {
            return;
          }
          try {
            context.put(key, Long.parseLong(value.toString()));
          } catch (NumberFormatException ignored) {
            // Non-numeric rule values (expressions, strings) are not QL bindings.
          }
        });
    context.putAll(metricValues);

    Object trigger = rules.get(TRIGGER_EXPR_KEY);
    String expression =
        trigger == null || trigger.toString().trim().isEmpty()
            ? "false"
            : trigger.toString().trim();
    return EVALUATOR.evaluateBool(expression, context);
  }
}
