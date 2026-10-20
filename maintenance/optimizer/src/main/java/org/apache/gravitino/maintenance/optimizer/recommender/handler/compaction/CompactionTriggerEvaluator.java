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
package org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.recommender.util.ExpressionEvaluator;
import org.apache.gravitino.maintenance.optimizer.recommender.util.QLExpressionEvaluator;
import org.apache.gravitino.policy.IcebergDataCompactionContent;

/**
 * Evaluates Iceberg compaction {@code trigger-expr} with the same QL expression stack used by
 * {@link CompactionStrategyHandler} / {@code BaseExpressionStrategyHandler}.
 *
 * <p>Intended for callers that already have table-level metric values (for example TMS in-job
 * sample JSON) without constructing a full strategy handler.
 */
public final class CompactionTriggerEvaluator {

  private static final ExpressionEvaluator EVALUATOR = new QLExpressionEvaluator();

  private CompactionTriggerEvaluator() {}

  /**
   * @param content compaction policy content (provides numeric rules + trigger-expr)
   * @param metricValues sampled metric name → value (e.g. {@code custom-data-file-mse})
   * @return whether maintenance should run
   */
  public static boolean shouldMaintain(
      IcebergDataCompactionContent content, Map<String, Object> metricValues) {
    Preconditions.checkNotNull(content, "content");
    Preconditions.checkNotNull(metricValues, "metricValues");

    Map<String, Object> context = new HashMap<>();
    // Mirror BaseExpressionStrategyHandler#getRulesExpressionContext: numeric rules only.
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
                // Non-numeric rule values (expressions, strings) are not QL bindings.
              }
            });
    context.putAll(metricValues);

    Object trigger = content.rules().get(IcebergDataCompactionContent.TRIGGER_EXPR_KEY);
    String expression =
        trigger == null || trigger.toString().trim().isEmpty()
            ? "false"
            : trigger.toString().trim();
    return EVALUATOR.evaluateBool(expression, context);
  }
}
