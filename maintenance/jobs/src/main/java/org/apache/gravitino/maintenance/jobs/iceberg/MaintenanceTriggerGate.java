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
package org.apache.gravitino.maintenance.jobs.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.TriggerExprEvaluator;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Spark maintain-or-skip gate after S0 metrics (§5.2.2).
 *
 * <p>Only the four Iceberg maintenance {@link Policy.BuiltInType}s are accepted. Snapshot expiry
 * and orphan cleanup always maintain. Compaction / rewrite-manifests evaluate enqueue-time {@code
 * trigger-expr} via {@link TriggerExprEvaluator} after required S0 fields are present.
 */
final class MaintenanceTriggerGate {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceTriggerGate.class);

  private MaintenanceTriggerGate() {}

  /**
   * @param policyType one of the four Iceberg maintenance {@link Policy.BuiltInType}s
   * @param rules per-task policy {@code rules()} object (may be empty → type defaults)
   * @param metricsJson S0 metrics JSON
   * @return whether the procedure should run
   */
  static boolean shouldMaintain(
      Policy.BuiltInType policyType,
      @Nullable Map<String, Object> rules,
      @Nullable String metricsJson) {
    Preconditions.checkNotNull(policyType, "policyType");
    switch (policyType) {
      case ICEBERG_SNAPSHOT_EXPIRATION:
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        // Optimizer schedules these; in-job gate does not re-evaluate trigger-expr.
        return true;
      case ICEBERG_COMPACTION:
        return evaluateTrigger(
            policyType,
            effectiveRules(rules, PolicyContents.icebergDataCompaction().rules()),
            metricsJson,
            "Compaction recommend skip: metrics JSON missing compaction fields",
            IcebergDataCompactionContent.DATA_FILE_MSE_METRIC,
            IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC);
      case ICEBERG_REWRITE_MANIFESTS:
        return evaluateTrigger(
            policyType,
            effectiveRules(rules, PolicyContents.icebergRewriteManifests().rules()),
            metricsJson,
            "Manifest rewrite recommend skip: metrics JSON missing manifest fields",
            IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC);
      default:
        throw new IllegalArgumentException(
            "Unsupported maintenance policy type: " + policyType.policyType());
    }
  }

  private static boolean evaluateTrigger(
      Policy.BuiltInType policyType,
      Map<String, Object> effectiveRules,
      @Nullable String metricsJson,
      String missingMetricsMessage,
      String... requiredMetricKeys) {
    Map<String, Object> metrics = parseNumericMetrics(metricsJson);
    if (metrics.isEmpty() || !containsAll(metrics, requiredMetricKeys)) {
      LOG.warn(missingMetricsMessage);
      return false;
    }

    boolean maintain = TriggerExprEvaluator.evaluate(effectiveRules, metrics);
    LOG.info(
        "In-Spark recommend type={} expr={} -> {}",
        policyType.policyType(),
        effectiveRules.get(TriggerExprEvaluator.TRIGGER_EXPR_KEY),
        maintain ? "MAINTAIN" : "SKIP");
    return maintain;
  }

  private static Map<String, Object> effectiveRules(
      @Nullable Map<String, Object> rules, Map<String, Object> typeDefaults) {
    if (rules != null && !rules.isEmpty()) {
      return rules;
    }
    return new HashMap<>(typeDefaults);
  }

  private static boolean containsAll(Map<String, Object> metrics, String... keys) {
    for (String key : keys) {
      if (!metrics.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, Object> parseNumericMetrics(@Nullable String metricsJson) {
    Map<String, Object> metrics = new HashMap<>();
    if (metricsJson == null || metricsJson.trim().isEmpty()) {
      return metrics;
    }
    try {
      JsonNode root = JsonUtils.anyFieldMapper().readTree(metricsJson);
      Iterator<Entry<String, JsonNode>> fields = root.fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> field = fields.next();
        JsonNode value = field.getValue();
        if (value == null || value.isNull() || !value.isNumber()) {
          continue;
        }
        metrics.put(
            field.getKey(), value.isIntegralNumber() ? value.longValue() : value.doubleValue());
      }
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to parse metrics JSON: {}", e.toString());
    }
    return metrics;
  }
}
