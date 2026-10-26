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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergMaintenanceJob.MaintenanceType;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionTriggerEvaluator;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.PolicyContents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Spark maintain-or-skip gate after S0 metrics (§5.2.2).
 *
 * <p>Evaluates enqueue-time policy rules locally. Does not call Gravitino. Expire /
 * rewrite-manifests placeholders always maintain (optimizer-only {@code trigger-expr=false} on
 * those types).
 */
final class MaintenanceTriggerGate {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceTriggerGate.class);

  /** Spark job argument key: JSON map of wire maintenance type → policy {@code rules()} object. */
  static final String RULES_ARG = "rules";

  private MaintenanceTriggerGate() {}

  /**
   * @param type maintenance type for this sample
   * @param argMap Spark job arguments (includes optional {@link #RULES_ARG})
   * @param metricsJson S0 metrics JSON
   * @return whether the procedure should run
   */
  static boolean shouldMaintain(
      MaintenanceType type, Map<String, String> argMap, @Nullable String metricsJson) {
    if (type == MaintenanceType.EXPIRE_SNAPSHOTS || type == MaintenanceType.REWRITE_MANIFESTS) {
      return true;
    }

    Map<String, Object> rules = rulesForType(type, argMap);
    Map<String, Object> metrics = parseNumericMetrics(metricsJson);
    if (type == MaintenanceType.REWRITE_DATA_FILES
        && (metrics.isEmpty()
            || !metrics.containsKey(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC)
            || !metrics.containsKey(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC))) {
      LOG.warn("Compaction recommend skip: metrics JSON missing compaction fields");
      return false;
    }

    boolean maintain = CompactionTriggerEvaluator.shouldMaintain(rules, metrics);
    LOG.info(
        "In-Spark recommend type={} expr={} -> {}",
        type.wireName(),
        rules.get(IcebergDataCompactionContent.TRIGGER_EXPR_KEY),
        maintain ? "MAINTAIN" : "SKIP");
    return maintain;
  }

  private static Map<String, Object> rulesForType(
      MaintenanceType type, Map<String, String> argMap) {
    Map<String, Map<String, Object>> byType = Collections.emptyMap();
    if (argMap != null) {
      String raw = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get(RULES_ARG));
      if (raw != null && !raw.trim().isEmpty()) {
        try {
          Map<String, Map<String, Object>> parsed =
              JsonUtils.anyFieldMapper()
                  .readValue(raw, new TypeReference<Map<String, Map<String, Object>>>() {});
          if (parsed != null) {
            byType = parsed;
          }
        } catch (IOException | RuntimeException e) {
          LOG.warn("Failed to parse {}: {}", RULES_ARG, e.toString());
        }
      }
    }
    Map<String, Object> rules = byType.get(type.wireName());
    if (rules != null && !rules.isEmpty()) {
      return rules;
    }
    if (type == MaintenanceType.REWRITE_DATA_FILES) {
      return new HashMap<>(PolicyContents.icebergDataCompaction().rules());
    }
    if (type == MaintenanceType.REMOVE_ORPHAN_FILES) {
      return new HashMap<>(PolicyContents.icebergOrphanFileRemoval().rules());
    }
    Map<String, Object> empty = new HashMap<>();
    empty.put(IcebergOrphanFileRemovalContent.TRIGGER_EXPR_KEY, "false");
    return empty;
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
