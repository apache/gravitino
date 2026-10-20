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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionTriggerEvaluator;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.maintenance.scheduler.RelationalPolicyLookup;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-job recommender for Iceberg compaction. Loads policy content and evaluates {@code
 * trigger-expr} via {@link CompactionTriggerEvaluator} (same QL path as the optimizer strategy
 * handler).
 */
public final class CompactionInJobRecommender implements InJobRecommender {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionInJobRecommender.class);

  private final PolicyLookup policyLookup;

  /** Uses relational policy lookup. */
  public CompactionInJobRecommender() {
    this(new RelationalPolicyLookup());
  }

  /**
   * @param policyLookup loads policy content by id
   */
  public CompactionInJobRecommender(PolicyLookup policyLookup) {
    this.policyLookup = Preconditions.checkNotNull(policyLookup);
  }

  @Override
  public RecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    Optional<PolicyEntity> loaded = policyLookup.findById(policyId);
    if (loaded.isEmpty()) {
      LOG.warn("Compaction recommend skip: policy {} not found (table={})", policyId, tableId);
      return RecommendDecision.SKIP;
    }
    PolicyContent content = loaded.get().content();
    if (!(content instanceof IcebergDataCompactionContent)) {
      LOG.warn(
          "Compaction recommend skip: policy {} is not IcebergDataCompactionContent (table={})",
          policyId,
          tableId);
      return RecommendDecision.SKIP;
    }
    IcebergDataCompactionContent compaction = (IcebergDataCompactionContent) content;
    Map<String, Object> metrics = parseMetricValues(metricsJson);
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

  private static Map<String, Object> parseMetricValues(String metricsJson) {
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
        if (value.isIntegralNumber()) {
          metrics.put(field.getKey(), value.longValue());
        } else {
          metrics.put(field.getKey(), value.doubleValue());
        }
      }
      return metrics;
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to parse compaction metrics JSON: {}", e.toString());
      return metrics;
    }
  }
}
