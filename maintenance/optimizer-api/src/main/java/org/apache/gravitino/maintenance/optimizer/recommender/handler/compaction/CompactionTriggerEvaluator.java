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
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.recommender.util.TriggerExprEvaluator;
import org.apache.gravitino.policy.IcebergDataCompactionContent;

/**
 * Compaction-only {@code trigger-expr} evaluation against sampled metrics.
 *
 * <p>For rewrite-manifests use {@code ManifestRewriteTriggerEvaluator}. Shared QL evaluation lives
 * in {@link TriggerExprEvaluator}.
 */
public final class CompactionTriggerEvaluator {

  private CompactionTriggerEvaluator() {}

  /**
   * @param content compaction policy content (provides numeric rules + trigger-expr)
   * @param metricValues sampled metric name → value (e.g. {@code custom-data-file-mse})
   * @return whether compaction should run
   */
  public static boolean shouldMaintain(
      IcebergDataCompactionContent content, Map<String, Object> metricValues) {
    Preconditions.checkNotNull(content, "content");
    return shouldMaintain(content.rules(), metricValues);
  }

  /**
   * Evaluates compaction {@code trigger-expr} from a policy {@code rules()} map against sampled
   * metrics.
   *
   * @param rules compaction policy rules (thresholds + {@code trigger-expr})
   * @param metricValues sampled metric name → value
   * @return whether compaction should run
   */
  public static boolean shouldMaintain(
      Map<String, Object> rules, Map<String, Object> metricValues) {
    return TriggerExprEvaluator.evaluate(rules, metricValues);
  }
}
