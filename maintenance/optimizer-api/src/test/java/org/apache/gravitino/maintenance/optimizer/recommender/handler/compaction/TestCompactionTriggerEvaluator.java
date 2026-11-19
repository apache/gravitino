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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CompactionTriggerEvaluator}. */
public class TestCompactionTriggerEvaluator {

  @Test
  public void testCompactionContentDefaultExpr() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent) PolicyContents.icebergDataCompaction();
    Map<String, Object> below = new HashMap<>();
    below.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 0L);
    below.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
    Assertions.assertFalse(CompactionTriggerEvaluator.shouldMaintain(content, below));

    Map<String, Object> above = new HashMap<>();
    above.put(
        IcebergDataCompactionContent.DATA_FILE_MSE_METRIC,
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE);
    above.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
    Assertions.assertTrue(CompactionTriggerEvaluator.shouldMaintain(content, above));
  }

  @Test
  public void testCustomTriggerExprAndNumericRuleBinding() {
    Map<String, Object> rules = new HashMap<>();
    rules.put(
        IcebergDataCompactionContent.TRIGGER_EXPR_KEY, "custom-data-file-mse >= minDataFileMse");
    rules.put(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY, 10);
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 10L);
    metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
    Assertions.assertTrue(CompactionTriggerEvaluator.shouldMaintain(rules, metrics));

    metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 9L);
    Assertions.assertFalse(CompactionTriggerEvaluator.shouldMaintain(rules, metrics));
  }

  @Test
  public void testMissingOrBlankTriggerExprIsFalse() {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 500L);
    metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
    Assertions.assertFalse(CompactionTriggerEvaluator.shouldMaintain(Map.of(), metrics));

    Map<String, Object> blank = new HashMap<>();
    blank.put(IcebergDataCompactionContent.TRIGGER_EXPR_KEY, "   ");
    Assertions.assertFalse(CompactionTriggerEvaluator.shouldMaintain(blank, metrics));
  }

  @Test
  public void testMetricValuesOverlayNumericRules() {
    Map<String, Object> rules = new HashMap<>();
    rules.put(
        IcebergDataCompactionContent.TRIGGER_EXPR_KEY, "custom-data-file-mse >= minDataFileMse");
    rules.put(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY, 100);
    // Metric uses the same name as a rule key; metric overlay wins.
    rules.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 0L);
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 100L);
    metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
    Assertions.assertTrue(CompactionTriggerEvaluator.shouldMaintain(rules, metrics));
  }
}
