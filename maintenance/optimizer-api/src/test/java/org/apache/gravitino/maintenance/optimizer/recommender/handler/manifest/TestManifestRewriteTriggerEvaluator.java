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
package org.apache.gravitino.maintenance.optimizer.recommender.handler.manifest;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ManifestRewriteTriggerEvaluator}. */
public class TestManifestRewriteTriggerEvaluator {

  @Test
  public void testManifestContentDefaultExpr() {
    IcebergRewriteManifestsContent content = PolicyContents.icebergRewriteManifests();
    Map<String, Object> below = new HashMap<>();
    below.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 99L);
    Assertions.assertFalse(ManifestRewriteTriggerEvaluator.shouldMaintain(content, below));

    Map<String, Object> atThreshold = new HashMap<>();
    atThreshold.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 100L);
    Assertions.assertTrue(ManifestRewriteTriggerEvaluator.shouldMaintain(content, atThreshold));
  }

  @Test
  public void testManifestRulesDefaultExpr() {
    Map<String, Object> rules = PolicyContents.icebergRewriteManifests().rules();
    Map<String, Object> below = new HashMap<>();
    below.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 99L);
    Assertions.assertFalse(ManifestRewriteTriggerEvaluator.shouldMaintain(rules, below));

    Map<String, Object> atThreshold = new HashMap<>();
    atThreshold.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 100L);
    Assertions.assertTrue(ManifestRewriteTriggerEvaluator.shouldMaintain(rules, atThreshold));
  }

  @Test
  public void testCustomTriggerExprAndNumericRuleBinding() {
    Map<String, Object> rules = new HashMap<>();
    rules.put(IcebergRewriteManifestsContent.TRIGGER_EXPR_KEY, "custom-manifest-number >= bound");
    rules.put("bound", 10);
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 10L);
    Assertions.assertTrue(ManifestRewriteTriggerEvaluator.shouldMaintain(rules, metrics));

    metrics.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 9L);
    Assertions.assertFalse(ManifestRewriteTriggerEvaluator.shouldMaintain(rules, metrics));
  }

  @Test
  public void testMissingOrBlankTriggerExprIsFalse() {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 500L);
    Assertions.assertFalse(ManifestRewriteTriggerEvaluator.shouldMaintain(Map.of(), metrics));

    Map<String, Object> blank = new HashMap<>();
    blank.put(IcebergRewriteManifestsContent.TRIGGER_EXPR_KEY, "   ");
    Assertions.assertFalse(ManifestRewriteTriggerEvaluator.shouldMaintain(blank, metrics));
  }

  @Test
  public void testMetricValuesOverlayNumericRules() {
    Map<String, Object> rules = new HashMap<>();
    rules.put(
        IcebergRewriteManifestsContent.TRIGGER_EXPR_KEY,
        "custom-manifest-number >= manifest_count_threshold");
    rules.put(IcebergRewriteManifestsContent.MANIFEST_COUNT_THRESHOLD, 100);
    // Metric uses the same name as a rule key; metric overlay wins.
    rules.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 0L);
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, 100L);
    Assertions.assertTrue(ManifestRewriteTriggerEvaluator.shouldMaintain(rules, metrics));
  }
}
