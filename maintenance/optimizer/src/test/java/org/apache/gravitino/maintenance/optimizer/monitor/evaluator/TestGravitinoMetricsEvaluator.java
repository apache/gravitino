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

package org.apache.gravitino.maintenance.optimizer.monitor.evaluator;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricSample;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricScope;
import org.apache.gravitino.maintenance.optimizer.common.MetricSampleImpl;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetricsEvaluator {

  @Test
  public void testEvaluateMetricsWithMaxOperation() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:max:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(
                    metric(100L, "row_count", 10L),
                    metric(101L, "row_count", 50L),
                    metric(102L, "row_count", 20L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 30L))));

    Assertions.assertTrue(result);
  }

  @Test
  public void testEvaluateMetricsWithMinOperation() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:min:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 10L), metric(101L, "row_count", 30L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 12L))));

    Assertions.assertFalse(result);
  }

  @Test
  public void testEvaluateMetricsWithAvgOperation() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:avg:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 10L), metric(101L, "row_count", 20L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 15L))));

    Assertions.assertTrue(result);
  }

  @Test
  public void testEvaluateMetricsWithLatestOperation() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:latest:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 100L), metric(105L, "row_count", 10L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 20L))));

    Assertions.assertFalse(result);
  }

  @Test
  public void testEvaluateMetricsForPartitionUsesTableRules() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:latest:le");
    PartitionPath partitionPath =
        PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2026-02-14")));
    MetricScope scope =
        MetricScope.forPartition(NameIdentifier.parse("catalog.db.table"), partitionPath);

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 100L), metric(105L, "row_count", 10L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 20L))));

    Assertions.assertFalse(result);
  }

  @Test
  public void testEvaluateMetricsDistinguishTableAndJobRules() {
    GravitinoMetricsEvaluator evaluator =
        createEvaluator("table:row_count:avg:le,job:duration:latest:le");

    boolean tableResult =
        evaluator.evaluateMetrics(
            MetricScope.forTable(NameIdentifier.parse("catalog.db.table")),
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 10L), metric(101L, "row_count", 20L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 15L))));
    Assertions.assertTrue(tableResult);

    boolean jobResult =
        evaluator.evaluateMetrics(
            MetricScope.forJob(NameIdentifier.parse("job1")),
            Map.of("duration", List.of(metric(100L, "duration", 3L), metric(105L, "duration", 5L))),
            Map.of("duration", List.of(metric(110L, "duration", 10L))));
    Assertions.assertFalse(jobResult);
  }

  @Test
  public void testEvaluateMetricsMissingSelectedMetricSkipsRule() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:avg:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of("other_metric", List.of(metric(100L, "other_metric", 10L))),
            Map.of("other_metric", List.of(metric(110L, "other_metric", 5L))));

    Assertions.assertTrue(result);
  }

  @Test
  public void testEvaluateMetricsNonNumericValueSkipsRule() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:avg:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of("row_count", List.of(metricString(100L, "row_count", "invalid"))),
            Map.of("row_count", List.of(metricString(110L, "row_count", "invalid2"))));

    Assertions.assertTrue(result);
  }

  @Test
  public void testEvaluateMetricsWithoutRulesForScopeReturnsTrue() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:avg:le");

    boolean result =
        evaluator.evaluateMetrics(
            MetricScope.forJob(NameIdentifier.parse("job1")),
            Map.of("duration", List.of(metric(100L, "duration", 10L))),
            Map.of("duration", List.of(metric(110L, "duration", 20L))));

    Assertions.assertTrue(result);
  }

  @Test
  public void testEvaluateMetricsWithMixedCaseRuleMetricName() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:Row_Count:avg:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 10L), metric(101L, "row_count", 20L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 15L))));

    Assertions.assertTrue(result);
  }

  @Test
  public void testEvaluateMetricsWithEmptyBeforeAndAfterMetricsReturnsTrue() {
    GravitinoMetricsEvaluator evaluator = createEvaluator("table:row_count:avg:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result = evaluator.evaluateMetrics(scope, Map.of(), Map.of());
    Assertions.assertTrue(result);

    boolean emptyListResult =
        evaluator.evaluateMetrics(
            scope, Map.of("row_count", List.of()), Map.of("row_count", List.of()));
    Assertions.assertTrue(emptyListResult);
  }

  @Test
  public void testEvaluateMetricsReturnsFalseWhenAnyRuleFails() {
    GravitinoMetricsEvaluator evaluator =
        createEvaluator("table:row_count:avg:le,table:file_count:max:le");
    MetricScope scope = MetricScope.forTable(NameIdentifier.parse("catalog.db.table"));

    boolean result =
        evaluator.evaluateMetrics(
            scope,
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 10L), metric(101L, "row_count", 20L)),
                "file_count",
                List.of(metric(100L, "file_count", 5L), metric(101L, "file_count", 10L))),
            Map.of(
                "row_count",
                List.of(metric(110L, "row_count", 15L)),
                "file_count",
                List.of(metric(110L, "file_count", 11L))));

    Assertions.assertFalse(result);
  }

  @Test
  public void testInitializeAcceptsWhitespaceAndCaseInsensitiveRuleTokens() {
    GravitinoMetricsEvaluator evaluator =
        createEvaluator(" TABLE : row_count : AVG : LE , JOB : duration : latest : le , ");

    boolean tableResult =
        evaluator.evaluateMetrics(
            MetricScope.forTable(NameIdentifier.parse("catalog.db.table")),
            Map.of(
                "row_count",
                List.of(metric(100L, "row_count", 10L), metric(101L, "row_count", 20L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 15L))));
    Assertions.assertTrue(tableResult);

    boolean jobResult =
        evaluator.evaluateMetrics(
            MetricScope.forJob(NameIdentifier.parse("job1")),
            Map.of("duration", List.of(metric(100L, "duration", 3L), metric(105L, "duration", 5L))),
            Map.of("duration", List.of(metric(110L, "duration", 10L))));
    Assertions.assertFalse(jobResult);
  }

  @Test
  public void testParseEvaluationRulesReturnsEmptyForBlankInput() {
    Assertions.assertTrue(GravitinoMetricsEvaluator.parseEvaluationRules("  ").isEmpty());
  }

  @Test
  public void testParseEvaluationRulesParsesExpectedScopesAndRules() {
    Map<?, ?> parsed =
        GravitinoMetricsEvaluator.parseEvaluationRules(
            "table:row_count:avg:le,job:duration:max:gt");

    Assertions.assertEquals(2, parsed.size());
    Map<?, ?> tableRules = findScopeRules(parsed, "TABLE");
    Map<?, ?> jobRules = findScopeRules(parsed, "JOB");
    Assertions.assertEquals(1, tableRules.size());
    Assertions.assertEquals(1, jobRules.size());
    Assertions.assertEquals("AVG:LE", tableRules.get("row_count").toString());
    Assertions.assertEquals("MAX:GT", jobRules.get("duration").toString());
  }

  @Test
  public void testParseEvaluationRulesCreatesImmutableMaps() {
    Map<?, ?> parsed = GravitinoMetricsEvaluator.parseEvaluationRules("table:row_count:avg:le");

    @SuppressWarnings("unchecked")
    Map<Object, Object> parsedMutableView = (Map<Object, Object>) parsed;
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> parsedMutableView.put("TABLE", Map.of()));
    Map<?, ?> tableRules = findScopeRules(parsed, "TABLE");
    @SuppressWarnings("unchecked")
    Map<Object, Object> tableRulesMutableView = (Map<Object, Object>) tableRules;
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> tableRulesMutableView.put("x", "y"));
  }

  @Test
  public void testParseEvaluationRulesRejectsInvalidAggregation() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("table:row_count:sum:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsPartitionScopeInRule() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("partition:row_count:avg:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsRuleWithoutScope() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("row_count:avg:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsDuplicateRuleInSameScope() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoMetricsEvaluator.parseEvaluationRules(
                "table:row_count:avg:le,table:row_count:max:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsMalformedRuleToken() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("table:row_count:avg:le:extra"));
  }

  @Test
  public void testParseEvaluationRulesRejectsLegacyScopeMetricRuleFormat() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("table.row_count:avg:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsRuleWithBlankMetricName() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("table::avg:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsRuleWithBlankScope() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules(":row_count:avg:le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsRuleWithBlankAggregation() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("table:row_count::le"));
  }

  @Test
  public void testParseEvaluationRulesRejectsRuleWithBlankComparison() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoMetricsEvaluator.parseEvaluationRules("table:row_count:avg:"));
  }

  @Test
  public void testParseEvaluationRulesRejectsDuplicateMetricRuleAfterNormalization() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoMetricsEvaluator.parseEvaluationRules(
                "table:Row_Count:avg:le,table:row_count:max:le"));
  }

  @Test
  public void testParseEvaluationRulesAllowsBlankRuleEntriesBetweenCommas() {
    Map<?, ?> parsed =
        GravitinoMetricsEvaluator.parseEvaluationRules(
            "table:row_count:avg:le,,job:duration:latest:le,");

    Assertions.assertEquals(2, parsed.size());
    Map<?, ?> tableRules = findScopeRules(parsed, "TABLE");
    Map<?, ?> jobRules = findScopeRules(parsed, "JOB");
    Assertions.assertEquals("AVG:LE", tableRules.get("row_count").toString());
    Assertions.assertEquals("LATEST:LE", jobRules.get("duration").toString());
  }

  @Test
  public void testInitializeAllowsBlankRules() {
    GravitinoMetricsEvaluator evaluator = new GravitinoMetricsEvaluator();
    evaluator.initialize(optimizerEnv("  "));

    boolean result =
        evaluator.evaluateMetrics(
            MetricScope.forTable(NameIdentifier.parse("catalog.db.table")),
            Map.of("row_count", List.of(metric(100L, "row_count", 10L))),
            Map.of("row_count", List.of(metric(110L, "row_count", 999L))));
    Assertions.assertTrue(result);
  }

  @Test
  public void testInitializeAllowsMissingRules() {
    GravitinoMetricsEvaluator evaluator = new GravitinoMetricsEvaluator();
    evaluator.initialize(new OptimizerEnv(new OptimizerConfig(Map.of())));

    boolean result =
        evaluator.evaluateMetrics(
            MetricScope.forJob(NameIdentifier.parse("job1")),
            Map.of("duration", List.of(metric(100L, "duration", 1L))),
            Map.of("duration", List.of(metric(110L, "duration", 999L))));
    Assertions.assertTrue(result);
  }

  @Test
  public void testInitializeRejectsInvalidComparison() {
    GravitinoMetricsEvaluator evaluator = new GravitinoMetricsEvaluator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> evaluator.initialize(optimizerEnv("table:row_count:avg:decrease")));
  }

  private static GravitinoMetricsEvaluator createEvaluator(String rules) {
    GravitinoMetricsEvaluator evaluator = new GravitinoMetricsEvaluator();
    evaluator.initialize(optimizerEnv(rules));
    return evaluator;
  }

  private static OptimizerEnv optimizerEnv(String rules) {
    return new OptimizerEnv(
        new OptimizerConfig(Map.of(GravitinoMetricsEvaluator.EVALUATION_RULES_CONFIG, rules)));
  }

  private static MetricSample metric(long timestamp, String name, long value) {
    return new MetricSampleImpl(timestamp, entry(name, StatisticValues.longValue(value)));
  }

  private static MetricSample metricString(long timestamp, String name, String value) {
    return new MetricSampleImpl(timestamp, entry(name, StatisticValues.stringValue(value)));
  }

  private static StatisticEntry<?> entry(
      String name, org.apache.gravitino.stats.StatisticValue<?> value) {
    return new StatisticEntryImpl(name, value);
  }

  private static Map<?, ?> findScopeRules(Map<?, ?> parsedRules, String scopeName) {
    return parsedRules.entrySet().stream()
        .filter(entry -> scopeName.equals(entry.getKey().toString()))
        .map(Map.Entry::getValue)
        .map(Map.class::cast)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing expected scope " + scopeName));
  }
}
