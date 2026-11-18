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
package org.apache.gravitino.maintenance.optimizer.recommender.handler;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.job.GravitinoManifestRewriteJobAdapter;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestManifestRewriteStrategyHandler {
  @ParameterizedTest
  @CsvSource({
    "0,0,false",
    "99,1,false",
    "100,8388607,true",
    "100,8388608,false",
    "100,8388609,false",
    "499,8388607,true",
    "499,8388608,false",
    "500,8388608,true",
    "500,16777216,true",
    "501,16777216,true"
  })
  void testBoundaries(long count, double average, boolean trigger) {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, null),
            statistics(count, average)));
    Assertions.assertEquals(trigger, handler.shouldTrigger());
    Assertions.assertEquals(trigger, handler.evaluate().jobExecutionContext().isPresent());
    if (trigger) {
      Assertions.assertEquals(count, handler.evaluate().score());
    }
  }

  @Test
  void testMissingSpecAndPartialMeasurementsDoNotTrigger() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    for (List<StatisticEntry<?>> stats :
        List.of(
            List.<StatisticEntry<?>>of(),
            List.of(statistics(500, 1).get(0), statistics(500, 1).get(2)),
            statistics(500, 1).subList(1, 3))) {
      handler.initialize(
          context(PolicyContents.icebergRewriteManifests(null, null, null, 1, null), stats));
      Assertions.assertFalse(handler.shouldTrigger());
    }
    handler.initialize(
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 2, null), statistics(500, 1)));
    Assertions.assertFalse(handler.shouldTrigger());
  }

  @Test
  void testMissingDefaultSkipsButExplicitSpecRetainsOptions() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    StrategyHandlerContext context =
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, null, false),
            statistics(100, 1));
    handler.initialize(context);
    Assertions.assertFalse(handler.shouldTrigger());
    handler.initialize(
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, false),
            statistics(100, 1)));
    Map<String, String> config =
        new GravitinoManifestRewriteJobAdapter()
            .jobConfig(handler.evaluate().jobExecutionContext().orElseThrow());
    Assertions.assertEquals(
        Map.of(
            "catalog_name",
            "iceberg",
            "table_identifier",
            "db.tbl",
            "spec_id",
            "1",
            "use_caching",
            "false"),
        config);
    handler.initialize(context);
    Assertions.assertFalse(handler.shouldTrigger());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "1", "2147483647"})
  void testDefaultSpecIsResolvedAndPinned(String specId) {
    Table metadata = Mockito.mock(Table.class);
    Mockito.when(metadata.properties())
        .thenReturn(Map.of("current-snapshot-id", "1", "default-spec-id", specId));
    StrategyHandlerContext context = context(PolicyContents.icebergRewriteManifests(), List.of());
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        StrategyHandlerContext.builder(context.nameIdentifier(), context.strategy())
            .withTableMetadata(metadata)
            .withTableStatistics(
                List.of(
                    new StatisticEntryImpl<>(
                        IcebergManifestStatistics.MANIFEST_NUMBER,
                        StatisticValues.objectValue(
                            Map.of(specId, StatisticValues.longValue(500)))),
                    new StatisticEntryImpl<>(
                        IcebergManifestStatistics.AVG_MANIFEST_SIZE,
                        StatisticValues.objectValue(
                            Map.of(specId, StatisticValues.doubleValue(1)))),
                    new StatisticEntryImpl<>(
                        IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.longValue(1))))
            .build());
    Assertions.assertTrue(handler.shouldTrigger());
    Mockito.when(metadata.properties())
        .thenReturn(Map.of("current-snapshot-id", "1", "default-spec-id", "2"));
    Assertions.assertEquals(
        specId,
        new GravitinoManifestRewriteJobAdapter()
            .jobConfig(handler.evaluate().jobExecutionContext().orElseThrow())
            .get("spec_id"));
  }

  @Test
  void testExplicitSpecOverridesCurrentDefault() {
    StrategyHandlerContext context =
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 0, null), statistics(0, 0));
    Table metadata = Mockito.mock(Table.class);
    Mockito.when(metadata.properties())
        .thenReturn(Map.of("current-snapshot-id", "1", "default-spec-id", "1"));
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        StrategyHandlerContext.builder(context.nameIdentifier(), context.strategy())
            .withTableMetadata(metadata)
            .withTableStatistics(context.tableStatistics())
            .build());
    Assertions.assertTrue(handler.shouldTrigger());
    Assertions.assertEquals(
        "0", handler.evaluate().jobExecutionContext().orElseThrow().jobOptions().get("spec_id"));
  }

  @Test
  void testConfigurableThresholds() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        context(PolicyContents.icebergRewriteManifests(10L, 5L, 2L, 1, true), statistics(5, 1)));
    Assertions.assertTrue(handler.shouldTrigger());
  }

  @Test
  void testOnlySelectedSpecCanTrigger() {
    List<StatisticEntry<?>> measurements = statistics(0, 0);
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        context(PolicyContents.icebergRewriteManifests(null, null, null, 1, null), measurements));
    Assertions.assertFalse(handler.shouldTrigger(), "Spec 1 has zero manifests");
    handler.initialize(
        context(PolicyContents.icebergRewriteManifests(null, null, null, 0, null), measurements));
    Assertions.assertTrue(handler.shouldTrigger(), "Spec 0 has 1000 manifests");
    Assertions.assertEquals(1000, handler.evaluate().score());
    Assertions.assertEquals(
        "0",
        new GravitinoManifestRewriteJobAdapter()
            .jobConfig(handler.evaluate().jobExecutionContext().orElseThrow())
            .get("spec_id"));
  }

  @ParameterizedTest
  @CsvSource({"true,true", "TRUE,true", "TrUe,true", "false,false", "FALSE,false", "FaLsE,false"})
  void testMixedCaseCachingReachesJobConfiguration(String input, String expected) {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        context(customContent(Map.of("spec_id", 1, "use_caching", input)), statistics(500, 1)));
    Assertions.assertEquals(
        expected,
        new GravitinoManifestRewriteJobAdapter()
            .jobConfig(handler.evaluate().jobExecutionContext().orElseThrow())
            .get("use_caching"));
  }

  @Test
  void testIntegralRuleValues() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    for (Object warning :
        new Object[] {(byte) 100, (short) 100, 100, 100L, BigInteger.valueOf(100), "100"}) {
      handler.initialize(
          context(
              customContent(Map.of("spec_id", 1, "manifest_count_warning", warning)),
              statistics(100, 1)));
      Assertions.assertTrue(handler.shouldTrigger());
    }
    handler.initialize(
        context(customContent(Map.of("spec_id", (long) Integer.MAX_VALUE)), statistics(500, 1)));
    Assertions.assertFalse(handler.shouldTrigger());
  }

  @Test
  void testNonIntegerRuleValuesAreRejected() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    for (Object warning :
        new Object[] {
          "+100",
          "100.0",
          "1e2",
          " 100",
          100.0d,
          100.0f,
          new BigDecimal("100"),
          new StringBuilder("100"),
          new BigInteger("9223372036854775808"),
          "9223372036854775808"
        }) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.initialize(
                  context(
                      customContent(Map.of("spec_id", 1, "manifest_count_warning", warning)),
                      statistics(100, 1))),
          warning.toString());
    }
  }

  @Test
  void testMalformedRulesAndMeasurements() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    for (Map<String, Object> rules :
        List.<Map<String, Object>>of(
            Map.of("spec_id", "2147483648"),
            Map.of("spec_id", 2147483648L),
            Map.of("spec_id", -1),
            Map.of("spec_id", 1.5),
            Map.of("spec_id", 1, "use_caching", "yes"),
            Map.of("spec_id", 1, "manifest_count_critical", 99))) {
      PolicyContent content = customContent(rules);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.initialize(context(content, statistics(500, 1))));
    }
    for (double average : new double[] {-1, Double.NaN, Double.POSITIVE_INFINITY}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.initialize(
                  context(
                      PolicyContents.icebergRewriteManifests(null, null, null, 1, null),
                      statistics(500, average))));
    }
  }

  @Test
  void testUnverifiedSnapshotsDoNotTriggerAndClearEarlierEvaluation() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    StrategyHandlerContext valid =
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, null), statistics(500, 1));
    for (Map<String, String> properties :
        List.of(
            Map.<String, String>of(), Map.of("current-snapshot-id", "2"),
            Map.of("current-snapshot-id", "none"), Map.of("current-snapshot-id", "invalid"))) {
      handler.initialize(valid);
      Assertions.assertTrue(handler.shouldTrigger());
      Table metadata = Mockito.mock(Table.class);
      Mockito.when(metadata.properties()).thenReturn(properties);
      handler.initialize(
          StrategyHandlerContext.builder(valid.nameIdentifier(), valid.strategy())
              .withTableStatistics(valid.tableStatistics())
              .withTableMetadata(metadata)
              .build());
      Assertions.assertFalse(handler.shouldTrigger());
    }
    handler.initialize(
        StrategyHandlerContext.builder(valid.nameIdentifier(), valid.strategy())
            .withTableStatistics(valid.tableStatistics())
            .build());
    Assertions.assertFalse(handler.shouldTrigger());
    handler.initialize(
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, null),
            statistics(500, 1).subList(0, 2)));
    Assertions.assertFalse(handler.shouldTrigger());
    handler.initialize(
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, null),
            List.of(
                statistics(500, 1).get(0),
                statistics(500, 1).get(1),
                new StatisticEntryImpl<>(
                    IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.stringValue("1")))));
    Assertions.assertFalse(handler.shouldTrigger());
  }

  @Test
  void testEmptyTableSnapshotIsAccepted() {
    StrategyHandlerContext valid =
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, null), statistics(0, 0));
    Table metadata = Mockito.mock(Table.class);
    Mockito.when(metadata.properties()).thenReturn(Map.of("current-snapshot-id", "none"));
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        StrategyHandlerContext.builder(valid.nameIdentifier(), valid.strategy())
            .withTableMetadata(metadata)
            .withTableStatistics(
                List.of(
                    statistics(0, 0).get(0),
                    statistics(0, 0).get(1),
                    new StatisticEntryImpl<>(
                        IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.longValue(-1))))
            .build());
    Assertions.assertFalse(handler.shouldTrigger());
  }

  private static PolicyContent customContent(Map<String, Object> rules) {
    return PolicyContents.custom(
        rules,
        PolicyContents.icebergRewriteManifests().supportedObjectTypes(),
        PolicyContents.icebergRewriteManifests().properties());
  }

  private static StrategyHandlerContext context(
      PolicyContent content, List<StatisticEntry<?>> statistics) {
    Policy policy = Mockito.mock(Policy.class);
    Mockito.when(policy.content()).thenReturn(content);
    Mockito.when(policy.name()).thenReturn("rewrite");
    Table metadata = Mockito.mock(Table.class);
    Mockito.when(metadata.properties()).thenReturn(Map.of("current-snapshot-id", "1"));
    return StrategyHandlerContext.builder(
            NameIdentifier.of("iceberg", "db", "tbl"), new GravitinoStrategy(policy))
        .withTableMetadata(metadata)
        .withTableStatistics(statistics)
        .build();
  }

  private static List<StatisticEntry<?>> statistics(long count, double average) {
    return List.of(
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.MANIFEST_NUMBER,
            StatisticValues.objectValue(
                Map.of(
                    "0", StatisticValues.longValue(1000), "1", StatisticValues.longValue(count)))),
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.AVG_MANIFEST_SIZE,
            StatisticValues.objectValue(
                Map.of(
                    "0",
                    StatisticValues.doubleValue(1),
                    "1",
                    StatisticValues.doubleValue(average)))),
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.longValue(1)));
  }
}
