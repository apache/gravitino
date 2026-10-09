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
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
            statistics(500, 1).subList(0, 1),
            statistics(500, 1).subList(1, 2))) {
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
  void testDefaultCycleRetainsResolvedSpecAndOptions() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    StrategyHandlerContext context =
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, null, false),
            statistics(100, 1));
    handler.initialize(context);
    Assertions.assertFalse(handler.shouldTrigger());
    handler.initialize(context, 1);
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
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            handler.initialize(
                context(
                    PolicyContents.icebergRewriteManifests(null, null, null, 0, null),
                    statistics(100, 1)),
                1));
    Assertions.assertFalse(handler.shouldTrigger());
  }

  @Test
  void testConfigurableThresholdsAndSpecIsolation() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    handler.initialize(
        context(PolicyContents.icebergRewriteManifests(10L, 5L, 2L, 1, true), statistics(5, 1)));
    Assertions.assertTrue(handler.shouldTrigger());
    handler.initialize(
        context(
            PolicyContents.icebergRewriteManifests(null, null, null, 1, null), statistics(99, 1)));
    // Spec 0 has 1000 manifests, but cannot trigger a rewrite of spec 1.
    Assertions.assertFalse(handler.shouldTrigger());
  }

  @Test
  void testMalformedRulesAndMeasurements() {
    ManifestRewriteStrategyHandler handler = new ManifestRewriteStrategyHandler();
    for (Map<String, Object> rules :
        List.<Map<String, Object>>of(
            Map.of("spec_id", "2147483648"),
            Map.of("spec_id", -1),
            Map.of("spec_id", 1.5),
            Map.of("spec_id", 1, "use_caching", "yes"),
            Map.of("spec_id", 1, "manifest_count_critical", 99))) {
      PolicyContent content =
          PolicyContents.custom(
              rules,
              PolicyContents.icebergRewriteManifests().supportedObjectTypes(),
              PolicyContents.icebergRewriteManifests().properties());
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

  private static StrategyHandlerContext context(
      PolicyContent content, List<StatisticEntry<?>> statistics) {
    Policy policy = Mockito.mock(Policy.class);
    Mockito.when(policy.content()).thenReturn(content);
    Mockito.when(policy.name()).thenReturn("rewrite");
    return StrategyHandlerContext.builder(
            NameIdentifier.of("iceberg", "db", "tbl"), new GravitinoStrategy(policy))
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
                    StatisticValues.doubleValue(average)))));
  }
}
