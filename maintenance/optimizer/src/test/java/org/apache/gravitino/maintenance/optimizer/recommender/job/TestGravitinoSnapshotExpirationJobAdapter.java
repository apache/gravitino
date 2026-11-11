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
package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandler.DataRequirement;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyHandlerContext;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot.SnapshotExpirationJobContext;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot.SnapshotExpirationStrategyHandler;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestGravitinoSnapshotExpirationJobAdapter {
  private static final String TEMPLATE = IcebergSnapshotExpirationContent.JOB_TEMPLATE_NAME_VALUE;
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-09-27T12:30:00Z"), ZoneOffset.UTC);
  private final GravitinoSnapshotExpirationJobAdapter adapter =
      new GravitinoSnapshotExpirationJobAdapter(CLOCK);

  @Test
  void policyToSubmissionWorksWithoutPartitionStatistics() {
    for (boolean partitioned : new boolean[] {false, true}) {
      Policy policy = Mockito.mock(Policy.class);
      Mockito.when(policy.content())
          .thenReturn(PolicyContents.icebergSnapshotExpiration(5L, 3L, true));
      GravitinoStrategy strategy = new GravitinoStrategy(policy);
      Table table = Mockito.mock(Table.class);
      Mockito.when(table.properties()).thenReturn(Map.of());
      Mockito.when(table.partitioning())
          .thenReturn(partitioned ? new Transform[] {Transforms.identity("id")} : new Transform[0]);
      SnapshotExpirationStrategyHandler handler = new SnapshotExpirationStrategyHandler();
      handler.initialize(
          StrategyHandlerContext.builder(NameIdentifier.of("catalog", "db", "table"), strategy)
              .withTableMetadata(table)
              .build());
      Assertions.assertEquals(Set.of(DataRequirement.TABLE_METADATA), handler.dataRequirements());
      Assertions.assertEquals(strategy.strategyType(), handler.strategyType());
      Assertions.assertTrue(handler.shouldTrigger());
      Assertions.assertEquals(1, handler.evaluate().score());
      SnapshotExpirationJobContext context =
          (SnapshotExpirationJobContext) handler.evaluate().jobExecutionContext().orElseThrow();
      Assertions.assertInstanceOf(
          GravitinoSnapshotExpirationJobAdapter.class,
          new GravitinoJobSubmitter().loadJobAdapter(context.jobTemplateName()));
      OptimizerConfig config =
          new OptimizerConfig(
              Map.of(
                  OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_name", "spark_catalog"));
      Map<String, String> job = GravitinoJobSubmitter.buildJobConfig(config, context, adapter);
      Assertions.assertEquals("spark_catalog", job.get("catalog_name"));
      Assertions.assertEquals("db.table", job.get("table_identifier"));
      // olderThanDays=5 matches Iceberg default → omit so table properties can apply
      Assertions.assertEquals("", job.get("older_than"));
      Assertions.assertEquals("3", job.get("retain_last"));
      Assertions.assertEquals("true", job.get("stream_results"));
    }
  }

  @Test
  void defaultsFillOptionalPlaceholders() {
    Map<String, String> job = adapter.jobConfig(context(Map.of()));
    Assertions.assertEquals("", job.get("older_than"));
    Assertions.assertEquals("", job.get("retain_last"));
    Assertions.assertEquals("false", job.get("stream_results"));
    Assertions.assertEquals(
        "2026-09-26 12:30:00Z",
        adapter.jobConfig(context(Map.of("olderThanDays", "1"))).get("older_than"));
  }

  @Test
  void rejectsUnsafeOptionsBeforeSubmission() {
    for (String days : new String[] {"0", "-1", "no", "9223372036854775807"}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> adapter.jobConfig(context(Map.of("olderThanDays", days))));
    }
    for (String retain : new String[] {"0", "-1", "no"}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> adapter.jobConfig(context(Map.of("retainLast", retain))));
    }
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> adapter.jobConfig(context(Map.of("streamResults", "yes"))));
  }

  @Test
  void validatesRetentionRangeAndReportsTimestampErrors() {
    long maximum = IcebergSnapshotExpirationContent.MAX_OLDER_THAN_DAYS;
    Assertions.assertEquals(
        "1926-10-22 12:30:00Z",
        adapter
            .jobConfig(context(Map.of("olderThanDays", String.valueOf(maximum))))
            .get("older_than"));
    for (String days :
        new String[] {
          "0", "-1", "no", String.valueOf(maximum + 1), String.valueOf(Long.MAX_VALUE)
        }) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> adapter.jobConfig(context(Map.of("olderThanDays", days))));
      Assertions.assertTrue(error.getMessage().contains("olderThanDays"));
    }
    GravitinoSnapshotExpirationJobAdapter ancient =
        new GravitinoSnapshotExpirationJobAdapter(Clock.fixed(Instant.MIN, ZoneOffset.UTC));
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ancient.jobConfig(context(Map.of("olderThanDays", "2"))));
    Assertions.assertTrue(error.getMessage().contains("olderThanDays"));
    Assertions.assertNotNull(error.getCause());
  }

  private SnapshotExpirationJobContext context(Map<String, String> options) {
    return new SnapshotExpirationJobContext(
        NameIdentifier.of("catalog", "db", "table"), options, TEMPLATE);
  }
}
