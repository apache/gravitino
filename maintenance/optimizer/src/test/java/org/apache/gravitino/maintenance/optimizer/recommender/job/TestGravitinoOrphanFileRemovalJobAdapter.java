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
import org.apache.gravitino.maintenance.optimizer.recommender.handler.orphan.OrphanFileRemovalJobContext;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.orphan.OrphanFileRemovalStrategyHandler;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestGravitinoOrphanFileRemovalJobAdapter {
  private static final String TEMPLATE = IcebergOrphanFileRemovalContent.JOB_TEMPLATE_NAME_VALUE;
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-09-27T12:30:00Z"), ZoneOffset.UTC);
  private final GravitinoOrphanFileRemovalJobAdapter adapter =
      new GravitinoOrphanFileRemovalJobAdapter(CLOCK);

  @Test
  void policyToSubmissionWorksWithoutPartitionStatistics() {
    for (boolean partitioned : new boolean[] {false, true}) {
      Policy policy = Mockito.mock(Policy.class);
      Mockito.when(policy.content())
          .thenReturn(PolicyContents.icebergOrphanFileRemoval(3, "s3://bucket/table/data", true));
      GravitinoStrategy strategy = new GravitinoStrategy(policy);
      Table table = Mockito.mock(Table.class);
      Mockito.when(table.properties()).thenReturn(Map.of("location", "s3://bucket/table"));
      Mockito.when(table.partitioning())
          .thenReturn(partitioned ? new Transform[] {Transforms.identity("id")} : new Transform[0]);
      OrphanFileRemovalStrategyHandler handler = new OrphanFileRemovalStrategyHandler();
      handler.initialize(
          StrategyHandlerContext.builder(NameIdentifier.of("catalog", "db", "table"), strategy)
              .withTableMetadata(table)
              .build());
      Assertions.assertEquals(Set.of(DataRequirement.TABLE_METADATA), handler.dataRequirements());
      Assertions.assertEquals(strategy.strategyType(), handler.strategyType());
      Assertions.assertTrue(handler.shouldTrigger());
      Assertions.assertEquals(1, handler.evaluate().score());
      OrphanFileRemovalJobContext context =
          (OrphanFileRemovalJobContext) handler.evaluate().jobExecutionContext().orElseThrow();
      Assertions.assertInstanceOf(
          GravitinoOrphanFileRemovalJobAdapter.class,
          new GravitinoJobSubmitter().loadJobAdapter(context.jobTemplateName()));
      OptimizerConfig config =
          new OptimizerConfig(
              Map.of(
                  OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_name", "spark_catalog",
                  OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "dry_run", "false"));
      Map<String, String> job = GravitinoJobSubmitter.buildJobConfig(config, context, adapter);
      Assertions.assertEquals("spark_catalog", job.get("catalog_name"));
      Assertions.assertEquals("db.table", job.get("table_identifier"));
      Assertions.assertEquals("2026-09-24 12:30:00Z", job.get("older_than"));
      Assertions.assertEquals("true", job.get("dry_run"));
      Assertions.assertEquals("s3://bucket/table/data", job.get("location"));
    }
  }

  @Test
  void defaultsFillOptionalPlaceholders() {
    Map<String, String> job = adapter.jobConfig(context(Map.of(), null));
    Assertions.assertEquals("", job.get("location"));
    Assertions.assertEquals("false", job.get("dry_run"));
    Assertions.assertEquals("2026-09-24 12:30:00Z", job.get("older_than"));
    Assertions.assertEquals(
        "2026-09-26 12:30:00Z",
        adapter.jobConfig(context(Map.of("olderThanDays", "1"), null)).get("older_than"));
  }

  @Test
  void rejectsUnsafeOptionsBeforeSubmission() {
    for (String days : new String[] {"0", "-1", "no", "9223372036854775807"}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> adapter.jobConfig(context(Map.of("olderThanDays", days), null)));
    }
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> adapter.jobConfig(context(Map.of("dryRun", "yes"), null)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> adapter.jobConfig(context(Map.of("location", "s3://bucket/table"), null)));
    for (String location :
        new String[] {
          "s3://bucket/table-other",
          "s3://bucket/table/../other",
          "s3://other/table",
          "s3://bucket/table/%2e%2e/other",
          "s3://bucket/table?query",
          ""
        }) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> adapter.jobConfig(context(Map.of("location", location), "s3://bucket/table")));
    }
  }

  @Test
  void validatesRetentionRangeAndReportsTimestampErrors() {
    long maximum = IcebergOrphanFileRemovalContent.MAX_OLDER_THAN_DAYS;
    Assertions.assertEquals(
        "1926-10-22 12:30:00Z",
        adapter
            .jobConfig(context(Map.of("olderThanDays", String.valueOf(maximum)), null))
            .get("older_than"));
    for (String days :
        new String[] {
          "0", "-1", "no", String.valueOf(maximum + 1), String.valueOf(Long.MAX_VALUE)
        }) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> adapter.jobConfig(context(Map.of("olderThanDays", days), null)));
      Assertions.assertTrue(error.getMessage().contains("olderThanDays"));
    }
    GravitinoOrphanFileRemovalJobAdapter ancient =
        new GravitinoOrphanFileRemovalJobAdapter(Clock.fixed(Instant.MIN, ZoneOffset.UTC));
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> ancient.jobConfig(context(Map.of(), null)));
    Assertions.assertTrue(error.getMessage().contains("olderThanDays"));
    Assertions.assertNotNull(error.getCause());
  }

  private OrphanFileRemovalJobContext context(Map<String, String> options, String location) {
    return new OrphanFileRemovalJobContext(
        NameIdentifier.of("catalog", "db", "table"), options, TEMPLATE, location);
  }
}
