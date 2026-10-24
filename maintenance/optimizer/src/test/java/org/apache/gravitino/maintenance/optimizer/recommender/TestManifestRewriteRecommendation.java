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
package org.apache.gravitino.maintenance.optimizer.recommender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.job.GravitinoManifestRewriteJobAdapter;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestManifestRewriteRecommendation {
  @Test
  void testBuiltInDiscoveryDryRunAndSubmission() throws Exception {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    Policy policy = Mockito.mock(Policy.class);
    Mockito.when(policy.name()).thenReturn("rewrite");
    Mockito.when(policy.content())
        .thenReturn(PolicyContents.icebergRewriteManifests(null, null, null, 7, false));
    GravitinoStrategy strategy = new GravitinoStrategy(policy);
    StrategyProvider strategies = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategies.strategies(identifier)).thenReturn(List.of(strategy));
    Mockito.when(strategies.strategy("rewrite")).thenReturn(strategy);
    SupportTableStatistics statistics = Mockito.mock(SupportTableStatistics.class);
    Mockito.when(statistics.tableStatistics(identifier))
        .thenReturn(
            List.of(
                new StatisticEntryImpl<>(
                    IcebergManifestStatistics.MANIFEST_NUMBER,
                    StatisticValues.objectValue(Map.of("7", StatisticValues.longValue(500)))),
                new StatisticEntryImpl<>(
                    IcebergManifestStatistics.AVG_MANIFEST_SIZE,
                    StatisticValues.objectValue(
                        Map.of("7", StatisticValues.doubleValue(16 * 1024 * 1024)))),
                new StatisticEntryImpl<>(
                    IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.longValue(1))));
    TableMetadataProvider metadata = Mockito.mock(TableMetadataProvider.class);
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Map.of("current-snapshot-id", "1"));
    Mockito.when(metadata.tableMetadata(identifier)).thenReturn(table);
    JobSubmitter submitter = Mockito.mock(JobSubmitter.class);
    Mockito.when(submitter.submitJob(Mockito.anyString(), Mockito.any())).thenReturn("job-1");
    try (Recommender recommender =
        new Recommender(
            strategies,
            statistics,
            metadata,
            submitter,
            new OptimizerEnv(new OptimizerConfig(Map.of())))) {
      Assertions.assertEquals(
          1, recommender.recommendForStrategyName(List.of(identifier), "rewrite", 1).size());
      Mockito.verifyNoInteractions(submitter);
      PolicyContent validContent = policy.content();
      for (Object invalid : new Object[] {"+500", new StringBuilder("500"), -1L}) {
        Mockito.when(policy.content())
            .thenReturn(
                PolicyContents.custom(
                    Map.of("spec_id", 7, "manifest_count_critical", invalid),
                    validContent.supportedObjectTypes(),
                    validContent.properties()));
        Assertions.assertTrue(
            recommender.recommendForStrategyName(List.of(identifier), "rewrite", 1).isEmpty());
      }
      Mockito.when(policy.content()).thenReturn(validContent);
      List<Recommender.RecommendationResult> results =
          recommender.submitForStrategyName(List.of(identifier), "rewrite");
      Assertions.assertEquals("job-1", results.get(0).jobId());
      ArgumentCaptor<JobExecutionContext> captured =
          ArgumentCaptor.forClass(JobExecutionContext.class);
      Mockito.verify(submitter)
          .submitJob(
              Mockito.eq(IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE),
              captured.capture());
      Assertions.assertEquals(
          Map.of(
              "catalog_name",
              "catalog",
              "table_identifier",
              "db.table",
              "spec_id",
              "7",
              "use_caching",
              "false"),
          new GravitinoManifestRewriteJobAdapter().jobConfig(captured.getValue()));
      Mockito.verify(metadata, Mockito.atLeastOnce()).tableMetadata(identifier);
      Mockito.when(table.properties()).thenReturn(Map.of("current-snapshot-id", "2"));
      Assertions.assertTrue(
          recommender.submitForStrategyName(List.of(identifier), "rewrite").isEmpty());
      Mockito.verifyNoMoreInteractions(submitter);
      Mockito.verify(statistics, Mockito.never()).partitionStatistics(Mockito.any());
      Mockito.when(statistics.tableStatistics(identifier)).thenReturn(List.of());
      Assertions.assertTrue(
          recommender.submitForStrategyName(List.of(identifier), "rewrite").isEmpty());
      Mockito.verifyNoMoreInteractions(submitter);
      Mockito.when(policy.content()).thenReturn(PolicyContents.icebergRewriteManifests());
      Assertions.assertTrue(
          recommender.submitForStrategyName(List.of(identifier), "rewrite").isEmpty());
      Mockito.verifyNoMoreInteractions(submitter);
    }
  }

  @ParameterizedTest
  @MethodSource("invalidMeasurements")
  void testInvalidStatisticsDoNotAbortOtherTables(List<StatisticEntry<?>> invalid)
      throws Exception {
    NameIdentifier bad = NameIdentifier.of("catalog", "db", "bad");
    NameIdentifier healthy = NameIdentifier.of("catalog", "db", "healthy");
    StrategyProvider strategies = manifestStrategies(bad, healthy);
    SupportTableStatistics statistics = Mockito.mock(SupportTableStatistics.class);
    Mockito.when(statistics.tableStatistics(bad)).thenReturn(invalid);
    Mockito.when(statistics.tableStatistics(healthy))
        .thenReturn(measurements(StatisticValues.longValue(500), 1));
    TableMetadataProvider metadata = Mockito.mock(TableMetadataProvider.class);
    Table table = tableAtSnapshot("1");
    Mockito.when(metadata.tableMetadata(Mockito.any())).thenReturn(table);
    JobSubmitter submitter = Mockito.mock(JobSubmitter.class);
    Mockito.when(submitter.submitJob(Mockito.anyString(), Mockito.any())).thenReturn("healthy-job");
    try (Recommender recommender =
        new Recommender(
            strategies,
            statistics,
            metadata,
            submitter,
            new OptimizerEnv(new OptimizerConfig(Map.of())))) {
      List<Recommender.RecommendationResult> results =
          recommender.submitForStrategyName(List.of(bad, healthy), "rewrite");
      Assertions.assertEquals(1, results.size());
      Assertions.assertEquals(healthy, results.get(0).identifier());
      Assertions.assertEquals("healthy-job", results.get(0).jobId());
      ArgumentCaptor<JobExecutionContext> captured =
          ArgumentCaptor.forClass(JobExecutionContext.class);
      Mockito.verify(submitter)
          .submitJob(
              Mockito.eq(IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE),
              captured.capture());
      Assertions.assertEquals(healthy, captured.getValue().nameIdentifier());
    }
  }

  @Test
  void testSnapshotChangeDuringStatisticsReadSkipsRecommendation() throws Exception {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    AtomicReference<Table> currentTable = new AtomicReference<>(tableAtSnapshot("1"));
    SupportTableStatistics statistics = Mockito.mock(SupportTableStatistics.class);
    Mockito.when(statistics.tableStatistics(identifier))
        .thenAnswer(
            invocation -> {
              currentTable.set(tableAtSnapshot("2"));
              return measurements(StatisticValues.longValue(500), 1);
            });
    TableMetadataProvider metadata = Mockito.mock(TableMetadataProvider.class);
    Mockito.when(metadata.tableMetadata(identifier)).thenAnswer(invocation -> currentTable.get());
    JobSubmitter submitter = Mockito.mock(JobSubmitter.class);
    try (Recommender recommender =
        new Recommender(
            manifestStrategies(identifier),
            statistics,
            metadata,
            submitter,
            new OptimizerEnv(new OptimizerConfig(Map.of())))) {
      Assertions.assertTrue(
          recommender.submitForStrategyName(List.of(identifier), "rewrite").isEmpty());
      Mockito.verifyNoInteractions(submitter);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"statistics", "metadata"})
  void testProviderFailuresStillPropagate(String failingProvider) throws Exception {
    NameIdentifier identifier = NameIdentifier.of("catalog", "db", "table");
    IllegalArgumentException failure = new IllegalArgumentException("Provider read failed");
    SupportTableStatistics statistics = Mockito.mock(SupportTableStatistics.class);
    Mockito.when(statistics.tableStatistics(identifier))
        .thenReturn(measurements(StatisticValues.longValue(500), 1));
    TableMetadataProvider metadata = Mockito.mock(TableMetadataProvider.class);
    if ("statistics".equals(failingProvider)) {
      Mockito.when(statistics.tableStatistics(identifier)).thenThrow(failure);
    } else {
      Mockito.when(metadata.tableMetadata(identifier)).thenThrow(failure);
    }
    JobSubmitter submitter = Mockito.mock(JobSubmitter.class);
    try (Recommender recommender =
        new Recommender(
            manifestStrategies(identifier),
            statistics,
            metadata,
            submitter,
            new OptimizerEnv(new OptimizerConfig(Map.of())))) {
      Assertions.assertSame(
          failure,
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> recommender.submitForStrategyName(List.of(identifier), "rewrite")));
      Mockito.verifyNoInteractions(submitter);
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "-1", "1.5", "+1", " 1", "2147483648", "invalid"})
  void testUnresolvableDefaultSkipsOnlyAffectedTable(String defaultSpec) throws Exception {
    NameIdentifier bad = NameIdentifier.of("catalog", "db", "bad");
    NameIdentifier healthy = NameIdentifier.of("catalog", "db", "healthy");
    SupportTableStatistics statistics = Mockito.mock(SupportTableStatistics.class);
    Mockito.when(statistics.tableStatistics(Mockito.any()))
        .thenReturn(measurements(StatisticValues.longValue(500), 1));
    TableMetadataProvider metadata = Mockito.mock(TableMetadataProvider.class);
    Table badTable = Mockito.mock(Table.class);
    Map<String, String> properties = new HashMap<>();
    properties.put("current-snapshot-id", "1");
    if (defaultSpec != null) {
      properties.put("default-spec-id", defaultSpec);
    }
    Mockito.when(badTable.properties()).thenReturn(properties);
    Table healthyTable = tableAtSnapshot("1");
    Mockito.when(metadata.tableMetadata(bad)).thenReturn(badTable);
    Mockito.when(metadata.tableMetadata(healthy)).thenReturn(healthyTable);
    JobSubmitter submitter = Mockito.mock(JobSubmitter.class);
    try (Recommender recommender =
        new Recommender(
            manifestStrategies(PolicyContents.icebergRewriteManifests(), bad, healthy),
            statistics,
            metadata,
            submitter,
            new OptimizerEnv(new OptimizerConfig(Map.of())))) {
      List<Recommender.RecommendationResult> results =
          recommender.submitForStrategyName(List.of(bad, healthy), "rewrite");
      Assertions.assertEquals(1, results.size());
      Assertions.assertEquals(healthy, results.get(0).identifier());
      Assertions.assertEquals("7", results.get(0).jobOptions().get("spec_id"));
      ArgumentCaptor<JobExecutionContext> captured =
          ArgumentCaptor.forClass(JobExecutionContext.class);
      Mockito.verify(submitter)
          .submitJob(
              Mockito.eq(IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE),
              captured.capture());
      Assertions.assertEquals("7", captured.getValue().jobOptions().get("spec_id"));
    }
  }

  private static Stream<List<StatisticEntry<?>>> invalidMeasurements() {
    return Stream.of(
        measurements(StatisticValues.stringValue("500"), 1),
        measurements(StatisticValues.longValue(-1), 1),
        measurements(StatisticValues.longValue(500), Double.NaN),
        measurements(StatisticValues.longValue(0), 1),
        List.of(
            new StatisticEntryImpl<>(
                IcebergManifestStatistics.MANIFEST_NUMBER, StatisticValues.longValue(500)),
            new StatisticEntryImpl<>(
                IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.longValue(1))));
  }

  private static List<StatisticEntry<?>> measurements(StatisticValue<?> count, double average) {
    return List.of(
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.MANIFEST_NUMBER,
            StatisticValues.objectValue(Map.of("7", count))),
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.AVG_MANIFEST_SIZE,
            StatisticValues.objectValue(Map.of("7", StatisticValues.doubleValue(average)))),
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.SNAPSHOT_ID, StatisticValues.longValue(1)));
  }

  private static StrategyProvider manifestStrategies(NameIdentifier... identifiers) {
    return manifestStrategies(
        PolicyContents.icebergRewriteManifests(null, null, null, 7, null), identifiers);
  }

  private static StrategyProvider manifestStrategies(
      PolicyContent content, NameIdentifier... identifiers) {
    Policy policy = Mockito.mock(Policy.class);
    Mockito.when(policy.name()).thenReturn("rewrite");
    Mockito.when(policy.content()).thenReturn(content);
    GravitinoStrategy strategy = new GravitinoStrategy(policy);
    StrategyProvider strategies = Mockito.mock(StrategyProvider.class);
    Mockito.when(strategies.strategy("rewrite")).thenReturn(strategy);
    for (NameIdentifier identifier : identifiers) {
      Mockito.when(strategies.strategies(identifier)).thenReturn(List.of(strategy));
    }
    return strategies;
  }

  private static Table tableAtSnapshot(String snapshot) {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties())
        .thenReturn(Map.of("current-snapshot-id", snapshot, "default-spec-id", "7"));
    return table;
  }
}
