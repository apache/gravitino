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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.job.GravitinoManifestRewriteJobAdapter;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategy;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
        .thenReturn(new IcebergManifestStatistics(7, 500, 16 * 1024 * 1024).statistics());
    TableMetadataProvider metadata = Mockito.mock(TableMetadataProvider.class);
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
      Mockito.verifyNoInteractions(metadata);
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
}
