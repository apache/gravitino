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

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionStrategyHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoJobSubmitter {
  @Test
  void loadJobAdapterReturnsCompactionAdapter() {
    GravitinoJobSubmitter submitter = new GravitinoJobSubmitter();
    GravitinoJobAdapter adapter = submitter.loadJobAdapter(CompactionStrategyHandler.NAME);
    Assertions.assertTrue(adapter instanceof GravitinoCompactionJobAdapter);
  }

  @Test
  void loadJobAdapterFallsBackToConfiguredClassName() {
    String jobTemplateName = "custom";
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(
                OptimizerConfig.GRAVITINO_URI,
                "http://localhost:8090",
                OptimizerConfig.GRAVITINO_METALAKE,
                "test-metalake",
                OptimizerConfig.JOB_ADAPTER_PREFIX + jobTemplateName + ".className",
                GravitinoCompactionJobAdapter.class.getName()));
    GravitinoJobSubmitter submitter = new GravitinoJobSubmitter();
    submitter.initialize(new OptimizerEnv(config));

    GravitinoJobAdapter adapter = submitter.loadJobAdapter(jobTemplateName);
    Assertions.assertTrue(adapter instanceof GravitinoCompactionJobAdapter);
  }

  @Test
  void buildJobConfigMergesWithExpectedPrecedence() {
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(
                OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "custom", "optimizer",
                OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "override", "optimizer"));
    GravitinoJobSubmitter submitter = new GravitinoJobSubmitter();
    submitter.initialize(new OptimizerEnv(config));

    JobExecutionContext context =
        new JobExecutionContext() {
          @Override
          public NameIdentifier nameIdentifier() {
            return NameIdentifier.of("db", "table");
          }

          @Override
          public Map<String, String> jobOptions() {
            return Map.of("context", "context", "override", "context");
          }

          @Override
          public String jobTemplateName() {
            return "compaction";
          }
        };

    GravitinoJobAdapter adapter =
        jobExecutionContext ->
            Map.of("table", "db.table", "options", "map('k','v')", "override", "adapter");

    Map<String, String> merged = GravitinoJobSubmitter.buildJobConfig(config, context, adapter);

    Assertions.assertEquals("optimizer", merged.get("custom"));
    Assertions.assertEquals("adapter", merged.get("override"));
    Assertions.assertEquals("db.table", merged.get("table"));
    Assertions.assertEquals("map('k','v')", merged.get("options"));
  }
}
