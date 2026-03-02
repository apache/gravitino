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

package org.apache.gravitino.maintenance.optimizer.updater.calculator.local;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.TableAndPartitionStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkJobStatistics;
import org.apache.gravitino.maintenance.optimizer.api.updater.SupportsCalculateBulkTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerContent;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/**
 * Statistics calculator that reads statistics from either a local file path or an inline payload.
 */
public class LocalStatisticsCalculator
    implements SupportsCalculateBulkTableStatistics, SupportsCalculateBulkJobStatistics {

  public static final String NAME = "local-stats-calculator";

  private StatisticsImporter statisticsImporter;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    OptimizerContent optimizerContent =
        optimizerEnv
            .content()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "LocalStatisticsCalculator requires runtime statistics input content."));
    Preconditions.checkArgument(
        optimizerContent instanceof StatisticsInputContent,
        "LocalStatisticsCalculator expects StatisticsInputContent, but got: %s",
        optimizerContent.getClass().getSimpleName());
    StatisticsInputContent statisticsInputContent = (StatisticsInputContent) optimizerContent;

    if (statisticsInputContent.hasFilePath()) {
      this.statisticsImporter =
          new FileStatisticsImporter(Path.of(statisticsInputContent.filePath()), defaultCatalog);
    } else if (statisticsInputContent.hasPayload()) {
      this.statisticsImporter =
          new PayloadStatisticsImporter(statisticsInputContent.payload(), defaultCatalog);
    } else {
      throw new IllegalArgumentException(
          "StatisticsInputContent must provide either file path or payload.");
    }
  }

  @Override
  public TableAndPartitionStatistics calculateTableStatistics(NameIdentifier tableIdentifier) {
    ensureInitialized();
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    return statisticsImporter.readTableStatistics(tableIdentifier);
  }

  @Override
  public Map<NameIdentifier, TableAndPartitionStatistics> calculateBulkTableStatistics() {
    ensureInitialized();
    return statisticsImporter.bulkReadAllTableStatistics();
  }

  @Override
  public List<StatisticEntry<?>> calculateJobStatistics(NameIdentifier jobIdentifier) {
    ensureInitialized();
    Preconditions.checkArgument(jobIdentifier != null, "jobIdentifier must not be null");
    return statisticsImporter.readJobStatistics(jobIdentifier);
  }

  @Override
  public Map<NameIdentifier, List<StatisticEntry<?>>> calculateAllJobStatistics() {
    ensureInitialized();
    return statisticsImporter.bulkReadAllJobStatistics();
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        statisticsImporter != null,
        "LocalStatisticsCalculator has not been initialized. Call initialize(optimizerEnv) first.");
  }
}
