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

package org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.BaseExpressionStrategyHandler;
import org.apache.gravitino.rel.Table;

/**
 * Strategy handler that builds compaction job contexts from table metadata and strategy settings.
 */
public class CompactionStrategyHandler extends BaseExpressionStrategyHandler {

  public static final String NAME = "compaction";

  @Override
  public Set<DataRequirement> dataRequirements() {
    return EnumSet.of(
        DataRequirement.TABLE_METADATA,
        DataRequirement.TABLE_STATISTICS,
        DataRequirement.PARTITION_STATISTICS);
  }

  @Override
  public String strategyType() {
    return NAME;
  }

  @Override
  protected JobExecutionContext buildJobExecutionContext(
      NameIdentifier nameIdentifier,
      Strategy strategy,
      Table tableMetadata,
      List<PartitionPath> partitions,
      Map<String, String> jobOptions) {
    List<PartitionPath> resolvedPartitions = partitions == null ? List.of() : partitions;
    return new CompactionJobContext(
        nameIdentifier,
        jobOptions,
        strategy.jobTemplateName(),
        tableMetadata.columns(),
        tableMetadata.partitioning(),
        resolvedPartitions);
  }
}
