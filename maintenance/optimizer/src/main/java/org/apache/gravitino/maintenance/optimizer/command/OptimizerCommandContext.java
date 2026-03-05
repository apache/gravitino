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

package org.apache.gravitino.maintenance.optimizer.command;

import java.io.PrintStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;

/** Context shared by optimizer command executors. */
public final class OptimizerCommandContext {
  private final OptimizerEnv optimizerEnv;
  private final String[] identifiers;
  private final String strategyName;
  private final boolean dryRun;
  private final String limit;
  private final String calculatorName;
  private final String actionTime;
  private final String rangeSeconds;
  private final String partitionPathRaw;
  private final UpdateStatsJobOptions updateStatsJobOptions;
  private final Optional<StatisticsInputContent> statisticsInputContent;
  private final PrintStream output;

  public OptimizerCommandContext(
      OptimizerEnv optimizerEnv,
      String[] identifiers,
      String strategyName,
      boolean dryRun,
      String limit,
      String calculatorName,
      String actionTime,
      String rangeSeconds,
      String partitionPathRaw,
      UpdateStatsJobOptions updateStatsJobOptions,
      Optional<StatisticsInputContent> statisticsInputContent,
      PrintStream output) {
    this.optimizerEnv = optimizerEnv;
    this.identifiers = identifiers;
    this.strategyName = strategyName;
    this.dryRun = dryRun;
    this.limit = limit;
    this.calculatorName = calculatorName;
    this.actionTime = actionTime;
    this.rangeSeconds = rangeSeconds;
    this.partitionPathRaw = partitionPathRaw;
    this.updateStatsJobOptions =
        Objects.requireNonNull(updateStatsJobOptions, "updateStatsJobOptions must not be null");
    this.statisticsInputContent = statisticsInputContent;
    this.output = output;
  }

  public OptimizerEnv optimizerEnv() {
    return optimizerEnv;
  }

  public String[] identifiers() {
    return identifiers;
  }

  public List<NameIdentifier> parsedIdentifiers() {
    return OptimizerCommandUtils.parseIdentifiers(identifiers);
  }

  public boolean hasIdentifiers() {
    return identifiers != null && identifiers.length > 0;
  }

  public String strategyName() {
    return strategyName;
  }

  public boolean dryRun() {
    return dryRun;
  }

  public String limit() {
    return limit;
  }

  public String calculatorName() {
    return calculatorName;
  }

  public String actionTime() {
    return actionTime;
  }

  public String rangeSeconds() {
    return rangeSeconds;
  }

  public String partitionPathRaw() {
    return partitionPathRaw;
  }

  public UpdateStatsJobOptions updateStatsJobOptions() {
    return updateStatsJobOptions;
  }

  public String updateMode() {
    return updateStatsJobOptions.updateMode();
  }

  public String targetFileSizeBytes() {
    return updateStatsJobOptions.targetFileSizeBytes();
  }

  public String updaterOptions() {
    return updateStatsJobOptions.updaterOptions();
  }

  public String sparkConf() {
    return updateStatsJobOptions.sparkConf();
  }

  public Optional<StatisticsInputContent> statisticsInputContent() {
    return statisticsInputContent;
  }

  public PrintStream output() {
    return output;
  }

  /** Submit-update-stats-job specific command options. */
  public static final class UpdateStatsJobOptions {
    private final String updateMode;
    private final String targetFileSizeBytes;
    private final String updaterOptions;
    private final String sparkConf;

    public UpdateStatsJobOptions(
        String updateMode, String targetFileSizeBytes, String updaterOptions, String sparkConf) {
      this.updateMode = updateMode;
      this.targetFileSizeBytes = targetFileSizeBytes;
      this.updaterOptions = updaterOptions;
      this.sparkConf = sparkConf;
    }

    public String updateMode() {
      return updateMode;
    }

    public String targetFileSizeBytes() {
      return targetFileSizeBytes;
    }

    public String updaterOptions() {
      return updaterOptions;
    }

    public String sparkConf() {
      return sparkConf;
    }
  }
}
