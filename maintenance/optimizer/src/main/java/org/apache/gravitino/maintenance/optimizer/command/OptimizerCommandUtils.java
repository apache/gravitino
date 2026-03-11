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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.monitor.MetricsProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticsInputContent;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;

/** Shared helper methods for optimizer command executors. */
public final class OptimizerCommandUtils {
  static final long METRICS_LIST_FROM_SECONDS = 0L;
  static final long METRICS_LIST_TO_SECONDS = Long.MAX_VALUE;

  private OptimizerCommandUtils() {}

  static List<NameIdentifier> parseIdentifiers(String[] identifiers) {
    if (identifiers == null) {
      return List.of();
    }
    return Arrays.stream(identifiers).map(NameIdentifier::parse).toList();
  }

  static Optional<PartitionPath> parsePartitionPath(String partitionPathStr) {
    if (StringUtils.isBlank(partitionPathStr)) {
      return Optional.empty();
    }
    String trimmed = partitionPathStr.trim();
    Preconditions.checkArgument(
        trimmed.startsWith("["),
        "--partition-path must be a JSON array, for example: [{\"p1\":\"v1\"},{\"p2\":\"v2\"}]");
    return Optional.of(PartitionUtils.decodePartitionPath(trimmed));
  }

  static long parseLongOption(String optionName, String optionValue, boolean allowZero) {
    try {
      long value = Long.parseLong(optionValue);
      Preconditions.checkArgument(
          allowZero ? value >= 0 : value > 0,
          "Option %s must be %s",
          optionName,
          allowZero ? ">= 0" : "> 0");
      return value;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Option %s must be a valid integer value.", optionName), e);
    }
  }

  static OptimizerEnv withStatisticsInput(
      OptimizerEnv optimizerEnv, Optional<StatisticsInputContent> statisticsInputContent) {
    return statisticsInputContent.map(optimizerEnv::withContent).orElse(optimizerEnv);
  }

  static MetricsProvider createMetricsProvider(OptimizerEnv optimizerEnv) {
    MetricsProvider provider =
        ProviderUtils.createMetricsProviderInstance(
            optimizerEnv.config().get(OptimizerConfig.METRICS_PROVIDER_CONFIG));
    provider.initialize(optimizerEnv);
    return provider;
  }
}
