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

package org.apache.gravitino.maintenance.optimizer.recommender.strategy;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionStrategy;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionStrategy.ScoreMode;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Strategy implementation backed by a Gravitino policy. */
public class GravitinoStrategy implements PartitionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoStrategy.class);

  @VisibleForTesting public static final String STRATEGY_TYPE_KEY = "strategy.type";

  @VisibleForTesting public static final String JOB_TEMPLATE_NAME_KEY = "job.template-name";

  private static final String JOB_OPTIONS_PREFIX = "job.options.";
  /** Rule key for the partition table score aggregation mode. */
  public static final String PARTITION_TABLE_SCORE_MODE = "partition_table_score_mode";
  /** Rule key for the maximum number of partitions selected for execution. */
  public static final String MAX_PARTITION_NUM = "max_partition_num";

  private static final int DEFAULT_MAX_PARTITION_NUM = 100;

  private final Policy policy;

  /**
   * Creates a strategy wrapper for the given policy.
   *
   * @param policy policy to wrap
   */
  public GravitinoStrategy(Policy policy) {
    this.policy = policy;
  }

  /**
   * Returns the strategy name.
   *
   * @return strategy name
   */
  @Override
  public String name() {
    return policy.name();
  }

  /**
   * Returns the strategy type declared in policy properties.
   *
   * @return strategy type
   */
  @Override
  public String strategyType() {
    return policy.content().properties().get(STRATEGY_TYPE_KEY);
  }

  /**
   * Returns policy properties as strategy properties.
   *
   * @return strategy properties
   */
  @Override
  public Map<String, String> properties() {
    return policy.content().properties();
  }

  /**
   * Returns policy rules as strategy rules.
   *
   * @return strategy rules
   */
  @Override
  public Map<String, Object> rules() {
    PolicyContent content = policy.content();
    Map<String, Object> rules = content.rules();
    return rules == null ? Map.of() : rules;
  }

  /**
   * Returns job options parsed from policy rules.
   *
   * @return job options
   */
  @Override
  public Map<String, String> jobOptions() {
    Map<String, String> jobOptions = new HashMap<>();
    rules()
        .forEach(
            (key, value) -> {
              if (key.startsWith(JOB_OPTIONS_PREFIX)) {
                jobOptions.put(key.substring(JOB_OPTIONS_PREFIX.length()), String.valueOf(value));
              }
            });
    return jobOptions;
  }

  /**
   * Returns the job template name for this strategy.
   *
   * @return job template name
   * @throws IllegalArgumentException if the template name is not configured
   */
  @Override
  public String jobTemplateName() {
    return Optional.ofNullable(policy.content().properties().get(JOB_TEMPLATE_NAME_KEY))
        .orElseThrow(() -> new IllegalArgumentException("job.template-name is not set"));
  }

  @Override
  public ScoreMode partitionTableScoreMode() {
    Object value = rules().get(PARTITION_TABLE_SCORE_MODE);
    if (value == null) {
      return ScoreMode.AVG;
    }
    if (value instanceof ScoreMode) {
      return (ScoreMode) value;
    }
    String mode = value.toString().trim().toLowerCase();
    if (mode.isEmpty()) {
      return ScoreMode.AVG;
    }
    switch (mode) {
      case "sum":
        return ScoreMode.SUM;
      case "max":
        return ScoreMode.MAX;
      case "avg":
        return ScoreMode.AVG;
      default:
        LOG.warn(
            "Unsupported partition table score mode '{}' for strategy {}, defaulting to avg",
            mode,
            name());
        return ScoreMode.AVG;
    }
  }

  @Override
  public int maxPartitionNum() {
    Object value = rules().get(MAX_PARTITION_NUM);
    if (value == null) {
      return DEFAULT_MAX_PARTITION_NUM;
    }
    String limit = value.toString().trim();
    if (limit.isEmpty()) {
      return DEFAULT_MAX_PARTITION_NUM;
    }
    try {
      int parsed = Integer.parseInt(limit);
      return parsed > 0 ? parsed : DEFAULT_MAX_PARTITION_NUM;
    } catch (NumberFormatException e) {
      return DEFAULT_MAX_PARTITION_NUM;
    }
  }
}
