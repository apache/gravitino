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

package org.apache.gravitino.maintenance.optimizer.common.conf;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.maintenance.optimizer.recommender.job.NoopJobSubmitter;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.GravitinoStatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategyProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.table.GravitinoTableMetadataProvider;

/**
 * Central configuration holder for the optimizer/recommender runtime. Keys are grouped under the
 * {@code gravitino.optimizer.*} prefix and capture both core connectivity (URI, metalake, default
 * catalog) and pluggable implementation wiring (statistics provider, strategy provider, table
 * metadata provider, job submitter).
 */
public class OptimizerConfig extends Config {

  public static final String OPTIMIZER_PREFIX = "gravitino.optimizer.";

  public static final String GRAVITINO_URI = OPTIMIZER_PREFIX + "gravitinoUri";
  public static final String GRAVITINO_METALAKE = OPTIMIZER_PREFIX + "gravitinoMetalake";
  public static final String GRAVITINO_DEFAULT_CATALOG =
      OPTIMIZER_PREFIX + "gravitinoDefaultCatalog";
  public static final String JOB_ADAPTER_PREFIX = OPTIMIZER_PREFIX + "jobAdapter.";
  public static final String JOB_SUBMITTER_CONFIG_PREFIX = OPTIMIZER_PREFIX + "jobSubmitterConfig.";

  private static final String RECOMMENDER_PREFIX = OPTIMIZER_PREFIX + "recommender.";
  private static final String STATISTICS_PROVIDER = RECOMMENDER_PREFIX + "statisticsProvider";
  private static final String STRATEGY_PROVIDER = RECOMMENDER_PREFIX + "strategyProvider";
  private static final String TABLE_META_PROVIDER = RECOMMENDER_PREFIX + "tableMetaProvider";
  private static final String JOB_SUBMITTER = RECOMMENDER_PREFIX + "jobSubmitter";

  private static final String UPDATER_PREFIX = OPTIMIZER_PREFIX + "updater.";
  private static final String STATISTICS_UPDATER = UPDATER_PREFIX + "statisticsUpdater";
  private static final String METRICS_UPDATER = UPDATER_PREFIX + "metricsUpdater";

  public static final ConfigEntry<String> STATISTICS_PROVIDER_CONFIG =
      new ConfigBuilder(STATISTICS_PROVIDER)
          .doc(
              "Statistics provider implementation name (matches Provider.name()) discoverable via "
                  + "ServiceLoader. Example: '"
                  + GravitinoStatisticsProvider.NAME
                  + "'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault(GravitinoStatisticsProvider.NAME);

  public static final ConfigEntry<String> STRATEGY_PROVIDER_CONFIG =
      new ConfigBuilder(STRATEGY_PROVIDER)
          .doc(
              "Strategy provider implementation name (matches Provider.name()) discoverable via "
                  + "ServiceLoader. Example: '"
                  + GravitinoStrategyProvider.NAME
                  + "'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault(GravitinoStrategyProvider.NAME);

  public static final ConfigEntry<String> TABLE_META_PROVIDER_CONFIG =
      new ConfigBuilder(TABLE_META_PROVIDER)
          .doc(
              "Table metadata provider implementation name (matches Provider.name()) discoverable "
                  + "via ServiceLoader. Example: '"
                  + GravitinoTableMetadataProvider.NAME
                  + "'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault(GravitinoTableMetadataProvider.NAME);

  public static final ConfigEntry<String> JOB_SUBMITTER_CONFIG =
      new ConfigBuilder(JOB_SUBMITTER)
          .doc(
              "Job submitter implementation name (matches Provider.name()) discoverable via "
                  + "ServiceLoader. Example: '"
                  + NoopJobSubmitter.NAME
                  + "'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault(NoopJobSubmitter.NAME);

  public static final ConfigEntry<String> STATISTICS_UPDATER_CONFIG =
      new ConfigBuilder(STATISTICS_UPDATER)
          .doc("The statistics updater implementation name (matches Provider.name()).")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> METRICS_UPDATER_CONFIG =
      new ConfigBuilder(METRICS_UPDATER)
          .doc("The metrics updater implementation name (matches Provider.name()).")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> GRAVITINO_URI_CONFIG =
      new ConfigBuilder(GRAVITINO_URI)
          .doc("The URI of the Gravitino server.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault("http://localhost:8090");

  public static final ConfigEntry<String> GRAVITINO_METALAKE_CONFIG =
      new ConfigBuilder(GRAVITINO_METALAKE)
          .doc("The metalake name in Gravitino.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> GRAVITINO_DEFAULT_CATALOG_CONFIG =
      new ConfigBuilder(GRAVITINO_DEFAULT_CATALOG)
          .doc("The default catalog name in Gravitino.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();

  /** Create an empty optimizer config to populate programmatically. */
  public OptimizerConfig() {
    super(false);
  }

  /**
   * Create an optimizer config from a pre-parsed map, typically from a configuration file or
   * command overrides.
   *
   * @param properties key/value pairs respecting the {@code gravitino.optimizer.*} namespace
   */
  public OptimizerConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  /**
   * Returns job submitter custom config entries with the {@code
   * gravitino.optimizer.jobSubmitterConfig.} prefix stripped.
   *
   * @return custom job submitter config map
   */
  public Map<String, String> jobSubmitterConfigs() {
    return getConfigsWithPrefix(JOB_SUBMITTER_CONFIG_PREFIX);
  }

  public String getStrategyHandlerClassName(String strategyHandlerName) {
    String configKey =
        String.format(OPTIMIZER_PREFIX + "strategyHandler.%s.className", strategyHandlerName);
    return configMap.get(configKey);
  }

  public String getJobAdapterClassName(String jobTemplateName) {
    String configKey = String.format(JOB_ADAPTER_PREFIX + "%s.className", jobTemplateName);
    return configMap.get(configKey);
  }
}
