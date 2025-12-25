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

  private static final String RECOMMENDER_PREFIX = OPTIMIZER_PREFIX + "recommender.";
  private static final String STATISTICS_PROVIDER = RECOMMENDER_PREFIX + "statisticsProvider";
  private static final String STRATEGY_PROVIDER = RECOMMENDER_PREFIX + "strategyProvider";
  private static final String TABLE_META_PROVIDER = RECOMMENDER_PREFIX + "tableMetaProvider";
  private static final String JOB_SUBMITTER = RECOMMENDER_PREFIX + "jobSubmitter";

  public static final ConfigEntry<String> STATISTICS_PROVIDER_CONFIG =
      new ConfigBuilder(STATISTICS_PROVIDER)
          .doc(
              "Statistics provider implementation name (matches Provider.name()) discoverable via "
                  + "ServiceLoader. Example: 'gravitino-statistics-provider'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> STRATEGY_PROVIDER_CONFIG =
      new ConfigBuilder(STRATEGY_PROVIDER)
          .doc(
              "Strategy provider implementation name (matches Provider.name()) discoverable via "
                  + "ServiceLoader. Example: 'gravitino-strategy-provider'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> TABLE_META_PROVIDER_CONFIG =
      new ConfigBuilder(TABLE_META_PROVIDER)
          .doc(
              "Table metadata provider implementation name (matches Provider.name()) discoverable "
                  + "via ServiceLoader. Example: 'gravitino-table-metadata-provider'.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> JOB_SUBMITTER_CONFIG =
      new ConfigBuilder(JOB_SUBMITTER)
          .doc(
              "Job submitter implementation name (matches Provider.name()) discoverable via "
                  + "ServiceLoader. Example: 'gravitino-job-submitter'.")
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
}
