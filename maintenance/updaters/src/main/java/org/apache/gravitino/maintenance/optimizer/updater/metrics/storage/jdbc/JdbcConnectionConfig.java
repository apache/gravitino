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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.utils.jdbc.JdbcDataSourceFactory;

/** Typed JDBC connection config holder for metrics storage repositories. */
public class JdbcConnectionConfig extends Config {

  public static final ConfigEntry<String> JDBC_URL_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.JDBC_URL)
          .doc("JDBC URL for metrics storage.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_USER_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.JDBC_USER)
          .doc("JDBC username for metrics storage.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(JdbcMetricsRepository.DEFAULT_USER);

  public static final ConfigEntry<String> JDBC_PASSWORD_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.JDBC_PASSWORD)
          .doc("JDBC password for metrics storage.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault(JdbcMetricsRepository.DEFAULT_PASSWORD);

  public static final ConfigEntry<String> JDBC_DRIVER_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.JDBC_DRIVER)
          .doc("Optional JDBC driver class name.")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .createWithDefault("");

  public static final ConfigEntry<Integer> POOL_MAX_SIZE_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.POOL_MAX_SIZE)
          .doc("JDBC connection pool max total size.")
          .version(ConfigConstants.VERSION_1_2_0)
          .intConf()
          .checkValue(v -> v > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(JdbcDataSourceFactory.DEFAULT_MAX_TOTAL);

  public static final ConfigEntry<Integer> POOL_MIN_IDLE_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.POOL_MIN_IDLE)
          .doc("JDBC connection pool min idle size.")
          .version(ConfigConstants.VERSION_1_2_0)
          .intConf()
          .checkValue(v -> v >= 0, "The value must be non-negative")
          .createWithDefault(JdbcDataSourceFactory.DEFAULT_MIN_IDLE);

  public static final ConfigEntry<Long> CONNECTION_TIMEOUT_MS_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.CONNECTION_TIMEOUT_MS)
          .doc("JDBC connection max wait timeout in milliseconds.")
          .version(ConfigConstants.VERSION_1_2_0)
          .longConf()
          .checkValue(v -> v > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(JdbcDataSourceFactory.DEFAULT_MAX_WAIT_MILLIS);

  public static final ConfigEntry<Boolean> TEST_ON_BORROW_CONFIG =
      new ConfigBuilder(GenericJdbcMetricsRepository.TEST_ON_BORROW)
          .doc("Whether to validate JDBC connections when borrowing.")
          .version(ConfigConstants.VERSION_1_2_0)
          .booleanConf()
          .createWithDefault(JdbcDataSourceFactory.DEFAULT_TEST_ON_BORROW);

  public JdbcConnectionConfig(Map<String, String> properties) {
    super(false);
    if (properties != null) {
      configMap.putAll(properties);
    }
  }

  public String jdbcUrl() {
    return get(JDBC_URL_CONFIG);
  }

  public String username() {
    return get(JDBC_USER_CONFIG);
  }

  public String password() {
    return get(JDBC_PASSWORD_CONFIG);
  }

  public String driverClassName() {
    return get(JDBC_DRIVER_CONFIG);
  }

  public int maxTotal() {
    return get(POOL_MAX_SIZE_CONFIG);
  }

  public int minIdle() {
    return get(POOL_MIN_IDLE_CONFIG);
  }

  public long maxWaitMillis() {
    return get(CONNECTION_TIMEOUT_MS_CONFIG);
  }

  public boolean testOnBorrow() {
    return get(TEST_ON_BORROW_CONFIG);
  }
}
