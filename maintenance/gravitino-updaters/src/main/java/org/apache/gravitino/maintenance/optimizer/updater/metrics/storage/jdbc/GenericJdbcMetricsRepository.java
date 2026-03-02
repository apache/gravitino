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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.utils.MapUtils;

/** Generic JDBC repository entry for metrics storage. */
public class GenericJdbcMetricsRepository extends JdbcMetricsRepository {

  public static final String JDBC_METRICS_PREFIX = "jdbcMetrics.";
  private static final String DEFAULT_H2_JDBC_URL =
      "jdbc:h2:file:./data/metrics.db;DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=TRUE";

  public static final String JDBC_URL = "jdbcUrl";
  public static final String JDBC_USER = "jdbcUser";
  public static final String JDBC_PASSWORD = "jdbcPassword";
  public static final String JDBC_DRIVER = "jdbcDriver";
  public static final String POOL_MAX_SIZE = "poolMaxSize";
  public static final String POOL_MIN_IDLE = "poolMinIdle";
  public static final String CONNECTION_TIMEOUT_MS = "connectionTimeoutMs";
  public static final String TEST_ON_BORROW = "testOnBorrow";

  @Override
  public void initialize(Map<String, String> optimizerProperties) {
    Map<String, String> jdbcProperties =
        MapUtils.getPrefixMap(
            optimizerProperties, OptimizerConfig.OPTIMIZER_PREFIX + JDBC_METRICS_PREFIX);
    Map<String, String> effectiveJdbcProperties = new HashMap<>(jdbcProperties);

    String jdbcUrl = effectiveJdbcProperties.getOrDefault(JDBC_URL, DEFAULT_H2_JDBC_URL);
    if (StringUtils.isBlank(jdbcUrl)) {
      jdbcUrl = DEFAULT_H2_JDBC_URL;
    }
    String username = effectiveJdbcProperties.getOrDefault(JDBC_USER, DEFAULT_USER);

    Preconditions.checkArgument(
        StringUtils.isNotBlank(username), "Property %s must be non-empty", JDBC_USER);

    effectiveJdbcProperties.put(JDBC_URL, jdbcUrl);
    DataSourceJdbcConnectionProvider connectionProvider =
        new DataSourceJdbcConnectionProvider(effectiveJdbcProperties);
    initializeStorage(connectionProvider);
  }
}
