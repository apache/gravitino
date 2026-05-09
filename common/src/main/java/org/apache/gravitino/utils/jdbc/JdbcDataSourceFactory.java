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

package org.apache.gravitino.utils.jdbc;

import java.time.Duration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;

/** Utility for creating a pooled JDBC {@link BasicDataSource}. */
public final class JdbcDataSourceFactory {

  /** Default maximum number of pooled connections. */
  public static final int DEFAULT_MAX_TOTAL = 10;
  /** Default minimum number of idle pooled connections. */
  public static final int DEFAULT_MIN_IDLE = 2;
  /** Default max wait time in milliseconds when borrowing a connection. */
  public static final long DEFAULT_MAX_WAIT_MILLIS = 30_000L;
  /** Default flag to validate connections when borrowing. */
  public static final boolean DEFAULT_TEST_ON_BORROW = true;
  /** Default validation SQL query. */
  public static final String DEFAULT_VALIDATION_QUERY = "SELECT 1";

  private JdbcDataSourceFactory() {}

  /**
   * Creates a configured {@link BasicDataSource}.
   *
   * @param config JDBC data source configuration
   * @return configured pooled data source
   */
  public static BasicDataSource create(JdbcDataSourceConfig config) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.jdbcUrl());
    dataSource.setUsername(config.username());
    dataSource.setPassword(config.password());

    if (StringUtils.isNotBlank(config.driverClassName())) {
      dataSource.setDriverClassName(config.driverClassName());
    }
    dataSource.setMaxTotal(config.maxTotal());
    dataSource.setMinIdle(config.minIdle());
    dataSource.setMaxWait(Duration.ofMillis(config.maxWaitMillis()));
    dataSource.setTestOnBorrow(config.testOnBorrow());
    if (config.testOnBorrow()) {
      dataSource.setValidationQuery(config.validationQuery());
    }
    return dataSource;
  }
}
