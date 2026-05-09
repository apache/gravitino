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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/** Immutable JDBC DataSource settings used to create a pooled {@code BasicDataSource}. */
public class JdbcDataSourceConfig {

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final String driverClassName;
  private final int maxTotal;
  private final int minIdle;
  private final long maxWaitMillis;
  private final boolean testOnBorrow;
  private final String validationQuery;

  /**
   * Creates a JDBC data source configuration.
   *
   * @param jdbcUrl JDBC URL
   * @param username JDBC user name
   * @param password JDBC password
   * @param driverClassName JDBC driver class name, optional when driver auto-loading is available
   * @param maxTotal max number of pooled connections
   * @param minIdle minimum number of idle pooled connections
   * @param maxWaitMillis max wait time in milliseconds when borrowing a connection
   * @param testOnBorrow whether to validate connections when borrowing
   * @param validationQuery validation SQL used when {@code testOnBorrow} is enabled
   */
  public JdbcDataSourceConfig(
      String jdbcUrl,
      String username,
      String password,
      String driverClassName,
      int maxTotal,
      int minIdle,
      long maxWaitMillis,
      boolean testOnBorrow,
      String validationQuery) {
    Preconditions.checkArgument(StringUtils.isNotBlank(jdbcUrl), "jdbcUrl must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(username), "username must not be blank");
    Preconditions.checkArgument(maxTotal > 0, "maxTotal must be positive");
    Preconditions.checkArgument(minIdle >= 0, "minIdle must be non-negative");
    Preconditions.checkArgument(maxWaitMillis > 0, "maxWaitMillis must be positive");
    Preconditions.checkArgument(
        !testOnBorrow || StringUtils.isNotBlank(validationQuery),
        "validationQuery must not be blank when testOnBorrow is enabled");
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.driverClassName = driverClassName;
    this.maxTotal = maxTotal;
    this.minIdle = minIdle;
    this.maxWaitMillis = maxWaitMillis;
    this.testOnBorrow = testOnBorrow;
    this.validationQuery = validationQuery;
  }

  /**
   * @return JDBC URL
   */
  public String jdbcUrl() {
    return jdbcUrl;
  }

  /**
   * @return JDBC user name
   */
  public String username() {
    return username;
  }

  /**
   * @return JDBC password
   */
  public String password() {
    return password;
  }

  /**
   * @return JDBC driver class name, or blank when not set
   */
  public String driverClassName() {
    return driverClassName;
  }

  /**
   * @return max number of pooled connections
   */
  public int maxTotal() {
    return maxTotal;
  }

  /**
   * @return minimum number of idle pooled connections
   */
  public int minIdle() {
    return minIdle;
  }

  /**
   * @return max wait time in milliseconds when borrowing a connection
   */
  public long maxWaitMillis() {
    return maxWaitMillis;
  }

  /**
   * @return whether to validate connections on borrow
   */
  public boolean testOnBorrow() {
    return testOnBorrow;
  }

  /**
   * @return validation SQL query
   */
  public String validationQuery() {
    return validationQuery;
  }
}
