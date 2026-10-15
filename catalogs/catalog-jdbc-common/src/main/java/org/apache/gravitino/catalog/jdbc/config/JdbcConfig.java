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

package org.apache.gravitino.catalog.jdbc.config;

import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class JdbcConfig extends Config {

  public static final ConfigEntry<String> JDBC_URL =
      new ConfigBuilder("jdbc-url")
          .doc("The url of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_DATABASE =
      new ConfigBuilder("jdbc-database")
          .doc(
              "The database of the JDBC connection. PostgreSQL requires a nonblank "
                  + "value here or a database in jdbc-url; other JDBC providers do not require it.")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_DRIVER =
      new ConfigBuilder("jdbc-driver")
          .doc("The driver of the jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> USERNAME =
      new ConfigBuilder("jdbc-user")
          .doc("The username of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> PASSWORD =
      new ConfigBuilder("jdbc-password")
          .doc("The password of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(Objects::nonNull, ConfigConstants.NOT_NULL_ERROR_MSG)
          .create();

  public static final ConfigEntry<Integer> POOL_MIN_SIZE =
      new ConfigBuilder("jdbc.pool.min-size")
          .doc("The minimum number of connections in the pool")
          .version(ConfigConstants.VERSION_0_3_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(2);

  public static final ConfigEntry<Integer> POOL_MAX_SIZE =
      new ConfigBuilder("jdbc.pool.max-size")
          .doc("The maximum number of connections in the pool")
          .version(ConfigConstants.VERSION_0_3_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(10);

  /**
   * The maximum number of idle connections a catalog pool retains. The default matches the DBCP
   * default that catalog pools used before this setting existed.
   */
  public static final ConfigEntry<Integer> POOL_MAX_IDLE =
      new ConfigBuilder("jdbc.pool.max-idle")
          .doc("The maximum number of idle connections retained in the pool")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(8);

  public static final ConfigEntry<Boolean> TEST_ON_BORROW =
      new ConfigBuilder("jdbc.pool.test-on-borrow")
          .doc("Whether to test the connection on borrow")
          .version(ConfigConstants.VERSION_0_5_0)
          .booleanConf()
          .createWithDefault(true);

  public static final ConfigEntry<Long> POOL_MAX_WAIT_MS =
      new ConfigBuilder("jdbc.pool.max-wait-ms")
          .doc("The maximum Duration that the pool will wait for a connection to be returned")
          .version(ConfigConstants.VERSION_1_1_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(30000L);

  /**
   * The canonical connect/read timeout, in milliseconds, used to bound a JDBC connection so an
   * unreachable or unresponsive backend fails fast instead of hanging. It is translated to each
   * driver family's own parameters (and unit) by {@link
   * org.apache.gravitino.catalog.jdbc.utils.JdbcConnectionTimeouts}.
   *
   * <p>It is applied differently to the two kinds of connection: the pooled connections used by
   * normal operations get only the <em>connect</em> timeout (a read timeout there would abort
   * legitimate long-running operations), while the one-off create-time verification probe gets both
   * the <em>connect</em> and the <em>read/login</em> timeout, because that probe runs under the
   * metalake write lock and a backend that accepts the socket but never speaks the protocol would
   * otherwise block the connect-only bound forever. An operator value set directly on the
   * connection or in the {@code jdbc-url} is never overridden.
   *
   * <p>The value is bounded by {@link Integer#MAX_VALUE} milliseconds because the MySQL and
   * PostgreSQL driver timeouts are {@code int}s (and the PostgreSQL millisecond-to-seconds
   * conversion stays within {@code int} range); a larger value would be accepted here and only fail
   * later when the driver parses it.
   */
  public static final ConfigEntry<Long> CONNECT_TIMEOUT_MS =
      new ConfigBuilder("jdbc.pool.connect-timeout-ms")
          .doc(
              "The connect/read timeout for a JDBC connection, in milliseconds (1.."
                  + Integer.MAX_VALUE
                  + "), translated per driver so an unreachable or silent backend fails fast")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(
              value -> value > 0 && value <= Integer.MAX_VALUE,
              "The value must be a positive number not greater than " + Integer.MAX_VALUE)
          .createWithDefault(30000L);

  /**
   * Whether a relational JDBC catalog verifies that its backend is reachable at create time. When
   * {@code true} (the default, and when absent) the catalog runs a bounded connection probe during
   * create and the create fails if the backend is unreachable; set it to {@code false} to register
   * a catalog whose backend is temporarily unavailable. It is a catalog property, not a connection
   * setting, so it is not forwarded to the driver.
   */
  public static final ConfigEntry<Boolean> VERIFY_ON_CREATE =
      new ConfigBuilder("verify-on-create")
          .doc("Whether a relational JDBC catalog verifies its backend is reachable at create time")
          .version(ConfigConstants.VERSION_2_0_0)
          .booleanConf()
          .createWithDefault(true);

  public String getJdbcUrl() {
    return get(JDBC_URL);
  }

  public String getJdbcDriver() {
    return get(JDBC_DRIVER);
  }

  public String getUsername() {
    return get(USERNAME);
  }

  public String getPassword() {
    return get(PASSWORD);
  }

  public int getPoolMinSize() {
    return get(POOL_MIN_SIZE);
  }

  public int getPoolMaxSize() {
    return get(POOL_MAX_SIZE);
  }

  /**
   * Returns the effective idle-connection limit, capped by the maximum pool size.
   *
   * @return the maximum number of idle connections to retain
   */
  public int getPoolMaxIdle() {
    return Math.min(get(POOL_MAX_IDLE), getPoolMaxSize());
  }

  public String getJdbcDatabase() {
    return get(JDBC_DATABASE);
  }

  public boolean getTestOnBorrow() {
    return get(TEST_ON_BORROW);
  }

  public long getMaxWaitMs() {
    return get(POOL_MAX_WAIT_MS);
  }

  /**
   * Returns the canonical connect/socket timeout for a single JDBC connection, in milliseconds.
   *
   * @return the connect/socket timeout in milliseconds
   */
  public long getConnectTimeoutMs() {
    return get(CONNECT_TIMEOUT_MS);
  }

  /**
   * Returns whether this catalog verifies that its backend is reachable at create time.
   *
   * @return {@code true} to verify the backend at create (default); {@code false} to skip the check
   */
  public boolean getVerifyOnCreate() {
    return get(VERIFY_ON_CREATE);
  }

  public JdbcConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
