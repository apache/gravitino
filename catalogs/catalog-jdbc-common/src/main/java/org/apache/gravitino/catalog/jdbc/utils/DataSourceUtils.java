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
package org.apache.gravitino.catalog.jdbc.utils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.JdbcUrlUtils;

/**
 * Utility class for creating a {@link DataSource} from a {@link JdbcConfig}. It is mainly
 * responsible for creating connection pool management of data sources and configuring some
 * connection pools. The apache-dbcp2 connection pool is used here.
 */
public class DataSourceUtils {

  /** SQL statements for database connection pool testing. */
  private static final String POOL_TEST_QUERY = "SELECT 1";

  public static DataSource createDataSource(Map<String, String> properties) {
    return createDataSource(new JdbcConfig(properties));
  }

  public static DataSource createDataSource(JdbcConfig jdbcConfig)
      throws GravitinoRuntimeException {
    // H2 is bundled as an embedded backend and must not be used through user-facing catalog
    // configuration. Its INIT parameter allows arbitrary SQL (and Java code via CREATE ALIAS)
    // to execute at connection time, and the H2 driver class must also be blocked to prevent
    // bypassing this check via a mismatched driver and URL combination.
    String decodedUrl = recursiveDecode(jdbcConfig.getJdbcUrl().toLowerCase());
    if (decodedUrl.startsWith("jdbc:h2")) {
      throw new GravitinoRuntimeException("H2 JDBC URL is not allowed in catalog configuration");
    }
    if (jdbcConfig.getJdbcDriver().toLowerCase().startsWith("org.h2.")) {
      throw new GravitinoRuntimeException("H2 JDBC driver is not allowed in catalog configuration");
    }
    try {
      return createDBCPDataSource(jdbcConfig);
    } catch (Exception exception) {
      if (isDriverClassMissing(exception)) {
        // Some JDBC drivers are not packaged with Gravitino and must be installed by the user.
        // Surface a clear, actionable message naming the driver instead of a raw
        // ClassNotFoundException.
        throw new GravitinoRuntimeException(
            exception,
            "JDBC driver class '%s' was not found on the catalog classpath. Install the driver "
                + "JAR in the catalog's libs directory and recreate the catalog.",
            jdbcConfig.getJdbcDriver());
      }
      throw new GravitinoRuntimeException(exception, "Error creating datasource");
    }
  }

  /**
   * Returns whether the given throwable chain indicates a missing JDBC driver class. DBCP2 reports
   * an absent driver as a {@link ClassNotFoundException} (sometimes wrapped in a {@link
   * SQLException} whose message is "Cannot load JDBC driver class ..."), so both the exception
   * chain and that message are checked.
   *
   * @param throwable the throwable thrown while creating the data source
   * @return {@code true} if the failure is due to a driver class that cannot be loaded
   */
  private static boolean isDriverClassMissing(Throwable throwable) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current instanceof ClassNotFoundException || current instanceof NoClassDefFoundError) {
        return true;
      }
      String message = current.getMessage();
      if (message != null && message.contains("Cannot load JDBC driver class")) {
        return true;
      }
      if (current.getCause() == current) {
        break;
      }
    }
    return false;
  }

  private static DataSource createDBCPDataSource(JdbcConfig jdbcConfig) throws Exception {
    JdbcUrlUtils.validateJdbcConfig(
        jdbcConfig.getJdbcDriver(), jdbcConfig.getJdbcUrl(), jdbcConfig.getAllConfig());
    BasicDataSource basicDataSource =
        BasicDataSourceFactory.createDataSource(getProperties(jdbcConfig));
    String jdbcUrl = jdbcConfig.getJdbcUrl();
    basicDataSource.setUrl(jdbcUrl);
    String driverClassName = jdbcConfig.getJdbcDriver();
    // DBCP2 loads the driver lazily on the first connection, so a missing driver would otherwise
    // only surface much later (and as an opaque error). Verify the driver class is on the catalog
    // classpath now, so catalog creation fails fast with a clear, actionable message. Loaded
    // without initialization; the H2 driver is already rejected above.
    verifyDriverPresent(driverClassName);
    basicDataSource.setDriverClassName(driverClassName);
    String userName = jdbcConfig.getUsername();
    basicDataSource.setUsername(userName);
    String password = jdbcConfig.getPassword();
    basicDataSource.setPassword(password);
    basicDataSource.setMaxTotal(jdbcConfig.getPoolMaxSize());
    basicDataSource.setMinIdle(jdbcConfig.getPoolMinSize());
    // Set each time a connection is taken out from the connection pool, a test statement will be
    // executed to confirm whether the connection is valid.
    basicDataSource.setTestOnBorrow(jdbcConfig.getTestOnBorrow());
    basicDataSource.setValidationQuery(POOL_TEST_QUERY);
    basicDataSource.setMaxWait(Duration.ofMillis(jdbcConfig.getMaxWaitMs()));
    return basicDataSource;
  }

  /**
   * Verifies that the configured JDBC driver class is loadable from the catalog classpath. The
   * class is resolved without initialization using the catalog's context class loader (falling back
   * to this class's loader), so an absent driver fails catalog creation immediately rather than on
   * the first connection.
   *
   * @param driverClassName the fully qualified JDBC driver class name
   * @throws ClassNotFoundException if the driver class is not on the catalog classpath
   */
  private static void verifyDriverPresent(String driverClassName) throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = DataSourceUtils.class.getClassLoader();
    }
    Class.forName(driverClassName, false, classLoader);
  }

  private static Properties getProperties(JdbcConfig jdbcConfig) {
    Properties properties = new Properties();
    properties.putAll(jdbcConfig.getAllConfig());
    return properties;
  }

  private static String recursiveDecode(String url) {
    String prev;
    String decoded = url;
    int max = 5;

    do {
      prev = decoded;
      try {
        decoded = java.net.URLDecoder.decode(prev, "UTF-8");
      } catch (Exception e) {
        throw new GravitinoRuntimeException("Unable to decode JDBC URL");
      }
    } while (!prev.equals(decoded) && --max > 0);

    return decoded;
  }

  public static void closeDataSource(DataSource dataSource) {
    if (null != dataSource) {
      try {
        if (dataSource instanceof BasicDataSource) {
          ((BasicDataSource) dataSource).close();
        } else {
          throw new UnsupportedOperationException(
              "close operation can only be called in BasicDataSource.");
        }
      } catch (SQLException ignore) {
        // no op
      }
    }
  }

  private DataSourceUtils() {}
}
