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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

/**
 * Utility class for creating a {@link DataSource} from a {@link JdbcConfig}. It is mainly
 * responsible for creating connection pool management of data sources and configuring some
 * connection pools. The apache-dbcp2 connection pool is used here.
 */
public class DataSourceUtils {

  /** SQL statements for database connection pool testing. */
  private static final String POOL_TEST_QUERY = "SELECT 1";

  /** list of bad parameters for MySQL */
  private static final List<String> mysqlParameters =
      Arrays.asList(
          "maxAllowedPacket",
          "autoDeserialize",
          "queryInterceptors",
          "statementInterceptors",
          "detectCustomCollations",
          "allowloadlocalinfile",
          "allowUrlInLocalInfile",
          "allowLoadLocalInfileInPath");

  /** list of bad parameters for Postgres */
  private static final List<String> pgParameters =
      Arrays.asList(
          "socketFactory",
          "socketFactoryArg",
          "sslfactory",
          "sslhostnameverifier",
          "sslpasswordcallback",
          "authenticationPluginClassName");

  public static DataSource createDataSource(Map<String, String> properties) {
    return createDataSource(new JdbcConfig(properties));
  }

  public static DataSource createDataSource(JdbcConfig jdbcConfig)
      throws GravitinoRuntimeException {
    try {
      return createDBCPDataSource(jdbcConfig);
    } catch (Exception exception) {
      throw new GravitinoRuntimeException(exception, "Error creating datasource");
    }
  }

  private static DataSource createDBCPDataSource(JdbcConfig jdbcConfig) throws Exception {
    validateJdbcConfig(jdbcConfig);
    BasicDataSource basicDataSource =
        BasicDataSourceFactory.createDataSource(getProperties(jdbcConfig));
    String jdbcUrl = jdbcConfig.getJdbcUrl();
    basicDataSource.setUrl(jdbcUrl);
    String driverClassName = jdbcConfig.getJdbcDriver();
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
    return basicDataSource;
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

  private static void checkUnsafeParameters(
      String url, Map<String, String> config, List<String> unsafeParams, String dbType) {

    String lowerUrl = url.toLowerCase();

    for (String param : unsafeParams) {
      String lowerParam = param.toLowerCase();
      if (lowerUrl.contains(lowerParam) || containsKeyIgnoreCase(config, param)) {
        throw new GravitinoRuntimeException(
            "Unsafe %s parameter '%s' detected in JDBC URL", dbType, param);
      }
    }
  }

  private static void validateJdbcConfig(JdbcConfig jdbcConfig) {
    String driver = jdbcConfig.getJdbcDriver();
    String url = jdbcConfig.getJdbcUrl();
    Map<String, String> all = jdbcConfig.getAllConfig();
    String lowerUrl = url.toLowerCase();
    String decodedUrl = recursiveDecode(lowerUrl);

    if (driver != null) {
      if (decodedUrl.startsWith("jdbc:mysql")) {
        checkUnsafeParameters(decodedUrl, all, mysqlParameters, "MySQL");
      } else if (decodedUrl.startsWith("jdbc:mariadb")) {
        checkUnsafeParameters(decodedUrl, all, mysqlParameters, "MariaDB");
      } else if (decodedUrl.startsWith("jdbc:postgresql")) {
        checkUnsafeParameters(decodedUrl, all, pgParameters, "PostgreSQL");
      }
    }
  }

  private static boolean containsKeyIgnoreCase(Map<String, String> map, String key) {
    Set<String> keys = map.keySet();
    for (String k : keys) {
      if (k.equalsIgnoreCase(key)) {
        return true;
      }
    }
    return false;
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
