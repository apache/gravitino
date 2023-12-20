/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.utils;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;

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
    try {
      return createDBCPDataSource(jdbcConfig);
    } catch (Exception exception) {
      throw new GravitinoRuntimeException("Error creating datasource", exception);
    }
  }

  private static DataSource createDBCPDataSource(JdbcConfig jdbcConfig) throws Exception {
    BasicDataSource basicDataSource =
        BasicDataSourceFactory.createDataSource(getProperties(jdbcConfig));
    String jdbcUrl = jdbcConfig.getJdbcUrl();
    Preconditions.checkArgument(StringUtils.isNotBlank(jdbcUrl), "The jdbc url can't be blank.");
    basicDataSource.setUrl(jdbcUrl);
    String driverClassName = jdbcConfig.getJdbcDriver();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(driverClassName), "The jdbc driver can't be blank.");
    basicDataSource.setDriverClassName(driverClassName);
    String userName = jdbcConfig.getUsername();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(userName), "The jdbc user name can't be blank.");
    basicDataSource.setUsername(userName);
    String password = jdbcConfig.getPassword();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(password), "The jdbc password can't be blank.");
    basicDataSource.setPassword(password);
    basicDataSource.setMaxTotal(jdbcConfig.getPoolMaxSize());
    basicDataSource.setMinIdle(jdbcConfig.getPoolMinSize());
    // Set each time a connection is taken out from the connection pool, a test statement will be
    // executed to confirm whether the connection is valid.
    basicDataSource.setTestOnBorrow(true);
    basicDataSource.setValidationQuery(POOL_TEST_QUERY);
    return basicDataSource;
  }

  private static Properties getProperties(JdbcConfig jdbcConfig) {
    Properties properties = new Properties();
    properties.putAll(jdbcConfig.getAllConfig());
    return properties;
  }

  public static void closeDataSource(DataSource dataSource) {
    if (null != dataSource) {
      try {
        assert dataSource instanceof BasicDataSource;
        ((BasicDataSource) dataSource).close();
      } catch (SQLException ignore) {
        // no op
      }
    }
  }
}
