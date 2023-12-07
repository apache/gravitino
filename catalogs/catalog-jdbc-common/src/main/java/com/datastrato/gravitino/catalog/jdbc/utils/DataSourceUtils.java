/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.utils;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

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
    basicDataSource.setUrl(jdbcConfig.getJdbcUrl());
    Optional<String> jdbcDriverOptional = jdbcConfig.getJdbcDriverOptional();
    if (jdbcDriverOptional.isPresent()) {
      basicDataSource.setDriverClassName(jdbcDriverOptional.get());
    } else {
      // Automatically load driver
      ServiceLoader<Driver> loadedDrivers = ServiceLoader.load(Driver.class);
      Iterator<Driver> iterator = loadedDrivers.iterator();
      try {
        while (iterator.hasNext()) {
          iterator.next();
        }
      } catch (Throwable t) {
        // Do nothing
      }
    }
    jdbcConfig.getUsernameOptional().ifPresent(basicDataSource::setUsername);
    jdbcConfig.getPasswordOptional().ifPresent(basicDataSource::setPassword);
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
