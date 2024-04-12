/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import com.datastrato.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.doris.converter.DorisExceptionConverter;
import com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter;
import com.datastrato.gravitino.catalog.doris.operation.DorisDatabaseOperations;
import com.datastrato.gravitino.catalog.doris.operation.DorisTableOperations;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.integration.test.TestJdbcAbstractIT;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestDorisAbstractIT extends TestJdbcAbstractIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  @BeforeAll
  public static void startup() {
    containerSuite.startDorisContainer();

    DataSource dataSource = DataSourceUtils.createDataSource(getDorisCatalogProperties());

    DATABASE_OPERATIONS = new DorisDatabaseOperations();
    TABLE_OPERATIONS = new DorisTableOperations();
    JDBC_EXCEPTION_CONVERTER = new DorisExceptionConverter();
    DATABASE_OPERATIONS.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new DorisTypeConverter(),
        new DorisColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  // Overwrite the stop method to close the data source and stop the container
  @AfterAll
  public static void stop() {
    DataSourceUtils.closeDataSource(DATA_SOURCE);
    if (null != CONTAINER) {
      CONTAINER.stop();
    }
  }

  private static Map<String, String> getDorisCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    DorisContainer dorisContainer = containerSuite.getDorisContainer();

    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            dorisContainer.getContainerIpAddress(), DorisContainer.FE_MYSQL_PORT);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), DorisContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), DorisContainer.PASSWORD);

    return catalogProperties;
  }
}
