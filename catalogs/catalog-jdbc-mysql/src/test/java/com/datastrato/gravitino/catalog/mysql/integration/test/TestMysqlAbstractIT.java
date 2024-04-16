/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.integration.test;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.integration.test.TestJdbcAbstractIT;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlExceptionConverter;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlTableOperations;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.integration.test.util.TestDatabaseName;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;

public class TestMysqlAbstractIT extends TestJdbcAbstractIT {
  public static final String defaultMysqlImageName = "mysql:8.0";
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static TestDatabaseName TEST_DB_NAME;

  @BeforeAll
  public static void startup() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    TEST_DB_NAME = TestDatabaseName.MYSQL_MYSQL_ABSTRACT_IT;
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    DataSource dataSource = DataSourceUtils.createDataSource(getMySQLCatalogProperties());

    DATABASE_OPERATIONS = new MysqlDatabaseOperations();
    TABLE_OPERATIONS = new MysqlTableOperations();
    JDBC_EXCEPTION_CONVERTER = new MysqlExceptionConverter();
    DATABASE_OPERATIONS.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new MysqlTypeConverter(),
        new MysqlColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  private static Map<String, String> getMySQLCatalogProperties() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    MySQLContainer mySQLContainer = containerSuite.getMySQLContainer();

    String jdbcUrl = mySQLContainer.getJdbcUrl(TEST_DB_NAME);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), mySQLContainer.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), mySQLContainer.getPassword());

    return catalogProperties;
  }
}
