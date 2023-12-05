/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlExceptionConverter;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlTableOperations;
import java.util.HashMap;
import javax.sql.DataSource;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.com.google.common.collect.Maps;

@Tag("gravitino-docker-it")
public class TestMysqlAbstractIT {

  private static MySQLContainer<?> MYSQL_CONTAINER;

  protected static MysqlDatabaseOperations MYSQL_DATABASE_OPERATIONS;

  protected static MysqlTableOperations MYSQL_TABLE_OPERATIONS;

  private static DataSource DATA_SOURCE;

  protected static final String TEST_DB_NAME = RandomUtils.nextInt(10000) + "_test_db";

  @BeforeAll
  public static void startup() {
    MYSQL_CONTAINER =
        new MySQLContainer<>("mysql:8.2.0")
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");

    MYSQL_CONTAINER.start();
    MYSQL_DATABASE_OPERATIONS = new MysqlDatabaseOperations();
    MYSQL_TABLE_OPERATIONS = new MysqlTableOperations();
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_URL.getKey(), MYSQL_CONTAINER.getJdbcUrl());
    properties.put(JdbcConfig.USERNAME.getKey(), MYSQL_CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), MYSQL_CONTAINER.getPassword());
    DATA_SOURCE = DataSourceUtils.createDataSource(properties);
    MYSQL_DATABASE_OPERATIONS.initialize(DATA_SOURCE, new MysqlExceptionConverter());
    MYSQL_TABLE_OPERATIONS.initialize(
        DATA_SOURCE, new MysqlExceptionConverter(), new MysqlTypeConverter());
  }

  @AfterAll
  public static void stop() {
    DataSourceUtils.closeDataSource(DATA_SOURCE);
    if (null != MYSQL_CONTAINER) {
      MYSQL_CONTAINER.stop();
    }
  }
}
