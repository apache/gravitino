/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql;

import com.datastrato.gravitino.catalog.mysql.converter.MysqlColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlExceptionConverter;
import com.datastrato.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations;
import com.datastrato.gravitino.catalog.mysql.operation.MysqlTableOperations;
import com.datastrato.gravitino.integration.test.catalog.jdbc.TestJdbcAbstractIT;
import java.util.Collections;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;

public class TestMysqlAbstractIT extends TestJdbcAbstractIT {

  @BeforeAll
  public static void startup() {
    CONTAINER =
        new MySQLContainer<>(CatalogMysqlIT.defaultMysqlImageName)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    DATABASE_OPERATIONS = new MysqlDatabaseOperations();
    TABLE_OPERATIONS = new MysqlTableOperations();
    JDBC_EXCEPTION_CONVERTER = new MysqlExceptionConverter();
    TestJdbcAbstractIT.startup();
    DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        DATA_SOURCE,
        JDBC_EXCEPTION_CONVERTER,
        new MysqlTypeConverter(),
        new MysqlColumnDefaultValueConverter(),
        Collections.emptyMap());
  }
}
