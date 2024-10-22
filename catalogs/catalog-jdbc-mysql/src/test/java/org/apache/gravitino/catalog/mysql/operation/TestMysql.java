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
package org.apache.gravitino.catalog.mysql.operation;

import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.mysql.converter.MysqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.BeforeAll;

public class TestMysql extends TestJdbc {
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
    JDBC_EXCEPTION_CONVERTER = new JdbcExceptionConverter();
    DATABASE_OPERATIONS.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new MysqlTypeConverter(),
        new MysqlColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  protected static Map<String, String> getMySQLCatalogProperties() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    MySQLContainer mySQLContainer = containerSuite.getMySQLContainer();

    String jdbcUrl = mySQLContainer.getJdbcUrl(TEST_DB_NAME);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), mySQLContainer.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), mySQLContainer.getPassword());

    return catalogProperties;
  }
}
