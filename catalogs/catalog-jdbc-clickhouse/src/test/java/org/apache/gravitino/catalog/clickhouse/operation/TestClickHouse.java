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
package org.apache.gravitino.catalog.clickhouse.operation;

import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.integration.test.container.ClickHouseContainer;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.BeforeAll;

public class TestClickHouse extends TestJdbc {
  public static final String defaultClickhouseImageName = "clickhouse:8.0";
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static TestDatabaseName TEST_DB_NAME;

  @BeforeAll
  public static void startup() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    TEST_DB_NAME = TestDatabaseName.CLICKHOUSE_CLICKHOUSE_ABSTRACT_IT;
    containerSuite.startClickHouseContainer(TEST_DB_NAME);
    DataSource dataSource = DataSourceUtils.createDataSource(getClickHouseCatalogProperties());

    DATABASE_OPERATIONS = new ClickHouseDatabaseOperations();
    TABLE_OPERATIONS = new ClickHouseTableOperations();
    JDBC_EXCEPTION_CONVERTER = new ClickHouseExceptionConverter();
    DATABASE_OPERATIONS.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  protected static Map<String, String> getClickHouseCatalogProperties() throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    ClickHouseContainer clickHouseContainer = containerSuite.getClickHouseContainer();

    String jdbcUrl = clickHouseContainer.getJdbcUrl(TEST_DB_NAME);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), clickHouseContainer.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), clickHouseContainer.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), clickHouseContainer.getPassword());

    return catalogProperties;
  }
}
