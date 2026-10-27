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
package org.apache.gravitino.catalog.postgresql.operation;

import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.integration.test.container.FixtureContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLFixture;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.BeforeAll;

@PostgreSQLFixture(database = TestDatabaseName.PG_TEST_PG_OPERATIONS)
public class TestPostgreSql extends TestJdbc {

  @FixtureContainer private static PostgreSQLContainer postgreSQLContainer;

  @BeforeAll
  public static void startup() throws Exception {
    // Each class gets a database of its own, as it did with a server of its own, so classes
    // sharing the server do not see each other's schemas.
    String database = RandomNameUtils.genRandomName("test_db_");
    try (Connection connection =
            DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(TestDatabaseName.PG_TEST_PG_OPERATIONS),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
    }
    DATA_SOURCE = DataSourceUtils.createDataSource(dataSourceProperties(database));
    DATABASE_OPERATIONS = new PostgreSqlSchemaOperations();
    JDBC_EXCEPTION_CONVERTER = new PostgreSqlExceptionConverter();
    Map<String, String> config = Map.of(JdbcConfig.JDBC_DATABASE.getKey(), database);
    PostgreSqlTableOperations tableOperations = new PostgreSqlTableOperations();
    TABLE_OPERATIONS = tableOperations;
    DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, config);
    TABLE_OPERATIONS.initialize(
        DATA_SOURCE,
        JDBC_EXCEPTION_CONVERTER,
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        config);
    tableOperations.setDatabaseOperation(DATABASE_OPERATIONS);
    DATABASE_OPERATIONS.create(TEST_DB_NAME, null, null);
  }

  /** Returns the data source properties for {@code database} on the shared server. */
  protected static Map<String, String> dataSourceProperties(String database) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), postgreSQLContainer.getJdbcUrl() + database);
    properties.put(JdbcConfig.USERNAME.getKey(), postgreSQLContainer.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), postgreSQLContainer.getPassword());
    return properties;
  }
}
