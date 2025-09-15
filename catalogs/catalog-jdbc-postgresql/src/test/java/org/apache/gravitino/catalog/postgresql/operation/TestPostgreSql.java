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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

public class TestPostgreSql extends TestJdbc {

  public static final String DEFAULT_POSTGRES_IMAGE = "postgres:13";

  @BeforeAll
  public static void startup() throws Exception {
    CONTAINER =
        new PostgreSQLContainer<>(DEFAULT_POSTGRES_IMAGE)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    DATABASE_OPERATIONS = new PostgreSqlSchemaOperations();
    JDBC_EXCEPTION_CONVERTER = new PostgreSqlExceptionConverter();
    TestJdbc.startup();
    String jdbcUrl = CONTAINER.getJdbcUrl();
    try {
      String database =
          new URI(CONTAINER.getJdbcUrl().substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.length()))
              .getPath();
      Map<String, String> config =
          new HashMap<String, String>() {
            {
              put(JdbcConfig.JDBC_DATABASE.getKey(), database);
            }
          };
      TABLE_OPERATIONS = new PostgreSqlTableOperations();
      DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, config);
      TABLE_OPERATIONS.initialize(
          DATA_SOURCE,
          JDBC_EXCEPTION_CONVERTER,
          new PostgreSqlTypeConverter(),
          new PostgreSqlColumnDefaultValueConverter(),
          config);
      if (TABLE_OPERATIONS instanceof RequireDatabaseOperation) {
        ((RequireDatabaseOperation) TABLE_OPERATIONS).setDatabaseOperation(DATABASE_OPERATIONS);
      }
      DATABASE_OPERATIONS.create(TEST_DB_NAME, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
