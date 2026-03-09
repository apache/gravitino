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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.HashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.SqliteDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcCatalogOperations {

  @Test
  public void testTestConnection() {
    try (JdbcCatalogOperations catalogOperations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            new SqliteDatabaseOperations("/illegal/path"),
            new SqliteTableOperations(),
            new SqliteColumnDefaultValueConverter())) {
      Assertions.assertThrows(
          GravitinoRuntimeException.class,
          () ->
              catalogOperations.testConnection(
                  NameIdentifier.of("metalake", "catalog"),
                  Catalog.Type.RELATIONAL,
                  "sqlite",
                  "comment",
                  ImmutableMap.of()));
    }
  }

  @Test
  public void testConfigTestOnBorrow() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.TEST_ON_BORROW.getKey(), "false");

    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertInstanceOf(BasicDataSource.class, dataSource);
    Assertions.assertFalse(((BasicDataSource) dataSource).getTestOnBorrow());
    ((BasicDataSource) dataSource).close();
  }
}
