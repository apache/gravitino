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
package com.apache.gravitino.catalog.postgresql;

import com.apache.gravitino.catalog.jdbc.JdbcCatalogOperations;
import com.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import java.sql.Driver;
import java.sql.DriverManager;

public class PostgreSQLCatalogOperations extends JdbcCatalogOperations {

  public PostgreSQLCatalogOperations(
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations databaseOperation,
      JdbcTableOperations tableOperation,
      JdbcColumnDefaultValueConverter columnDefaultValueConverter) {
    super(
        exceptionConverter,
        jdbcTypeConverter,
        databaseOperation,
        tableOperation,
        columnDefaultValueConverter);
  }

  @Override
  public void close() {
    super.close();
    try {
      // Unload the PostgreSQL driver, only Unload the driver if it is loaded by
      // IsolatedClassLoader.
      Driver pgDriver = DriverManager.getDriver("jdbc:postgresql://dummy_address:12345/");
      deregisterDriver(pgDriver);
    } catch (Exception e) {
      LOG.warn("Failed to deregister PostgreSQL driver", e);
    }
  }
}
