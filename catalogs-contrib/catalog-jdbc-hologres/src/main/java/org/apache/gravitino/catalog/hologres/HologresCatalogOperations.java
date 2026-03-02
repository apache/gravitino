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
package org.apache.gravitino.catalog.hologres;

import java.sql.Driver;
import java.sql.DriverManager;
import org.apache.gravitino.catalog.jdbc.JdbcCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;

/**
 * Hologres catalog operations implementation.
 *
 * <p>Since Hologres uses the PostgreSQL JDBC driver, this class handles the driver deregistration
 * properly to avoid memory leaks when the catalog is closed.
 */
public class HologresCatalogOperations extends JdbcCatalogOperations {

  public HologresCatalogOperations(
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
      // Unload the PostgreSQL driver, only unload the driver if it is loaded by
      // IsolatedClassLoader. Hologres uses PostgreSQL JDBC driver.
      Driver pgDriver = DriverManager.getDriver("jdbc:postgresql://dummy_address:12345/");
      deregisterDriver(pgDriver);
    } catch (Exception e) {
      LOG.warn("Failed to deregister PostgreSQL driver for Hologres", e);
    }
  }
}
