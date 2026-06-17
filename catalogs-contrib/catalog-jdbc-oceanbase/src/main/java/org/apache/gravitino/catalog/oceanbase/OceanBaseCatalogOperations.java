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
package org.apache.gravitino.catalog.oceanbase;

import org.apache.gravitino.catalog.jdbc.JdbcCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operations for interacting with the OceanBase catalog in Apache Gravitino.
 *
 * <p>OceanBase supports both MySQL Connector/J and the OceanBase JDBC driver, so it should not use
 * {@code MySQLProtocolCompatibleCatalogOperations}, which rejects non-MySQL Connector/J drivers by
 * major version during catalog initialization.
 */
public class OceanBaseCatalogOperations extends JdbcCatalogOperations {
  private static final Logger LOG = LoggerFactory.getLogger(OceanBaseCatalogOperations.class);

  /**
   * Constructs a new instance of {@link OceanBaseCatalogOperations}.
   *
   * @param exceptionConverter The exception converter to be used by the operations.
   * @param jdbcTypeConverter The type converter to be used by the operations.
   * @param databaseOperation The database operations to be used by the operations.
   * @param tableOperation The table operations to be used by the operations.
   * @param columnDefaultValueConverter The column default value converter to be used by the
   *     operations.
   */
  public OceanBaseCatalogOperations(
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

  /** Closes the OceanBase catalog and shuts down MySQL Connector/J cleanup resources if present. */
  @Override
  public void close() {
    super.close();

    try {
      Class.forName("com.mysql.cj.jdbc.AbandonedConnectionCleanupThread")
          .getMethod("uncheckedShutdown")
          .invoke(null);
      LOG.info("AbandonedConnectionCleanupThread has been shutdown...");
    } catch (ClassNotFoundException e) {
      LOG.debug("MySQL AbandonedConnectionCleanupThread was not found", e);
    } catch (Exception e) {
      LOG.warn("Failed to shutdown AbandonedConnectionCleanupThread", e);
    }
  }
}
