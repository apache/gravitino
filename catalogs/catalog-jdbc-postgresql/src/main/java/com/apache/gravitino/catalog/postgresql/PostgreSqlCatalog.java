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

import com.apache.gravitino.catalog.jdbc.JdbcCatalog;
import com.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import com.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import com.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import com.apache.gravitino.catalog.postgresql.operation.PostgreSqlSchemaOperations;
import com.apache.gravitino.catalog.postgresql.operation.PostgreSqlTableOperations;
import com.apache.gravitino.connector.CatalogOperations;
import com.apache.gravitino.connector.capability.Capability;
import java.util.Map;

public class PostgreSqlCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-postgresql";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter jdbcTypeConverter = createJdbcTypeConverter();
    return new PostgreSQLCatalogOperations(
        createExceptionConverter(),
        jdbcTypeConverter,
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  public Capability newCapability() {
    return new PostgreSqlCatalogCapability();
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new PostgreSqlExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new PostgreSqlTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new PostgreSqlSchemaOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new PostgreSqlTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new PostgreSqlColumnDefaultValueConverter();
  }
}
