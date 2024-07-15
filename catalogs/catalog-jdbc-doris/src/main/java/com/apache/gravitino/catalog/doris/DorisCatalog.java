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
package com.apache.gravitino.catalog.doris;

import com.apache.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import com.apache.gravitino.catalog.doris.converter.DorisExceptionConverter;
import com.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import com.apache.gravitino.catalog.doris.operation.DorisDatabaseOperations;
import com.apache.gravitino.catalog.doris.operation.DorisTableOperations;
import com.apache.gravitino.catalog.jdbc.JdbcCatalog;
import com.apache.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import com.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.apache.gravitino.connector.CatalogOperations;
import com.apache.gravitino.connector.capability.Capability;
import java.util.Map;

/** Implementation of an Apache Doris catalog in Apache Gravitino. */
public class DorisCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-doris";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter jdbcTypeConverter = createJdbcTypeConverter();
    return new MySQLProtocolCompatibleCatalogOperations(
        createExceptionConverter(),
        jdbcTypeConverter,
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  public Capability newCapability() {
    return new DorisCatalogCapability();
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new DorisExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new DorisTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new DorisDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new DorisTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new DorisColumnDefaultValueConverter();
  }
}
