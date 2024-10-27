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
package org.apache.gravitino.catalog.mysql;

import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.mysql.converter.MysqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import org.apache.gravitino.catalog.mysql.operation.MysqlDatabaseOperations;
import org.apache.gravitino.catalog.mysql.operation.MysqlTableOperations;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

/** Implementation of a Mysql catalog in Apache Gravitino. */
public class MysqlCatalog extends JdbcCatalog {

  private static final MysqlTablePropertiesMetadata TABLE_PROPERTIES_META =
      new MysqlTablePropertiesMetadata();

  @Override
  public String shortName() {
    return "jdbc-mysql";
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
    return new MysqlCatalogCapability();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new MysqlTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new MysqlDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new MysqlTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new MysqlColumnDefaultValueConverter();
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_META;
  }
}
