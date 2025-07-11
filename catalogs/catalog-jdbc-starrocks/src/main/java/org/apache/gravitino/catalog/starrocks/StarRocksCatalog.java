/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.starrocks;

import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksColumnDefaultValueConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksExceptionConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksDatabaseOperations;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksTableOperations;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

public class StarRocksCatalog extends JdbcCatalog {
  public static final StarRocksTablePropertiesMeta STARROCKS_TABLE_PROPERTIES_META =
      new StarRocksTablePropertiesMeta();

  @Override
  public String shortName() {
    return "jdbc-starrocks";
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
    return new StarRocksCatalogCapability();
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new StarRocksExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new StarRocksTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new StarRocksDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new StarRocksTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new StarRocksColumnDefaultValueConverter();
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return STARROCKS_TABLE_PROPERTIES_META;
  }
}
