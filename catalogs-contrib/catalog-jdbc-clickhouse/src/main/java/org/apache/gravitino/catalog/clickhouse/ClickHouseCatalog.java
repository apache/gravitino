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
package org.apache.gravitino.catalog.clickhouse;

import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.clickhouse.operations.ClickHouseDatabaseOperations;
import org.apache.gravitino.catalog.clickhouse.operations.ClickHouseTableOperations;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

public class ClickHouseCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-clickhouse";
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new ClickHouseExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new ClickHouseTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new ClickHouseDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new ClickHouseTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new ClickHouseColumnDefaultValueConverter();
  }

  @Override
  public Capability newCapability() {
    return new ClickHouseCatalogCapability();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return new ClickHouseCatalogPropertiesMetadata();
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return new ClickHouseSchemaPropertiesMetadata();
  }
}
