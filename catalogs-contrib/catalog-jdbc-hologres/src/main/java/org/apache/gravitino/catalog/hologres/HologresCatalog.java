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

import java.util.Map;
import org.apache.gravitino.catalog.hologres.converter.HologresColumnDefaultValueConverter;
import org.apache.gravitino.catalog.hologres.converter.HologresExceptionConverter;
import org.apache.gravitino.catalog.hologres.converter.HologresTypeConverter;
import org.apache.gravitino.catalog.hologres.operation.HologresSchemaOperations;
import org.apache.gravitino.catalog.hologres.operation.HologresTableOperations;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.capability.Capability;

/**
 * Implementation of a Hologres catalog in Apache Gravitino.
 *
 * <p>Hologres is a real-time interactive analytics service developed by Alibaba Cloud that is fully
 * compatible with PostgreSQL. It uses the PostgreSQL JDBC driver for connections.
 *
 * @see <a href="https://help.aliyun.com/zh/hologres">Hologres Documentation</a>
 */
public class HologresCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-hologres";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter jdbcTypeConverter = createJdbcTypeConverter();
    return new HologresCatalogOperations(
        createExceptionConverter(),
        jdbcTypeConverter,
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  public Capability newCapability() {
    return new HologresCatalogCapability();
  }

  @Override
  protected JdbcExceptionConverter createExceptionConverter() {
    return new HologresExceptionConverter();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new HologresTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new HologresSchemaOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new HologresTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new HologresColumnDefaultValueConverter();
  }
}
