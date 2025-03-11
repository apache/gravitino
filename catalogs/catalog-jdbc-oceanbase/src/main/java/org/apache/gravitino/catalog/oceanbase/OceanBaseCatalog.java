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

import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.JdbcCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.oceanbase.converter.OceanBaseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.oceanbase.converter.OceanBaseTypeConverter;
import org.apache.gravitino.catalog.oceanbase.operation.OceanBaseDatabaseOperations;
import org.apache.gravitino.catalog.oceanbase.operation.OceanBaseTableOperations;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.capability.Capability;

/** Implementation of a OceanBase catalog in Apache Gravitino. */
public class OceanBaseCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-oceanbase";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new JdbcCatalogOperations(
        createExceptionConverter(),
        createJdbcTypeConverter(),
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  public Capability newCapability() {
    return new OceanBaseCatalogCapability();
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new OceanBaseTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new OceanBaseDatabaseOperations();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new OceanBaseTableOperations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new OceanBaseColumnDefaultValueConverter();
  }
}
