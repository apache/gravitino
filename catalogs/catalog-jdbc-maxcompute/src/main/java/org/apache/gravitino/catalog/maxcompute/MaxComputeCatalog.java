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
package org.apache.gravitino.catalog.maxcompute;

import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcCatalog;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.maxcompute.converter.MaxComputeColumnDefaultValueConverter;
import org.apache.gravitino.catalog.maxcompute.converter.MaxComputeTypeConverter;
import org.apache.gravitino.catalog.maxcompute.operation.MaxComputeSchemaOpeartions;
import org.apache.gravitino.catalog.maxcompute.operation.MaxComputeTableOpeations;
import org.apache.gravitino.connector.CatalogOperations;

public class MaxComputeCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-maxcompute";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new MaxComputeCatalogOperations(
        createExceptionConverter(),
        createJdbcTypeConverter(),
        createJdbcDatabaseOperations(),
        createJdbcTableOperations(),
        createJdbcColumnDefaultValueConverter());
  }

  @Override
  protected JdbcTypeConverter createJdbcTypeConverter() {
    return new MaxComputeTypeConverter();
  }

  @Override
  protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
    return new MaxComputeSchemaOpeartions();
  }

  @Override
  protected JdbcTableOperations createJdbcTableOperations() {
    return new MaxComputeTableOpeations();
  }

  @Override
  protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
    return new MaxComputeColumnDefaultValueConverter();
  }
}
