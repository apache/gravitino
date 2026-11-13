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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksViewOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;

/** Exposes native view listing and lookup in addition to the existing JDBC table operations. */
public class StarRocksCatalogOperations extends MySQLProtocolCompatibleCatalogOperations
    implements ViewCatalog {
  private final StarRocksViewOperations starRocksTables;

  /**
   * Creates the StarRocks operations.
   *
   * @param exceptionConverter Converts JDBC exceptions into Gravitino exceptions.
   * @param jdbcTypeConverter Converts between StarRocks and Gravitino types.
   * @param databaseOperation The StarRocks database operations.
   * @param tableOperation The StarRocks table operations, also used for view lookups. It must be a
   *     {@link StarRocksViewOperations}.
   * @param columnDefaultValueConverter Converts column default values.
   */
  public StarRocksCatalogOperations(
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
    this.starRocksTables = requireViewOperations(tableOperation);
  }

  /**
   * Lists the database's logical views, so they show up in the web UI, the view REST API and the
   * Flink connector. Read-only: create, alter and drop stay unsupported (the ViewCatalog defaults).
   */
  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    String databaseName = NameIdentifier.of(namespace.levels()).name();
    return starRocksTables.listViews(databaseName).stream()
        .map(view -> NameIdentifier.of(namespace, view))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Loads a native logical view, including its comment, output columns and SQL definition.
   *
   * @param ident The view identifier.
   * @return The view metadata.
   * @throws NoSuchViewException If the view does not exist, or is not a logical view.
   */
  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    String databaseName = NameIdentifier.of(ident.namespace().levels()).name();
    return starRocksTables.loadView(databaseName, ident.name());
  }

  /**
   * Narrows the generic table operations to the StarRocks one. {@code createJdbcTableOperations()}
   * keeps returning {@link JdbcTableOperations} so the factory contract stays the same as the other
   * JDBC catalogs, and the concrete type is checked here instead of being assumed.
   */
  private static StarRocksViewOperations requireViewOperations(JdbcTableOperations tableOperation) {
    if (!(tableOperation instanceof StarRocksViewOperations)) {
      throw new IllegalArgumentException(
          String.format(
              "jdbc-starrocks requires %s, got %s",
              StarRocksViewOperations.class.getName(), tableOperation.getClass().getName()));
    }
    return (StarRocksViewOperations) tableOperation;
  }
}
