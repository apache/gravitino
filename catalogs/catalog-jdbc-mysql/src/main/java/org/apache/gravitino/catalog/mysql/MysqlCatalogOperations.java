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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.JdbcViewCatalogOperations;
import org.apache.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;

/** MySQL-specific catalog operations with view support. */
public class MysqlCatalogOperations extends MySQLProtocolCompatibleCatalogOperations
    implements ViewCatalog {

  private final JdbcViewOperations viewOperations;
  private JdbcViewCatalogOperations viewCatalogOps;

  /**
   * Constructs a new instance of MysqlCatalogOperations.
   *
   * @param exceptionConverter The exception converter.
   * @param jdbcTypeConverter The type converter.
   * @param databaseOperation The database operations.
   * @param tableOperation The table operations.
   * @param columnDefaultValueConverter The column default value converter.
   * @param viewOperations The view operations.
   */
  public MysqlCatalogOperations(
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations databaseOperation,
      JdbcTableOperations tableOperation,
      JdbcColumnDefaultValueConverter columnDefaultValueConverter,
      JdbcViewOperations viewOperations) {
    super(
        exceptionConverter,
        jdbcTypeConverter,
        databaseOperation,
        tableOperation,
        columnDefaultValueConverter);
    this.viewOperations = viewOperations;
  }

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    super.initialize(conf, info, propertiesMetadata);
    viewOperations.initialize(
        getDataSource(), getExceptionConverter(), getJdbcTypeConverter(), conf);
    this.viewCatalogOps = new JdbcViewCatalogOperations(viewOperations, this::schemaExists);
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    return viewCatalogOps.listViews(namespace);
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    return viewCatalogOps.loadView(ident);
  }

  @Override
  public View createView(
      NameIdentifier ident,
      String comment,
      Column[] columns,
      Representation[] representations,
      String defaultCatalog,
      String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    return viewCatalogOps.createView(
        ident, comment, columns, representations, defaultCatalog, defaultSchema, properties);
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    return viewCatalogOps.alterView(ident, changes);
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    return viewCatalogOps.dropView(ident);
  }
}
