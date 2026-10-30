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
package org.apache.gravitino.trino.connector.metadata;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;

/**
 * Helps the Apache Gravitino connector access view metadata using the Trino dialect SQL
 * representation from the Gravitino client.
 */
public class GravitinoView {

  private final String schemaName;
  private final String viewName;
  private final List<GravitinoColumn> columns;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  @Nullable private final String sql;
  @Nullable private final String defaultCatalog;
  @Nullable private final String defaultSchema;

  /**
   * Constructs a new GravitinoView by unwrapping the {@link Dialects#TRINO} SQL representation from
   * a Gravitino {@link View}.
   *
   * @param schemaName the schema name
   * @param viewName the view name
   * @param view the Gravitino view metadata
   */
  public GravitinoView(String schemaName, String viewName, View view) {
    this.schemaName = schemaName;
    this.viewName = viewName;

    ImmutableList.Builder<GravitinoColumn> viewColumns = ImmutableList.builder();
    for (int i = 0; i < view.columns().length; i++) {
      viewColumns.add(new GravitinoColumn(view.columns()[i], i));
    }
    this.columns = viewColumns.build();
    this.comment = view.comment();
    this.properties = view.properties();

    Optional<SQLRepresentation> trinoRepresentation = view.sqlFor(Dialects.TRINO);
    this.sql = trinoRepresentation.map(SQLRepresentation::sql).orElse(null);
    this.defaultCatalog = view.defaultCatalog();
    this.defaultSchema = view.defaultSchema();
  }

  /**
   * Constructs a new GravitinoView for the Trino-to-Gravitino creation direction.
   *
   * @param schemaName the schema name
   * @param viewName the view name
   * @param columns the view output columns
   * @param comment the view comment
   * @param properties the view properties
   * @param sql the Trino dialect SQL definition of the view
   * @param defaultCatalog the default catalog used to resolve unqualified identifiers referenced by
   *     the view definition, or {@code null} if not set
   * @param defaultSchema the default schema used to resolve unqualified identifiers referenced by
   *     the view definition, or {@code null} if not set
   */
  public GravitinoView(
      String schemaName,
      String viewName,
      List<GravitinoColumn> columns,
      @Nullable String comment,
      Map<String, String> properties,
      String sql,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema) {
    this.schemaName = schemaName;
    this.viewName = viewName;
    this.columns = columns;
    this.comment = comment;
    this.properties = properties;
    this.sql = sql;
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
  }

  /**
   * Retrieves the schema name of the view.
   *
   * @return the schema name of the view
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Retrieves the name of the view.
   *
   * @return the name of the view
   */
  public String getName() {
    return viewName;
  }

  /**
   * Retrieves the columns of the view.
   *
   * @return the columns of the view
   */
  public List<GravitinoColumn> getColumns() {
    return columns;
  }

  /**
   * Retrieves the raw columns of the view.
   *
   * @return the raw columns of the view
   */
  public Column[] getRawColumns() {
    Column[] gravitinoColumns = new Column[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      GravitinoColumn column = columns.get(i);
      gravitinoColumns[i] =
          Column.of(
              column.getName(),
              column.getType(),
              column.getComment(),
              column.isNullable(),
              column.isAutoIncrement(),
              column.getDefaultValue());
    }
    return gravitinoColumns;
  }

  /**
   * Retrieves the comment of the view.
   *
   * @return the comment of the view, or {@code null} if not set
   */
  @Nullable
  public String getComment() {
    return comment;
  }

  /**
   * Retrieves the properties of the view.
   *
   * @return the properties of the view
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Retrieves the Trino dialect SQL definition of the view, or {@code null} if the view does not
   * have a Trino dialect representation.
   *
   * @return the Trino dialect SQL, or {@code null}
   */
  @Nullable
  public String getSql() {
    return sql;
  }

  /**
   * Retrieves the default catalog used to resolve unqualified identifiers referenced by the view
   * definition, or {@code null} if not set.
   *
   * @return the default catalog, or {@code null}
   */
  @Nullable
  public String getDefaultCatalog() {
    return defaultCatalog;
  }

  /**
   * Retrieves the default schema used to resolve unqualified identifiers referenced by the view
   * definition, or {@code null} if not set.
   *
   * @return the default schema, or {@code null}
   */
  @Nullable
  public String getDefaultSchema() {
    return defaultSchema;
  }
}
