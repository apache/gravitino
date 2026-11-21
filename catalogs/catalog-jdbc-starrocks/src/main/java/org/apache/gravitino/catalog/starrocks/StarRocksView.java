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

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;

/**
 * A native StarRocks view, read from {@code information_schema}. Gravitino does not create or alter
 * these views; it exposes them read-only so they can be authorized and used as Semantic Model
 * sources.
 */
public final class StarRocksView implements View {
  /** Dialect name of the SQL representation, matching the catalog provider's engine. */
  public static final String DIALECT = "starrocks";

  private static final Column[] NO_COLUMNS = new Column[0];

  private final String name;
  @Nullable private final String comment;
  private final Column[] columns;
  private final Representation[] representations;
  @Nullable private final String defaultSchema;

  /** Creates a view with no column or SQL information. */
  public StarRocksView(String name, @Nullable String comment) {
    this(name, comment, NO_COLUMNS, null, null);
  }

  /**
   * Creates a view.
   *
   * @param name view name
   * @param comment view comment, or null
   * @param columns output columns in ordinal order
   * @param definition the {@code VIEW_DEFINITION} text, or null when StarRocks does not report it
   * @param defaultSchema the database the view lives in; unqualified names in the definition
   *     resolve against it
   */
  public StarRocksView(
      String name,
      @Nullable String comment,
      Column[] columns,
      @Nullable String definition,
      @Nullable String defaultSchema) {
    this.name = Objects.requireNonNull(name, "name");
    this.comment = comment;
    this.columns = Objects.requireNonNull(columns, "columns").clone();
    this.representations =
        definition == null || definition.trim().isEmpty()
            ? new Representation[0]
            : new Representation[] {
              SQLRepresentation.builder().withDialect(DIALECT).withSql(definition).build()
            };
    this.defaultSchema = defaultSchema;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  @Nullable
  public String comment() {
    return comment;
  }

  @Override
  public Column[] columns() {
    return columns.clone();
  }

  @Override
  public Representation[] representations() {
    return representations.clone();
  }

  @Override
  @Nullable
  public String defaultSchema() {
    return defaultSchema;
  }

  @Override
  public AuditInfo auditInfo() {
    return AuditInfo.EMPTY;
  }
}
