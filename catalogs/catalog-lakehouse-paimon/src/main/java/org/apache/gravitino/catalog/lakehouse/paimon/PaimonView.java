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
package org.apache.gravitino.catalog.lakehouse.paimon;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;
import org.apache.paimon.view.ViewImpl;

/** Represents a Gravitino view object converted from Apache Paimon view metadata. */
final class PaimonView implements View {

  static final String DEFAULT_CATALOG_PROPERTY = "gravitino.view.default-catalog";
  static final String DEFAULT_SCHEMA_PROPERTY = "gravitino.view.default-schema";

  private static final String PAIMON_VIEW_QUERY = "query";

  private final String name;
  @Nullable private final String comment;
  private final Column[] columns;
  private final Representation[] representations;
  @Nullable private final String defaultCatalog;
  @Nullable private final String defaultSchema;
  private final Map<String, String> properties;
  private final AuditInfo auditInfo;

  private PaimonView(
      String name,
      @Nullable String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties,
      AuditInfo auditInfo) {
    this.name = name;
    this.comment = comment;
    this.columns = Arrays.copyOf(columns, columns.length);
    this.representations = Arrays.copyOf(representations, representations.length);
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
    this.properties = new HashMap<>(properties);
    this.auditInfo = auditInfo;
  }

  static PaimonView fromPaimonView(org.apache.paimon.view.View view) {
    Map<String, String> options =
        view.options() == null ? new HashMap<>() : new HashMap<>(view.options());

    String defaultCatalog = options.remove(DEFAULT_CATALOG_PROPERTY);
    String defaultSchema = options.remove(DEFAULT_SCHEMA_PROPERTY);

    return new PaimonView(
        view.name(),
        view.comment().orElse(null),
        GravitinoPaimonColumn.fromPaimonRowType(view.rowType()).toArray(new Column[0]),
        toRepresentations(view.query(), view.dialects()),
        defaultCatalog,
        defaultSchema,
        options,
        AuditInfo.EMPTY);
  }

  static org.apache.paimon.view.View toPaimonView(
      NameIdentifier ident,
      @Nullable String comment,
      @Nullable Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties) {
    Preconditions.checkArgument(ident != null, "View identifier must not be null");
    Preconditions.checkArgument(
        representations != null && representations.length > 0,
        "representations must not be null or empty");

    Column[] safeColumns = columns == null ? new Column[0] : columns;
    List<DataField> fields = new ArrayList<>(safeColumns.length);
    for (int index = 0; index < safeColumns.length; index++) {
      fields.add(GravitinoPaimonColumn.toPaimonColumn(index, safeColumns[index]));
    }

    Map<String, String> dialectQueries = new HashMap<>();
    String query = null;
    for (Representation representation : representations) {
      Preconditions.checkArgument(
          representation instanceof SQLRepresentation, "Paimon only supports SQL representations");
      SQLRepresentation sqlRepresentation = (SQLRepresentation) representation;
      String normalizedDialect = sqlRepresentation.dialect().toLowerCase(Locale.ROOT);
      Preconditions.checkArgument(
          !dialectQueries.containsKey(normalizedDialect),
          "Only one representation per dialect is allowed (case-insensitive). Found duplicate: %s",
          sqlRepresentation.dialect());
      dialectQueries.put(normalizedDialect, sqlRepresentation.sql());
      if (query == null) {
        query = sqlRepresentation.sql();
      }
    }

    Preconditions.checkArgument(
        query != null && !query.isEmpty(), "representations must not be null or empty");

    Map<String, String> options = properties == null ? new HashMap<>() : new HashMap<>(properties);
    if (defaultCatalog != null) {
      options.put(DEFAULT_CATALOG_PROPERTY, defaultCatalog);
    } else {
      options.remove(DEFAULT_CATALOG_PROPERTY);
    }

    if (defaultSchema != null) {
      options.put(DEFAULT_SCHEMA_PROPERTY, defaultSchema);
    } else {
      options.remove(DEFAULT_SCHEMA_PROPERTY);
    }

    String[] namespaceLevels = ident.namespace().levels();
    Preconditions.checkArgument(namespaceLevels.length > 0, "View namespace must not be empty");
    Identifier paimonIdentifier =
        Identifier.create(namespaceLevels[namespaceLevels.length - 1], ident.name());

    return new ViewImpl(paimonIdentifier, fields, query, dialectQueries, comment, options);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Column[] columns() {
    return Arrays.copyOf(columns, columns.length);
  }

  @Override
  public Representation[] representations() {
    return Arrays.copyOf(representations, representations.length);
  }

  @Override
  public String defaultCatalog() {
    return defaultCatalog;
  }

  @Override
  public String defaultSchema() {
    return defaultSchema;
  }

  @Override
  public Map<String, String> properties() {
    return Collections.unmodifiableMap(properties);
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  private static Representation[] toRepresentations(String query, Map<String, String> dialects) {
    Preconditions.checkArgument(
        query != null && !query.isEmpty(), "Paimon view query must not be null or empty");

    Map<String, String> safeDialects =
        dialects == null ? Collections.emptyMap() : new HashMap<>(dialects);
    List<Representation> sqlRepresentations = new ArrayList<>(safeDialects.size() + 1);
    sqlRepresentations.add(
        SQLRepresentation.builder().withDialect(PAIMON_VIEW_QUERY).withSql(query).build());

    safeDialects.forEach(
        (dialect, sql) -> {
          if (!PAIMON_VIEW_QUERY.equalsIgnoreCase(dialect)) {
            sqlRepresentations.add(
                SQLRepresentation.builder().withDialect(dialect).withSql(sql).build());
          }
        });

    return sqlRepresentations.toArray(new Representation[0]);
  }
}
