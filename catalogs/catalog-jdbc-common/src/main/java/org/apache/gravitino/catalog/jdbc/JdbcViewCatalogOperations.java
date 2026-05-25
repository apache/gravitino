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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.operation.JdbcViewOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Shared helper that implements {@link ViewCatalog} methods for JDBC catalogs. Both MySQL and
 * PostgreSQL catalog operations classes delegate to this helper to avoid duplicating namespace
 * extraction, audit, and exception-handling logic.
 */
public class JdbcViewCatalogOperations implements ViewCatalog {

  private final JdbcViewOperations viewOps;
  private final Predicate<String> schemaExistsChecker;

  /**
   * Creates a new helper.
   *
   * @param viewOps The database-specific view operations.
   * @param schemaExistsChecker A predicate that checks whether a schema/database exists by name.
   */
  public JdbcViewCatalogOperations(
      JdbcViewOperations viewOps, Predicate<String> schemaExistsChecker) {
    this.viewOps = viewOps;
    this.schemaExistsChecker = schemaExistsChecker;
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    String databaseName = NameIdentifier.of(namespace.levels()).name();
    if (!schemaExistsChecker.test(databaseName)) {
      throw new NoSuchSchemaException("Schema %s does not exist", databaseName);
    }
    return viewOps.listViews(databaseName).stream()
        .map(name -> NameIdentifier.of(namespace, name))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    String databaseName = NameIdentifier.of(ident.namespace().levels()).name();
    return viewOps.load(databaseName, ident.name());
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
    String databaseName = NameIdentifier.of(ident.namespace().levels()).name();
    if (!schemaExistsChecker.test(databaseName)) {
      throw new NoSuchSchemaException("Schema %s does not exist", databaseName);
    }

    SQLRepresentation sqlRep =
        extractSqlRepresentation(representations, viewOps.dialectName(), ident);
    viewOps.create(databaseName, ident.name(), comment, sqlRep.sql());

    JdbcView loaded = viewOps.load(databaseName, ident.name());
    return JdbcView.builder()
        .withName(loaded.name())
        .withComment(comment)
        .withColumns(loaded.columns())
        .withRepresentations(
            Arrays.stream(loaded.representations())
                .filter(SQLRepresentation.class::isInstance)
                .map(SQLRepresentation.class::cast)
                .toArray(SQLRepresentation[]::new))
        .withProperties(loaded.properties())
        .withDefaultCatalog(defaultCatalog)
        .withDefaultSchema(defaultSchema)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentUserName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    String databaseName = NameIdentifier.of(ident.namespace().levels()).name();
    String currentName = ident.name();

    for (ViewChange change : changes) {
      if (change instanceof ViewChange.SetProperty || change instanceof ViewChange.RemoveProperty) {
        throw new UnsupportedOperationException(
            "JDBC catalogs do not support view properties. "
                + "Use an Iceberg REST catalog for managed view properties.");
      } else if (!(change instanceof ViewChange.RenameView)
          && !(change instanceof ViewChange.ReplaceView)) {
        throw new IllegalArgumentException(
            "Unsupported view change type: " + change.getClass().getSimpleName());
      }
    }

    for (ViewChange change : changes) {
      if (change instanceof ViewChange.RenameView) {
        String newName = ((ViewChange.RenameView) change).getNewName();
        viewOps.rename(databaseName, currentName, newName);
        currentName = newName;
      } else if (change instanceof ViewChange.ReplaceView) {
        ViewChange.ReplaceView replace = (ViewChange.ReplaceView) change;
        SQLRepresentation sqlRep =
            extractSqlRepresentation(replace.getRepresentations(), viewOps.dialectName(), ident);
        viewOps.replaceDefinition(databaseName, currentName, replace.getComment(), sqlRep.sql());
      }
    }

    return loadView(NameIdentifier.of(ident.namespace(), currentName));
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    String databaseName = NameIdentifier.of(ident.namespace().levels()).name();
    return viewOps.drop(databaseName, ident.name());
  }

  private static SQLRepresentation extractSqlRepresentation(
      Representation[] representations, String dialect, NameIdentifier ident) {
    Preconditions.checkArgument(
        representations != null && representations.length > 0,
        "At least one representation is required to create a JDBC view");
    for (Representation rep : representations) {
      if (rep instanceof SQLRepresentation
          && dialect.equalsIgnoreCase(((SQLRepresentation) rep).dialect())) {
        return (SQLRepresentation) rep;
      }
    }
    for (Representation rep : representations) {
      if (rep instanceof SQLRepresentation) {
        return (SQLRepresentation) rep;
      }
    }
    throw new IllegalArgumentException(
        "JDBC catalog requires at least one SQL representation to create view " + ident);
  }
}
