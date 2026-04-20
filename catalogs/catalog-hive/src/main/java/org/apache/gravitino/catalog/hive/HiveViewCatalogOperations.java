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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;
import static org.apache.gravitino.catalog.hive.HiveConstants.HIVE_FILTER_FIELD_PARAMS;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveViewCatalogOperations implements ViewCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HiveViewCatalogOperations.class);
  private static final short MAX_TABLES = -1;

  private final Supplier<CachedClientPool> clientPoolSupplier;
  private final Supplier<String> catalogNameSupplier;
  private final Predicate<NameIdentifier> schemaExistsChecker;

  HiveViewCatalogOperations(
      Supplier<CachedClientPool> clientPoolSupplier,
      Supplier<String> catalogNameSupplier,
      Predicate<NameIdentifier> schemaExistsChecker) {
    this.clientPoolSupplier = clientPoolSupplier;
    this.catalogNameSupplier = catalogNameSupplier;
    this.schemaExistsChecker = schemaExistsChecker;
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExistsChecker.test(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", namespace);
    }
    try {
      String viewFilter =
          String.format("%stableType like \"VIRTUAL_VIEW\"", HIVE_FILTER_FIELD_PARAMS);
      List<String> views =
          clientPool()
              .run(
                  c ->
                      c.listTableNamesByFilter(
                          catalogName(), schemaIdent.name(), viewFilter, MAX_TABLES));
      return views.stream()
          .map(name -> NameIdentifier.of(namespace, name))
          .toArray(NameIdentifier[]::new);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to list Hive views in " + namespace, e);
    }
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    return loadHiveView(ident);
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
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemaExistsChecker.test(schemaIdent)) {
      throw new NoSuchSchemaException("Schema %s does not exist", schemaIdent);
    }
    SQLRepresentation sqlRepresentation = extractSupportedSqlRepresentation(representations, ident);

    try {
      Map<String, String> params =
          Maps.newHashMap(properties == null ? ImmutableMap.of() : properties);
      if (comment != null) {
        params.put(COMMENT, comment);
      }
      params.put(TABLE_TYPE, TableType.VIRTUAL_VIEW.name());
      String viewOriginalText = toHmsViewOriginalText(sqlRepresentation, ident);

      HiveTable hiveTable =
          HiveTable.builder()
              .withName(ident.name())
              .withComment(null)
              .withColumns(new Column[0])
              .withProperties(params)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentUserName())
                      .withCreateTime(Instant.now())
                      .build())
              .withCatalogName(catalogName())
              .withDatabaseName(schemaIdent.name())
              .withViewOriginalText(viewOriginalText)
              .build();

      clientPool()
          .run(
              c -> {
                c.createTable(hiveTable);
                return null;
              });

      LOG.info("Created Hive view {} in Hive Metastore", ident.name());
      return loadHiveView(ident);
    } catch (Exception e) {
      if (isAlreadyExistsError(e)) {
        throw new ViewAlreadyExistsException("View %s already exists in Hive Metastore", ident);
      }
      throw new RuntimeException("Failed to create Hive view " + ident, e);
    }
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, ViewAlreadyExistsException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());

    for (ViewChange change : changes) {
      if (change instanceof ViewChange.RenameView) {
        NameIdentifier targetIdent =
            NameIdentifier.of(ident.namespace(), ((ViewChange.RenameView) change).getNewName());
        if (viewExists(targetIdent)) {
          throw new ViewAlreadyExistsException(
              "View %s already exists in Hive Metastore", targetIdent);
        }
      }
    }

    try {
      HiveTable currentHiveTable =
          clientPool().run(c -> c.getTable(catalogName(), schemaIdent.name(), ident.name()));
      if (!TableType.VIRTUAL_VIEW
          .name()
          .equalsIgnoreCase(currentHiveTable.properties().get(TABLE_TYPE))) {
        throw new NoSuchViewException("No view named %s (it is a table, not a view)", ident.name());
      }

      Map<String, String> params = Maps.newHashMap(currentHiveTable.properties());
      String newSql = currentHiveTable.viewOriginalText();
      String newName = ident.name();

      for (ViewChange change : changes) {
        if (change instanceof ViewChange.RenameView) {
          newName = ((ViewChange.RenameView) change).getNewName();
        } else if (change instanceof ViewChange.SetProperty) {
          ViewChange.SetProperty sp = (ViewChange.SetProperty) change;
          params.put(sp.getProperty(), sp.getValue());
        } else if (change instanceof ViewChange.RemoveProperty) {
          params.remove(((ViewChange.RemoveProperty) change).getProperty());
        } else if (change instanceof ViewChange.ReplaceView) {
          ViewChange.ReplaceView replace = (ViewChange.ReplaceView) change;
          SQLRepresentation sqlRepresentation =
              extractSupportedSqlRepresentation(replace.getRepresentations(), ident);
          if (replace.getComment() == null) {
            params.remove(COMMENT);
          } else {
            params.put(COMMENT, replace.getComment());
          }
          newSql = toHmsViewOriginalText(sqlRepresentation, ident);
        } else {
          throw new IllegalArgumentException(
              "Unsupported view change type: " + change.getClass().getSimpleName());
        }
      }

      final String originalName = ident.name();
      final String finalNewName = newName;
      final String finalSql = newSql;
      HiveTable updatedHiveTable =
          HiveTable.builder()
              .withName(finalNewName)
              .withComment(null)
              .withColumns(new Column[0])
              .withProperties(params)
              .withAuditInfo(currentHiveTable.auditInfo())
              .withCatalogName(catalogName())
              .withDatabaseName(schemaIdent.name())
              .withViewOriginalText(finalSql)
              .build();

      clientPool()
          .run(
              c -> {
                c.alterTable(catalogName(), schemaIdent.name(), originalName, updatedHiveTable);
                return null;
              });

      LOG.info("Altered Hive view {} (now {})", ident.name(), finalNewName);
      return loadHiveView(NameIdentifier.of(ident.namespace(), finalNewName));
    } catch (NoSuchViewException | ViewAlreadyExistsException e) {
      throw e;
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      if (isAlreadyExistsError(e)) {
        throw new ViewAlreadyExistsException(
            "View %s already exists in Hive Metastore",
            NameIdentifier.of(ident.namespace(), extractRenameTargetName(ident.name(), changes)));
      }
      throw new RuntimeException("Failed to alter Hive view " + ident, e);
    }
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      clientPool()
          .run(
              c -> {
                c.dropTable(catalogName(), schemaIdent.name(), ident.name(), false, false);
                return null;
              });
      LOG.info("Dropped Hive view {}", ident.name());
      return true;
    } catch (Exception e) {
      if (isNotFoundError(e)) {
        return false;
      }
      throw new RuntimeException("Failed to drop Hive view " + ident, e);
    }
  }

  @Override
  public boolean viewExists(NameIdentifier ident) {
    try {
      loadHiveView(ident);
      return true;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  private HiveView loadHiveView(NameIdentifier ident) throws NoSuchViewException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      HiveTable hiveTable =
          clientPool().run(c -> c.getTable(catalogName(), schemaIdent.name(), ident.name()));
      if (!TableType.VIRTUAL_VIEW.name().equalsIgnoreCase(hiveTable.properties().get(TABLE_TYPE))) {
        throw new NoSuchViewException("No view named %s (it is a table, not a view)", ident);
      }

      Map<String, String> params =
          Maps.newHashMap(
              hiveTable.properties() != null ? hiveTable.properties() : ImmutableMap.of());
      String viewOriginalText = hiveTable.viewOriginalText();
      String detectedDialect = HiveView.detectDialect(viewOriginalText, params);
      if (!HiveView.HIVE_DIALECT.equalsIgnoreCase(detectedDialect)) {
        // TODO(design-docs/gravitino-logical-view-management.md): support loading trino/spark HMS
        // views.
        throw new UnsupportedOperationException(
            String.format(
                "Hive catalog currently supports only '%s' view dialect, but found '%s' for view %s",
                HiveView.HIVE_DIALECT, detectedDialect, ident));
      }

      SQLRepresentation rep =
          SQLRepresentation.builder()
              .withDialect(HiveView.HIVE_DIALECT)
              .withSql(viewOriginalText != null ? viewOriginalText : "")
              .build();

      return HiveView.builder()
          .withName(ident.name())
          .withComment(params.get(COMMENT))
          .withColumns(new Column[0])
          .withRepresentations(new SQLRepresentation[] {rep})
          .withProperties(params)
          .withAuditInfo(hiveTable.auditInfo())
          .build();

    } catch (NoSuchViewException e) {
      throw e;
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      if (isNotFoundError(e)) {
        throw new NoSuchViewException(e, "View %s does not exist in Hive Metastore", ident);
      }
      throw new RuntimeException("Failed to load Hive view " + ident, e);
    }
  }

  private SQLRepresentation extractSupportedSqlRepresentation(
      Representation[] representations, NameIdentifier ident) {
    Preconditions.checkArgument(
        representations != null && representations.length > 0,
        "At least one representation is required to create a Hive view");
    SQLRepresentation selected = null;
    Set<String> dialects = new HashSet<>();
    for (Representation rep : representations) {
      if (rep instanceof SQLRepresentation) {
        SQLRepresentation sqlRep = (SQLRepresentation) rep;
        String dialect = normalizeDialect(sqlRep.dialect());
        dialects.add(dialect);
        if (selected == null) {
          selected = sqlRep;
        }
      }
    }
    if (selected == null) {
      throw new IllegalArgumentException(
          "Hive catalog requires at least one SQL representation to create view " + ident);
    }
    Preconditions.checkArgument(
        dialects.size() == 1,
        "Hive catalog supports exactly one dialect per view in HMS, but got %s for view %s",
        dialects,
        ident);

    String selectedDialect = normalizeDialect(selected.dialect());
    if (!HiveView.HIVE_DIALECT.equals(selectedDialect)) {
      // TODO(design-docs/gravitino-logical-view-management.md): support creating trino/spark HMS
      // views.
      throw new UnsupportedOperationException(
          String.format(
              "Hive catalog currently supports only '%s' view dialect, but got '%s' for view %s",
              HiveView.HIVE_DIALECT, selected.dialect(), ident));
    }
    return selected;
  }

  private String toHmsViewOriginalText(SQLRepresentation representation, NameIdentifier ident) {
    String dialect = normalizeDialect(representation.dialect());
    if (!HiveView.HIVE_DIALECT.equals(dialect)) {
      // TODO(design-docs/gravitino-logical-view-management.md): support serializing trino/spark HMS
      // view definitions.
      throw new UnsupportedOperationException(
          String.format(
              "Hive catalog currently supports only '%s' view dialect, but got '%s' for view %s",
              HiveView.HIVE_DIALECT, representation.dialect(), ident));
    }
    return representation.sql();
  }

  private String extractRenameTargetName(String originalName, ViewChange[] changes) {
    for (ViewChange change : changes) {
      if (change instanceof ViewChange.RenameView) {
        return ((ViewChange.RenameView) change).getNewName();
      }
    }
    return originalName;
  }

  private String normalizeDialect(String dialect) {
    Preconditions.checkArgument(dialect != null, "View dialect cannot be null");
    return dialect.toLowerCase();
  }

  private boolean isAlreadyExistsError(Throwable t) {
    Throwable current = t;
    while (current != null) {
      String className = current.getClass().getSimpleName();
      String message = current.getMessage();
      if (className.contains("AlreadyExistsException")
          || className.contains("TableAlreadyExistsException")
          || (message != null && message.contains("AlreadyExistsException"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private boolean isNotFoundError(Exception e) {
    return e.getMessage() != null
        && (e.getMessage().contains("NoSuchObjectException")
            || e.getMessage().contains("does not exist"));
  }

  private CachedClientPool clientPool() {
    return clientPoolSupplier.get();
  }

  private String catalogName() {
    return catalogNameSupplier.get();
  }
}
