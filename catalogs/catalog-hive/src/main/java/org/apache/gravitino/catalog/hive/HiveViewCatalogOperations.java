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

import static org.apache.gravitino.catalog.hive.HiveCatalogOperations.ALL_TABLE_PATTERN;
import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Dialects;
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
      List<String> views =
          clientPool()
              .run(
                  c ->
                      c.listTablesByType(
                          catalogName(),
                          schemaIdent.name(),
                          ALL_TABLE_PATTERN,
                          TableType.VIRTUAL_VIEW.name()));
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
    SQLRepresentation sqlRepresentation =
        validateSQLRepresentation(representations, defaultCatalog, defaultSchema, ident);

    try {
      Map<String, String> params =
          Maps.newHashMap(properties == null ? ImmutableMap.of() : properties);
      params.put(TABLE_TYPE, TableType.VIRTUAL_VIEW.name());
      if (!Dialects.HIVE.equalsIgnoreCase(sqlRepresentation.dialect())) {
        params.put(HiveView.GRAVITINO_VIEW_DIALECT_KEY, sqlRepresentation.dialect());
      }
      String viewOriginalText = toHmsViewOriginalText(sqlRepresentation, ident);

      HiveTable hiveTable =
          HiveTable.builder()
              .withName(ident.name())
              .withComment(comment)
              .withColumns(copyColumns(columns))
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
      return toHiveView(
          ident,
          hiveTable.comment(),
          hiveTable.properties(),
          hiveTable.viewOriginalText(),
          hiveTable.columns(),
          hiveTable.auditInfo());
    } catch (TableAlreadyExistsException e) {
      throw new ViewAlreadyExistsException(e, "View %s already exists in Hive Metastore", ident);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to create Hive view " + ident, e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Hive view " + ident, e);
    }
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, ViewAlreadyExistsException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());

    try {
      HiveTable currentHiveTable =
          clientPool().run(c -> c.getTable(catalogName(), schemaIdent.name(), ident.name()));
      if (!TableType.VIRTUAL_VIEW
          .name()
          .equalsIgnoreCase(currentHiveTable.properties().get(TABLE_TYPE))) {
        throw new NoSuchViewException("No view named %s (it is a table, not a view)", ident.name());
      }

      String newViewName = currentHiveTable.name();
      String updatedViewOriginalText = currentHiveTable.viewOriginalText();
      Map<String, String> updatedProperties = Maps.newHashMap(currentHiveTable.properties());
      Column[] updatedColumns = copyColumns(currentHiveTable.columns());
      String updatedComment = currentHiveTable.comment();
      updatedProperties.remove(COMMENT);

      for (ViewChange change : changes) {
        if (change instanceof ViewChange.RenameView) {
          String renameTarget = ((ViewChange.RenameView) change).getNewName();
          NameIdentifier targetIdent = NameIdentifier.of(ident.namespace(), renameTarget);
          if (viewExists(targetIdent)) {
            throw new ViewAlreadyExistsException(
                "View %s already exists in Hive Metastore", targetIdent);
          }
          newViewName = renameTarget;
        } else if (change instanceof ViewChange.SetProperty) {
          ViewChange.SetProperty sp = (ViewChange.SetProperty) change;
          if (COMMENT.equals(sp.getProperty())) {
            updatedComment = sp.getValue();
          } else {
            updatedProperties.put(sp.getProperty(), sp.getValue());
          }
        } else if (change instanceof ViewChange.RemoveProperty) {
          String property = ((ViewChange.RemoveProperty) change).getProperty();
          if (COMMENT.equals(property)) {
            updatedComment = null;
          } else {
            updatedProperties.remove(property);
          }
        } else if (change instanceof ViewChange.ReplaceView) {
          ViewChange.ReplaceView replace = (ViewChange.ReplaceView) change;
          SQLRepresentation sqlRepresentation =
              validateSQLRepresentation(
                  replace.getRepresentations(),
                  replace.getDefaultCatalog(),
                  replace.getDefaultSchema(),
                  ident);
          updatedColumns = copyColumns(replace.getColumns());
          updatedComment = replace.getComment();
          updatedViewOriginalText = toHmsViewOriginalText(sqlRepresentation, ident);
        } else {
          throw new IllegalArgumentException(
              "Unsupported view change type: " + change.getClass().getSimpleName());
        }
      }

      HiveTable updatedHiveTable =
          buildAlteredHiveView(
              currentHiveTable,
              schemaIdent,
              newViewName,
              updatedComment,
              updatedProperties,
              updatedColumns,
              updatedViewOriginalText);

      final String originalName = ident.name();
      clientPool()
          .run(
              c -> {
                c.alterTable(catalogName(), schemaIdent.name(), originalName, updatedHiveTable);
                return null;
              });

      LOG.info("Altered Hive view {} (now {})", ident.name(), newViewName);
      NameIdentifier updatedIdent = NameIdentifier.of(ident.namespace(), newViewName);
      return toHiveView(
          updatedIdent,
          updatedHiveTable.comment(),
          updatedHiveTable.properties(),
          updatedHiveTable.viewOriginalText(),
          updatedHiveTable.columns(),
          updatedHiveTable.auditInfo());
    } catch (NoSuchTableException e) {
      throw new NoSuchViewException(e, "View %s does not exist in Hive Metastore", ident);
    } catch (TableAlreadyExistsException e) {
      throw new ViewAlreadyExistsException(
          e,
          "View %s already exists in Hive Metastore",
          NameIdentifier.of(ident.namespace(), extractRenameTargetName(ident.name(), changes)));
    } catch (NoSuchViewException | ViewAlreadyExistsException | IllegalArgumentException e) {
      throw e;
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to alter Hive view " + ident, e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter Hive view " + ident, e);
    }
  }

  private HiveTable buildAlteredHiveView(
      HiveTable currentHiveTable,
      NameIdentifier schemaIdent,
      String viewName,
      String comment,
      Map<String, String> properties,
      Column[] columns,
      String viewOriginalText) {
    return HiveTable.builder()
        .withName(viewName)
        .withComment(comment)
        .withColumns(copyColumns(columns))
        .withProperties(properties)
        .withAuditInfo(currentHiveTable.auditInfo())
        .withCatalogName(catalogName())
        .withDatabaseName(schemaIdent.name())
        .withViewOriginalText(viewOriginalText)
        .build();
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      HiveTable hiveTable =
          clientPool().run(c -> c.getTable(catalogName(), schemaIdent.name(), ident.name()));
      if (!TableType.VIRTUAL_VIEW.name().equalsIgnoreCase(hiveTable.properties().get(TABLE_TYPE))) {
        return false;
      }

      clientPool()
          .run(
              c -> {
                c.dropTable(catalogName(), schemaIdent.name(), ident.name(), false, false);
                return null;
              });
      LOG.info("Dropped Hive view {}", ident.name());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to drop Hive view " + ident, e);
    } catch (Exception e) {
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

      return toHiveView(
          ident,
          hiveTable.comment(),
          hiveTable.properties(),
          hiveTable.viewOriginalText(),
          hiveTable.columns(),
          hiveTable.auditInfo());

    } catch (NoSuchViewException | UnsupportedOperationException e) {
      throw e;
    } catch (NoSuchTableException e) {
      throw new NoSuchViewException(e, "View %s does not exist in Hive Metastore", ident);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to load Hive view " + ident, e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load Hive view " + ident, e);
    }
  }

  private HiveView toHiveView(
      NameIdentifier ident,
      String comment,
      Map<String, String> properties,
      String viewOriginalText,
      Column[] columns,
      AuditInfo auditInfo) {
    Map<String, String> params =
        Maps.newHashMap(properties != null ? properties : ImmutableMap.of());
    String representationSql = viewOriginalText;
    String detectedDialect = HiveView.detectDialect(representationSql, params);
    if (!Dialects.HIVE.equalsIgnoreCase(detectedDialect)
        && !Dialects.FLINK.equalsIgnoreCase(detectedDialect)) {
      // TODO(design-docs/gravitino-logical-view-management.md): support loading trino/spark HMS
      // views.
      throw new UnsupportedOperationException(
          String.format(
              "Hive catalog currently supports only '%s' and '%s' view dialects, but found '%s' for view %s",
              Dialects.HIVE, Dialects.FLINK, detectedDialect, ident));
    }

    SQLRepresentation rep =
        SQLRepresentation.builder()
            .withDialect(detectedDialect)
            .withSql(StringUtils.defaultString(representationSql))
            .build();

    return HiveView.builder()
        .withName(ident.name())
        .withComment(comment)
        .withColumns(copyColumns(columns))
        .withRepresentations(new SQLRepresentation[] {rep})
        .withProperties(params)
        .withAuditInfo(auditInfo)
        .build();
  }

  private SQLRepresentation validateSQLRepresentation(
      Representation[] representations,
      String defaultCatalog,
      String defaultSchema,
      NameIdentifier ident) {
    int representationCount = representations == null ? 0 : representations.length;
    Representation firstRepresentation =
        ArrayUtils.isEmpty(representations) ? null : representations[0];
    Preconditions.checkArgument(
        representationCount == 1 && firstRepresentation instanceof SQLRepresentation,
        "Hive catalog requires exactly one SQL representation for view %s, but got %s"
            + " representation(s), first representation type is %s",
        ident,
        representationCount,
        firstRepresentation == null ? "null" : firstRepresentation.getClass().getSimpleName());

    SQLRepresentation selected = (SQLRepresentation) firstRepresentation;
    switch (selected.dialect().toLowerCase(java.util.Locale.ROOT)) {
      case Dialects.HIVE:
      case Dialects.FLINK:
        Preconditions.checkArgument(
            defaultCatalog == null && defaultSchema == null,
            "Dialect '%s' does not support non-null defaultCatalog/defaultSchema, but got "
                + "defaultCatalog=%s, defaultSchema=%s for view %s",
            selected.dialect(),
            defaultCatalog,
            defaultSchema,
            ident);
        return selected;
      default:
        // TODO(design-docs/gravitino-logical-view-management.md): support creating trino/spark HMS
        // views.
        throw new UnsupportedOperationException(
            String.format(
                "Hive catalog currently supports only '%s' and '%s' view dialects, but got '%s' for view %s",
                Dialects.HIVE, Dialects.FLINK, selected.dialect(), ident));
    }
  }

  private String toHmsViewOriginalText(SQLRepresentation representation, NameIdentifier ident) {
    if (!Dialects.HIVE.equalsIgnoreCase(representation.dialect())
        && !Dialects.FLINK.equalsIgnoreCase(representation.dialect())) {
      // TODO(design-docs/gravitino-logical-view-management.md): support serializing trino/spark HMS
      // view definitions.
      throw new UnsupportedOperationException(
          String.format(
              "Hive catalog currently supports only '%s' and '%s' view dialects, but got '%s' for view %s",
              Dialects.HIVE, Dialects.FLINK, representation.dialect(), ident));
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

  private Column[] copyColumns(Column[] columns) {
    return columns == null ? new Column[0] : columns.clone();
  }

  private CachedClientPool clientPool() {
    return clientPoolSupplier.get();
  }

  private String catalogName() {
    return catalogNameSupplier.get();
  }
}
