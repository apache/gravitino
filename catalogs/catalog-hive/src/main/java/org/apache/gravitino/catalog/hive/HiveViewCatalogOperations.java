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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveViewCatalogOperations implements ViewCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HiveViewCatalogOperations.class);
  private static final String SUPPORTED_VIEW_DIALECTS =
      String.join(", ", Dialects.HIVE, Dialects.TRINO, Dialects.FLINK, Dialects.SPARK);

  /**
   * The HMS-level representation derived from a logical view definition: the columns, comment, and
   * {@code viewOriginalText} actually stored on the underlying HMS table. For a Trino dialect view
   * these differ from the caller-supplied values (see {@link #encodeHmsView}); for every other
   * dialect they are passed through unchanged.
   */
  private static final class HmsViewEncoding {
    final Column[] columns;
    final String comment;
    final String viewOriginalText;

    HmsViewEncoding(Column[] columns, String comment, String viewOriginalText) {
      this.columns = columns;
      this.comment = comment;
      this.viewOriginalText = viewOriginalText;
    }
  }

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
    Map<String, String> safeProperties = properties == null ? ImmutableMap.of() : properties;
    SQLRepresentation sqlRepresentation =
        validateSQLRepresentation(
            representations, defaultCatalog, defaultSchema, safeProperties, columns, ident);

    try {
      Map<String, String> params = Maps.newHashMap(safeProperties);
      params.put(TABLE_TYPE, TableType.VIRTUAL_VIEW.name());
      HmsViewEncoding encoding =
          encodeHmsView(
              sqlRepresentation, columns, comment, defaultCatalog, defaultSchema, params, ident);

      HiveTable hiveTable =
          HiveTable.builder()
              .withName(ident.name())
              .withComment(encoding.comment)
              .withColumns(encoding.columns)
              .withProperties(params)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentUserName())
                      .withCreateTime(Instant.now())
                      .build())
              .withCatalogName(catalogName())
              .withDatabaseName(schemaIdent.name())
              .withViewOriginalText(encoding.viewOriginalText)
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

      // Reuse the same dialect detection as loadHiveView()/toHiveView() so that a presto_view
      // entry that is not a plain Trino view (e.g. a Trino/Presto materialized view) is rejected
      // here too, instead of being silently treated as a non-Trino view.
      boolean isTrinoView =
          Dialects.TRINO.equalsIgnoreCase(HiveView.detectDialect(currentHiveTable.properties()));
      // Gravitino's view model has no owner/runAsInvoker/path concept, so replacing a native Trino
      // view that carries a non-default value for any of them would silently discard it (e.g. a
      // SECURITY DEFINER view with an owner would silently become an ownerless SECURITY INVOKER
      // view). Reject the replace instead of doing that.
      boolean currentTrinoViewHasUnrepresentableFields = false;
      if (isTrinoView) {
        TrinoNativeViewCodec.ViewDefinition currentDefinition;
        try {
          currentDefinition = TrinoNativeViewCodec.decode(currentHiveTable.viewOriginalText());
        } catch (IllegalArgumentException e) {
          throw new UnsupportedOperationException(
              "View "
                  + ident
                  + " carries the presto_view marker but its payload cannot be "
                  + "decoded",
              e);
        }
        currentTrinoViewHasUnrepresentableFields =
            currentDefinition.owner != null
                || !currentDefinition.runAsInvoker
                || !currentDefinition.path.isEmpty();
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
            if (isTrinoView) {
              throw new UnsupportedOperationException(
                  "Trino dialect views store their comment inside the encoded view payload; use "
                      + "ReplaceView to change it, not SetProperty(comment)");
            }
            updatedComment = sp.getValue();
          } else if (TrinoNativeViewCodec.PRESTO_VIEW_FLAG.equals(sp.getProperty())) {
            throw new UnsupportedOperationException(
                "Property '"
                    + TrinoNativeViewCodec.PRESTO_VIEW_FLAG
                    + "' is reserved for native Trino view storage and cannot be set directly; "
                    + "use ReplaceView to change the view's dialect");
          } else {
            updatedProperties.put(sp.getProperty(), sp.getValue());
          }
        } else if (change instanceof ViewChange.RemoveProperty) {
          String property = ((ViewChange.RemoveProperty) change).getProperty();
          if (COMMENT.equals(property)) {
            if (isTrinoView) {
              throw new UnsupportedOperationException(
                  "Trino dialect views store their comment inside the encoded view payload; use "
                      + "ReplaceView to change it, not RemoveProperty(comment)");
            }
            updatedComment = null;
          } else if (TrinoNativeViewCodec.PRESTO_VIEW_FLAG.equals(property)) {
            throw new UnsupportedOperationException(
                "Property '"
                    + TrinoNativeViewCodec.PRESTO_VIEW_FLAG
                    + "' is reserved for native Trino view storage and cannot be removed "
                    + "directly; use ReplaceView to change the view's dialect");
          } else {
            updatedProperties.remove(property);
          }
        } else if (change instanceof ViewChange.ReplaceView) {
          if (currentTrinoViewHasUnrepresentableFields) {
            throw new UnsupportedOperationException(
                "View "
                    + ident
                    + " is a native Trino view with a non-default owner, runAsInvoker, or SQL "
                    + "path; Gravitino cannot represent these fields, so replacing it would "
                    + "silently discard them");
          }
          ViewChange.ReplaceView replace = (ViewChange.ReplaceView) change;
          SQLRepresentation sqlRepresentation =
              validateSQLRepresentation(
                  replace.getRepresentations(),
                  replace.getDefaultCatalog(),
                  replace.getDefaultSchema(),
                  updatedProperties,
                  replace.getColumns(),
                  ident);
          HmsViewEncoding encoding =
              encodeHmsView(
                  sqlRepresentation,
                  replace.getColumns(),
                  replace.getComment(),
                  replace.getDefaultCatalog(),
                  replace.getDefaultSchema(),
                  updatedProperties,
                  ident);
          updatedColumns = encoding.columns;
          updatedViewOriginalText = encoding.viewOriginalText;
          updatedComment = encoding.comment;
          isTrinoView = Dialects.TRINO.equalsIgnoreCase(sqlRepresentation.dialect());
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
    } catch (UnsupportedOperationException e) {
      // The HMS entry exists but Gravitino cannot fully interpret it (e.g. a materialized view or
      // an undecodable native Trino view payload); treat it as existing so callers (e.g. a rename
      // target check) don't collide with it.
      return true;
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
    String detectedDialect = HiveView.detectDialect(params);
    switch (detectedDialect.toLowerCase(Locale.ROOT)) {
      case Dialects.HIVE:
      case Dialects.TRINO:
      case Dialects.FLINK:
      case Dialects.SPARK:
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Hive catalog currently supports only [%s] view dialects, but found '%s' for view %s",
                SUPPORTED_VIEW_DIALECTS, detectedDialect, ident));
    }

    String representationSql;
    String resolvedComment;
    String restoredDefaultCatalog = null;
    String restoredDefaultSchema = null;
    Column[] resolvedColumns;
    if (Dialects.TRINO.equalsIgnoreCase(detectedDialect)) {
      // Trino dialect views are encoded using Trino's native "Presto View" format, so the SQL,
      // comment, default catalog/schema, and real columns all live in the encoded payload; the
      // underlying HMS table only carries a single dummy column (see hmsColumns()).
      TrinoNativeViewCodec.ViewDefinition decoded;
      try {
        decoded = TrinoNativeViewCodec.decode(viewOriginalText);
      } catch (IllegalArgumentException e) {
        throw new UnsupportedOperationException(
            "View " + ident + " carries the presto_view marker but its payload cannot be decoded",
            e);
      }
      representationSql = decoded.originalSql;
      resolvedComment = decoded.comment;
      restoredDefaultCatalog = decoded.catalog;
      restoredDefaultSchema = decoded.schema;
      resolvedColumns =
          decoded.columns.stream()
              .map(
                  c ->
                      Column.of(
                          c.name, TrinoNativeViewCodec.fromTrinoTypeString(c.type), c.comment))
              .toArray(Column[]::new);
    } else {
      representationSql = viewOriginalText;
      resolvedComment = comment;
      resolvedColumns = copyColumns(columns);
    }

    SQLRepresentation rep =
        SQLRepresentation.builder()
            .withDialect(detectedDialect)
            .withSql(StringUtils.defaultString(representationSql))
            .build();

    return HiveView.builder()
        .withName(ident.name())
        .withComment(resolvedComment)
        .withColumns(resolvedColumns)
        .withRepresentations(new SQLRepresentation[] {rep})
        .withProperties(params)
        .withAuditInfo(auditInfo)
        .withDefaultCatalog(restoredDefaultCatalog)
        .withDefaultSchema(restoredDefaultSchema)
        .build();
  }

  /**
   * Validates that {@code representations} contains exactly one {@link SQLRepresentation} with a
   * supported dialect, and that dialect-specific constraints are satisfied.
   *
   * @param properties view properties used to verify dialect marker keys are present (e.g. {@code
   *     spark.sql.create.version} for Spark, a {@code flink.*} key for Flink)
   */
  private SQLRepresentation validateSQLRepresentation(
      Representation[] representations,
      String defaultCatalog,
      String defaultSchema,
      Map<String, String> properties,
      Column[] columns,
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
    switch (selected.dialect().toLowerCase(Locale.ROOT)) {
      case Dialects.HIVE:
        Preconditions.checkArgument(
            defaultCatalog == null && defaultSchema == null,
            "Dialect '%s' does not support non-null defaultCatalog/defaultSchema, but got "
                + "defaultCatalog=%s, defaultSchema=%s for view %s",
            selected.dialect(),
            defaultCatalog,
            defaultSchema,
            ident);
        return selected;
      case Dialects.TRINO:
        // The default catalog/schema are encoded into the Trino native view payload by
        // toHmsViewOriginalText() and restored in toHiveView(), so no value is required to be null
        // here.
        Preconditions.checkArgument(
            columns != null && columns.length > 0,
            "Dialect '%s' requires at least one column for view %s; without it the encoded "
                + "payload cannot be decoded on the next load",
            selected.dialect(),
            ident);
        Preconditions.checkArgument(
            defaultSchema == null || defaultCatalog != null,
            "Dialect '%s' does not support a defaultSchema without a defaultCatalog for view "
                + "%s, since Trino's native view format rejects such a payload",
            selected.dialect(),
            ident);
        return selected;
      case Dialects.FLINK:
        Preconditions.checkArgument(
            defaultCatalog == null && defaultSchema == null,
            "Dialect '%s' does not support non-null defaultCatalog/defaultSchema, but got "
                + "defaultCatalog=%s, defaultSchema=%s for view %s",
            selected.dialect(),
            defaultCatalog,
            defaultSchema,
            ident);
        Preconditions.checkArgument(
            properties.keySet().stream()
                .anyMatch(k -> k.startsWith(HiveView.FLINK_PROPERTY_PREFIX)),
            "Flink dialect view '%s' requires at least one property with prefix '%s' to be set; "
                + "without it the view silently round-trips as Hive dialect on reload",
            ident,
            HiveView.FLINK_PROPERTY_PREFIX);
        return selected;
      case Dialects.SPARK:
        Preconditions.checkArgument(
            properties.containsKey(HiveView.SPARK_VERSION_KEY),
            "Spark dialect view '%s' requires property '%s' to be set; "
                + "without it the view silently round-trips as Hive dialect on reload",
            ident,
            HiveView.SPARK_VERSION_KEY);
        return selected;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Hive catalog currently supports only [%s] view dialects, but got '%s' for view %s",
                SUPPORTED_VIEW_DIALECTS, selected.dialect(), ident));
    }
  }

  /**
   * Sets or clears the {@code presto_view} marker in the given HMS property map, so that a Trino
   * dialect view is recognized as a native Trino view (see {@link TrinoNativeViewCodec}).
   */
  private void applyTrinoViewMarker(Map<String, String> params, String dialect) {
    if (!Dialects.TRINO.equalsIgnoreCase(dialect)) {
      params.remove(TrinoNativeViewCodec.PRESTO_VIEW_FLAG);
      return;
    }
    params.put(TrinoNativeViewCodec.PRESTO_VIEW_FLAG, "true");
  }

  /**
   * Derives the HMS-level representation of a view from its logical definition, shared by {@link
   * #createView} and the {@code ReplaceView} branch of {@link #alterView}. The caller supplies the
   * logical SQL representation, output columns, user comment, and default catalog/schema; {@code
   * properties} is mutated in place to set or clear the {@code presto_view} marker.
   */
  private HmsViewEncoding encodeHmsView(
      SQLRepresentation sqlRepresentation,
      Column[] columns,
      String comment,
      String defaultCatalog,
      String defaultSchema,
      Map<String, String> properties,
      NameIdentifier ident) {
    applyTrinoViewMarker(properties, sqlRepresentation.dialect());
    String viewOriginalText =
        toHmsViewOriginalText(
            sqlRepresentation, columns, comment, defaultCatalog, defaultSchema, ident);
    String hmsComment =
        Dialects.TRINO.equalsIgnoreCase(sqlRepresentation.dialect())
            ? TrinoNativeViewCodec.PRESTO_VIEW_COMMENT
            : comment;
    return new HmsViewEncoding(
        hmsColumns(columns, sqlRepresentation.dialect()), hmsComment, viewOriginalText);
  }

  private String toHmsViewOriginalText(
      SQLRepresentation representation,
      Column[] columns,
      String comment,
      String defaultCatalog,
      String defaultSchema,
      NameIdentifier ident) {
    switch (representation.dialect().toLowerCase(Locale.ROOT)) {
      case Dialects.HIVE:
      case Dialects.FLINK:
      case Dialects.SPARK:
        return representation.sql();
      case Dialects.TRINO:
        List<TrinoNativeViewCodec.ViewColumn> viewColumns =
            Arrays.stream(columns == null ? new Column[0] : columns)
                .map(
                    c ->
                        new TrinoNativeViewCodec.ViewColumn(
                            c.name(),
                            TrinoNativeViewCodec.toTrinoTypeString(c.dataType()),
                            c.comment()))
                .collect(Collectors.toList());
        return TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                representation.sql(),
                defaultCatalog,
                defaultSchema,
                viewColumns,
                comment,
                /* owner= */ null,
                /* runAsInvoker= */ true,
                /* path= */ Collections.emptyList()));
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Hive catalog currently supports only [%s] view dialects, but got '%s' for view %s",
                SUPPORTED_VIEW_DIALECTS, representation.dialect(), ident));
    }
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

  /**
   * Builds the columns to store on the underlying HMS table. Trino dialect views store their real
   * columns inside the encoded Trino native view payload (see {@link #toHmsViewOriginalText}), so
   * the HMS table itself only carries a single dummy column, matching real Trino's own behavior
   * (see {@code io.trino.plugin.hive.HiveMetadata#createView}).
   */
  private Column[] hmsColumns(Column[] columns, String dialect) {
    if (Dialects.TRINO.equalsIgnoreCase(dialect)) {
      return new Column[] {Column.of("dummy", Types.StringType.get())};
    }
    return copyColumns(columns);
  }

  private CachedClientPool clientPool() {
    return clientPoolSupplier.get();
  }

  private String catalogName() {
    return catalogNameSupplier.get();
  }
}
