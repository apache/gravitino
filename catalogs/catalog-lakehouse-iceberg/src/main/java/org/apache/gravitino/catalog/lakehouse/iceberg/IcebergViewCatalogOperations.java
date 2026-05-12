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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.ConvertUtil;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** View catalog operations for Iceberg. */
class IcebergViewCatalogOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergViewCatalogOperations.class);

  private static final int INITIAL_VIEW_VERSION_ID = 1;

  /** Iceberg sentinel value for {@code SetCurrentViewVersion}: "use the last added version". */
  private static final int ICEBERG_LAST_ADDED_VIEW_VERSION = -1;

  private final IcebergCatalogWrapper icebergCatalogWrapper;

  IcebergViewCatalogOperations(IcebergCatalogWrapper icebergCatalogWrapper) {
    Preconditions.checkArgument(
        icebergCatalogWrapper != null, "icebergCatalogWrapper must not be null");
    this.icebergCatalogWrapper = icebergCatalogWrapper;
  }

  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    try {
      LoadViewResponse response =
          icebergCatalogWrapper.loadView(
              IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(ident));
      return IcebergView.fromLoadViewResponse(response, ident.name());
    } catch (org.apache.iceberg.exceptions.NoSuchViewException e) {
      throw new NoSuchViewException(e, "Iceberg view %s does not exist", ident);
    }
  }

  public boolean viewExists(NameIdentifier ident) {
    return icebergCatalogWrapper.viewExists(
        IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(ident));
  }

  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    org.apache.iceberg.catalog.Namespace icebergNamespace =
        IcebergCatalogWrapperHelper.getIcebergNamespace(namespace);
    if (!icebergCatalogWrapper.namespaceExists(icebergNamespace)) {
      throw new NoSuchSchemaException("Schema %s does not exist", namespace);
    }
    try {
      ListTablesResponse response = icebergCatalogWrapper.listView(icebergNamespace);
      return response.identifiers().stream()
          .map(id -> NameIdentifier.of(ArrayUtils.add(namespace.levels(), id.name())))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException("Schema does not exist %s in Iceberg", namespace);
    }
  }

  public View createView(
      NameIdentifier ident,
      String comment,
      Column[] columns,
      Representation[] representations,
      String defaultCatalog,
      String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    org.apache.iceberg.catalog.Namespace icebergNamespace =
        IcebergCatalogWrapperHelper.getIcebergNamespace(ident.namespace());

    Schema schema = ConvertUtil.toIcebergSchema(columns);
    List<ViewRepresentation> viewRepresentations = new ArrayList<>();
    for (Representation representation : representations) {
      viewRepresentations.add(toSqlViewRepresentation(representation));
    }
    Representation[] sqlRepresentations =
        viewRepresentations.stream()
            .map(IcebergViewCatalogOperations::toSqlRepresentation)
            .toArray(SQLRepresentation[]::new);

    org.apache.iceberg.catalog.Namespace defaultNamespace =
        defaultSchema != null
            ? IcebergCatalogWrapperHelper.getIcebergNamespace(defaultSchema)
            : icebergNamespace;

    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(INITIAL_VIEW_VERSION_ID)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(schema.schemaId())
            .defaultNamespace(defaultNamespace)
            .representations(viewRepresentations)
            .putSummary("operation", "create")
            .build();

    Map<String, String> allProperties =
        properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
    if (comment != null) {
      allProperties.put("comment", comment);
    }
    if (defaultCatalog != null) {
      allProperties.put("default-catalog", defaultCatalog);
    }

    ImmutableCreateViewRequest request =
        ImmutableCreateViewRequest.builder()
            .name(ident.name())
            .schema(schema)
            .viewVersion(viewVersion)
            .properties(allProperties)
            .build();

    try {
      icebergCatalogWrapper.createView(icebergNamespace, request);
      LOG.info("Created Iceberg view {}", ident);
      return IcebergView.builder()
          .withName(ident.name())
          .withComment(comment)
          .withColumns(columns)
          .withRepresentations(sqlRepresentations)
          .withProperties(allProperties)
          .withAuditInfo(
              AuditInfo.builder().withCreator(currentUser()).withCreateTime(Instant.now()).build())
          .build();
    } catch (AlreadyExistsException e) {
      throw new ViewAlreadyExistsException(e, "View %s already exists in Iceberg catalog", ident);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Schema does not exist for view %s in Iceberg catalog", ident);
    }
  }

  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    Optional<ViewChange> renameOptional =
        Arrays.stream(changes).filter(c -> c instanceof ViewChange.RenameView).reduce((a, b) -> b);
    if (renameOptional.isPresent()) {
      String otherChange =
          Arrays.stream(changes)
              .filter(c -> !(c instanceof ViewChange.RenameView))
              .map(String::valueOf)
              .collect(Collectors.joining("\n"));
      Preconditions.checkArgument(
          StringUtils.isEmpty(otherChange),
          "Rename cannot be combined with other view changes: " + otherChange);
      return renameView(ident, (ViewChange.RenameView) renameOptional.get());
    }
    return internalUpdateView(ident, changes);
  }

  public boolean dropView(NameIdentifier ident) {
    try {
      icebergCatalogWrapper.dropView(
          IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(ident));
      LOG.info("Dropped Iceberg view {}", ident);
      return true;
    } catch (org.apache.iceberg.exceptions.NoSuchViewException e) {
      LOG.warn("Iceberg view {} does not exist, skip dropping", ident);
      return false;
    }
  }

  private View renameView(NameIdentifier ident, ViewChange.RenameView rename)
      throws NoSuchViewException {
    try {
      RenameTableRequest request =
          RenameTableRequest.builder()
              .withSource(IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(ident))
              .withDestination(
                  IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(
                      ident.namespace(), rename.getNewName()))
              .build();
      icebergCatalogWrapper.renameView(request);
      return loadView(
          NameIdentifier.of(ArrayUtils.add(ident.namespace().levels(), rename.getNewName())));
    } catch (org.apache.iceberg.exceptions.NoSuchViewException e) {
      throw new NoSuchViewException(e, "Iceberg view %s does not exist", ident);
    }
  }

  private View internalUpdateView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    TableIdentifier viewId = IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(ident);
    try {
      LoadViewResponse current = icebergCatalogWrapper.loadView(viewId);
      ViewMetadata metadata = current.metadata();

      Map<String, String> setProps = new HashMap<>();
      Set<String> removeProps = new HashSet<>();
      Optional<ViewChange.ReplaceView> replaceOpt =
          collectPropertyChanges(changes, setProps, removeProps);

      replaceOpt.ifPresent(replace -> applyReplaceViewProperties(replace, setProps, removeProps));

      List<MetadataUpdate> updates =
          new ArrayList<>(buildPropertyMetadataUpdates(setProps, removeProps));
      replaceOpt.ifPresent(
          replaceView -> updates.addAll(buildNewViewVersionUpdates(ident, replaceView, metadata)));

      if (updates.isEmpty()) {
        return loadView(ident);
      }

      UpdateTableRequest request =
          UpdateTableRequest.create(viewId, Collections.emptyList(), updates);
      LoadViewResponse response = icebergCatalogWrapper.updateView(viewId, request);
      LOG.info("Altered Iceberg view {}", ident);
      return IcebergView.fromLoadViewResponse(response, ident.name());
    } catch (org.apache.iceberg.exceptions.NoSuchViewException e) {
      throw new NoSuchViewException(e, "Iceberg view %s does not exist", ident);
    }
  }

  /**
   * Iterates {@code changes} collecting SetProperty/RemoveProperty entries into {@code setProps}
   * and {@code removeProps}, resolving conflicts so the last change for a key wins. Returns the
   * last {@link ViewChange.ReplaceView} change found, or empty if none.
   */
  private static Optional<ViewChange.ReplaceView> collectPropertyChanges(
      ViewChange[] changes, Map<String, String> setProps, Set<String> removeProps) {
    ViewChange.ReplaceView replace = null;
    for (ViewChange change : changes) {
      if (change instanceof ViewChange.SetProperty) {
        ViewChange.SetProperty setProperty = (ViewChange.SetProperty) change;
        String property = setProperty.getProperty();
        setProps.put(property, setProperty.getValue());
        removeProps.remove(property);
      } else if (change instanceof ViewChange.RemoveProperty) {
        String property = ((ViewChange.RemoveProperty) change).getProperty();
        removeProps.add(property);
        setProps.remove(property);
      } else if (change instanceof ViewChange.ReplaceView) {
        replace = (ViewChange.ReplaceView) change;
      } else {
        throw new IllegalArgumentException(
            "Unsupported view change type: " + change.getClass().getSimpleName());
      }
    }
    return Optional.ofNullable(replace);
  }

  /**
   * Applies the implicit property side-effects of a {@link ViewChange.ReplaceView}: sets or removes
   * {@code comment} and {@code default-catalog}, and always sets the {@code
   * replace.drop-dialect.allowed} flag.
   */
  private static void applyReplaceViewProperties(
      ViewChange.ReplaceView replace, Map<String, String> setProps, Set<String> removeProps) {
    setProps.put("replace.drop-dialect.allowed", "true");
    if (replace.getComment() == null) {
      removeProps.add("comment");
      setProps.remove("comment");
    } else {
      setProps.put("comment", replace.getComment());
      removeProps.remove("comment");
    }
    if (replace.getDefaultCatalog() == null) {
      removeProps.add("default-catalog");
      setProps.remove("default-catalog");
    } else {
      setProps.put("default-catalog", replace.getDefaultCatalog());
      removeProps.remove("default-catalog");
    }
  }

  /** Converts {@code setProps} and {@code removeProps} into the corresponding MetadataUpdates. */
  private static List<MetadataUpdate> buildPropertyMetadataUpdates(
      Map<String, String> setProps, Set<String> removeProps) {
    List<MetadataUpdate> updates = new ArrayList<>();
    if (!setProps.isEmpty()) {
      updates.add(new MetadataUpdate.SetProperties(setProps));
    }
    if (!removeProps.isEmpty()) {
      updates.add(new MetadataUpdate.RemoveProperties(removeProps));
    }
    return updates;
  }

  /**
   * Builds the schema-and-version MetadataUpdates required to replace a view's SQL definition.
   * Reuses an existing schema when the column layout matches; otherwise adds a new one.
   */
  private static List<MetadataUpdate> buildNewViewVersionUpdates(
      NameIdentifier ident, ViewChange.ReplaceView replace, ViewMetadata metadata) {
    List<MetadataUpdate> updates = new ArrayList<>();
    ViewVersion currentVersion = metadata.currentVersion();

    int newVersionId =
        currentVersion != null ? currentVersion.versionId() + 1 : INITIAL_VIEW_VERSION_ID;

    Schema replacementSchema = ConvertUtil.toIcebergSchema(replace.getColumns());
    Optional<Schema> existingSchema =
        metadata.schemas().stream()
            .filter(schema -> schema.sameSchema(replacementSchema))
            .findFirst();
    int schemaId;
    if (existingSchema.isPresent()) {
      schemaId = existingSchema.get().schemaId();
    } else {
      int newSchemaId = metadata.schemas().stream().mapToInt(Schema::schemaId).max().orElse(-1) + 1;
      Schema newSchema =
          new Schema(
              newSchemaId, replacementSchema.columns(), replacementSchema.identifierFieldIds());
      updates.add(new MetadataUpdate.AddSchema(newSchema));
      schemaId = newSchemaId;
    }

    org.apache.iceberg.catalog.Namespace defaultNamespace =
        replace.getDefaultSchema() != null
            ? IcebergCatalogWrapperHelper.getIcebergNamespace(replace.getDefaultSchema())
            : (currentVersion != null
                ? currentVersion.defaultNamespace()
                : IcebergCatalogWrapperHelper.getIcebergNamespace(ident.namespace()));

    List<ViewRepresentation> newRepresentations = new ArrayList<>();
    for (Representation representation : replace.getRepresentations()) {
      newRepresentations.add(toSqlViewRepresentation(representation));
    }

    ViewVersion newVersion =
        ImmutableViewVersion.builder()
            .versionId(newVersionId)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(schemaId)
            .defaultNamespace(defaultNamespace)
            .representations(newRepresentations)
            .putSummary("operation", "alter")
            .build();

    updates.add(new MetadataUpdate.AddViewVersion(newVersion));
    updates.add(new MetadataUpdate.SetCurrentViewVersion(ICEBERG_LAST_ADDED_VIEW_VERSION));
    return updates;
  }

  private static ViewRepresentation toSqlViewRepresentation(Representation representation) {
    SQLRepresentation sqlRepresentation = normalizeSqlRepresentation(representation);
    return ImmutableSQLViewRepresentation.builder()
        .sql(sqlRepresentation.sql())
        .dialect(sqlRepresentation.dialect())
        .build();
  }

  private static SQLRepresentation toSqlRepresentation(ViewRepresentation viewRepresentation) {
    Preconditions.checkArgument(
        viewRepresentation instanceof SQLViewRepresentation,
        "Iceberg catalog only supports SQL view representation, but got: %s",
        viewRepresentation == null ? "null" : viewRepresentation.getClass().getSimpleName());
    SQLViewRepresentation sqlViewRepresentation = (SQLViewRepresentation) viewRepresentation;
    return SQLRepresentation.builder()
        .withDialect(sqlViewRepresentation.dialect())
        .withSql(sqlViewRepresentation.sql())
        .build();
  }

  private static SQLRepresentation normalizeSqlRepresentation(Representation representation) {
    Preconditions.checkArgument(
        representation != null, "Iceberg catalog only supports SQLRepresentation, but got: null");
    if (representation instanceof SQLRepresentation) {
      return (SQLRepresentation) representation;
    }

    Preconditions.checkArgument(
        Representation.TYPE_SQL.equalsIgnoreCase(representation.type()),
        "Iceberg catalog only supports SQLRepresentation, but got: %s",
        representation.getClass().getSimpleName());
    try {
      String dialect =
          (String) representation.getClass().getMethod("dialect").invoke(representation);
      String sql = (String) representation.getClass().getMethod("sql").invoke(representation);
      return SQLRepresentation.builder().withDialect(dialect).withSql(sql).build();
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          String.format(
              "SQL representation %s must expose dialect() and sql() methods",
              representation.getClass().getSimpleName()),
          e);
    }
  }

  private static String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }
}
