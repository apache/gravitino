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
package org.apache.gravitino.client;

import static org.apache.gravitino.dto.util.DTOConverters.toDTO;
import static org.apache.gravitino.dto.util.DTOConverters.toDTOs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.dto.requests.TableUpdatesRequest;
import org.apache.gravitino.dto.requests.ViewCreateRequest;
import org.apache.gravitino.dto.requests.ViewUpdateRequest;
import org.apache.gravitino.dto.requests.ViewUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.TableResponse;
import org.apache.gravitino.dto.responses.ViewResponse;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rest.RESTUtils;

/**
 * Relational catalog is a catalog implementation that supports relational database like metadata
 * operations, for example, schemas and tables list, creation, update and deletion. A Relational
 * catalog is under the metalake.
 */
class RelationalCatalog extends BaseSchemaCatalog implements TableCatalog, ViewCatalog {

  public static final String PRIVILEGES = "privileges";

  RelationalCatalog(
      Namespace namespace,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(namespace, name, type, provider, comment, properties, auditDTO, restClient);
  }

  @Override
  public TableCatalog asTableCatalog() {
    return this;
  }

  @Override
  public ViewCatalog asViewCatalog() {
    return this;
  }

  /**
   * List all the tables under the given Schema namespace.
   *
   * @param namespace The namespace to list the tables under it. This namespace should have 1 level,
   *     which is the schema name;
   * @return A list of {@link NameIdentifier} of the tables under the given namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    checkTableNamespace(namespace);

    Namespace fullNamespace = getEntityFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Load the table with specified identifier.
   *
   * @param ident The identifier of the table to load, which should be "schema.table" format.
   * @return The {@link Table} with specified identifier.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  @Override
  public Table loadTable(NameIdentifier ident, Set<Privilege.Name> requiredPrivilegeNames)
      throws NoSuchTableException {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    Map<String, String> queryParams = Maps.newHashMap();
    queryParams.put(PRIVILEGES, Joiner.on(",").join(requiredPrivilegeNames));
    TableResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            queryParams,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  /**
   * Create a new table with specified identifier, columns, comment and properties.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @param columns The columns of the table.
   * @param comment The comment of the table.
   * @param properties The properties of the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   * @return The created {@link Table}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws TableAlreadyExistsException if the table with specified identifier already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    checkTableNameIdentifier(ident);

    TableCreateRequest req =
        new TableCreateRequest(
            ident.name(),
            comment,
            toDTOs(columns),
            properties,
            toDTOs(sortOrders),
            toDTO(distribution),
            toDTOs(partitioning),
            toDTOs(indexes));
    req.validate();

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.post(
            formatTableRequestPath(fullNamespace),
            req,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  /**
   * Alter the table with specified identifier by applying the changes.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @param changes Table changes to apply to the table.
   * @return The altered {@link Table}.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    checkTableNameIdentifier(ident);

    List<TableUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toTableUpdateRequest)
            .collect(Collectors.toList());
    TableUpdatesRequest updatesRequest = new TableUpdatesRequest(reqs);
    updatesRequest.validate();

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.put(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            updatesRequest,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  /**
   * Drop the table with specified identifier.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @return true if the table is dropped successfully, false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * Purge the table with specified identifier.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @return true if the table is purged successfully, false if the table does not exist.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    Map<String, String> params = new HashMap<>();
    params.put("purge", "true");
    DropResponse resp =
        restClient.delete(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            params,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * List all the views under the given Schema namespace.
   *
   * @param namespace The namespace to list the views under it. This namespace should have 1 level,
   *     which is the schema name.
   * @return An array of {@link NameIdentifier} of the views under the given namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    checkViewNamespace(namespace);

    Namespace fullNamespace = getEntityFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatViewRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.viewErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Load the view with specified identifier.
   *
   * @param ident The identifier of the view to load, which should be "schema.view" format.
   * @return The {@link View} with specified identifier.
   * @throws NoSuchViewException if the view with specified identifier does not exist.
   */
  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    checkViewNameIdentifier(ident);

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    ViewResponse resp =
        restClient.get(
            formatViewRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            ViewResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.viewErrorHandler());
    resp.validate();

    return resp.getView();
  }

  /**
   * Create a new view with specified identifier, columns, representations and other metadata.
   *
   * @param ident The identifier of the view, which should be "schema.view" format.
   * @param comment The comment of the view, may be {@code null}.
   * @param columns The output columns of the view.
   * @param representations The representations of the view. At least one representation is
   *     expected.
   * @param defaultCatalog The default catalog used to resolve unqualified identifiers referenced by
   *     the view definition, or {@code null} if not set.
   * @param defaultSchema The default schema used to resolve unqualified identifiers referenced by
   *     the view definition, or {@code null} if not set.
   * @param properties The properties of the view.
   * @return The created {@link View}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws ViewAlreadyExistsException if the view with specified identifier already exists.
   */
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
    checkViewNameIdentifier(ident);

    ColumnDTO[] columnDTOs = toDTOs(columns);
    RepresentationDTO[] representationDTOs = toDTOs(representations);

    ViewCreateRequest req =
        new ViewCreateRequest(
            ident.name(),
            comment,
            columnDTOs,
            representationDTOs,
            defaultCatalog,
            defaultSchema,
            properties);
    req.validate();

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    ViewResponse resp =
        restClient.post(
            formatViewRequestPath(fullNamespace),
            req,
            ViewResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.viewErrorHandler());
    resp.validate();

    return resp.getView();
  }

  /**
   * Alter the view with specified identifier by applying the changes.
   *
   * @param ident The identifier of the view, which should be "schema.view" format.
   * @param changes View changes to apply to the view.
   * @return The altered {@link View}.
   * @throws NoSuchViewException if the view with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid or rejected by the implementation.
   */
  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    checkViewNameIdentifier(ident);

    List<ViewUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(RelationalCatalog::toViewUpdateRequest)
            .collect(Collectors.toList());
    ViewUpdatesRequest updatesRequest = new ViewUpdatesRequest(reqs);
    updatesRequest.validate();

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    ViewResponse resp =
        restClient.put(
            formatViewRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            updatesRequest,
            ViewResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.viewErrorHandler());
    resp.validate();

    return resp.getView();
  }

  /**
   * Drop the view with specified identifier.
   *
   * @param ident The identifier of the view, which should be "schema.view" format.
   * @return true if the view is dropped successfully, false if the view does not exist.
   */
  @Override
  public boolean dropView(NameIdentifier ident) {
    checkViewNameIdentifier(ident);

    Namespace fullNamespace = getEntityFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatViewRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.viewErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  @VisibleForTesting
  static String formatTableRequestPath(Namespace ns) {
    checkFullSchemaNamespace(ns, "Table");
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(ns.level(2)))
        .append("/tables")
        .toString();
  }

  /**
   * Check whether the namespace of a table is valid, which should be "schema".
   *
   * @param namespace The namespace to check.
   */
  static void checkTableNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Table namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a table is valid.
   *
   * @param ident The NameIdentifier to check, which should be "schema.table" format.
   */
  static void checkTableNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkTableNamespace(ident.namespace());
  }

  /**
   * Format the request path for view operations.
   *
   * @param ns The full namespace in "metalake.catalog.schema" format.
   * @return The request path for view operations.
   */
  @VisibleForTesting
  static String formatViewRequestPath(Namespace ns) {
    checkFullSchemaNamespace(ns, "View");
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(ns.level(2)))
        .append("/views")
        .toString();
  }

  /**
   * Check whether the namespace of a view is valid, which should be "schema".
   *
   * @param namespace The namespace to check.
   */
  static void checkViewNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "View namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a view is valid.
   *
   * @param ident The NameIdentifier to check, which should be "schema.view" format.
   */
  static void checkViewNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkViewNamespace(ident.namespace());
  }

  private static ViewUpdateRequest toViewUpdateRequest(ViewChange change) {
    if (change instanceof ViewChange.RenameView) {
      return new ViewUpdateRequest.RenameViewRequest(((ViewChange.RenameView) change).getNewName());

    } else if (change instanceof ViewChange.SetProperty) {
      ViewChange.SetProperty setProperty = (ViewChange.SetProperty) change;
      return new ViewUpdateRequest.SetViewPropertyRequest(
          setProperty.getProperty(), setProperty.getValue());

    } else if (change instanceof ViewChange.RemoveProperty) {
      return new ViewUpdateRequest.RemoveViewPropertyRequest(
          ((ViewChange.RemoveProperty) change).getProperty());

    } else if (change instanceof ViewChange.ReplaceView) {
      ViewChange.ReplaceView replace = (ViewChange.ReplaceView) change;
      return new ViewUpdateRequest.ReplaceViewRequest(
          toDTOs(replace.getColumns()),
          toDTOs(replace.getRepresentations()),
          replace.getDefaultCatalog(),
          replace.getDefaultSchema(),
          replace.getComment());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  /**
   * Get the full namespace of an entity with the given short namespace (schema name).
   *
   * @param entityNamespace The entity's short namespace, which is the schema name.
   * @return full namespace of the entity, which is "metalake.catalog.schema" format.
   */
  private Namespace getEntityFullNamespace(Namespace entityNamespace) {
    return Namespace.of(this.catalogNamespace().level(0), this.name(), entityNamespace.level(0));
  }

  private static void checkFullSchemaNamespace(Namespace namespace, String entityType) {
    Namespace.check(
        namespace != null && namespace.length() == 3,
        "%s full namespace must be non-null and have 3 levels in the format \"metalake.catalog.schema\", the input namespace is %s",
        entityType,
        namespace);
  }

  /**
   * Create a new builder for the relational catalog.
   *
   * @return A new builder for the relational catalog.
   */
  public static Builder builder() {
    return new Builder();
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    /** The REST client to send the requests. */
    private RESTClient restClient;
    /** The namespace of the catalog */
    private Namespace namespace;

    protected Builder() {}

    Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public RelationalCatalog build() {
      Namespace.check(
          namespace != null && namespace.length() == 1,
          "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
          namespace);
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new RelationalCatalog(
          namespace, name, type, provider, comment, properties, audit, restClient);
    }
  }
}
