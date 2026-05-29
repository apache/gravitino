/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.lang.reflect.Parameter;
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.server.web.filter.IcebergLoadAuthzHandlerHelper.LoadContext;
import org.apache.gravitino.server.web.filter.IcebergLoadAuthzHandlerHelper.LoadTarget;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Handler for LOAD_TABLE operations. Validates that the requested entity is not a metadata table,
 * rejects identifiers that resolve to views (so clients can fall back to {@code /views/}), and
 * performs authorization aligned with the REST {@link AuthorizationExpression} on {@code
 * loadTable}.
 *
 * <p>Authorization follows {@link
 * org.apache.gravitino.iceberg.service.rest.IcebergTableOperations#loadTable}: primary {@code
 * LOAD_TABLE} expression from {@link AuthorizationExpression#expression()}, then {@link
 * AuthorizationExpression#allowCheckExistence()} when the primary denies (existence-probe path for
 * clients such as Trino/Spark).
 */
public class LoadTableAuthzHandler implements AuthorizationHandler {
  private final AuthorizationExpression authorizationExpression;
  private final Parameter[] parameters;
  private final Object[] args;

  public LoadTableAuthzHandler(
      AuthorizationExpression authorizationExpression, Parameter[] parameters, Object[] args) {
    this.authorizationExpression = authorizationExpression;
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    LoadTarget loadTarget =
        IcebergLoadAuthzHandlerHelper.extractLoadTarget(parameters, args, RequestType.LOAD_TABLE);

    if (loadTarget.hasMissingPart()) {
      throw new NoSuchTableException("Table not found - missing table name or namespace");
    }

    String tableName = loadTarget.name();
    Namespace namespace = loadTarget.namespace();

    // Validate that this is not a metadata table access
    if (isMetadataTable(tableName, namespace)) {
      throw new NoSuchTableException("Table %s not found", tableName);
    }

    LoadContext loadContext =
        IcebergLoadAuthzHandlerHelper.extractLoadContext(
            nameIdentifierMap,
            () ->
                new NoSuchTableException(
                    "Missing catalog or schema context for table authorization"));

    // Per Iceberg REST spec, /tables/ endpoint should only serve tables, not views.
    // 1. Authorize the table access (see performTableAuthorization) -> 403 if unauthorized
    // 2. Let request proceed - the actual loadTable() call will handle non-existence
    IcebergCatalogWrapper catalogWrapper =
        IcebergLoadAuthzHandlerHelper.getCatalogWrapper(loadContext.catalog());
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

    NameIdentifier tableId =
        NameIdentifierUtil.ofTable(
            loadContext.metalakeName(), loadContext.catalog(), loadContext.schema(), tableName);
    nameIdentifierMap.put(EntityType.TABLE, tableId);
    performTableAuthorization(nameIdentifierMap, catalogWrapper, tableIdentifier, tableId);
  }

  @Override
  public boolean authorizationCompleted() {
    // This handler performs complete authorization
    return true;
  }

  /**
   * Perform TABLE-level authorization aligned with {@link
   * org.apache.gravitino.iceberg.service.rest.IcebergTableOperations#loadTable}: evaluate the
   * primary expression first; if it denies, evaluate {@link
   * AuthorizationExpression#allowCheckExistence()} when non-blank. Before primary expression
   * evaluation, if the catalog supports views and the identifier resolves to a view, throws {@link
   * NoSuchTableException} so clients can fall back to {@code /views/} (see Iceberg REST spec for
   * {@code /tables/}).
   */
  private void performTableAuthorization(
      Map<EntityType, NameIdentifier> nameIdentifierMap,
      IcebergCatalogWrapper catalogWrapper,
      TableIdentifier tableIdentifier,
      NameIdentifier tableId) {
    if (catalogWrapper.supportsViewOperations() && catalogWrapper.viewExists(tableIdentifier)) {
      throw new NoSuchTableException("Table %s not found", tableIdentifier.name());
    }

    IcebergLoadAuthzHandlerHelper.authorizeLoadEntity(
        nameIdentifierMap,
        IcebergLoadAuthzHandlerHelper.resolveExpression(
            authorizationExpression,
            AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION),
        IcebergLoadAuthzHandlerHelper.resolveAllowCheckExistenceExpression(
            authorizationExpression,
            AuthorizationExpressionConstants
                .ICEBERG_TABLE_EXISTS_SECONDARY_AUTHORIZATION_EXPRESSION),
        () -> catalogWrapper.tableExists(tableIdentifier),
        () -> new NoSuchTableException("Table %s not found", tableIdentifier.name()),
        "table",
        tableId);
  }

  /**
   * Check if the table is a metadata table. Metadata tables have special names like
   * `table$snapshots`, `table$files`, etc., and are accessed with longer namespace paths
   * (catalog.db.table instead of catalog.db).
   *
   * @param tableName The table name to check
   * @param namespace The Iceberg namespace of the table
   * @return true if this is a metadata table access, false otherwise
   */
  private boolean isMetadataTable(String tableName, Namespace namespace) {
    MetadataTableType metadataTableType = MetadataTableType.from(tableName);
    if (metadataTableType == null) {
      return false;
    }

    // Metadata tables have namespace length > 1 (e.g., catalog.db.table has 3 levels).
    // Regular tables have namespace length = 1 (e.g., catalog.db has 2 levels, but we get "db").
    return namespace.levels().length > 1;
  }
}
