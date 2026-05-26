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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTUtil;

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
    // Find the table name and namespace from parameters
    String tableName = null;
    Namespace namespace = null;

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];

      IcebergAuthorizationMetadata icebergMetadata =
          parameter.getAnnotation(IcebergAuthorizationMetadata.class);
      if (icebergMetadata != null && icebergMetadata.type() == RequestType.LOAD_TABLE) {
        // TODO: Refactor to move decode logic to interceptor in a generic way
        // See: https://docs.google.com/document/d/18yx88tBbU3S9LB8hhL7xUVzSWHIWkXJ7sRNmLh2v_kQ/
        // Consider consolidating custom authorization handlers and standardizing parameter decoding
        tableName = RESTUtil.decodeString(String.valueOf(args[i]));
      }

      AuthorizationMetadata authMetadata = parameter.getAnnotation(AuthorizationMetadata.class);
      if (authMetadata != null && authMetadata.type() == EntityType.SCHEMA) {
        // Decode the raw Iceberg namespace parameter
        namespace = RESTUtil.decodeNamespace(String.valueOf(args[i]));
      }
    }

    if (tableName == null || namespace == null) {
      throw new NoSuchTableException("Table not found - missing table name or namespace");
    }

    // Validate that this is not a metadata table access
    if (isMetadataTable(tableName, namespace)) {
      throw new NoSuchTableException("Table %s not found", tableName);
    }

    NameIdentifier catalogId = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schemaId = nameIdentifierMap.get(EntityType.SCHEMA);

    if (catalogId == null || schemaId == null) {
      throw new NoSuchTableException("Missing catalog or schema context for table authorization");
    }

    String metalakeName = catalogId.namespace().level(0);
    String catalog = catalogId.name();
    String schema = schemaId.name();

    // Per Iceberg REST spec, /tables/ endpoint should only serve tables, not views.
    // 1. Authorize the table access (see performTableAuthorization) -> 403 if unauthorized
    // 2. Let request proceed - the actual loadTable() call will handle non-existence
    IcebergCatalogWrapperManager wrapperManager =
        IcebergRESTServerContext.getInstance().catalogWrapperManager();
    IcebergCatalogWrapper catalogWrapper = wrapperManager.getCatalogWrapper(catalog);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

    nameIdentifierMap.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalakeName, catalog, schema, tableName));
    performTableAuthorization(nameIdentifierMap, catalogWrapper, tableIdentifier);
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
      TableIdentifier tableIdentifier) {

    Map<String, Object> emptyPathParams = Collections.emptyMap();
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    Optional<String> emptyEntityType = Optional.empty();

    if (catalogWrapper.supportsViewOperations() && catalogWrapper.viewExists(tableIdentifier)) {
      throw new NoSuchTableException("Table %s not found", tableIdentifier.name());
    }

    String primaryExpression = resolvePrimaryExpression();
    AuthorizationExpressionEvaluator primaryEvaluator =
        new AuthorizationExpressionEvaluator(primaryExpression);
    if (primaryEvaluator.evaluate(
        nameIdentifierMap, emptyPathParams, requestContext, emptyEntityType)) {
      return;
    }

    String allowCheckExistenceExpression = resolveAllowCheckExistenceExpression();
    AuthorizationExpressionEvaluator allowExistenceEvaluator =
        new AuthorizationExpressionEvaluator(allowCheckExistenceExpression);
    if (allowExistenceEvaluator.evaluate(
        nameIdentifierMap, emptyPathParams, requestContext, emptyEntityType)) {
      if (!catalogWrapper.tableExists(tableIdentifier)) {
        throw new NoSuchTableException("Table %s not found", tableIdentifier.name());
      }
    }

    String currentUser = PrincipalUtils.getCurrentUserName();
    NameIdentifier tableId = nameIdentifierMap.get(EntityType.TABLE);
    throw new ForbiddenException(
        "User '%s' is not authorized to load table '%s'", currentUser, tableId);
  }

  private String resolvePrimaryExpression() {
    if (authorizationExpression != null
        && authorizationExpression.expression() != null
        && !authorizationExpression.expression().isBlank()) {
      return authorizationExpression.expression();
    }
    return AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION;
  }

  private String resolveAllowCheckExistenceExpression() {
    if (authorizationExpression != null) {
      String expr = authorizationExpression.allowCheckExistence();
      if (expr != null && !expr.isBlank()) {
        return expr;
      }
    }
    return AuthorizationExpressionConstants.ICEBERG_TABLE_EXISTS_SECONDARY_AUTHORIZATION_EXPRESSION;
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
