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
import java.util.HashMap;
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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Handler for LOAD_VIEW operations. Validates the view identifier and performs authorization
 * aligned with {@link org.apache.gravitino.iceberg.service.rest.IcebergViewOperations#loadView}.
 *
 * <p>Authorization follows the REST method: primary {@code ICEBERG_LOAD_VIEW} from {@link
 * AuthorizationExpression#expression()}, then non-blank {@link
 * AuthorizationExpression#allowCheckExistence()} when the primary denies.
 */
public class LoadViewAuthzHandler implements AuthorizationHandler {
  private final AuthorizationExpression authorizationExpression;
  private final Parameter[] parameters;
  private final Object[] args;

  public LoadViewAuthzHandler(
      AuthorizationExpression authorizationExpression, Parameter[] parameters, Object[] args) {
    this.authorizationExpression = authorizationExpression;
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    String viewName = null;
    Namespace namespace = null;

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];

      IcebergAuthorizationMetadata icebergMetadata =
          parameter.getAnnotation(IcebergAuthorizationMetadata.class);
      if (icebergMetadata != null && icebergMetadata.type() == RequestType.LOAD_VIEW) {
        viewName = RESTUtil.decodeString(String.valueOf(args[i]));
      }

      AuthorizationMetadata authMetadata = parameter.getAnnotation(AuthorizationMetadata.class);
      if (authMetadata != null && authMetadata.type() == EntityType.SCHEMA) {
        namespace = RESTUtil.decodeNamespace(String.valueOf(args[i]));
      }
    }

    if (viewName == null || namespace == null) {
      throw new NoSuchViewException("View not found - missing view name or namespace");
    }

    NameIdentifier catalogId = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schemaId = nameIdentifierMap.get(EntityType.SCHEMA);

    if (catalogId == null || schemaId == null) {
      throw new NoSuchViewException("Missing catalog or schema context for view authorization");
    }

    String metalakeName = catalogId.namespace().level(0);
    String catalog = catalogId.name();
    String schema = schemaId.name();

    IcebergCatalogWrapperManager wrapperManager =
        IcebergRESTServerContext.getInstance().catalogWrapperManager();
    IcebergCatalogWrapper catalogWrapper = wrapperManager.getCatalogWrapper(catalog);
    TableIdentifier viewIdentifier = TableIdentifier.of(namespace, viewName);

    nameIdentifierMap.put(
        EntityType.VIEW, NameIdentifierUtil.ofView(metalakeName, catalog, schema, viewName));
    nameIdentifierMap.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalakeName, catalog, schema, viewName));

    performViewAuthorization(nameIdentifierMap, catalogWrapper, viewIdentifier);
  }

  @Override
  public boolean authorizationCompleted() {
    return true;
  }

  /**
   * Perform VIEW-level authorization aligned with {@link
   * org.apache.gravitino.iceberg.service.rest.IcebergViewOperations#loadView}: evaluate the primary
   * expression first; if it denies, evaluate {@link AuthorizationExpression#allowCheckExistence()}
   * when non-blank.
   */
  private void performViewAuthorization(
      Map<EntityType, NameIdentifier> nameIdentifierMap,
      IcebergCatalogWrapper catalogWrapper,
      TableIdentifier viewIdentifier) {

    Map<String, Object> emptyPathParams = new HashMap<>();
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    Optional<String> emptyEntityType = Optional.empty();

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
      if (!catalogWrapper.viewExists(viewIdentifier)) {
        throw new NoSuchViewException("View %s not found", viewIdentifier.name());
      }
    }

    String currentUser = PrincipalUtils.getCurrentUserName();
    NameIdentifier viewId = nameIdentifierMap.get(EntityType.VIEW);
    throw new ForbiddenException(
        "User '%s' is not authorized to load view '%s'", currentUser, viewId);
  }

  private String resolvePrimaryExpression() {
    if (authorizationExpression != null
        && authorizationExpression.expression() != null
        && !authorizationExpression.expression().isBlank()) {
      return authorizationExpression.expression();
    }
    return AuthorizationExpressionConstants.ICEBERG_LOAD_VIEW_AUTHORIZATION_EXPRESSION;
  }

  private String resolveAllowCheckExistenceExpression() {
    if (authorizationExpression != null) {
      String expr = authorizationExpression.allowCheckExistence();
      if (expr != null && !expr.isBlank()) {
        return expr;
      }
    }
    return AuthorizationExpressionConstants.ICEBERG_LOAD_VIEW_SECONDARY_AUTHORIZATION_EXPRESSION;
  }
}
