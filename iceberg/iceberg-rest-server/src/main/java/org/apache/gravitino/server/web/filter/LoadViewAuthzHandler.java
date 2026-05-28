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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchViewException;

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
    LoadTarget loadTarget =
        IcebergLoadAuthzHandlerHelper.extractLoadTarget(parameters, args, RequestType.LOAD_VIEW);

    if (loadTarget.hasMissingPart()) {
      throw new NoSuchViewException("View not found - missing view name or namespace");
    }

    String viewName = loadTarget.name();
    Namespace namespace = loadTarget.namespace();
    LoadContext loadContext =
        IcebergLoadAuthzHandlerHelper.extractLoadContext(
            nameIdentifierMap,
            () ->
                new NoSuchViewException(
                    "Missing catalog or schema context for view authorization"));

    IcebergCatalogWrapper catalogWrapper =
        IcebergLoadAuthzHandlerHelper.getCatalogWrapper(loadContext.catalog());
    TableIdentifier viewIdentifier = TableIdentifier.of(namespace, viewName);

    NameIdentifier viewId =
        NameIdentifierUtil.ofView(
            loadContext.metalakeName(), loadContext.catalog(), loadContext.schema(), viewName);
    nameIdentifierMap.put(EntityType.VIEW, viewId);
    // Spark probes loadView(name) before falling back to loadTable(name), so the existence-check
    // expression grants the existence probe to principals holding privileges on a like-named
    // table. Register a TABLE identifier under the same name so that expression has a TABLE entity
    // to evaluate against; this is what lets a table owner get 404 (not 403) for an absent view.
    nameIdentifierMap.put(
        EntityType.TABLE,
        NameIdentifierUtil.ofTable(
            loadContext.metalakeName(), loadContext.catalog(), loadContext.schema(), viewName));

    performViewAuthorization(nameIdentifierMap, catalogWrapper, viewIdentifier, viewId);
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
      TableIdentifier viewIdentifier,
      NameIdentifier viewId) {
    IcebergLoadAuthzHandlerHelper.authorizeLoadEntity(
        nameIdentifierMap,
        IcebergLoadAuthzHandlerHelper.resolveExpression(
            authorizationExpression,
            AuthorizationExpressionConstants.ICEBERG_LOAD_VIEW_AUTHORIZATION_EXPRESSION),
        IcebergLoadAuthzHandlerHelper.resolveAllowCheckExistenceExpression(
            authorizationExpression,
            AuthorizationExpressionConstants
                .ICEBERG_LOAD_VIEW_EXISTENCE_CHECK_AUTHORIZATION_EXPRESSION),
        () -> catalogWrapper.viewExists(viewIdentifier),
        () -> new NoSuchViewException("View %s not found", viewIdentifier.name()),
        "view",
        viewId);
  }
}
