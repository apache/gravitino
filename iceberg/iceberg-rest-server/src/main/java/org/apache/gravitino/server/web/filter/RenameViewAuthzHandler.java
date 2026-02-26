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
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RenameTableRequest;

/**
 * Handler for RENAME_VIEW operations. Performs authorization checks for cross-namespace renames
 * which require stricter checks than same-namespace renames, following MySQL privilege model.
 */
@SuppressWarnings("FormatStringAnnotation")
public class RenameViewAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;
  private boolean crossNamespaceRename = false;

  public RenameViewAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    RenameTableRequest renameViewRequest = null;
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.RENAME_VIEW) {
        renameViewRequest = (RenameTableRequest) args[i];
        break;
      }
    }

    if (renameViewRequest == null) {
      throw new ForbiddenException("RenameViewRequest not found in parameters");
    }

    // Extract metalake and catalog from nameIdentifierMap
    NameIdentifier metalakeIdent = nameIdentifierMap.get(EntityType.METALAKE);
    NameIdentifier catalogIdent = nameIdentifierMap.get(EntityType.CATALOG);

    if (metalakeIdent == null || catalogIdent == null) {
      throw new ForbiddenException("Missing metalake or catalog context for authorization");
    }

    String metalakeName = metalakeIdent.name();
    String catalog = catalogIdent.name();

    // Extract source view information from the request and add to map
    // The source view is NOT extracted via standard @AuthorizationMetadata annotations
    // because it's embedded in the RenameTableRequest body
    String sourceSchema = renameViewRequest.source().namespace().level(0);
    String sourceView = renameViewRequest.source().name();

    nameIdentifierMap.put(
        EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalakeName, catalog, sourceSchema));
    nameIdentifierMap.put(
        EntityType.VIEW,
        NameIdentifierUtil.ofView(metalakeName, catalog, sourceSchema, sourceView));

    String destSchema = renameViewRequest.destination().namespace().level(0);
    if (!sourceSchema.equals(destSchema)) {
      // Cross-namespace rename - perform complete authorization here
      crossNamespaceRename = true;
      validateCrossNamespaceRename(catalog, metalakeName, sourceSchema, sourceView, destSchema);
    }
  }

  @Override
  public boolean authorizationCompleted() {
    // Return true if we performed complete authorization (cross-namespace case)
    return crossNamespaceRename;
  }

  /**
   * Validates authorization for cross-namespace view renames following MySQL privilege model: -
   * Requires ownership on source view (equivalent to DROP privilege) - Requires CREATE_VIEW
   * privilege on destination schema
   *
   * @param catalog The catalog name
   * @param metalakeName The metalake name
   * @param sourceSchema The source schema name
   * @param sourceView The source view name
   * @param destSchema The destination schema name
   * @throws ForbiddenException if the user lacks required privileges
   */
  private void validateCrossNamespaceRename(
      String catalog,
      String metalakeName,
      String sourceSchema,
      String sourceView,
      String destSchema) {
    String currentUser = PrincipalUtils.getCurrentUserName();
    Map<EntityType, NameIdentifier> sourceContext = new HashMap<>();
    sourceContext.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    sourceContext.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalog));
    sourceContext.put(
        EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalakeName, catalog, sourceSchema));
    sourceContext.put(
        EntityType.VIEW,
        NameIdentifierUtil.ofView(metalakeName, catalog, sourceSchema, sourceView));

    String sourceExpression =
        "ANY(OWNER, METALAKE, CATALOG) || "
            + "SCHEMA_OWNER_WITH_USE_CATALOG || "
            + "ANY_USE_CATALOG && ANY_USE_SCHEMA && VIEW::OWNER";

    AuthorizationExpressionEvaluator sourceEvaluator =
        new AuthorizationExpressionEvaluator(sourceExpression);

    boolean sourceAuthorized =
        sourceEvaluator.evaluate(
            sourceContext, new HashMap<>(), new AuthorizationRequestContext(), Optional.empty());

    if (!sourceAuthorized) {
      String notAuthzMessage =
          String.format(
              "User '%s' is not authorized to drop/move view '%s' from schema '%s'. "
                  + "Only the view owner can move a view to a different schema.",
              currentUser, sourceView, sourceSchema);
      throw new ForbiddenException(notAuthzMessage);
    }

    // Check CREATE_VIEW privilege on destination schema
    Map<EntityType, NameIdentifier> destContext = new HashMap<>();
    destContext.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    destContext.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalog));
    destContext.put(
        EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalakeName, catalog, destSchema));

    String destExpression =
        "ANY(OWNER, METALAKE, CATALOG) || "
            + "SCHEMA_OWNER_WITH_USE_CATALOG || "
            + "ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_CREATE_VIEW";

    AuthorizationExpressionEvaluator destEvaluator =
        new AuthorizationExpressionEvaluator(destExpression);

    boolean destAuthorized =
        destEvaluator.evaluate(
            destContext, new HashMap<>(), new AuthorizationRequestContext(), Optional.empty());

    if (!destAuthorized) {
      String notAuthzMessage =
          String.format(
              "User '%s' is not authorized to create view in destination schema '%s'",
              currentUser, destSchema);
      throw new ForbiddenException(notAuthzMessage);
    }
  }
}
