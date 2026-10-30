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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.ICEBERG_CREATE_TABLE_AUTHORIZATION_EXPRESSION;

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
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Handler for UPDATE_TABLE operations.
 *
 * <p>An update request that carries {@code AssertTableDoesNotExist} is the commit of a staged
 * create: Iceberg builds a new table from the request alone. It is a creation, not a modification,
 * so this handler authorizes it with the create-table expression on the schema and completes
 * authorization. Otherwise the table expression on the endpoint ({@code MODIFY_TABLE}) still runs.
 *
 * <p>A create commit on an existing table fails in Iceberg on the {@code AssertTableDoesNotExist}
 * requirement, so authorizing it as a create does not open a path to modify an existing table.
 */
public class UpdateTableAuthzHandler implements AuthorizationHandler {
  private final Parameter[] parameters;
  private final Object[] args;
  private boolean stagedCreate = false;

  /**
   * Creates a handler for an update-table request.
   *
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   */
  public UpdateTableAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    UpdateTableRequest request = extractRequest();
    // Same predicate as the hook dispatcher and Iceberg's CatalogHandlers use to detect a create,
    // so the authorization path and the create path cannot disagree.
    stagedCreate =
        request.requirements() != null
            && request.requirements().stream()
                .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
    if (!stagedCreate) {
      return;
    }

    // The table does not exist yet, so evaluate on the schema like a plain create table.
    Map<EntityType, NameIdentifier> schemaContext = new HashMap<>(nameIdentifierMap);
    NameIdentifier tableIdent = schemaContext.remove(EntityType.TABLE);
    boolean authorized =
        new AuthorizationExpressionEvaluator(ICEBERG_CREATE_TABLE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                schemaContext,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.of(EntityType.SCHEMA.name()));
    if (!authorized) {
      throw new ForbiddenException(
          "User '%s' is not authorized to create table '%s'",
          PrincipalUtils.getCurrentUserName(), tableIdent);
    }
  }

  @Override
  public boolean authorizationCompleted() {
    return stagedCreate;
  }

  private UpdateTableRequest extractRequest() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.UPDATE_TABLE
          && args[i] instanceof UpdateTableRequest) {
        return (UpdateTableRequest) args[i];
      }
    }
    throw new ForbiddenException("UpdateTableRequest not found in method parameters");
  }
}
