/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.web.filter;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.server.web.rest.ExceptionHandlers;
import org.apache.gravitino.server.web.rest.OperationType;

/** Authorizes static catalog credentials while allowing short-lived token vending. */
public class CatalogCredentialAuthzHandler implements AuthorizationHandler {

  private String catalogName;
  private boolean includeStaticCredentials;

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    NameIdentifier identifier =
        Objects.requireNonNull(nameIdentifierMap.get(EntityType.CATALOG), "catalog identifier");
    catalogName = identifier.name();
    includeStaticCredentials =
        MetadataAuthzHelper.checkAccess(
                identifier,
                EntityType.CATALOG,
                AuthorizationExpressionConstants.FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION)
            && MetadataAuthzHelper.checkAccess(
                identifier,
                EntityType.CATALOG,
                AuthorizationExpressionConstants
                    .FILTER_INCLUDE_CREDENTIAL_SECRETS_AUTHORIZATION_EXPRESSION);
  }

  @Override
  public boolean authorizationCompleted() {
    // The standard metadata access expression must still authorize the request.
    return false;
  }

  @Override
  public EntityType entityType() {
    return EntityType.CATALOG;
  }

  @Override
  public Object processResponse(Object response) {
    if (includeStaticCredentials || !(response instanceof Response)) {
      return response;
    }
    Response restResponse = (Response) response;
    if (restResponse.getStatus() != Response.Status.OK.getStatusCode()
        || !(restResponse.getEntity() instanceof CredentialResponse)) {
      return response;
    }
    CredentialDTO[] credentials = ((CredentialResponse) restResponse.getEntity()).getCredentials();
    CredentialDTO[] tokens =
        Arrays.stream(credentials)
            .filter(credential -> credential.expireTimeInMs() > 0)
            .toArray(CredentialDTO[]::new);
    if (tokens.length == credentials.length) {
      return response;
    }
    if (tokens.length == 0) {
      return ExceptionHandlers.handleCredentialException(
          OperationType.GET,
          catalogName,
          new ForbiddenException(
              "Catalog credentials require USE_SECRETS and INCLUDE_CREDENTIAL_SECRETS"));
    }
    return Response.fromResponse(restResponse).entity(new CredentialResponse(tokens)).build();
  }
}
