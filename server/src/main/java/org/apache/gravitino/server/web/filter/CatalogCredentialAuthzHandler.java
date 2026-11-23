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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.CredentialUtils;
import org.apache.gravitino.credential.SupportsStaticCredential;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.AuthorizationRequestScope;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;

/** Authorizes static catalog credentials while allowing short-lived token vending. */
public class CatalogCredentialAuthzHandler implements AuthorizationHandler {

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    NameIdentifier identifier =
        Objects.requireNonNull(nameIdentifierMap.get(EntityType.CATALOG), "catalog identifier");
    boolean includeStaticCredentials =
        MetadataAuthzHelper.checkAccess(
                identifier,
                EntityType.CATALOG,
                AuthorizationExpressionConstants.FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION)
            && MetadataAuthzHelper.checkAccess(
                identifier,
                EntityType.CATALOG,
                AuthorizationExpressionConstants
                    .FILTER_INCLUDE_CREDENTIAL_SECRETS_AUTHORIZATION_EXPRESSION);
    if (includeStaticCredentials) {
      return;
    }
    boolean dynamicProvidersAvailable =
        GravitinoEnv.getInstance()
            .catalogManager()
            .doWithCatalog(
                identifier,
                catalog -> {
                  Set<String> providers =
                      CredentialUtils.getCredentialProvidersByOrder(
                          catalog::propertiesWithCredentialProviders);
                  return providers.isEmpty()
                      || providers.stream()
                          .anyMatch(
                              type ->
                                  catalog
                                      .catalogCredentialManager()
                                      .getCredentialProvider(type)
                                      .filter(
                                          provider ->
                                              !(provider instanceof SupportsStaticCredential))
                                      .isPresent());
                });
    if (!dynamicProvidersAvailable) {
      throw new ForbiddenException(
          "Catalog credentials require USE_SECRETS and INCLUDE_CREDENTIAL_SECRETS");
    }
    AuthorizationRequestScope.requireDynamicCredentials(identifier);
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
}
