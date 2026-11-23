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
package org.apache.gravitino.server.web.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.SupportsStaticCredential;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.AuthorizationRequestScope;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Tests authorization using configured credential providers and their capabilities. */
public class TestCatalogCredentialAuthzHandler {
  /**
   * Checks dynamic, static, mixed, empty and unrecognized provider configurations.
   *
   * @param configuration the provider configuration to authorize
   * @throws Exception if the catalog callback fails
   */
  @ParameterizedTest
  @ValueSource(strings = {"dynamic", "static", "mixed", "empty", "missing"})
  public void testProviderAuthorization(String configuration) throws Exception {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    CatalogCredentialManager credentialManager = mock(CatalogCredentialManager.class);
    when(catalog.catalogCredentialManager()).thenReturn(credentialManager);
    when(credentialManager.getCredentialProvider("custom-session"))
        .thenReturn(Optional.of(mock(CredentialProvider.class)));
    when(credentialManager.getCredentialProvider("custom-stored"))
        .thenReturn(Optional.of(mock(SupportsStaticCredential.class)));
    String providers =
        switch (configuration) {
          case "dynamic" -> "custom-session";
          case "static" -> "custom-stored";
          case "mixed" -> "custom-stored,custom-session";
          case "missing" -> "unrecognized-provider";
          default -> "";
        };
    when(catalog.propertiesWithCredentialProviders())
        .thenReturn(Map.of("credential-providers", providers));
    CatalogManager catalogManager = mock(CatalogManager.class);
    when(catalogManager.doWithCatalog(eq(identifier), any()))
        .thenAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, ?> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            });
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.catalogManager()).thenReturn(catalogManager);
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class);
        MockedStatic<MetadataAuthzHelper> auth = mockStatic(MetadataAuthzHelper.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      for (boolean useSecrets : new boolean[] {false, true}) {
        for (boolean includeSecrets : new boolean[] {false, true}) {
          auth.when(
                  () ->
                      MetadataAuthzHelper.checkAccess(
                          any(),
                          any(),
                          eq(
                              AuthorizationExpressionConstants
                                  .FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION)))
              .thenReturn(useSecrets);
          auth.when(
                  () ->
                      MetadataAuthzHelper.checkAccess(
                          any(),
                          any(),
                          eq(
                              AuthorizationExpressionConstants
                                  .FILTER_INCLUDE_CREDENTIAL_SECRETS_AUTHORIZATION_EXPRESSION)))
              .thenReturn(includeSecrets);
          try (AuthorizationRequestScope scope = AuthorizationRequestScope.open()) {
            CatalogCredentialAuthzHandler handler = new CatalogCredentialAuthzHandler();
            boolean staticOnly = "static".equals(configuration) || "missing".equals(configuration);
            if (staticOnly && !(useSecrets && includeSecrets)) {
              Assertions.assertThrows(
                  ForbiddenException.class,
                  () -> handler.process(Map.of(Entity.EntityType.CATALOG, identifier)));
            } else {
              handler.process(Map.of(Entity.EntityType.CATALOG, identifier));
              Assertions.assertEquals(
                  !(useSecrets && includeSecrets),
                  AuthorizationRequestScope.dynamicCredentialsOnly(identifier));
            }
            Assertions.assertFalse(handler.authorizationCompleted());
            Assertions.assertEquals(Entity.EntityType.CATALOG, handler.entityType());
          }
          Assertions.assertFalse(AuthorizationRequestScope.dynamicCredentialsOnly(identifier));
        }
      }
    }
    verify(credentialManager, never()).getCredential(any(), any());
  }
}
