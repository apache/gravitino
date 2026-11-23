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
package org.apache.gravitino.server.web.filter.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.server.authorization.AuthorizationRequestScope;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Tests conservative provider selection when no secondary privilege check is configured. */
public class TestCredentialAuthorizationExecutor {
  /**
   * Checks that an unconfigured secondary expression never grants access to static providers.
   *
   * @param staticOnly whether the catalog has only static providers
   * @throws Exception if authorization fails unexpectedly
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testUnconfiguredStaticPrivilegeExpression(boolean staticOnly) throws Exception {
    NameIdentifier catalog = NameIdentifier.of("metalake", "catalog");
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    when(provider.getGravitinoAuthorizer()).thenReturn(mock(GravitinoAuthorizer.class));
    CatalogManager manager = mock(CatalogManager.class);
    when(manager.doWithCatalog(any(), any())).thenReturn(!staticOnly);
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.catalogManager()).thenReturn(manager);
    try (MockedStatic<GravitinoAuthorizerProvider> providers =
            mockStatic(GravitinoAuthorizerProvider.class);
        MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class);
        AuthorizationRequestScope scope = AuthorizationRequestScope.open()) {
      providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      CredentialAuthorizationExecutor executor =
          new CredentialAuthorizationExecutor(
              "true",
              Map.of(Entity.EntityType.CATALOG, catalog),
              Map.of(),
              Optional.of(Entity.EntityType.CATALOG.name()),
              "false",
              ExpressionCondition.NEVER);

      Assertions.assertEquals(!staticOnly, executor.execute(new AuthorizationRequestContext()));
      Assertions.assertTrue(AuthorizationRequestScope.dynamicCredentialsOnly(catalog));
    }
    Assertions.assertFalse(AuthorizationRequestScope.dynamicCredentialsOnly(catalog));
  }
}
