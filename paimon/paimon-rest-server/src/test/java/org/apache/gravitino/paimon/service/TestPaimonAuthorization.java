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
package org.apache.gravitino.paimon.service;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.paimon.common.PaimonCatalogException;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager.CatalogLease;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.glassfish.jersey.server.CloseableService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Verifies per-request authorization, branch identity and catalog-wide credential privileges. */
public class TestPaimonAuthorization {
  @Test
  void testListingsHonorCreationAndProbeVisibilityFromTheMainApi() {
    try (MockedStatic<MetadataAuthzHelper> checks = mockStatic(MetadataAuthzHelper.class)) {
      checks
          .when(() -> MetadataAuthzHelper.checkAccess(any(), any(), anyString()))
          .thenAnswer(
              invocation ->
                  invocation
                      .getArgument(2)
                      .equals(
                          AuthorizationExpressionConstants
                              .LIST_TABLE_LIKE_AUTHORIZATION_EXPRESSION));
      PaimonCatalogRequest request =
          new PaimonCatalogRequest(
              mock(PaimonCatalogWrapperManager.class),
              () -> mock(CloseableService.class),
              new PaimonAuthorization(true, "ml"),
              new PaimonMetadataHooks(false, "ml"));
      assertTrue(request.canReadTable("sales", "db", "t"));
    }
  }

  @Test
  void testCreationProbesKeepMissingResources404AndProtectExistingMetadata() {
    try (MockedStatic<MetadataAuthzHelper> checks = mockStatic(MetadataAuthzHelper.class)) {
      checks
          .when(() -> MetadataAuthzHelper.checkAccess(any(), any(), anyString()))
          .thenAnswer(
              invocation -> {
                String expression = invocation.getArgument(2);
                return expression.equals(
                        AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION)
                    || expression.equals(
                        AuthorizationExpressionConstants.PROBE_SCHEMA_AUTHORIZATION_EXPRESSION)
                    || expression.equals(
                        AuthorizationExpressionConstants.PROBE_TABLE_LIKE_AUTHORIZATION_EXPRESSION);
              });
      PaimonCatalogWrapper wrapper = mock(PaimonCatalogWrapper.class);
      CatalogLease lease = mock(CatalogLease.class);
      when(lease.wrapper()).thenReturn(wrapper);
      PaimonCatalogWrapperManager manager = mock(PaimonCatalogWrapperManager.class);
      when(manager.acquire("sales")).thenReturn(lease);
      CloseableService closeables = mock(CloseableService.class);
      when(closeables.add(any())).thenReturn(true);
      PaimonCatalogRequest request =
          new PaimonCatalogRequest(
              manager,
              () -> closeables,
              new PaimonAuthorization(true, "ml"),
              new PaimonMetadataHooks(false, "ml"));
      Identifier identifier = Identifier.create("db", "missing");
      PaimonCatalogException missing = mock(PaimonCatalogException.class);
      when(missing.reason()).thenReturn(PaimonCatalogException.Reason.NOT_FOUND);
      when(wrapper.getFileTable(identifier)).thenThrow(missing);
      when(wrapper.getDatabase("missing")).thenThrow(missing);
      assertSame(
          missing,
          assertThrows(
              PaimonCatalogException.class, () -> request.getTableWrapper("sales", identifier)));
      assertSame(
          missing,
          assertThrows(
              PaimonCatalogException.class, () -> request.getSchemaWrapper("sales", "missing")));
      assertThrows(
          ForbiddenException.class,
          () -> request.getTableWrapper("sales", Identifier.create("db", "existing")));
      assertThrows(ForbiddenException.class, () -> request.getSchemaWrapper("sales", "existing"));
      verify(wrapper, never()).getTable(any());
    }
  }

  @Test
  void testStaticModeDoesNotRequireGravitinoInitialization() {
    PaimonAuthorization authorization = new PaimonAuthorization(false, "ml");
    assertTrue(authorization.catalog("sales", "unused"));
    assertTrue(authorization.schema("sales", "db", "unused"));
    assertTrue(authorization.table("sales", "db", "t", "unused"));
  }

  @Test
  void testAuthorizationUsesCurrentPrincipalOnEveryRequest() throws Exception {
    try (MockedStatic<MetadataAuthzHelper> checks = mockStatic(MetadataAuthzHelper.class)) {
      checks
          .when(() -> MetadataAuthzHelper.checkAccess(any(), any(), anyString()))
          .thenAnswer(invocation -> PrincipalUtils.getCurrentUserName().equals("alice"));
      PaimonAuthorization authorization = new PaimonAuthorization(true, "ml");
      PaimonCatalogRequest request =
          new PaimonCatalogRequest(
              mock(PaimonCatalogWrapperManager.class),
              () -> mock(CloseableService.class),
              authorization,
              new PaimonMetadataHooks(false, "ml"));
      PrincipalUtils.doAs(
          new UserPrincipal("alice"),
          () -> {
            request.requireTable(
                "sales",
                new Identifier("db", "t", "b1"),
                AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION);
            request.requireCredentials("sales");
            return null;
          });
      PrincipalUtils.doAs(
          new UserPrincipal("bob"),
          () -> {
            assertThrows(
                ForbiddenException.class,
                () ->
                    request.requireTable(
                        "sales",
                        Identifier.create("db", "t"),
                        AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION));
            assertThrows(ForbiddenException.class, () -> request.requireCredentials("sales"));
            return null;
          });
      checks.verify(
          () ->
              MetadataAuthzHelper.checkAccess(
                  NameIdentifier.of("ml", "sales", "db", "t"),
                  Entity.EntityType.TABLE,
                  AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION));
      checks.verify(
          () ->
              MetadataAuthzHelper.checkAccess(
                  NameIdentifier.of("ml", "sales"),
                  Entity.EntityType.CATALOG,
                  AuthorizationExpressionConstants
                      .CATALOG_STATIC_CREDENTIALS_AUTHORIZATION_EXPRESSION),
          times(2));
    }
  }
}
