/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests retained-table authorization through the Iceberg REST interceptor handler. */
public class TestIcebergTableDeletionAuthzHandler {

  private static final String PARENT_USE = "ANY_USE_CATALOG && ANY_USE_SCHEMA";
  private static final NameIdentifier TABLE =
      NameIdentifier.of("metalake", "catalog", "schema", "orders");
  private static final NameIdentifier SCHEMA = NameIdentifier.of("metalake", "catalog", "schema");

  @BeforeEach
  void setUpContext() {
    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn("metalake");
    when(provider.getDefaultCatalogName()).thenReturn("catalog");
    IcebergRESTServerContext.create(provider, true, true, false, null);
  }

  @Test
  void testLiveTableFallsThroughToStandardAuthorization() throws Exception {
    TableDeletionService tableDeletionService = mock(TableDeletionService.class);
    IcebergTableDeletionAuthzHandler handler = handler();
    Map<EntityType, NameIdentifier> identifiers = identifiers();

    try (MockedStatic<EntityIdService> entityIds = mockStatic(EntityIdService.class);
        MockedStatic<TableDeletionService> tableDeletions =
            mockStatic(TableDeletionService.class)) {
      entityIds.when(() -> EntityIdService.getEntityId(SCHEMA, EntityType.SCHEMA)).thenReturn(3L);
      tableDeletions.when(TableDeletionService::getInstance).thenReturn(tableDeletionService);
      when(tableDeletionService.getRetainedTable(3L, "orders")).thenReturn(null);

      handler.process(identifiers);

      assertFalse(handler.authorizationCompleted());
      assertEquals(TABLE, identifiers.get(EntityType.TABLE));
    }
  }

  @Test
  void testStandaloneModeFallsThroughWithoutRelationalLookup() throws Exception {
    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn("metalake");
    when(provider.getDefaultCatalogName()).thenReturn("catalog");
    IcebergRESTServerContext.create(provider, true, false, false, null);

    try (MockedStatic<EntityIdService> entityIds = mockStatic(EntityIdService.class);
        MockedStatic<TableDeletionService> tableDeletions =
            mockStatic(TableDeletionService.class)) {
      IcebergTableDeletionAuthzHandler handler = handler();
      handler.process(identifiers());

      assertFalse(handler.authorizationCompleted());
      entityIds.verifyNoInteractions();
      tableDeletions.verifyNoInteractions();
    }
  }

  @Test
  void testRetainedOwnerFallbackRequiresCurrentParentUse() throws Exception {
    TableDeletionService tableDeletionService = mock(TableDeletionService.class);
    UserPrincipal principal = new UserPrincipal("alice");
    when(tableDeletionService.getRetainedTable(3L, "orders")).thenReturn(table(42L));
    when(tableDeletionService.isRetainedOwner(42L, principal)).thenReturn(true);

    try (MockedStatic<EntityIdService> entityIds = mockStatic(EntityIdService.class);
        MockedStatic<MetadataAuthzHelper> authorization = mockStatic(MetadataAuthzHelper.class);
        MockedStatic<TableDeletionService> tableDeletions = mockStatic(TableDeletionService.class);
        MockedStatic<PrincipalUtils> principals = mockStatic(PrincipalUtils.class)) {
      entityIds.when(() -> EntityIdService.getEntityId(SCHEMA, EntityType.SCHEMA)).thenReturn(3L);
      tableDeletions.when(TableDeletionService::getInstance).thenReturn(tableDeletionService);
      principals.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      authorization
          .when(
              () ->
                  MetadataAuthzHelper.checkAccess(
                      TABLE,
                      EntityType.TABLE,
                      AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION))
          .thenReturn(false);
      authorization
          .when(() -> MetadataAuthzHelper.checkAccess(TABLE, EntityType.TABLE, PARENT_USE))
          .thenReturn(true);

      IcebergTableDeletionAuthzHandler handler = handler();
      handler.process(identifiers());
      assertTrue(handler.authorizationCompleted());

      authorization
          .when(() -> MetadataAuthzHelper.checkAccess(TABLE, EntityType.TABLE, PARENT_USE))
          .thenReturn(false);
      assertThrows(NoSuchTableException.class, () -> handler().process(identifiers()));
    }
  }

  @Test
  void testAuthorizedRetainedGenerationIsBoundToRequest() throws Exception {
    TableDeletionService tableDeletionService = mock(TableDeletionService.class);
    HttpServletRequest request = requestWithAttributes();
    when(tableDeletionService.getRetainedTable(3L, "orders")).thenReturn(table(42L));

    try (MockedStatic<EntityIdService> entityIds = mockStatic(EntityIdService.class);
        MockedStatic<MetadataAuthzHelper> authorization = mockStatic(MetadataAuthzHelper.class);
        MockedStatic<TableDeletionService> tableDeletions =
            mockStatic(TableDeletionService.class)) {
      entityIds.when(() -> EntityIdService.getEntityId(SCHEMA, EntityType.SCHEMA)).thenReturn(3L);
      tableDeletions.when(TableDeletionService::getInstance).thenReturn(tableDeletionService);
      authorization
          .when(
              () ->
                  MetadataAuthzHelper.checkAccess(
                      TABLE,
                      EntityType.TABLE,
                      AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION))
          .thenReturn(true);

      handler(request).process(identifiers());

      IcebergTableDeletionAuthzHandler.AuthorizedDeletionTarget target =
          IcebergTableDeletionAuthzHandler.authorizedDeletion(request);
      assertNotNull(target);
      assertEquals(42L, target.tableId());
      assertEquals("D1", target.deletionId());
    }
  }

  @Test
  void testRestCatalogDelegationDoesNotSkipRetainedAuthorization() {
    IcebergCatalogWrapperManager wrapperManager = mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn("metalake");
    when(provider.getDefaultCatalogName()).thenReturn("catalog");
    when(wrapperManager.getCatalogWrapper("catalog")).thenReturn(wrapper);
    when(wrapper.isRESTCatalog()).thenReturn(true);
    IcebergRESTServerContext.create(provider, true, true, true, wrapperManager);

    try (MockedStatic<MetadataAuthzHelper> authorization = mockStatic(MetadataAuthzHelper.class);
        MockedStatic<TableDeletionService> tableDeletions =
            mockStatic(TableDeletionService.class)) {
      authorization
          .when(
              () ->
                  MetadataAuthzHelper.checkAccess(
                      TABLE,
                      EntityType.TABLE,
                      AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION))
          .thenReturn(true);

      assertTrue(IcebergTableDeletionAuthzHandler.canListRetained(TABLE, 42L));
      authorization.verify(
          () ->
              MetadataAuthzHelper.checkAccess(
                  TABLE,
                  EntityType.TABLE,
                  AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION));
      tableDeletions.verifyNoInteractions();
    }
  }

  private static IcebergTableDeletionAuthzHandler handler() throws Exception {
    return handler(mock(HttpServletRequest.class));
  }

  private static IcebergTableDeletionAuthzHandler handler(HttpServletRequest request)
      throws Exception {
    Method method =
        TestIcebergTableDeletionAuthzHandler.class.getDeclaredMethod(
            "manage", String.class, String.class, String.class, HttpServletRequest.class);
    return new IcebergTableDeletionAuthzHandler(
        method.getAnnotation(AuthorizationExpression.class),
        method.getParameters(),
        new Object[] {"catalog", "schema", "orders", request});
  }

  private static HttpServletRequest requestWithAttributes() {
    Map<String, Object> attributes = new HashMap<>();
    HttpServletRequest request = mock(HttpServletRequest.class);
    doAnswer(
            invocation -> {
              attributes.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(request)
        .setAttribute(anyString(), any());
    when(request.getAttribute(anyString()))
        .thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
    return request;
  }

  private static Map<EntityType, NameIdentifier> identifiers() {
    Map<EntityType, NameIdentifier> identifiers = new EnumMap<>(EntityType.class);
    identifiers.put(EntityType.METALAKE, NameIdentifier.of("metalake"));
    identifiers.put(EntityType.CATALOG, NameIdentifier.of("metalake", "catalog"));
    identifiers.put(EntityType.SCHEMA, SCHEMA);
    return identifiers;
  }

  private static TablePO table(long tableId) {
    return TablePO.builder()
        .withTableId(tableId)
        .withTableName("orders")
        .withMetalakeId(1L)
        .withCatalogId(2L)
        .withSchemaId(3L)
        .withAuditInfo("{}")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(100L)
        .withDeletionId("D1")
        .build();
  }

  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.TABLE)
  @SuppressWarnings("unused")
  private void manage(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @IcebergAuthorizationMetadata(type = RequestType.MANAGE_TABLE_DELETION) String table,
      HttpServletRequest request) {}
}
