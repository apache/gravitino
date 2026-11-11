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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.rest.IcebergNamespaceOperations;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.server.AuthorizationDenialFailureEvent;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Tests that Iceberg REST {@code registerTable} authz requires modify/owner for overwrite and
 * load-table rights when adopting another live table's metadata.
 */
class TestRegisterTableAuthzHandler {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String TABLE = "existing_table";
  private static final String META_LOC = "file:/warehouse/other/secret/metadata/v1.metadata.json";
  private static final String TABLE_LOC = "file:/warehouse/other/secret";
  private static final String PROCEEDED = "PROCEEDED";

  private CatalogWrapperForREST catalogWrapper;
  private GravitinoAuthorizer authorizer;
  private IcebergMetadataAuthorizationMethodInterceptor interceptor;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  void setUp() {
    catalogWrapper = mock(CatalogWrapperForREST.class);
    when(catalogWrapper.isRESTCatalog()).thenReturn(false);
    stubEmptyCatalog();
    when(catalogWrapper.loadTableMetadataFromLocation(any())).thenReturn(tableMetadata());

    IcebergCatalogWrapperManager wrapperManager = mock(IcebergCatalogWrapperManager.class);
    when(wrapperManager.getCatalogWrapper(CATALOG)).thenReturn(catalogWrapper);

    IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn(METALAKE);
    when(configProvider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(configProvider, true, false, false, wrapperManager);

    authorizer = mock(GravitinoAuthorizer.class);
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    principalUtils = mockStatic(PrincipalUtils.class);
    authorizationUtils = mockStatic(AuthorizationUtils.class);
    authorizerProvider = mockStatic(GravitinoAuthorizerProvider.class);

    principalUtils
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal("tester"));
    principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
    authorizerProvider.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
    when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(false);
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(false);
    interceptor = new IcebergMetadataAuthorizationMethodInterceptor();
  }

  @AfterEach
  void tearDown() {
    authorizerProvider.close();
    authorizationUtils.close();
    principalUtils.close();
  }

  @Test
  void testCreateTableAloneCannotOverwrite() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    MethodInvocation overwrite = registerInvocation(true, TABLE, META_LOC);
    Object result = interceptor.invoke(overwrite);
    assertForbidden(result);
    verify(overwrite, never()).proceed();
  }

  @Test
  void testModifyTableAllowsOverwrite() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(true, TABLE, META_LOC)));
  }

  @Test
  void testTableOwnerAllowsOverwriteWithoutModifyPrivilege() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA);
    ownerOf(MetadataObject.Type.TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(true, TABLE, META_LOC)));
  }

  @Test
  void testCreateTableStillAllowsPlainRegisterOfOrphanedMetadata() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(false, TABLE, META_LOC)));
  }

  @Test
  void testCreateTableAloneCannotAdoptTrackedSource() throws Throwable {
    TableIdentifier source = TableIdentifier.of("other", "secret");
    stubCatalogWithSource(source);
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    MethodInvocation invocation = registerInvocation(false, "copy", META_LOC);
    Object result = interceptor.invoke(invocation);
    assertForbidden(result);
    verify(invocation, never()).proceed();
  }

  @Test
  void testSelectTableAllowsAdoptTrackedSource() throws Throwable {
    TableIdentifier source = TableIdentifier.of("other", "secret");
    stubCatalogWithSource(source);
    allow(
        Privilege.Name.USE_CATALOG,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.SELECT_TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(false, "copy", META_LOC)));
  }

  @Test
  void testOverwriteDenialChecksTableBeforeScanningSources() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    MethodInvocation overwrite = registerInvocation(true, TABLE, META_LOC);
    assertForbidden(interceptor.invoke(overwrite));
    verify(catalogWrapper, never()).listNamespace(any());
    verify(catalogWrapper, never()).loadTableMetadataFromLocation(any());
  }

  @Test
  void testCreateDenialChecksDestinationBeforeReadingMetadata() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA);

    MethodInvocation create = registerInvocation(false, TABLE, META_LOC);
    assertForbidden(interceptor.invoke(create));
    verify(catalogWrapper, never()).loadTableMetadataFromLocation(any());
    verify(catalogWrapper, never()).listNamespace(any());
  }

  @Test
  void testOverwriteDenialDispatchesAuditAgainstTable() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    EventBus eventBus = mock(EventBus.class);
    try (MockedStatic<GravitinoEnv> env = mockGravitinoEnv(eventBus)) {
      assertForbidden(interceptor.invoke(registerInvocation(true, TABLE, META_LOC)));

      ArgumentCaptor<BaseEvent> event = ArgumentCaptor.forClass(BaseEvent.class);
      verify(eventBus).dispatchEvent(event.capture());
      assertEquals(
          NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE),
          assertInstanceOf(AuthorizationDenialFailureEvent.class, event.getValue()).identifier());
    }
  }

  @Test
  void testSourceDenialDispatchesAuditAgainstSchema() throws Throwable {
    TableIdentifier source = TableIdentifier.of("other", "secret");
    stubCatalogWithSource(source);
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    EventBus eventBus = mock(EventBus.class);
    try (MockedStatic<GravitinoEnv> env = mockGravitinoEnv(eventBus)) {
      assertForbidden(interceptor.invoke(registerInvocation(false, "copy", META_LOC)));

      ArgumentCaptor<BaseEvent> event = ArgumentCaptor.forClass(BaseEvent.class);
      verify(eventBus).dispatchEvent(event.capture());
      assertEquals(
          NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA),
          assertInstanceOf(AuthorizationDenialFailureEvent.class, event.getValue()).identifier());
    }
  }

  @Test
  void testUnreadableSourceMetadataBlocksRegister() throws Throwable {
    TableIdentifier source = TableIdentifier.of("other", "secret");
    stubCatalogWithSource(source);
    when(catalogWrapper.getTableMetadataLocation(source))
        .thenReturn(Optional.of("file:/warehouse/other/secret/metadata/v2.metadata.json"));
    when(catalogWrapper.loadTableMetadata(source)).thenThrow(new RuntimeException("corrupt"));
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    MethodInvocation invocation = registerInvocation(false, "copy", META_LOC);
    interceptor.invoke(invocation);
    verify(invocation, never()).proceed();
  }

  @Test
  void testMissingRequestBodyIsBadRequest() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    MethodInvocation invocation = registerInvocationWithRequest(null);
    Object result = interceptor.invoke(invocation);
    assertBadRequest(result);
    verify(invocation, never()).proceed();
  }

  @Test
  void testMissingTableNameIsBadRequest() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    RegisterTableRequest nullName = mock(RegisterTableRequest.class);
    when(nullName.name()).thenReturn(null);
    when(nullName.metadataLocation()).thenReturn(META_LOC);
    MethodInvocation nullNameInvocation = registerInvocationWithRequest(nullName);
    assertBadRequest(interceptor.invoke(nullNameInvocation));
    verify(nullNameInvocation, never()).proceed();

    MethodInvocation emptyNameInvocation = registerInvocation(false, "", META_LOC);
    assertBadRequest(interceptor.invoke(emptyNameInvocation));
    verify(emptyNameInvocation, never()).proceed();
  }

  @Test
  void testUnreadableMetadataLocationIsBadRequestNotAuthzInternalError() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    when(catalogWrapper.loadTableMetadataFromLocation(any()))
        .thenThrow(
            new org.apache.iceberg.exceptions.BadRequestException(
                "Metadata location %s is not a valid metadata file: directory", META_LOC));

    MethodInvocation invocation = registerInvocation(false, TABLE, META_LOC);
    Object result = interceptor.invoke(invocation);
    assertBadRequest(result);
    verify(invocation, never()).proceed();
  }

  private MethodInvocation registerInvocation(
      boolean overwrite, String tableName, String metadataLocation) throws Throwable {
    RegisterTableRequest request =
        ImmutableRegisterTableRequest.builder()
            .name(tableName)
            .metadataLocation(metadataLocation)
            .overwrite(overwrite)
            .build();
    return registerInvocationWithRequest(request);
  }

  private MethodInvocation registerInvocationWithRequest(RegisterTableRequest request)
      throws Throwable {
    Method method =
        IcebergNamespaceOperations.class.getMethod(
            "registerTable", String.class, String.class, RegisterTableRequest.class, String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[] {CATALOG + "/", SCHEMA, request, null});
    when(invocation.proceed()).thenReturn(PROCEEDED);
    return invocation;
  }

  private void stubEmptyCatalog() {
    ListNamespacesResponse emptyNs = mock(ListNamespacesResponse.class);
    when(emptyNs.namespaces()).thenReturn(Collections.emptyList());
    ListTablesResponse emptyTables = mock(ListTablesResponse.class);
    when(emptyTables.identifiers()).thenReturn(Collections.emptyList());
    when(catalogWrapper.listNamespace(any())).thenReturn(emptyNs);
    when(catalogWrapper.listTable(any())).thenReturn(emptyTables);
  }

  private void stubCatalogWithSource(TableIdentifier source) {
    Namespace ns = source.namespace();
    ListNamespacesResponse rootNs = mock(ListNamespacesResponse.class);
    when(rootNs.namespaces()).thenReturn(List.of(ns));
    ListNamespacesResponse childNs = mock(ListNamespacesResponse.class);
    when(childNs.namespaces()).thenReturn(Collections.emptyList());
    ListTablesResponse childTables = mock(ListTablesResponse.class);
    when(childTables.identifiers()).thenReturn(List.of(source));

    when(catalogWrapper.listNamespace(Namespace.empty())).thenReturn(rootNs);
    when(catalogWrapper.listNamespace(ns)).thenReturn(childNs);
    // Like Iceberg's HiveCatalog, the root namespace cannot list tables.
    when(catalogWrapper.listTable(Namespace.empty()))
        .thenThrow(new IllegalArgumentException("Missing database in namespace: "));
    when(catalogWrapper.listTable(ns)).thenReturn(childTables);
    when(catalogWrapper.getTableMetadataLocation(source)).thenReturn(Optional.of(META_LOC));
    when(catalogWrapper.loadTableMetadata(source)).thenReturn(tableMetadata());
  }

  private static TableMetadata tableMetadata() {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), TABLE_LOC, Collections.emptyMap());
  }

  private void ownerOf(MetadataObject.Type... types) {
    Set<MetadataObject.Type> owned = Set.of(types);
    doAnswer(
            invocation -> {
              MetadataObject metadataObject = invocation.getArgument(2);
              return metadataObject != null && owned.contains(metadataObject.type());
            })
        .when(authorizer)
        .isOwner(any(), any(), any(), any());
  }

  private void allow(Privilege.Name... privileges) {
    Set<Privilege.Name> allowed = Set.of(privileges);
    doAnswer(invocation -> allowed.contains(invocation.getArgument(3)))
        .when(authorizer)
        .authorize(any(), any(), any(), any(), any());
  }

  private static MockedStatic<GravitinoEnv> mockGravitinoEnv(EventBus eventBus) {
    MockedStatic<GravitinoEnv> envStatic = mockStatic(GravitinoEnv.class);
    GravitinoEnv env = mock(GravitinoEnv.class);
    envStatic.when(GravitinoEnv::getInstance).thenReturn(env);
    when(env.eventBus()).thenReturn(eventBus);
    return envStatic;
  }

  private static void assertForbidden(Object result) {
    Response response = assertInstanceOf(Response.class, result);
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertInstanceOf(ErrorResponse.class, response.getEntity());
  }

  private static void assertBadRequest(Object result) {
    Response response = assertInstanceOf(Response.class, result);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertInstanceOf(ErrorResponse.class, response.getEntity());
  }
}
