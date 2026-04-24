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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

/** Test for {@link IcebergMetadataAuthorizationMethodInterceptor}. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestIcebergMetadataAuthorizationMethodInterceptor {

  private static final String TEST_METALAKE = "test_metalake";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_SCHEMA = "test_schema";

  @BeforeAll
  public void init() {
    // Initialize IcebergRESTServerContext with a mock config provider
    IcebergConfigProvider mockConfigProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(mockConfigProvider.getMetalakeName()).thenReturn(TEST_METALAKE);
    Mockito.when(mockConfigProvider.getDefaultCatalogName()).thenReturn(TEST_CATALOG);
    IcebergRESTServerContext.create(mockConfigProvider, false, false, true, null);
  }

  @Test
  public void testExtractTableNameIdentifierWithEncodedTableName() throws Exception {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    // Get the method with annotations
    Method testMethod =
        TestOperations.class.getMethod(
            "testTableOperation", String.class, String.class, String.class);
    Parameter[] parameters = testMethod.getParameters();

    // Test with encoded table name (e.g., table name with special characters)
    String encodedTableName = RESTUtil.encodeString("my-table");
    Object[] args = new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, encodedTableName};

    // Extract name identifiers
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
        interceptor.extractNameIdentifierFromParameters(parameters, args);

    // Verify metalake
    NameIdentifier metalakeId = nameIdentifierMap.get(Entity.EntityType.METALAKE);
    assertNotNull(metalakeId);
    assertEquals(TEST_METALAKE, metalakeId.name());

    // Verify catalog
    NameIdentifier catalogId = nameIdentifierMap.get(Entity.EntityType.CATALOG);
    assertNotNull(catalogId);
    assertEquals(TEST_CATALOG, catalogId.name());
    assertEquals(
        NameIdentifierUtil.ofCatalog(TEST_METALAKE, TEST_CATALOG).toString(), catalogId.toString());

    // Verify schema
    NameIdentifier schemaId = nameIdentifierMap.get(Entity.EntityType.SCHEMA);
    assertNotNull(schemaId);
    assertEquals(TEST_SCHEMA, schemaId.name());
    assertEquals(
        NameIdentifierUtil.ofSchema(TEST_METALAKE, TEST_CATALOG, TEST_SCHEMA).toString(),
        schemaId.toString());

    // Verify table - this tests that RESTUtil.decodeString is properly called on line 81
    NameIdentifier tableId = nameIdentifierMap.get(Entity.EntityType.TABLE);
    assertNotNull(tableId);
    assertEquals("my-table", tableId.name());
    assertEquals(
        NameIdentifierUtil.ofTable(TEST_METALAKE, TEST_CATALOG, TEST_SCHEMA, "my-table").toString(),
        tableId.toString());
  }

  @Test
  public void testExtractTableNameIdentifierWithSpecialCharacters() throws Exception {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    Method testMethod =
        TestOperations.class.getMethod(
            "testTableOperation", String.class, String.class, String.class);
    Parameter[] parameters = testMethod.getParameters();

    // Test with table name containing special characters that need URL encoding
    String tableNameWithSpecialChars = "table/with%special&chars";
    String encodedTableName = RESTUtil.encodeString(tableNameWithSpecialChars);
    Object[] args = new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, encodedTableName};

    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
        interceptor.extractNameIdentifierFromParameters(parameters, args);

    // Verify that the table name is properly decoded
    NameIdentifier tableId = nameIdentifierMap.get(Entity.EntityType.TABLE);
    assertNotNull(tableId);
    assertEquals(tableNameWithSpecialChars, tableId.name());
  }

  @Test
  public void testExtractTableNameIdentifierWithSimpleTableName() throws Exception {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    Method testMethod =
        TestOperations.class.getMethod(
            "testTableOperation", String.class, String.class, String.class);
    Parameter[] parameters = testMethod.getParameters();

    // Test with simple table name (no special characters)
    String simpleTableName = "simple_table";
    Object[] args = new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, simpleTableName};

    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
        interceptor.extractNameIdentifierFromParameters(parameters, args);

    // Verify that the table name is properly processed
    NameIdentifier tableId = nameIdentifierMap.get(Entity.EntityType.TABLE);
    assertNotNull(tableId);
    assertEquals(simpleTableName, tableId.name());
  }

  @Test
  public void testIsExceptionPropagate() {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    // Test Iceberg exception propagation
    Exception icebergException = new org.apache.iceberg.exceptions.NoSuchTableException("test");
    assertTrue(interceptor.isExceptionPropagate(icebergException));

    // Test non-Iceberg exception
    Exception otherException = new RuntimeException("test");
    assertFalse(interceptor.isExceptionPropagate(otherException));
  }

  @Test
  public void testExtractNestedSchemaNamespace() throws Exception {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    Method testMethod =
        TestOperations.class.getMethod(
            "testTableOperation", String.class, String.class, String.class);
    Parameter[] parameters = testMethod.getParameters();

    // Namespace "A/B/C" encoded using Iceberg REST convention (%1F as level separator)
    String encodedNamespace =
        RESTUtil.encodeNamespace(org.apache.iceberg.catalog.Namespace.of("A", "B", "C"));
    Object[] args = new Object[] {TEST_CATALOG + "/", encodedNamespace, "my_table"};

    org.apache.gravitino.Config mockConfig = Mockito.mock(org.apache.gravitino.Config.class);
    Mockito.when(mockConfig.get(Configs.SCHEMA_NAMESPACE_SEPARATOR)).thenReturn(":");
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);
    try {
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
          interceptor.extractNameIdentifierFromParameters(parameters, args);

      NameIdentifier schemaId = nameIdentifierMap.get(Entity.EntityType.SCHEMA);
      assertNotNull(schemaId);
      // Expect full logical path, not just the last level
      assertEquals("A:B:C", schemaId.name());
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
    }
  }

  @Test
  public void testExtractFlatSchemaNamespace() throws Exception {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    Method testMethod =
        TestOperations.class.getMethod(
            "testTableOperation", String.class, String.class, String.class);
    Parameter[] parameters = testMethod.getParameters();

    String encodedNamespace =
        RESTUtil.encodeNamespace(org.apache.iceberg.catalog.Namespace.of("my_schema"));
    Object[] args = new Object[] {TEST_CATALOG + "/", encodedNamespace, "my_table"};

    org.apache.gravitino.Config mockConfig = Mockito.mock(org.apache.gravitino.Config.class);
    Mockito.when(mockConfig.get(Configs.SCHEMA_NAMESPACE_SEPARATOR)).thenReturn(":");
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", mockConfig, true);
    try {
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
          interceptor.extractNameIdentifierFromParameters(parameters, args);

      NameIdentifier schemaId = nameIdentifierMap.get(Entity.EntityType.SCHEMA);
      assertNotNull(schemaId);
      assertEquals("my_schema", schemaId.name());
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
    }
  }

  @Test
  public void testInvokeSkipsAuthorizationForRestCatalog() throws Throwable {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST wrapper = Mockito.mock(CatalogWrapperForREST.class);
    RESTCatalog restCatalog = Mockito.mock(RESTCatalog.class);
    Mockito.when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(wrapper);
    Mockito.when(wrapper.getCatalog()).thenReturn(restCatalog);
    Mockito.when(wrapper.isRESTCatalog()).thenReturn(true);
    resetContext(wrapperManager, true);

    Method method =
        TestOperations.class.getMethod(
            "testTableOperationWithAuthorizationExpression",
            String.class,
            String.class,
            String.class);
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments())
        .thenReturn(new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, "tbl"});
    Mockito.when(invocation.proceed()).thenReturn("PROCEEDED");

    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor() {
          @Override
          protected Optional<AuthorizationHandler> createAuthorizationHandler(
              Parameter[] parameters, Object[] args) {
            return Optional.of(
                new AuthorizationHandler() {
                  @Override
                  public void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap)
                      throws ForbiddenException {
                    throw new RuntimeException("test");
                  }

                  @Override
                  public boolean authorizationCompleted() {
                    return false;
                  }
                });
          }
        };
    Object result = interceptor.invoke(invocation);

    assertEquals("PROCEEDED", result);
    Mockito.verify(invocation, Mockito.times(1)).proceed();
  }

  @Test
  public void testInvokeDoesNotSkipAuthorizationForNonRestCatalog() throws Throwable {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST wrapper = Mockito.mock(CatalogWrapperForREST.class);
    Catalog nonRestCatalog = Mockito.mock(Catalog.class);
    Mockito.when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(wrapper);
    Mockito.when(wrapper.getCatalog()).thenReturn(nonRestCatalog);
    Mockito.when(wrapper.isRESTCatalog()).thenReturn(false);
    resetContext(wrapperManager, true);

    Method method =
        TestOperations.class.getMethod(
            "testTableOperationWithAuthorizationExpression",
            String.class,
            String.class,
            String.class);
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments())
        .thenReturn(new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, "tbl"});
    Mockito.when(invocation.proceed()).thenReturn("PROCEEDED");

    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor() {
          @Override
          protected Optional<AuthorizationHandler> createAuthorizationHandler(
              Parameter[] parameters, Object[] args) {
            return Optional.of(
                new AuthorizationHandler() {
                  @Override
                  public void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap)
                      throws ForbiddenException {
                    throw new RuntimeException("test");
                  }

                  @Override
                  public boolean authorizationCompleted() {
                    return false;
                  }
                });
          }
        };
    Object result = interceptor.invoke(invocation);

    assertNotEquals("PROCEEDED", result);
  }

  private void resetContext(IcebergCatalogWrapperManager wrapperManager) {
    resetContext(wrapperManager, true);
  }

  private void resetContext(
      IcebergCatalogWrapperManager wrapperManager, boolean skipAuthorizationForRestBackend) {
    IcebergConfigProvider mockConfigProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(mockConfigProvider.getMetalakeName()).thenReturn(TEST_METALAKE);
    Mockito.when(mockConfigProvider.getDefaultCatalogName()).thenReturn(TEST_CATALOG);
    IcebergRESTServerContext.create(
        mockConfigProvider, false, false, skipAuthorizationForRestBackend, wrapperManager);
  }

  @Test
  public void testInvokeDoesNotSkipAuthorizationForRestCatalogWhenFlagDisabled() throws Throwable {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST wrapper = Mockito.mock(CatalogWrapperForREST.class);
    RESTCatalog restCatalog = Mockito.mock(RESTCatalog.class);
    Mockito.when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(wrapper);
    Mockito.when(wrapper.getCatalog()).thenReturn(restCatalog);
    Mockito.when(wrapper.isRESTCatalog()).thenReturn(true);
    resetContext(wrapperManager, false);

    Method method =
        TestOperations.class.getMethod(
            "testTableOperationWithAuthorizationExpression",
            String.class,
            String.class,
            String.class);
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments())
        .thenReturn(new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, "tbl"});
    Mockito.when(invocation.proceed()).thenReturn("PROCEEDED");

    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor() {
          @Override
          protected Optional<AuthorizationHandler> createAuthorizationHandler(
              Parameter[] parameters, Object[] args) {
            throw new RuntimeException("test");
          }
        };
    Object result = interceptor.invoke(invocation);

    assertNotEquals("PROCEEDED", result);
  }

  @Test
  public void testInvokeSkipAuthorizationStillUsesCommonProceedExceptionMapping() throws Throwable {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST wrapper = Mockito.mock(CatalogWrapperForREST.class);
    RESTCatalog restCatalog = Mockito.mock(RESTCatalog.class);
    Mockito.when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(wrapper);
    Mockito.when(wrapper.getCatalog()).thenReturn(restCatalog);
    Mockito.when(wrapper.isRESTCatalog()).thenReturn(true);
    resetContext(wrapperManager, true);

    Method method =
        TestOperations.class.getMethod(
            "testTableOperationWithAuthorizationExpression",
            String.class,
            String.class,
            String.class);
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments())
        .thenReturn(new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, "tbl"});
    Mockito.when(invocation.proceed()).thenThrow(new RuntimeException("operation failed"));

    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();
    Object result = interceptor.invoke(invocation);

    assertTrue(result instanceof Response);
    Response response = (Response) result;
    assertEquals(500, response.getStatus());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("operation failed", errorResponse.message());
  }

  @Test
  public void testInvokeShouldSkipAuthorizationWrapperLookupFailureReturnsAuthInternalError()
      throws Throwable {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    Mockito.when(wrapperManager.getCatalogWrapper(TEST_CATALOG))
        .thenThrow(new IllegalArgumentException("wrapper lookup failed"));
    resetContext(wrapperManager, true);

    Method method =
        TestOperations.class.getMethod(
            "testTableOperationWithAuthorizationExpression",
            String.class,
            String.class,
            String.class);
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments())
        .thenReturn(new Object[] {TEST_CATALOG + "/", TEST_SCHEMA, "tbl"});
    Mockito.when(invocation.proceed()).thenReturn("PROCEEDED");

    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();
    Object result = interceptor.invoke(invocation);

    assertTrue(result instanceof Response);
    Response response = (Response) result;
    assertEquals(500, response.getStatus());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("RuntimeException", errorResponse.type());
    assertTrue(
        errorResponse.message().contains("Authorization failed due to system internal error"));
  }

  /** Test operations class to provide method annotations for testing. */
  @SuppressWarnings("unused")
  public static class TestOperations {
    public void testTableOperation(
        @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String prefix,
        @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String namespace,
        @AuthorizationMetadata(type = Entity.EntityType.TABLE) String table) {
      // Test method
    }

    @AuthorizationExpression(expression = "true")
    public void testTableOperationWithAuthorizationExpression(
        @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String prefix,
        @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String namespace,
        @AuthorizationMetadata(type = Entity.EntityType.TABLE) String table) {
      // Test method
    }
  }
}
