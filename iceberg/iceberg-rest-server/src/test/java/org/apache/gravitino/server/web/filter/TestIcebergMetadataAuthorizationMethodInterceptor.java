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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.rest.RESTUtil;
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
    IcebergRESTServerContext.create(mockConfigProvider, false, false, null);
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
  public void testExtractTableNameIdentifierWithNestedNamespace() throws Exception {
    IcebergMetadataAuthorizationMethodInterceptor interceptor =
        new IcebergMetadataAuthorizationMethodInterceptor();

    Method testMethod =
        TestOperations.class.getMethod(
            "testTableOperation", String.class, String.class, String.class);
    Parameter[] parameters = testMethod.getParameters();

    Object[] args = new Object[] {TEST_CATALOG + "/", "analytics.sales", "orders"};
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap =
        interceptor.extractNameIdentifierFromParameters(parameters, args);

    NameIdentifier schemaId = nameIdentifierMap.get(Entity.EntityType.SCHEMA);
    assertNotNull(schemaId);
    assertEquals("analytics.sales", schemaId.name());

    NameIdentifier tableId = nameIdentifierMap.get(Entity.EntityType.TABLE);
    assertNotNull(tableId);
    assertEquals(
        NameIdentifierUtil.ofTable(TEST_METALAKE, TEST_CATALOG, "analytics.sales", "orders")
            .toString(),
        tableId.toString());
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
  public void testTryAlternativeAuthorizationFallsBackToParentScope() {
    TestableIcebergMetadataAuthorizationMethodInterceptor interceptor =
        new TestableIcebergMetadataAuthorizationMethodInterceptor(
            Set.of("analytics.sales"), Set.of());
    Map<EntityType, NameIdentifier> nameIdentifierMap =
        nameIdentifierMap("analytics.sales.orders");

    boolean authorized =
        interceptor.tryAlternativeAuthorization(
            "ANY_USE_CATALOG && ANY_USE_SCHEMA",
            nameIdentifierMap,
            new HashMap<>(),
            new AuthorizationRequestContext());

    assertTrue(authorized);
  }

  @Test
  public void testTryAlternativeAuthorizationRespectsDenyOnCurrentScope() {
    TestableIcebergMetadataAuthorizationMethodInterceptor interceptor =
        new TestableIcebergMetadataAuthorizationMethodInterceptor(
            Set.of("analytics.sales"), Set.of("analytics.sales.orders"));
    Map<EntityType, NameIdentifier> nameIdentifierMap =
        nameIdentifierMap("analytics.sales.orders");

    boolean authorized =
        interceptor.tryAlternativeAuthorization(
            "ANY_USE_CATALOG && ANY_USE_SCHEMA",
            nameIdentifierMap,
            new HashMap<>(),
            new AuthorizationRequestContext());

    assertFalse(authorized);
  }

  @Test
  public void testTryAlternativeAuthorizationSkipsNonNestedSchema() {
    AtomicInteger evaluateCalls = new AtomicInteger(0);
    TestableIcebergMetadataAuthorizationMethodInterceptor interceptor =
        new TestableIcebergMetadataAuthorizationMethodInterceptor(
            Set.of("analytics"), Set.of(), evaluateCalls);
    Map<EntityType, NameIdentifier> nameIdentifierMap = nameIdentifierMap("analytics");

    boolean authorized =
        interceptor.tryAlternativeAuthorization(
            "ANY_USE_CATALOG && ANY_USE_SCHEMA",
            nameIdentifierMap,
            new HashMap<>(),
            new AuthorizationRequestContext());

    assertFalse(authorized);
    assertEquals(0, evaluateCalls.get());
  }

  private static Map<EntityType, NameIdentifier> nameIdentifierMap(String schemaName) {
    Map<EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(TEST_METALAKE));
    nameIdentifierMap.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(TEST_METALAKE, TEST_CATALOG));
    nameIdentifierMap.put(
        EntityType.SCHEMA, NameIdentifierUtil.ofSchema(TEST_METALAKE, TEST_CATALOG, schemaName));
    return nameIdentifierMap;
  }

  private static class TestableIcebergMetadataAuthorizationMethodInterceptor
      extends IcebergMetadataAuthorizationMethodInterceptor {

    private static final String DENY_USE_SCHEMA_EXPRESSION =
        "ANY(DENY_USE_SCHEMA, METALAKE, CATALOG, SCHEMA)";

    private final Set<String> allowedSchemas;
    private final Set<String> deniedSchemas;
    private final AtomicInteger evaluateCalls;

    private TestableIcebergMetadataAuthorizationMethodInterceptor(
        Set<String> allowedSchemas, Set<String> deniedSchemas) {
      this(allowedSchemas, deniedSchemas, new AtomicInteger(0));
    }

    private TestableIcebergMetadataAuthorizationMethodInterceptor(
        Set<String> allowedSchemas, Set<String> deniedSchemas, AtomicInteger evaluateCalls) {
      this.allowedSchemas = allowedSchemas;
      this.deniedSchemas = deniedSchemas;
      this.evaluateCalls = evaluateCalls;
    }

    @Override
    protected AuthorizationExpressionEvaluator createAuthorizationExpressionEvaluator(
        String expression) {
      return new AuthorizationExpressionEvaluator("METALAKE::OWNER", null) {
        @Override
        public boolean evaluate(
            Map<EntityType, NameIdentifier> metadataNames,
            Map<String, Object> pathParams,
            AuthorizationRequestContext requestContext,
            Optional<String> entityType) {
          evaluateCalls.incrementAndGet();
          String schemaName = metadataNames.get(EntityType.SCHEMA).name();
          if (DENY_USE_SCHEMA_EXPRESSION.equals(expression)) {
            return deniedSchemas.contains(schemaName);
          }
          return allowedSchemas.contains(schemaName);
        }
      };
    }
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
  }
}
