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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Test for {@link LoadTableAuthzHandler}. */
public class TestLoadTableAuthzHandler {

  private static final String TEST_METALAKE = "test_metalake";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_SCHEMA = "test_schema";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @BeforeAll
  public static void initAuthorizer() {
    GravitinoAuthorizerProvider.getInstance().initialize(new ServerConfig());
  }

  @Test
  public void testLoadTableDoesNotRejectMetadataTableLikeNameInHierarchicalSchema()
      throws Exception {
    // The hierarchical schema "a.b" holds a real table "files". A table a.b exists too, so
    // a.b.files could also name its metadata table; the real table must win.
    InMemoryCatalog catalog = newCatalog();
    catalog.createNamespace(Namespace.of("a"));
    catalog.createNamespace(Namespace.of("a", "b"));
    catalog.createTable(TableIdentifier.of(Namespace.of("a"), "b"), SCHEMA);
    catalog.createTable(TableIdentifier.of(Namespace.of("a", "b"), "files"), SCHEMA);
    resetContext(catalog);

    LoadTableAuthzHandler handler =
        createHandler(new Object[] {TEST_CATALOG + "/", "a%1Fb", "files"});

    // The real table must reach authorization instead of being rejected as a metadata table.
    assertDoesNotThrow(() -> handler.process(baseIdentifierMap()));
  }

  @Test
  public void testLoadTableRejectsMetadataTableWhenNamespaceHasNoSuchTable() throws Exception {
    // Both the namespace [db, t] and the table db.t exist, but [db, t] has no table "files",
    // so db.t.files is a metadata-table request and must get 404 for the client fallback.
    InMemoryCatalog catalog = newCatalog();
    catalog.createNamespace(Namespace.of("db"));
    catalog.createNamespace(Namespace.of("db", "t"));
    catalog.createTable(TableIdentifier.of(Namespace.of("db"), "t"), SCHEMA);
    resetContext(catalog);

    LoadTableAuthzHandler handler =
        createHandler(new Object[] {TEST_CATALOG + "/", "db%1Ft", "files"});

    assertThrows(NoSuchTableException.class, () -> handler.process(baseIdentifierMap()));
  }

  @Test
  public void testLoadTableStillRejectsGenuineMetadataTableRequests() throws Exception {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST catalogWrapper = Mockito.mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(catalogWrapper);
    // [db, table] is not an existing namespace - this is a metadata-table request.
    when(catalogWrapper.namespaceExists(Namespace.of("db", "t"))).thenReturn(false);
    resetContext(wrapperManager);

    LoadTableAuthzHandler handler =
        createHandler(new Object[] {TEST_CATALOG + "/", "db%1Ft", "files"});

    assertThrows(NoSuchTableException.class, () -> handler.process(baseIdentifierMap()));
    verify(catalogWrapper, never()).resolvesToMetadataTable(any(TableIdentifier.class));
  }

  @Test
  public void testBackendFailureKeepsMetadataTableAnswer404() throws Exception {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST catalogWrapper = Mockito.mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(catalogWrapper);
    when(catalogWrapper.namespaceExists(Namespace.of("db", "t")))
        .thenThrow(new RuntimeException("remote catalog unavailable"));
    resetContext(wrapperManager);

    LoadTableAuthzHandler handler =
        createHandler(new Object[] {TEST_CATALOG + "/", "db%1Ft", "files"});

    // A failing existence check must keep the guaranteed 404 for metadata-shaped requests,
    // not surface as a server error.
    assertThrows(NoSuchTableException.class, () -> handler.process(baseIdentifierMap()));
  }

  @Test
  public void testLoadFailureOfExistingNamespaceGoesToAuthorization() throws Exception {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST catalogWrapper = Mockito.mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(catalogWrapper);
    when(catalogWrapper.namespaceExists(Namespace.of("a", "b"))).thenReturn(true);
    when(catalogWrapper.resolvesToMetadataTable(any(TableIdentifier.class)))
        .thenThrow(new ServiceUnavailableException("remote catalog unavailable"));
    resetContext(wrapperManager);

    LoadTableAuthzHandler handler =
        createHandler(new Object[] {TEST_CATALOG + "/", "a%1Fb", "files"});

    // A 404 would make the client fall back to the metadata table of a.b, so the request goes on
    // to authorization and the real load reports the failure.
    assertDoesNotThrow(() -> handler.process(baseIdentifierMap()));
  }

  private static LoadTableAuthzHandler createHandler(Object[] args) throws Exception {
    Method method =
        TestOperations.class.getMethod("loadTable", String.class, String.class, String.class);
    return new LoadTableAuthzHandler(
        method.getAnnotation(AuthorizationExpression.class), method.getParameters(), args);
  }

  private static Map<Entity.EntityType, NameIdentifier> baseIdentifierMap() {
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(TEST_METALAKE));
    nameIdentifierMap.put(
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(TEST_METALAKE, TEST_CATALOG));
    nameIdentifierMap.put(
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema(TEST_METALAKE, TEST_CATALOG, TEST_SCHEMA));
    return nameIdentifierMap;
  }

  private static InMemoryCatalog newCatalog() {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("memory", ImmutableMap.of());
    return catalog;
  }

  /** Points the handler at a catalog wrapper backed by {@code catalog}. */
  private static void resetContext(InMemoryCatalog catalog) {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST catalogWrapper = Mockito.mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(catalogWrapper);
    when(catalogWrapper.getCatalog()).thenReturn(catalog);
    when(catalogWrapper.namespaceExists(any(Namespace.class)))
        .thenAnswer(invocation -> catalog.namespaceExists(invocation.getArgument(0)));
    when(catalogWrapper.resolvesToMetadataTable(any(TableIdentifier.class))).thenCallRealMethod();
    resetContext(wrapperManager);
  }

  private static void resetContext(IcebergCatalogWrapperManager wrapperManager) {
    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn(TEST_METALAKE);
    when(configProvider.getDefaultCatalogName()).thenReturn(TEST_CATALOG);
    IcebergRESTServerContext.create(configProvider, false, false, true, wrapperManager);
  }

  /** Test operations class to provide method annotations for testing. */
  @SuppressWarnings("unused")
  public static class TestOperations {
    @AuthorizationExpression(
        expression = AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION,
        allowCheckExistence =
            AuthorizationExpressionConstants
                .ICEBERG_TABLE_EXISTS_SECONDARY_AUTHORIZATION_EXPRESSION,
        accessMetadataType = MetadataObject.Type.TABLE)
    public void loadTable(
        @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String prefix,
        @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String namespace,
        @IcebergAuthorizationMetadata(type = IcebergAuthorizationMetadata.RequestType.LOAD_TABLE)
            @AuthorizationMetadata(type = Entity.EntityType.TABLE)
            String table) {
      // Test method
    }
  }
}
