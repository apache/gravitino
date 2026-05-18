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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Test for {@link LoadViewAuthzHandler}. */
public class TestLoadViewAuthzHandler {

  private static final String TEST_METALAKE = "test_metalake";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_SCHEMA = "test_schema";

  @BeforeAll
  public static void initAuthorizer() {
    GravitinoAuthorizerProvider.getInstance().initialize(new ServerConfig());
  }

  @Test
  public void testLoadViewDoesNotRejectMetadataTableLikeName() throws Exception {
    IcebergCatalogWrapperManager wrapperManager = Mockito.mock(IcebergCatalogWrapperManager.class);
    CatalogWrapperForREST catalogWrapper = Mockito.mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper(TEST_CATALOG)).thenReturn(catalogWrapper);
    resetContext(wrapperManager);

    Method method =
        TestOperations.class.getMethod("loadView", String.class, String.class, String.class);
    LoadViewAuthzHandler handler =
        new LoadViewAuthzHandler(
            method.getAnnotation(AuthorizationExpression.class),
            method.getParameters(),
            new Object[] {TEST_CATALOG + "/", "nested.table", "files"});

    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(TEST_METALAKE));
    nameIdentifierMap.put(
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(TEST_METALAKE, TEST_CATALOG));
    nameIdentifierMap.put(
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema(TEST_METALAKE, TEST_CATALOG, TEST_SCHEMA));

    assertDoesNotThrow(() -> handler.process(nameIdentifierMap));
    verify(catalogWrapper, never()).viewExists(any(TableIdentifier.class));
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
        expression = AuthorizationExpressionConstants.ICEBERG_LOAD_VIEW_AUTHORIZATION_EXPRESSION,
        allowCheckExistence =
            AuthorizationExpressionConstants.ICEBERG_LOAD_VIEW_SECONDARY_AUTHORIZATION_EXPRESSION,
        accessMetadataType = MetadataObject.Type.VIEW)
    public void loadView(
        @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String prefix,
        @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String namespace,
        @IcebergAuthorizationMetadata(type = IcebergAuthorizationMetadata.RequestType.LOAD_VIEW)
            @AuthorizationMetadata(type = Entity.EntityType.VIEW)
            String view) {
      // Test method
    }
  }
}
