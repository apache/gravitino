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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link CreateNamespaceAuthzHandler} parent-schema injection logic. */
public class TestCreateNamespaceAuthzHandler {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";

  /**
   * Build a minimal nameIdentifierMap as the interceptor would produce before calling the handler.
   */
  private Map<EntityType, NameIdentifier> baseMap() {
    Map<EntityType, NameIdentifier> map = new HashMap<>();
    map.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(METALAKE));
    map.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(METALAKE, CATALOG));
    return map;
  }

  /**
   * Build a Parameter array and args array that simulate an annotated CREATE_NAMESPACE method
   * parameter, using reflection on a helper inner class.
   */
  private Object[] buildArgs(CreateNamespaceRequest request) throws Exception {
    return new Object[] {request};
  }

  private Parameter[] buildParams() throws Exception {
    return TestCreateNamespaceOp.class
        .getMethod("createNamespace", CreateNamespaceRequest.class)
        .getParameters();
  }

  /** Runs process() with a mocked separator config and returns the resulting nameIdentifierMap. */
  private Map<EntityType, NameIdentifier> runProcess(Namespace namespace) throws Exception {
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();
    Parameter[] params = buildParams();
    Object[] args = buildArgs(request);

    Map<EntityType, NameIdentifier> map = baseMap();

    GravitinoEnv mockEnv = Mockito.mock(GravitinoEnv.class);
    org.apache.gravitino.Config mockConfig = Mockito.mock(org.apache.gravitino.Config.class);
    Mockito.when(mockConfig.get(Configs.SCHEMA_NAMESPACE_SEPARATOR)).thenReturn(":");
    Mockito.when(mockEnv.config()).thenReturn(mockConfig);

    // Override performAuthorization by subclassing to skip actual authz in unit test
    CreateNamespaceAuthzHandler handler =
        new CreateNamespaceAuthzHandler(params, args) {
          @Override
          public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
            // Only run parent injection; skip performAuthorization
            Namespace ns = ((CreateNamespaceRequest) args[0]).namespace();
            String sep =
                GravitinoEnv.getInstance().config().get(Configs.SCHEMA_NAMESPACE_SEPARATOR);
            NameIdentifier catalogId = nameIdentifierMap.get(EntityType.CATALOG);
            String metalake = catalogId.namespace().level(0);
            String catalog = catalogId.name();
            String fullPath = String.join(sep, ns.levels());
            if (org.apache.gravitino.catalog.HierarchicalSchemaUtil.isNested(fullPath, sep)) {
              String parentPath = fullPath.substring(0, fullPath.lastIndexOf(sep));
              nameIdentifierMap.put(
                  EntityType.SCHEMA,
                  NameIdentifierUtil.ofSchema(metalake, catalog, parentPath));
            }
          }
        };

    try (MockedStatic<GravitinoEnv> mockedStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      mockedStatic.when(GravitinoEnv::getInstance).thenReturn(mockEnv);
      handler.process(map);
    }
    return map;
  }

  @Test
  public void testFlatNamespaceDoesNotInjectParentSchema() throws Exception {
    // For "A" (flat), no SCHEMA should be injected (no parent)
    Map<EntityType, NameIdentifier> map = runProcess(Namespace.of("A"));
    // SCHEMA should not be present — the existing CATALOG-level check applies
    assertEquals(null, map.get(EntityType.SCHEMA));
  }

  @Test
  public void testTwoLevelNamespaceInjectsParentSchema() throws Exception {
    // For "A:B", parent = "A"
    Map<EntityType, NameIdentifier> map = runProcess(Namespace.of("A", "B"));
    NameIdentifier schemaId = map.get(EntityType.SCHEMA);
    assertEquals("A", schemaId.name());
  }

  @Test
  public void testThreeLevelNamespaceInjectsImmediateParent() throws Exception {
    // For "A:B:C", parent = "A:B" (immediate parent, inheritance handles "A" automatically)
    Map<EntityType, NameIdentifier> map = runProcess(Namespace.of("A", "B", "C"));
    NameIdentifier schemaId = map.get(EntityType.SCHEMA);
    assertEquals("A:B", schemaId.name());
  }

  @Test
  public void testMissingCatalogContextThrowsForbidden() throws Exception {
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of("A", "B")).build();
    Parameter[] params = buildParams();
    Object[] args = buildArgs(request);

    Map<EntityType, NameIdentifier> emptyMap = new HashMap<>();

    GravitinoEnv mockEnv = Mockito.mock(GravitinoEnv.class);
    org.apache.gravitino.Config mockConfig = Mockito.mock(org.apache.gravitino.Config.class);
    Mockito.when(mockConfig.get(Configs.SCHEMA_NAMESPACE_SEPARATOR)).thenReturn(":");
    Mockito.when(mockEnv.config()).thenReturn(mockConfig);

    CreateNamespaceAuthzHandler handler = new CreateNamespaceAuthzHandler(params, args);

    try (MockedStatic<GravitinoEnv> mockedStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      mockedStatic.when(GravitinoEnv::getInstance).thenReturn(mockEnv);
      assertThrows(ForbiddenException.class, () -> handler.process(emptyMap));
    }
  }

  /** Minimal test helper providing a CREATE_NAMESPACE annotated method. */
  @SuppressWarnings("unused")
  public static class TestCreateNamespaceOp {
    public void createNamespace(
        @IcebergAuthorizationMetadata(
                type = IcebergAuthorizationMetadata.RequestType.CREATE_NAMESPACE)
            CreateNamespaceRequest request) {}
  }
}
