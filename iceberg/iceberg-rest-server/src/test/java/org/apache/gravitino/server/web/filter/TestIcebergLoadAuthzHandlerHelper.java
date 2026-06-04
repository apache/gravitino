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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.web.filter.IcebergLoadAuthzHandlerHelper.LoadTarget;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test for {@link IcebergLoadAuthzHandlerHelper}. */
public class TestIcebergLoadAuthzHandlerHelper {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String NAME = "probe_target";

  // Models the loadView probe Spark issues before falling back to loadTable: the primary
  // expression requires view ownership, while allowCheckExistence lets a table owner probe. The
  // three branches of authorizeLoadEntity decide whether such a caller sees the entity, a 404, or
  // a 403.
  private static final String PRIMARY_EXPRESSION = "VIEW::OWNER";
  private static final String ALLOW_CHECK_EXISTENCE_EXPRESSION = "TABLE::OWNER";

  @Test
  public void testPrimaryGrantAllows() {
    // View owner: primary grants regardless of existence, so the load is authorized.
    assertDoesNotThrow(
        () -> runAuthorizeLoadEntity(true, false, false),
        "view owner must be authorized even when the existence check would deny");
  }

  @Test
  public void testAbsentProbeNotFound() {
    // Table owner probing a view that does not exist: primary denies, existence check allows, and
    // the view is absent, so the caller gets 404 instead of 403 (the bug this PR fixes, which lets
    // Spark fall back to loadTable).
    assertThrows(NoSuchViewException.class, () -> runAuthorizeLoadEntity(false, true, false));
  }

  @Test
  public void testPresentProbeForbidden() {
    // Table owner probing a view that actually exists: the existence check must not leak access to
    // a real view the caller has no privilege on, so it stays 403.
    assertThrows(ForbiddenException.class, () -> runAuthorizeLoadEntity(false, true, true));
  }

  @Test
  public void testNoAccessForbidden() {
    // Caller satisfies neither expression: denied with 403 without probing existence.
    assertThrows(ForbiddenException.class, () -> runAuthorizeLoadEntity(false, false, false));
  }

  private void runAuthorizeLoadEntity(
      boolean primaryGranted, boolean existenceCheckGranted, boolean exists) {
    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> providerStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtils
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");

      GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
      when(authorizer.isOwner(
              any(),
              any(),
              argThat(object -> object != null && object.type() == MetadataObject.Type.VIEW),
              any()))
          .thenReturn(primaryGranted);
      when(authorizer.isOwner(
              any(),
              any(),
              argThat(object -> object != null && object.type() == MetadataObject.Type.TABLE),
              any()))
          .thenReturn(existenceCheckGranted);

      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);

      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
      nameIdentifierMap.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(METALAKE));
      nameIdentifierMap.put(
          Entity.EntityType.VIEW, NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, NAME));
      nameIdentifierMap.put(
          Entity.EntityType.TABLE, NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, NAME));

      BooleanSupplier existsSupplier = () -> exists;
      IcebergLoadAuthzHandlerHelper.authorizeLoadEntity(
          nameIdentifierMap,
          PRIMARY_EXPRESSION,
          ALLOW_CHECK_EXISTENCE_EXPRESSION,
          existsSupplier,
          () -> new NoSuchViewException("View %s not found", NAME),
          "view",
          NameIdentifierUtil.ofView(METALAKE, CATALOG, SCHEMA, NAME));
    }
  }

  @Test
  public void testExtractLoadTarget() throws Exception {
    Method method =
        TestOperations.class.getMethod("loadTable", String.class, String.class, String.class);

    LoadTarget loadTarget =
        IcebergLoadAuthzHandlerHelper.extractLoadTarget(
            method.getParameters(),
            new Object[] {"test_catalog/", "test_schema", "orders%20table"},
            IcebergAuthorizationMetadata.RequestType.LOAD_TABLE);

    assertEquals("orders table", loadTarget.name());
    assertEquals(Namespace.of("test_schema"), loadTarget.namespace());
  }

  @Test
  public void testResolveExpression() throws Exception {
    Method method =
        TestOperations.class.getMethod("loadTable", String.class, String.class, String.class);
    AuthorizationExpression authorizationExpression =
        method.getAnnotation(AuthorizationExpression.class);

    assertEquals(
        "custom_primary",
        IcebergLoadAuthzHandlerHelper.resolveExpression(
            authorizationExpression, "default_primary"));
    assertEquals(
        "custom_existence",
        IcebergLoadAuthzHandlerHelper.resolveAllowCheckExistenceExpression(
            authorizationExpression, "default_existence"));
    assertEquals(
        "default_primary",
        IcebergLoadAuthzHandlerHelper.resolveExpression(null, "default_primary"));
    assertEquals(
        "default_existence",
        IcebergLoadAuthzHandlerHelper.resolveAllowCheckExistenceExpression(
            null, "default_existence"));
  }

  @SuppressWarnings("unused")
  public static class TestOperations {
    @AuthorizationExpression(
        expression = "custom_primary",
        allowCheckExistence = "custom_existence",
        accessMetadataType = MetadataObject.Type.TABLE)
    public void loadTable(
        @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String prefix,
        @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String namespace,
        @IcebergAuthorizationMetadata(type = IcebergAuthorizationMetadata.RequestType.LOAD_TABLE)
            @AuthorizationMetadata(type = Entity.EntityType.TABLE)
            String table) {}
  }
}
