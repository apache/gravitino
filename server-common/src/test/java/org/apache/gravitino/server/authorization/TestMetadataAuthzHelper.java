/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.Executor;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test of {@link MetadataAuthzHelper} */
public class TestMetadataAuthzHelper {

  private static MockedStatic<GravitinoEnv> mockedStaticGravitinoEnv;

  @BeforeAll
  public static void setup() {
    mockedStaticGravitinoEnv = mockStatic(GravitinoEnv.class);
    GravitinoEnv gravitinoEnv = mock(GravitinoEnv.class);
    mockedStaticGravitinoEnv.when(GravitinoEnv::getInstance).thenReturn(gravitinoEnv);
    Config configMock = mock(Config.class);
    when(gravitinoEnv.config()).thenReturn(configMock);
    when(configMock.get(eq(Configs.ENABLE_AUTHORIZATION))).thenReturn(true);
  }

  @AfterAll
  public static void stop() {
    if (mockedStaticGravitinoEnv != null) {
      mockedStaticGravitinoEnv.close();
    }
  }

  @Test
  public void testFilterByExpression() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();

      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());
      NameIdentifier[] nameIdentifiers = new NameIdentifier[3];
      nameIdentifiers[0] = NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "testSchema");
      nameIdentifiers[1] =
          NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "testSchema2");
      nameIdentifiers[2] =
          NameIdentifierUtil.ofSchema("testMetalake", "testCatalog2", "testSchema");
      NameIdentifier[] filtered =
          MetadataAuthzHelper.filterByExpression(
              "testMetalake",
              "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA",
              Entity.EntityType.SCHEMA,
              nameIdentifiers);
      Assertions.assertEquals(1, filtered.length);
      Assertions.assertEquals("testMetalake.testCatalog.testSchema", filtered[0].toString());
      NameIdentifier[] filtered2 =
          MetadataAuthzHelper.filterByExpression(
              "testMetalake", "CATALOG::USE_CATALOG", Entity.EntityType.SCHEMA, nameIdentifiers);
      Assertions.assertEquals(2, filtered2.length);
      Assertions.assertEquals("testMetalake.testCatalog.testSchema", filtered2[0].toString());
      Assertions.assertEquals("testMetalake.testCatalog.testSchema2", filtered2[1].toString());
    }
  }

  @Test
  public void testFilterMetadataObject() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();

      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());

      // Create test MetadataObjectDTO instances
      MetadataObjectDTO[] metadataObjects = new MetadataObjectDTO[3];
      metadataObjects[0] =
          MetadataObjectDTO.builder()
              .withName("testSchema")
              .withParent("testCatalog")
              .withType(MetadataObject.Type.SCHEMA)
              .build();
      metadataObjects[1] =
          MetadataObjectDTO.builder()
              .withName("testSchema2")
              .withParent("testCatalog")
              .withType(MetadataObject.Type.SCHEMA)
              .build();
      metadataObjects[2] =
          MetadataObjectDTO.builder()
              .withName("testSchema3")
              .withParent("testCatalog2")
              .withType(MetadataObject.Type.SCHEMA)
              .build();

      MetadataObjectDTO[] filtered =
          MetadataAuthzHelper.filterMetadataObject("testMetalake", metadataObjects);

      // Based on the MockGravitinoAuthorizer, 2 of the 3 schemas should be accessible
      Assertions.assertEquals(1, filtered.length);
      Assertions.assertEquals("testSchema", filtered[0].name());
    }
  }

  @Test
  public void testFilterMetadataObjectDTO() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();

      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());

      // Create test MetadataObjectDTO instances
      MetadataObjectDTO[] metadataObjects = new MetadataObjectDTO[3];
      metadataObjects[0] =
          MetadataObjectDTO.builder()
              .withName("testSchema")
              .withParent("testCatalog")
              .withType(MetadataObject.Type.SCHEMA)
              .build();
      metadataObjects[1] =
          MetadataObjectDTO.builder()
              .withName("testSchema2")
              .withParent("testCatalog")
              .withType(MetadataObject.Type.SCHEMA)
              .build();
      metadataObjects[2] =
          MetadataObjectDTO.builder()
              .withName("testSchema3")
              .withParent("testCatalog")
              .withType(MetadataObject.Type.SCHEMA)
              .build();

      MetadataObjectDTO[] filtered =
          MetadataAuthzHelper.filterMetadataObject("testMetalake", metadataObjects);

      // Based on the MockGravitinoAuthorizer, 2 of the 3 tables should be accessible
      Assertions.assertEquals(1, filtered.length);
      Assertions.assertEquals("testSchema", filtered[0].name());
    }
  }

  /**
   * Builds three table identifiers under the same schema, where a parent (schema) level
   * SELECT_TABLE grant exists and the middle table additionally carries a table-level deny. The
   * deny gate is controlled separately so each branch of the list short-circuit can be exercised.
   */
  private GravitinoAuthorizer mockTableListAuthorizer(boolean owner, boolean denyGate) {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    // Parent-scope SELECT_TABLE grant at the schema level (applies to every table in the schema).
    lenient()
        .when(authorizer.authorize(any(), eq("testMetalake"), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              Privilege.Name privilege = invocation.getArgument(3);
              return object.type() == MetadataObject.Type.SCHEMA
                  && privilege == Privilege.Name.SELECT_TABLE;
            });
    // The middle table (t2) has a table-level deny on SELECT_TABLE.
    lenient()
        .when(authorizer.deny(any(), eq("testMetalake"), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              Privilege.Name privilege = invocation.getArgument(3);
              return object.type() == MetadataObject.Type.TABLE
                  && "t2".equals(object.name())
                  && privilege == Privilege.Name.SELECT_TABLE;
            });
    lenient().when(authorizer.isOwner(any(), eq("testMetalake"), any(), any())).thenReturn(owner);
    lenient()
        .when(authorizer.hasDenyPolicy(any(), eq("testMetalake"), anySet(), any()))
        .thenReturn(denyGate);
    return authorizer;
  }

  /**
   * Builds an authorizer that grants {@code grantPrivilege} at a single ancestor scope ({@code
   * grantType}) and reports no object-level deny, so a list of children under that ancestor is
   * fully visible via the parent-scope short-circuit.
   */
  private GravitinoAuthorizer mockParentGrantAuthorizer(
      MetadataObject.Type grantType, Privilege.Name grantPrivilege) {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    lenient()
        .when(authorizer.authorize(any(), eq("testMetalake"), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              Privilege.Name privilege = invocation.getArgument(3);
              return object.type() == grantType && privilege == grantPrivilege;
            });
    lenient()
        .when(authorizer.deny(any(), eq("testMetalake"), any(), any(), any()))
        .thenReturn(false);
    lenient().when(authorizer.isOwner(any(), eq("testMetalake"), any(), any())).thenReturn(false);
    lenient()
        .when(authorizer.hasDenyPolicy(any(), eq("testMetalake"), anySet(), any()))
        .thenReturn(false);
    return authorizer;
  }

  @Test
  public void testListShortCircuitSchemaViaCatalogGrant() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      GravitinoAuthorizer authorizer =
          mockParentGrantAuthorizer(MetadataObject.Type.CATALOG, Privilege.Name.USE_SCHEMA);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(authorizer);

      NameIdentifier[] schemas =
          new NameIdentifier[] {
            NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "s1"),
            NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "s2")
          };
      NameIdentifier[] filtered =
          MetadataAuthzHelper.filterByExpression(
              "testMetalake",
              AuthorizationExpressionConstants.FILTER_SCHEMA_AUTHORIZATION_EXPRESSION,
              Entity.EntityType.SCHEMA,
              schemas);

      Assertions.assertEquals(2, filtered.length);
    }
  }

  @Test
  public void testListShortCircuitCatalogViaMetalakeGrant() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      GravitinoAuthorizer authorizer =
          mockParentGrantAuthorizer(MetadataObject.Type.METALAKE, Privilege.Name.USE_CATALOG);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(authorizer);

      NameIdentifier[] catalogs =
          new NameIdentifier[] {
            NameIdentifierUtil.ofCatalog("testMetalake", "c1"),
            NameIdentifierUtil.ofCatalog("testMetalake", "c2")
          };
      NameIdentifier[] filtered =
          MetadataAuthzHelper.filterByExpression(
              "testMetalake",
              AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION,
              Entity.EntityType.CATALOG,
              catalogs);

      Assertions.assertEquals(2, filtered.length);
    }
  }

  private static NameIdentifier[] threeTables() {
    return new NameIdentifier[] {
      NameIdentifierUtil.ofTable("testMetalake", "testCatalog", "testSchema", "t1"),
      NameIdentifierUtil.ofTable("testMetalake", "testCatalog", "testSchema", "t2"),
      NameIdentifierUtil.ofTable("testMetalake", "testCatalog", "testSchema", "t3")
    };
  }

  @Test
  public void testListShortCircuitParentGrantWithoutDenyReturnsAll() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      GravitinoAuthorizer authorizer = mockTableListAuthorizer(false, false);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(authorizer);

      NameIdentifier[] filtered =
          MetadataAuthzHelper.filterByExpression(
              "testMetalake",
              AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION,
              Entity.EntityType.TABLE,
              threeTables());

      // The schema-level grant covers every table and no object-level deny is reported, so the
      // whole list is returned without consulting the per-table deny on t2.
      Assertions.assertEquals(3, filtered.length);
    }
  }

  @Test
  public void testListShortCircuitFallsBackWhenDenyMayExist() {
    makeCompletableFutureUseCurrentThread();
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      GravitinoAuthorizer authorizer = mockTableListAuthorizer(false, true);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(authorizer);

      NameIdentifier[] filtered =
          MetadataAuthzHelper.filterByExpression(
              "testMetalake",
              AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION,
              Entity.EntityType.TABLE,
              threeTables());

      // A deny may exist, so the short-circuit is disabled and per-table filtering excludes t2.
      Assertions.assertEquals(2, filtered.length);
      Assertions.assertTrue(
          Arrays.stream(filtered).noneMatch(id -> "t2".equals(id.name())),
          "t2 must be filtered out by its table-level deny");
    }
  }

  private static void makeCompletableFutureUseCurrentThread() {
    try {
      Executor currentThread = Runnable::run;
      Class<MetadataAuthzHelper> jcasbinAuthorizerClass = MetadataAuthzHelper.class;
      Field field = jcasbinAuthorizerClass.getDeclaredField("executor");
      field.setAccessible(true);
      field.set(null, currentThread);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
