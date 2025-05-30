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

package org.apache.gravitino.server.authorization.expression;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.MockGravitinoAuthorizer;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test for {@link AuthorizationExpressionEvaluator} */
public class TestAuthorizationExpressionEvaluator {

  @Test
  public void testEvaluator() {
    String expression =
        "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA && (TABLE::SELECT_TABLE || TABLE::MODIFY_TABLE)";
    AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
        new AuthorizationExpressionEvaluator(expression);
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());
      Map<Entity.EntityType, NameIdentifier> metadataNames = new HashMap<>();
      metadataNames.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("testMetalake"));
      metadataNames.put(
          Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog("testMetalake", "testCatalog"));
      metadataNames.put(
          Entity.EntityType.SCHEMA,
          NameIdentifierUtil.ofSchema("testMetalake", "testCatalog", "testSchema"));
      metadataNames.put(
          Entity.EntityType.TABLE,
          NameIdentifierUtil.ofTable(
              "testMetalake", "testCatalog", "testSchema", "testTableHasNotPermission"));
      Assertions.assertFalse(authorizationExpressionEvaluator.evaluate(metadataNames));
      metadataNames.put(
          Entity.EntityType.TABLE,
          NameIdentifierUtil.ofTable("testMetalake", "testCatalog", "testSchema", "testTable"));
      Assertions.assertTrue(authorizationExpressionEvaluator.evaluate(metadataNames));
    }
  }

  @Test
  public void testEvaluatorWithOwner() {
    String expression = "METALAKE::OWNER || CATALOG::CREATE_CATALOG";
    AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
        new AuthorizationExpressionEvaluator(expression);
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());
      Map<Entity.EntityType, NameIdentifier> metadataNames = new HashMap<>();
      metadataNames.put(
          Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("metalakeWithOutOwner"));
      metadataNames.put(
          Entity.EntityType.CATALOG,
          NameIdentifierUtil.ofCatalog("metalakeWithOwner", "testCatalog"));
      Assertions.assertFalse(authorizationExpressionEvaluator.evaluate(metadataNames));
      metadataNames.put(
          Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("metalakeWithOwner"));
      Assertions.assertTrue(authorizationExpressionEvaluator.evaluate(metadataNames));
    }
  }
}
