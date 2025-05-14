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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test of {@link MetadataFilterHelper} */
public class TestMetadataFilterHelper {

  @Test
  public void testFilter() {
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
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
          MetadataFilterHelper.filter(
              "testMetalake",
              MetadataObject.Type.SCHEMA,
              Privilege.Name.USE_SCHEMA.name(),
              nameIdentifiers);
      Assertions.assertEquals(2, filtered.length);
      Assertions.assertEquals("testMetalake.testCatalog.testSchema", filtered[0].toString());
      Assertions.assertEquals("testMetalake.testCatalog2.testSchema", filtered[1].toString());
    }
  }

  @Test
  public void testFilterByExpression() {
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
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
          MetadataFilterHelper.filterByExpression(
              "testMetalake",
              "CATALOG::USE_CATALOG && SCHEMA::USE_SCHEMA",
              MetadataObject.Type.SCHEMA,
              nameIdentifiers);
      Assertions.assertEquals(1, filtered.length);
      Assertions.assertEquals("testMetalake.testCatalog.testSchema", filtered[0].toString());
      NameIdentifier[] filtered2 =
          MetadataFilterHelper.filterByExpression(
              "testMetalake", "CATALOG::USE_CATALOG", MetadataObject.Type.SCHEMA, nameIdentifiers);
      Assertions.assertEquals(2, filtered2.length);
      Assertions.assertEquals("testMetalake.testCatalog.testSchema", filtered2[0].toString());
      Assertions.assertEquals("testMetalake.testCatalog.testSchema2", filtered2[1].toString());
    }
  }
}
