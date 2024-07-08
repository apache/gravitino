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
package com.datastrato.gravitino.connector.authorization;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.TestCatalog;
import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.authorization.authorization1.TestAuthorizationPlugin1;
import com.datastrato.gravitino.connector.authorization.authorization2.TestAuthorizationPlugin2;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAuthorization {
  private static TestCatalog testCatalog1;
  private static TestCatalog testCatalog2;

  @BeforeAll
  public static void setUp() throws Exception {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    CatalogEntity catalogTest1 =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog-test1")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(BaseCatalog.AUTHORIZATION_IMPL, "test1"))
            .withProvider("test")
            .withAuditInfo(auditInfo)
            .build();

    testCatalog1 =
        new TestCatalog().withCatalogConf(ImmutableMap.of()).withCatalogEntity(catalogTest1);

    CatalogEntity catalogTest2 =
        CatalogEntity.builder()
            .withId(2L)
            .withName("catalog-test2")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProperties(ImmutableMap.of(BaseCatalog.AUTHORIZATION_IMPL, "test2"))
            .withProvider("test")
            .withAuditInfo(auditInfo)
            .build();

    testCatalog2 =
        new TestCatalog().withCatalogConf(ImmutableMap.of()).withCatalogEntity(catalogTest2);
  }

  @Test
  public void testAuthorizationCatalog1() {
    AuthorizationPlugin authPlugin1 = testCatalog1.getAuthorizationPlugin();
    Assertions.assertInstanceOf(TestAuthorizationPlugin1.class, authPlugin1);
    TestAuthorizationPlugin1 testAuthOps1 = (TestAuthorizationPlugin1) authPlugin1;
    Assertions.assertFalse(testAuthOps1.callOnCreateRole1);
    authPlugin1.onCreateRole(null, null);
    Assertions.assertTrue(testAuthOps1.callOnCreateRole1);
  }

  @Test
  public void testAuthorizationCatalog2() {
    AuthorizationPlugin authPlugin2 = testCatalog2.getAuthorizationPlugin();
    Assertions.assertInstanceOf(TestAuthorizationPlugin2.class, authPlugin2);
    TestAuthorizationPlugin2 testAuthOps2 = (TestAuthorizationPlugin2) authPlugin2;
    Assertions.assertFalse(testAuthOps2.callOnCreateRole2);
    authPlugin2.onCreateRole(null, null);
    Assertions.assertTrue(testAuthOps2.callOnCreateRole2);
  }
}
