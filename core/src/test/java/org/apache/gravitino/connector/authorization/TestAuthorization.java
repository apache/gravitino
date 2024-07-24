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
package org.apache.gravitino.connector.authorization;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.authorization.mysql.TestMySQLAuthorizationPlugin;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationPlugin;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAuthorization {
  private static TestCatalog hiveCatalog;
  private static TestCatalog mySQLCatalog;

  @BeforeAll
  public static void setUp() throws Exception {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    CatalogEntity hiveCatalogEntity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog-test1")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(auditInfo)
            .build();

    hiveCatalog =
        new TestCatalog()
            .withCatalogConf(ImmutableMap.of(Catalog.AUTHORIZATION_PROVIDER, "ranger"))
            .withCatalogEntity(hiveCatalogEntity);

    CatalogEntity mySQLEntity =
        CatalogEntity.builder()
            .withId(2L)
            .withName("catalog-test2")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(auditInfo)
            .build();

    mySQLCatalog =
        new TestCatalog()
            .withCatalogConf(ImmutableMap.of(Catalog.AUTHORIZATION_PROVIDER, "mysql"))
            .withCatalogEntity(mySQLEntity);
  }

  @Test
  public void testRangerAuthorization() {
    AuthorizationPlugin rangerAuthPlugin = hiveCatalog.getAuthorizationPlugin();
    Assertions.assertInstanceOf(TestRangerAuthorizationPlugin.class, rangerAuthPlugin);
    TestRangerAuthorizationPlugin testRangerAuthPlugin =
        (TestRangerAuthorizationPlugin) rangerAuthPlugin;
    Assertions.assertFalse(testRangerAuthPlugin.callOnCreateRole1);
    rangerAuthPlugin.onRoleCreated(null);
    Assertions.assertTrue(testRangerAuthPlugin.callOnCreateRole1);
  }

  @Test
  public void testMySQLAuthorization() {
    AuthorizationPlugin mySQLAuthPlugin = mySQLCatalog.getAuthorizationPlugin();
    Assertions.assertInstanceOf(TestMySQLAuthorizationPlugin.class, mySQLAuthPlugin);
    TestMySQLAuthorizationPlugin testMySQLAuthPlugin =
        (TestMySQLAuthorizationPlugin) mySQLAuthPlugin;
    Assertions.assertFalse(testMySQLAuthPlugin.callOnCreateRole2);
    mySQLAuthPlugin.onRoleCreated(null);
    Assertions.assertTrue(testMySQLAuthPlugin.callOnCreateRole2);
  }
}
