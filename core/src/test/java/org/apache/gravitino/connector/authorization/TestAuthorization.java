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
import java.util.Collections;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHDFS;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHDFSPlugin;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHadoopSQL;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAuthorization {
  private static TestCatalog hiveCatalog;
  private static TestCatalog filesetCatalog;

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
            .withCatalogConf(
                ImmutableMap.of(
                    Catalog.AUTHORIZATION_PROVIDER, TestRangerAuthorizationHadoopSQL.SHORT_NAME))
            .withCatalogEntity(hiveCatalogEntity);
    IsolatedClassLoader isolatedClassLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    hiveCatalog.initAuthorizationPluginInstance(isolatedClassLoader);

    CatalogEntity filesetEntity =
        CatalogEntity.builder()
            .withId(2L)
            .withName("catalog-test2")
            .withNamespace(Namespace.of("default"))
            .withType(Catalog.Type.FILESET)
            .withProvider("test")
            .withAuditInfo(auditInfo)
            .build();

    filesetCatalog =
        new TestCatalog()
            .withCatalogConf(
                ImmutableMap.of(
                    Catalog.AUTHORIZATION_PROVIDER, TestRangerAuthorizationHDFS.SHORT_NAME))
            .withCatalogEntity(filesetEntity);
    filesetCatalog.initAuthorizationPluginInstance(isolatedClassLoader);
  }

  @Test
  public void testRangerHadoopSQLAuthorization() {
    AuthorizationPlugin rangerHiveAuthPlugin = hiveCatalog.getAuthorizationPlugin();
    Assertions.assertInstanceOf(TestRangerAuthorizationHadoopSQLPlugin.class, rangerHiveAuthPlugin);
    TestRangerAuthorizationHadoopSQLPlugin testRangerAuthHadoopSQLPlugin =
        (TestRangerAuthorizationHadoopSQLPlugin) rangerHiveAuthPlugin;
    Assertions.assertFalse(testRangerAuthHadoopSQLPlugin.callOnCreateRole1);
    rangerHiveAuthPlugin.onRoleCreated(null);
    Assertions.assertTrue(testRangerAuthHadoopSQLPlugin.callOnCreateRole1);
  }

  @Test
  public void testRangerHDFSAuthorization() {
    AuthorizationPlugin rangerHDFSAuthPlugin = filesetCatalog.getAuthorizationPlugin();
    Assertions.assertInstanceOf(TestRangerAuthorizationHDFSPlugin.class, rangerHDFSAuthPlugin);
    TestRangerAuthorizationHDFSPlugin testRangerAuthHDFSPlugin =
        (TestRangerAuthorizationHDFSPlugin) rangerHDFSAuthPlugin;
    Assertions.assertFalse(testRangerAuthHDFSPlugin.callOnCreateRole2);
    rangerHDFSAuthPlugin.onRoleCreated(null);
    Assertions.assertTrue(testRangerAuthHDFSPlugin.callOnCreateRole2);
  }
}
