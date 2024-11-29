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
package org.apache.gravitino.authorization.chain;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHDFSPlugin;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestChainAuthorization {
  private static TestCatalog hiveCatalog;

  @BeforeAll
  public static void setUp() {
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

    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(Catalog.AUTHORIZATION_PROVIDER, "chain");
    catalogConf.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "ranger1,mysql1");
    catalogConf.put("authorization.chain.ranger1.provider", "test_ranger");
    catalogConf.put("authorization.chain.ranger1.ranger.auth.types", "simple");
    catalogConf.put("authorization.chain.ranger1.ranger.admin.url", "http://localhost:6080");
    catalogConf.put("authorization.chain.ranger1.ranger.username", "admin");
    catalogConf.put("authorization.chain.ranger1.ranger.password", "admin");
    catalogConf.put("authorization.chain.ranger1.ranger.service.name", "hiveDev1");
    catalogConf.put("authorization.chain.mysql1.provider", "test_mysql");

    hiveCatalog =
        new TestCatalog().withCatalogConf(catalogConf).withCatalogEntity(hiveCatalogEntity);
    IsolatedClassLoader isolatedClassLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    hiveCatalog.initAuthorizationPluginInstance(isolatedClassLoader);
  }

  @Test
  public void testChainAuthorization() {
    AuthorizationPlugin chainAuthPlugin = hiveCatalog.getAuthorizationPlugin();
    Assertions.assertInstanceOf(ChainAuthorizationPlugin.class, chainAuthPlugin);
    ChainAuthorizationPlugin chainAuthorizationPlugin = (ChainAuthorizationPlugin) chainAuthPlugin;

    chainAuthorizationPlugin
        .getPlugins()
        .forEach(
            plugin -> {
              if (plugin instanceof TestRangerAuthorizationHadoopSQLPlugin) {
                Assertions.assertFalse(
                    ((TestRangerAuthorizationHadoopSQLPlugin) plugin).callOnCreateRole1);
              } else if (plugin instanceof TestRangerAuthorizationHDFSPlugin) {
                Assertions.assertFalse(
                    ((TestRangerAuthorizationHDFSPlugin) plugin).callOnCreateRole2);
              }
            });

    chainAuthPlugin.onRoleCreated(null);

    chainAuthorizationPlugin
        .getPlugins()
        .forEach(
            plugin -> {
              if (plugin instanceof TestRangerAuthorizationHadoopSQLPlugin) {
                Assertions.assertTrue(
                    ((TestRangerAuthorizationHadoopSQLPlugin) plugin).callOnCreateRole1);
              } else if (plugin instanceof TestRangerAuthorizationHDFSPlugin) {
                Assertions.assertTrue(
                    ((TestRangerAuthorizationHDFSPlugin) plugin).callOnCreateRole2);
              }
            });
  }
}
