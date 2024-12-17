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

import com.google.common.collect.Lists;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHDFSPlugin;
import org.apache.gravitino.connector.authorization.ranger.TestRangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestChainAuthorization {
  private static TestCatalog hiveCatalog;
  static AuditInfo auditInfo =
          AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  @BeforeAll
  public static void setUp() {
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
    catalogConf.put(
        ChainAuthorizationProperties.CHAIN_PLUGINS_PROPERTIES_KEY, "hive1,hdfs1");
    catalogConf.put(
        "authorization.chain.hive1.provider", "test-ranger");
    catalogConf.put("authorization.chain.hive1.ranger.auth.type", "simple");
    catalogConf.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    catalogConf.put("authorization.chain.hive1.ranger.username", "admin");
    catalogConf.put("authorization.chain.hive1.ranger.password", "admin");
    catalogConf.put("authorization.chain.hive1.ranger.service.type", "HadoopSQL");
    catalogConf.put("authorization.chain.hive1.ranger.service.name", "hiveDev1");
    catalogConf.put("authorization.chain.hdfs1.provider", "test-ranger");
    catalogConf.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    catalogConf.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    catalogConf.put("authorization.chain.hdfs1.ranger.username", "admin");
    catalogConf.put("authorization.chain.hdfs1.ranger.password", "admin");
    catalogConf.put("authorization.chain.hdfs1.ranger.service.type", "HDFS");
    catalogConf.put("authorization.chain.hdfs1.ranger.service.name", "hiveDev1");

    hiveCatalog =
        new TestCatalog().withCatalogConf(catalogConf).withCatalogEntity(hiveCatalogEntity);
    IsolatedClassLoader isolatedClassLoader =
        new IsolatedClassLoader(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    hiveCatalog.initAuthorizationPluginInstance(isolatedClassLoader);
  }

  public RoleEntity mock3TableRole(String roleName) {
    SecurableObject securableObject1 =
            SecurableObjects.parse(
                    String.format("catalog.%s", roleName), // use unique db name to avoid conflict
                    MetadataObject.Type.SCHEMA,
                    Lists.newArrayList(Privileges.CreateTable.allow()));

    SecurableObject securableObject2 =
            SecurableObjects.parse(
                    String.format("catalog.%s.tab2", roleName),
                    SecurableObject.Type.TABLE,
                    Lists.newArrayList(Privileges.SelectTable.allow()));

    SecurableObject securableObject3 =
            SecurableObjects.parse(
                    String.format("catalog.%s.tab3", roleName),
                    SecurableObject.Type.TABLE,
                    Lists.newArrayList(Privileges.ModifyTable.allow()));

    return RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                    Lists.newArrayList(securableObject1, securableObject2, securableObject3))
            .build();
  }

  @Test
  public void testChainAuthorization() {
    AuthorizationPlugin authPlugin = hiveCatalog.getAuthorizationPlugin();
    Assertions.assertInstanceOf(ChainAuthorizationPlugin.class, authPlugin);
    ChainAuthorizationPlugin chainAuthPlugin = (ChainAuthorizationPlugin) authPlugin;
    Assertions.assertEquals(2, chainAuthPlugin.getPlugins().size());

    chainAuthPlugin
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

    RoleEntity roleEntity = mock3TableRole("role1");
    chainAuthPlugin.onRoleCreated(roleEntity);

    chainAuthPlugin
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
