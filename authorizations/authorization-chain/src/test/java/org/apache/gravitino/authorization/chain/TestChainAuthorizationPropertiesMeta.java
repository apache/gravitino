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

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.authorization.AuthorizationPluginProvider;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestChainAuthorizationPropertiesMeta {
  @Test
  void testChainHiveCatalog() {
    String pluginName = "hive1";
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.METASTORE_URIS, "thrift://localhost:9083");
    properties.put("gravitino.bypass.hive.metastore.client.capability.check", "true");
    properties.put(IMPERSONATION_ENABLE, "true");
    properties.put(AUTHORIZATION_PROVIDER, AuthorizationPluginProvider.Type.Chain.getName());
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, pluginName);
    properties.put(
        AuthorizationPropertiesMeta.generateChainPluginsKey(
            pluginName, AuthorizationPropertiesMeta.getChainProviderKey()),
        AuthorizationPluginProvider.Type.Chain.getName());
    properties.put(
        AuthorizationPropertiesMeta.generateChainPluginsKey(
            pluginName, AuthorizationPropertiesMeta.getRangerAuthTypeKey()),
        RangerContainer.authType);
    properties.put(
        AuthorizationPropertiesMeta.generateChainPluginsKey(
            pluginName, AuthorizationPropertiesMeta.getRangerAdminUrlKey()),
        "http://localhost:" + RangerContainer.RANGER_SERVER_PORT);
    properties.put(
        AuthorizationPropertiesMeta.generateChainPluginsKey(
            pluginName, AuthorizationPropertiesMeta.getRangerUsernameKey()),
        RangerContainer.rangerUserName);
    properties.put(
        AuthorizationPropertiesMeta.generateChainPluginsKey(
            pluginName, AuthorizationPropertiesMeta.getRangerPasswordKey()),
        RangerContainer.rangerPassword);
    properties.put(
        AuthorizationPropertiesMeta.generateChainPluginsKey(
            pluginName, AuthorizationPropertiesMeta.getRangerServiceNameKey()),
        RangerITEnv.RANGER_HIVE_REPO_NAME);
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsOne() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsTwo() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsHasSpace() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1, hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsOneButHasTowPluginConfig() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsHasPoint() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug.1, hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.plug.1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug.1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug.1.ranger.username", "admin");
    properties.put("authorization.chain.plug.1.ranger.password", "admin");
    properties.put("authorization.chain.plug.1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginErrorPluginName() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.plug3.ranger.service.name", "hdfsDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginDuplicationPluginName() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1,hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.types", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginErrorPropertyKey() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "hive1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger-error.auth.types", "simple");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }
}
