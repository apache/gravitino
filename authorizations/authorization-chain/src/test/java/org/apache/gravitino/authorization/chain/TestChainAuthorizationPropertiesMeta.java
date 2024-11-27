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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestChainAuthorizationPropertiesMeta {
  @Test
  void testWildcardPropertyChainPluginsOne() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug1");
    properties.put("authorization.chain.plug1.provider", "ranger");
    properties.put("authorization.chain.plug1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug1.ranger.username", "admin");
    properties.put("authorization.chain.plug1.ranger.password", "admin");
    properties.put("authorization.chain.plug1.ranger.service.name", "hiveDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsTwo() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug1,plug2");
    properties.put("authorization.chain.plug1.provider", "ranger");
    properties.put("authorization.chain.plug1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug1.ranger.username", "admin");
    properties.put("authorization.chain.plug1.ranger.password", "admin");
    properties.put("authorization.chain.plug1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.plug2.provider", "ranger");
    properties.put("authorization.chain.plug2.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug2.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug2.ranger.username", "admin");
    properties.put("authorization.chain.plug2.ranger.password", "admin");
    properties.put("authorization.chain.plug2.ranger.service.name", "hiveDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsHasSpace() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug1, plug2");
    properties.put("authorization.chain.plug1.provider", "ranger");
    properties.put("authorization.chain.plug1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug1.ranger.username", "admin");
    properties.put("authorization.chain.plug1.ranger.password", "admin");
    properties.put("authorization.chain.plug1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.plug2.provider", "ranger");
    properties.put("authorization.chain.plug2.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug2.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug2.ranger.username", "admin");
    properties.put("authorization.chain.plug2.ranger.password", "admin");
    properties.put("authorization.chain.plug2.ranger.service.name", "hiveDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }

  @Test
  void testWildcardPropertyChainPluginsOneButHasTowPluginConfig() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug1");
    properties.put("authorization.chain.plug1.provider", "ranger");
    properties.put("authorization.chain.plug1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug1.ranger.username", "admin");
    properties.put("authorization.chain.plug1.ranger.password", "admin");
    properties.put("authorization.chain.plug1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.plug2.provider", "ranger");
    properties.put("authorization.chain.plug2.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug2.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug2.ranger.username", "admin");
    properties.put("authorization.chain.plug2.ranger.password", "admin");
    properties.put("authorization.chain.plug2.ranger.service.name", "hiveDev");
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
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug.1, plug2");
    properties.put("authorization.chain.plug1.provider", "ranger");
    properties.put("authorization.chain.plug.1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug.1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug.1.ranger.username", "admin");
    properties.put("authorization.chain.plug.1.ranger.password", "admin");
    properties.put("authorization.chain.plug.1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.plug2.provider", "ranger");
    properties.put("authorization.chain.plug2.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug2.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug2.ranger.username", "admin");
    properties.put("authorization.chain.plug2.ranger.password", "admin");
    properties.put("authorization.chain.plug2.ranger.service.name", "hiveDev");
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
    properties.put(AuthorizationPropertiesMeta.CHAIN_PLUGINS, "plug1,plug2");
    properties.put("authorization.chain.plug1.provider", "ranger");
    properties.put("authorization.chain.plug1.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug1.ranger.username", "admin");
    properties.put("authorization.chain.plug1.ranger.password", "admin");
    properties.put("authorization.chain.plug1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.plug2.provider", "ranger");
    properties.put("authorization.chain.plug2.ranger.auth.types", "simple");
    properties.put("authorization.chain.plug2.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.plug2.ranger.username", "admin");
    properties.put("authorization.chain.plug2.ranger.password", "admin");
    properties.put("authorization.chain.plug3.ranger.service.name", "hiveDev");
    PropertiesMetadata authorizationPropertiesMeta = new AuthorizationPropertiesMeta();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                authorizationPropertiesMeta, properties));
  }
}
