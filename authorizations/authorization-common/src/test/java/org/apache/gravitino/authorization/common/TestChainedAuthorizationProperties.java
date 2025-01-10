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
package org.apache.gravitino.authorization.common;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestChainedAuthorizationProperties {
  static final String METASTORE_URIS = "metastore.uris";
  public static final String IMPERSONATION_ENABLE = "impersonation-enable";

  @Test
  void testChainedOnePlugin() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "hive1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertDoesNotThrow(() -> chainedAuthzProperties.validate());
  }

  @Test
  void testChainedTwoPlugins() {
    Map<String, String> properties = new HashMap<>();
    properties.put(METASTORE_URIS, "thrift://localhost:9083");
    properties.put("gravitino.bypass.hive.metastore.client.capability.check", "true");
    properties.put(IMPERSONATION_ENABLE, "true");
    properties.put(AUTHORIZATION_PROVIDER, "chain");
    properties.put("authorization.chain.plugins", "hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertDoesNotThrow(() -> chainedAuthzProperties.validate());
  }

  @Test
  void testWithoutPlugins() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> chainedAuthzProperties.validate());
  }

  @Test
  void testPluginsHasSpace() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "hive1, hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertDoesNotThrow(() -> chainedAuthzProperties.validate());
  }

  @Test
  void testPluginsOneButHasTowPluginConfig() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "hive1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> chainedAuthzProperties.validate());
  }

  @Test
  void testPluginsHasPoint() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "hive.1,hdfs1");
    properties.put("authorization.chain.hive.1.provider", "ranger");
    properties.put("authorization.chain.hive.1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive.1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive.1.ranger.username", "admin");
    properties.put("authorization.chain.hive.1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive.1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> chainedAuthzProperties.validate());
  }

  @Test
  void testErrorPluginName() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.plug3.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> chainedAuthzProperties.validate());
  }

  @Test
  void testDuplicationPluginName() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.chain.plugins", "hive1,hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> chainedAuthzProperties.validate());
  }

  @Test
  void testFetchRangerPrpoerties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(METASTORE_URIS, "thrift://localhost:9083");
    properties.put("gravitino.bypass.hive.metastore.client.capability.check", "true");
    properties.put(IMPERSONATION_ENABLE, "true");
    properties.put(AUTHORIZATION_PROVIDER, "chain");
    properties.put("authorization.chain.plugins", "hive1,hdfs1");
    properties.put("authorization.chain.hive1.provider", "ranger");
    properties.put("authorization.chain.hive1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hive1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hive1.ranger.username", "admin");
    properties.put("authorization.chain.hive1.ranger.password", "admin");
    properties.put("authorization.chain.hive1.ranger.service.type", "hive");
    properties.put("authorization.chain.hive1.ranger.service.name", "hiveDev");
    properties.put("authorization.chain.hdfs1.provider", "ranger");
    properties.put("authorization.chain.hdfs1.ranger.auth.type", "simple");
    properties.put("authorization.chain.hdfs1.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.chain.hdfs1.ranger.username", "admin");
    properties.put("authorization.chain.hdfs1.ranger.password", "admin");
    properties.put("authorization.chain.hdfs1.ranger.service.type", "hadoop");
    properties.put("authorization.chain.hdfs1.ranger.service.name", "hdfsDev");
    ChainedAuthorizationProperties chainedAuthzProperties =
        new ChainedAuthorizationProperties(properties);

    Assertions.assertDoesNotThrow(
        () -> {
          Map<String, String> rangerHiveProperties =
              chainedAuthzProperties.fetchAuthPluginProperties("hive1");
          RangerAuthorizationProperties rangerAuthProperties =
              new RangerAuthorizationProperties(rangerHiveProperties);
          rangerAuthProperties.validate();
        });

    Assertions.assertDoesNotThrow(
        () -> {
          Map<String, String> rangerHDFSProperties =
              chainedAuthzProperties.fetchAuthPluginProperties("hdfs1");
          RangerAuthorizationProperties rangerAuthProperties =
              new RangerAuthorizationProperties(rangerHDFSProperties);
          rangerAuthProperties.validate();
        });
  }
}
