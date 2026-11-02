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
package org.apache.gravitino.server.web;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJettyServerConfig {

  @Test
  public void testIncludeErrorStackTrace() {
    Config config = new Config() {};
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(config, "");
    Assertions.assertTrue(jettyServerConfig.isIncludeErrorStackTrace());

    config.set(JettyServerConfig.INCLUDE_ERROR_STACK_TRACE, false);
    jettyServerConfig = JettyServerConfig.fromConfig(config, "");
    Assertions.assertFalse(jettyServerConfig.isIncludeErrorStackTrace());
  }

  @Test
  public void testCipherAlgorithms() {
    Config noIntersectConfig = new Config() {};
    noIntersectConfig.set(JettyServerConfig.ENABLE_CIPHER_ALGORITHMS, "test1,test2");
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(noIntersectConfig, "");

    Assertions.assertIterableEquals(
        Collections.emptySet(), jettyServerConfig.getSupportedAlgorithms());

    Set<String> supportAlgorithms = jettyServerConfig.getSupportedCipherSuites();
    Assertions.assertFalse(supportAlgorithms.isEmpty());
    String algorithm = supportAlgorithms.iterator().next();
    Config containConfig = new Config() {};
    containConfig.set(JettyServerConfig.ENABLE_CIPHER_ALGORITHMS, algorithm);
    jettyServerConfig = JettyServerConfig.fromConfig(containConfig, "");
    Assertions.assertIterableEquals(
        Sets.newHashSet(algorithm), jettyServerConfig.getSupportedAlgorithms());
    Config partConfig = new Config() {};
    partConfig.set(JettyServerConfig.ENABLE_CIPHER_ALGORITHMS, algorithm + ",test1");
    jettyServerConfig = JettyServerConfig.fromConfig(partConfig, "");
    Assertions.assertIterableEquals(
        Sets.newHashSet(algorithm), jettyServerConfig.getSupportedAlgorithms());

    Config protocolConfig = new Config() {};
    protocolConfig.set(JettyServerConfig.SSL_PROTOCOL, Optional.of("TLS"));
    protocolConfig.set(JettyServerConfig.ENABLE_CIPHER_ALGORITHMS, algorithm + ",test1");
    jettyServerConfig = JettyServerConfig.fromConfig(protocolConfig, "");
    Assertions.assertIterableEquals(
        Sets.newHashSet(algorithm), jettyServerConfig.getSupportedAlgorithms());
  }

  @Test
  public void testCustomFilters() {
    Config emptyConfig = new Config() {};
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(emptyConfig, "");
    Assertions.assertNotNull(jettyServerConfig.getCustomFilters());
    Assertions.assertTrue(jettyServerConfig.getCustomFilters().isEmpty());

    Config quotedEmptyConfig = new Config() {};
    quotedEmptyConfig.set(JettyServerConfig.CUSTOM_FILTERS, List.of("\"\""));
    jettyServerConfig = JettyServerConfig.fromConfig(quotedEmptyConfig, "");
    Assertions.assertNotNull(jettyServerConfig.getCustomFilters());
    Assertions.assertTrue(jettyServerConfig.getCustomFilters().isEmpty());

    Config singleQuotedEmptyConfig = new Config() {};
    singleQuotedEmptyConfig.set(JettyServerConfig.CUSTOM_FILTERS, List.of("''"));
    jettyServerConfig = JettyServerConfig.fromConfig(singleQuotedEmptyConfig, "");
    Assertions.assertNotNull(jettyServerConfig.getCustomFilters());
    Assertions.assertTrue(jettyServerConfig.getCustomFilters().isEmpty());

    Config mixedEmptyConfig = new Config() {};
    mixedEmptyConfig.set(
        JettyServerConfig.CUSTOM_FILTERS,
        Arrays.asList(" 1", " ", "", " \"\" ", " '' ", "", " 2 "));
    jettyServerConfig = JettyServerConfig.fromConfig(mixedEmptyConfig, "");
    Assertions.assertIterableEquals(
        Sets.newHashSet("1", "2"), jettyServerConfig.getCustomFilters());

    Config somethingConfig = new Config() {};
    somethingConfig.set(JettyServerConfig.CUSTOM_FILTERS, List.of("1,2"));
    somethingConfig.set(new ConfigBuilder("1.1").stringConf(), "test");
    somethingConfig.set(new ConfigBuilder("1.2").stringConf(), "test");
    jettyServerConfig = JettyServerConfig.fromConfig(somethingConfig, "");
    Assertions.assertIterableEquals(
        Sets.newHashSet("1", "2"), jettyServerConfig.getCustomFilters());
    Assertions.assertTrue(jettyServerConfig.getAllWithPrefix("2.").isEmpty());
    Assertions.assertEquals(2, jettyServerConfig.getAllWithPrefix("1.").size());
  }

  @Test
  public void testTrustedProxiesDefaultIsEmpty() {
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(new Config() {}, "");
    Assertions.assertTrue(jettyServerConfig.getTrustedProxies().isEmpty());
    Assertions.assertNotNull(jettyServerConfig.getClientAddressResolver());
  }

  @Test
  public void testTrustedProxiesParsedWithPrefix() {
    Config config = new Config(false) {};
    config.loadFromMap(
        ImmutableMap.of(
            JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX + "trustedProxies",
            " 10.0.0.1, 172.16.0.0/12 ,, fd00::/8, ::ffff:192.168.0.0/112 "),
        k -> true);

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(config, JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX);

    Assertions.assertEquals(
        Arrays.asList("10.0.0.1", "172.16.0.0/12", "fd00::/8", "::ffff:192.168.0.0/112"),
        jettyServerConfig.getTrustedProxies());
  }

  @Test
  public void testInvalidTrustedProxyFailsWithClearMessage() {
    for (String invalid :
        Arrays.asList(
            "proxy.example.com",
            "10.0.0.0/33",
            "fd00::/129",
            "10.0.0.0/abc",
            "10.0.0",
            "10.0.0.1/")) {
      Config config = new Config() {};
      config.set(JettyServerConfig.TRUSTED_PROXIES, List.of("10.0.0.1", invalid));
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> JettyServerConfig.fromConfig(config, ""),
              "Expected failure for " + invalid);
      Assertions.assertTrue(
          e.getMessage().contains(JettyServerConfig.TRUSTED_PROXIES.getKey()), e.getMessage());
      Assertions.assertTrue(e.getMessage().contains(invalid), e.getMessage());
    }
  }

  @Test
  public void testInvalidTrustedProxyErrorNamesFullKey() {
    String serverKey = JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX + "trustedProxies";
    Config serverConfig = new Config(false) {};
    serverConfig.loadFromMap(ImmutableMap.of(serverKey, "proxy.example.com"), k -> true);
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                JettyServerConfig.fromConfig(
                    serverConfig, JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX));
    Assertions.assertTrue(e.getMessage().contains(serverKey), e.getMessage());

    // An auxiliary service configuration has its prefix stripped already.
    Config serviceConfig = new Config(false) {};
    serviceConfig.loadFromMap(ImmutableMap.of("trustedProxies", "10.0.0.0/33"), k -> true);
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> JettyServerConfig.fromServiceConfig(serviceConfig, "gravitino.iceberg-rest."));
    Assertions.assertTrue(
        e.getMessage().contains("gravitino.iceberg-rest.trustedProxies"), e.getMessage());
  }

  @Test
  public void testFromServiceConfigReadsUnprefixedKeys() {
    Config serviceConfig = new Config(false) {};
    serviceConfig.loadFromMap(ImmutableMap.of("trustedProxies", "10.0.0.1"), k -> true);
    Assertions.assertEquals(
        Arrays.asList("10.0.0.1"),
        JettyServerConfig.fromServiceConfig(serviceConfig, "gravitino.lance-rest.")
            .getTrustedProxies());
  }
}
