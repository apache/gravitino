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

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJettyServerConfig {

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
    Config emptyconfig = new Config() {};
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(emptyconfig, "");
    Assertions.assertTrue(jettyServerConfig.getCustomFilters().isEmpty());

    Config somethingConfig = new Config() {};
    somethingConfig.set(JettyServerConfig.CUSTOM_FILTERS, Optional.of("1,2"));
    somethingConfig.set(new ConfigBuilder("1.1").stringConf(), "test");
    somethingConfig.set(new ConfigBuilder("1.2").stringConf(), "test");
    jettyServerConfig = JettyServerConfig.fromConfig(somethingConfig, "");
    Assertions.assertIterableEquals(
        Sets.newHashSet("1", "2"), jettyServerConfig.getCustomFilters());
    Assertions.assertTrue(jettyServerConfig.getAllWithPrefix("2.").isEmpty());
    Assertions.assertEquals(2, jettyServerConfig.getAllWithPrefix("1.").size());
  }
}
