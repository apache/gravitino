/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Config;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
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
}
