/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestGravitonConfig {

  @BeforeAll
  public static void startup() throws Exception {}

  @AfterAll
  public static void shutdown() throws Exception {}

  @Test
  public void testGetURI() {
    String gravitonUrl = "http://127.0.0.1:8000";
    ImmutableMap<String, String> configMap = ImmutableMap.of("graviton.uri", gravitonUrl);

    GravitonConfig config = new GravitonConfig(configMap);
    Assertions.assertEquals(gravitonUrl, config.getURI());
  }

  @Test
  public void testGetMetalake() {
    String metalake = "user_001";
    ImmutableMap<String, String> configMap = ImmutableMap.of("graviton.metalake", metalake);

    GravitonConfig config = new GravitonConfig(configMap);
    Assertions.assertEquals(metalake, config.getMetalake());
  }
}
