/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link PaimonConfig}. */
public class TestPaimonConfig {

  @Test
  public void testLoadPaimonConfig() {
    ConfigEntry<String> testConf = new ConfigBuilder("k1").stringConf().create();
    Map<String, String> properties = ImmutableMap.of(testConf.getKey(), "v1");

    PaimonConfig paimonConfig = new PaimonConfig();
    paimonConfig.loadFromMap(properties, k -> k.startsWith("gravitino."));
    Assertions.assertNull(paimonConfig.get(testConf));

    paimonConfig = new PaimonConfig(properties);
    Assertions.assertEquals("v1", paimonConfig.get(testConf));
  }
}
