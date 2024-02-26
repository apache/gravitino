/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.config;

import com.google.common.collect.Maps;
import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcConfig {

  @Test
  void testCreateDataSourceConfig() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    Assertions.assertDoesNotThrow(() -> new JdbcConfig(properties));
  }
}
