/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.utils;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcConfig {

  @Test
  void testOnBorrow() {
    JdbcConfig jdbcConfig = new JdbcConfig(Maps.newHashMap());
    Assertions.assertTrue(jdbcConfig.getTestOnBorrow());

    ImmutableMap immutableMap = ImmutableMap.of("jdbc.pool.test-on-borrow", "false");
    jdbcConfig = new JdbcConfig(immutableMap);
    Assertions.assertFalse(jdbcConfig.getTestOnBorrow());
  }
}
