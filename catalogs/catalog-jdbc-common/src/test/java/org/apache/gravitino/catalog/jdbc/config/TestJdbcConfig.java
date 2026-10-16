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
package org.apache.gravitino.catalog.jdbc.config;

import com.google.common.collect.Maps;
import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcConfig {

  @Test
  public void testCreateDataSourceConfig() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    Assertions.assertDoesNotThrow(() -> new JdbcConfig(properties));
  }

  @Test
  public void testMaxIdleDefaultsAndCap() {
    JdbcConfig defaultConfig = new JdbcConfig(Maps.newHashMap());
    Assertions.assertEquals(8, defaultConfig.getPoolMaxIdle());

    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "6");
    Assertions.assertEquals(6, new JdbcConfig(properties).getPoolMaxIdle());

    properties.put(JdbcConfig.POOL_MAX_IDLE.getKey(), "4");
    Assertions.assertEquals(4, new JdbcConfig(properties).getPoolMaxIdle());

    properties.put(JdbcConfig.POOL_MAX_IDLE.getKey(), "0");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new JdbcConfig(properties).getPoolMaxIdle());
  }
}
