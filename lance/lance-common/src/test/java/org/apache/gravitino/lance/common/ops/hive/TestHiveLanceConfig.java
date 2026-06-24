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
package org.apache.gravitino.lance.common.ops.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceBackend;
import org.junit.jupiter.api.Test;

/** Unit tests for the Hive-related {@link LanceConfig} keys and backend wiring. */
class TestHiveLanceConfig {

  @Test
  void testHiveConfigKeysParse() {
    LanceConfig config =
        new LanceConfig(
            ImmutableMap.of(
                "hive-metastore-uris", "thrift://host:9083",
                "hive-warehouse", "s3://bucket/wh",
                "hive-client-pool-size", "7"));
    assertEquals("thrift://host:9083", config.getHiveMetastoreUris());
    assertEquals("s3://bucket/wh", config.getHiveWarehouse());
    assertEquals(7, config.getHiveClientPoolSize());
  }

  @Test
  void testHiveClientPoolSizeDefault() {
    LanceConfig config = new LanceConfig(ImmutableMap.of());
    assertEquals(3, config.getHiveClientPoolSize());
  }

  @Test
  void testBackendFromTypeHive() {
    LanceNamespaceBackend backend = LanceNamespaceBackend.fromType("hive");
    assertEquals(LanceNamespaceBackend.HIVE, backend);
    assertEquals(HiveLanceNamespaceWrapper.class, backend.getWrapperClass());
  }
}
