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
package org.apache.gravitino.integration.test.container;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/** Tests the ClickHouse container as built on the Testcontainers module, without Docker. */
public class TestClickHouseContainer {

  @Test
  void testRunsOnTheClickHouseModuleWithItsReadinessCheck() {
    ClickHouseContainer clickHouse =
        ClickHouseContainer.builder()
            .withEnvVars(Map.of("CLICKHOUSE_PASSWORD", ClickHouseContainer.PASSWORD))
            .build();
    GenericContainer<?> container = clickHouse.getContainer();

    Assertions.assertInstanceOf(org.testcontainers.clickhouse.ClickHouseContainer.class, container);
    org.testcontainers.clickhouse.ClickHouseContainer module =
        (org.testcontainers.clickhouse.ClickHouseContainer) container;
    Assertions.assertEquals(ClickHouseContainer.USER_NAME, module.getUsername());
    Assertions.assertEquals(ClickHouseContainer.PASSWORD, module.getPassword());
    Assertions.assertEquals(
        ClickHouseContainer.PASSWORD, container.getEnvMap().get("CLICKHOUSE_PASSWORD"));
    Assertions.assertTrue(
        container.getExposedPorts().contains(ClickHouseContainer.CLICKHOUSE_PORT));
    Assertions.assertNull(clickHouse.waitStrategy());
  }

  @Test
  void testAcceptsTheLibraryAndTheVendorImages() {
    for (String image :
        List.of(
            TestImages.CLICKHOUSE,
            "clickhouse/clickhouse-server:23.8.16.16",
            "mirror.example.com/clickhouse:24.8.14")) {
      Assertions.assertDoesNotThrow(
          () -> ClickHouseContainer.builder().withImage(image).build(), image);
    }
  }
}
