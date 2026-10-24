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

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.oceanbase.OceanBaseCEContainer;

/** Tests the OceanBase container as built on the Testcontainers module, without Docker. */
public class TestOceanBaseContainer {

  @Test
  void testRunsOnTheOceanBaseModuleAndKeepsTheServerCheck() {
    OceanBaseContainer oceanBase =
        OceanBaseContainer.builder().withEnvVars(Map.of("OB_MEMORY_LIMIT", "6G")).build();
    GenericContainer<?> container = oceanBase.getContainer();

    Assertions.assertInstanceOf(OceanBaseCEContainer.class, container);
    OceanBaseCEContainer module = (OceanBaseCEContainer) container;
    Assertions.assertEquals(OceanBaseContainer.USER_NAME, module.getUsername());
    Assertions.assertEquals(OceanBaseContainer.PASSWORD, module.getPassword());
    Assertions.assertEquals("6G", container.getEnvMap().get("OB_MEMORY_LIMIT"));
    Assertions.assertTrue(container.getExposedPorts().contains(OceanBaseContainer.OCEANBASE_PORT));
    Assertions.assertInstanceOf(WaitAllStrategy.class, oceanBase.waitStrategy());
  }

  @Test
  void testAcceptsAMirroredImage() {
    Assertions.assertDoesNotThrow(
        () ->
            OceanBaseContainer.builder()
                .withImage("mirror.example.com/oceanbase/oceanbase-ce:4.2.1-lts")
                .build());
  }
}
