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

package org.apache.gravitino.maintenance.optimizer.common.conf;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOptimizerConfig {

  @Test
  void testOptimizerConfigLoadsPropertiesCorrectly() {
    Map<String, String> properties =
        Map.of(
            OptimizerConfig.GRAVITINO_URI, "http://example.com",
            OptimizerConfig.GRAVITINO_METALAKE, "example-metalake",
            OptimizerConfig.GRAVITINO_DEFAULT_CATALOG, "example-catalog");
    OptimizerConfig config = new OptimizerConfig(properties);

    Assertions.assertEquals("http://example.com", config.get(OptimizerConfig.GRAVITINO_URI_CONFIG));
    Assertions.assertEquals(
        "example-metalake", config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG));
    Assertions.assertEquals(
        "example-catalog", config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG));
  }

  @Test
  void testOptimizerConfigHandlesMissingPropertiesGracefully() {
    Map<String, String> properties = Map.of();
    OptimizerConfig config = new OptimizerConfig(properties);

    Assertions.assertEquals(
        "http://localhost:8090", config.get(OptimizerConfig.GRAVITINO_URI_CONFIG));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG));
    Assertions.assertNull(config.get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG));
  }

  @Test
  void testJobSubmitterConfigsWithPrefix() {
    Map<String, String> properties =
        Map.of(
            OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark.master",
            "yarn",
            OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "queue",
            "default",
            OptimizerConfig.GRAVITINO_URI,
            "http://example.com");
    OptimizerConfig config = new OptimizerConfig(properties);

    Assertions.assertEquals(
        Map.of("spark.master", "yarn", "queue", "default"), config.jobSubmitterConfigs());
  }
}
