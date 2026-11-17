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
package org.apache.gravitino.maintenance;

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTableMaintenanceConfig {

  @Test
  void resolveJobSubmitterEmptyWhenAuthorizationDisabled() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(Configs.ENABLE_AUTHORIZATION.getKey(), "false"),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(Optional.empty(), TableMaintenanceConfig.resolveJobSubmitter(config));
  }

  @Test
  void resolveJobSubmitterRequiresConfigWhenAuthorizationEnabled() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(Configs.ENABLE_AUTHORIZATION.getKey(), "true"), key -> key.startsWith("gravitino."));

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TableMaintenanceConfig.resolveJobSubmitter(config));
    Assertions.assertTrue(
        thrown.getMessage().contains(TableMaintenanceConfig.JOB_SUBMITTER.getKey()));
  }

  @Test
  void resolveJobSubmitterReturnsTrimmedNameWhenAuthorizationEnabled() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            TableMaintenanceConfig.JOB_SUBMITTER.getKey(),
            "  tms-bot  "),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(
        Optional.of("tms-bot"), TableMaintenanceConfig.resolveJobSubmitter(config));
  }
}
