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
package org.apache.gravitino.maintenance.policy;

import org.apache.gravitino.server.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceAuthConfigs {

  @Test
  void configKeysMatchDeclaredPrefixes() {
    Assertions.assertEquals(
        MaintenanceAuthConfigs.GRAVITINO_AUTH_PREFIX + MaintenanceAuthConfigs.AUTH_TYPE_SUFFIX,
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE.getKey());
    Assertions.assertEquals(
        MaintenanceAuthConfigs.IRC_AUTH_PREFIX + MaintenanceAuthConfigs.OAUTH_CREDENTIAL_SUFFIX,
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_IRC_AUTH_OAUTH_CREDENTIAL.getKey());
  }

  @Test
  void optionalAuthValuesLoadFromServerConfig() {
    ServerConfig config = new ServerConfig();
    config.loadFromMap(
        java.util.Map.of(
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE.getKey(),
            "oauth2",
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_IRC_AUTH_PASSWORD.getKey(),
            "secret"),
        key -> key.startsWith("gravitino."));

    Assertions.assertEquals(
        "oauth2",
        config.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_TYPE).get());
    Assertions.assertEquals(
        "secret", config.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_IRC_AUTH_PASSWORD).get());
    Assertions.assertTrue(
        config.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_IRC_AUTH_TYPE).isEmpty());
  }
}
