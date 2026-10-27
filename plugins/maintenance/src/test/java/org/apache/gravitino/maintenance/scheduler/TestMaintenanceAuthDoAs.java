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
package org.apache.gravitino.maintenance.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceAuthDoAs {

  @Test
  void resolvePrincipalEmptyWhenAuthUnset() {
    Assertions.assertTrue(MaintenanceAuthDoAs.resolvePrincipal(new Config() {}).isEmpty());
  }

  @Test
  void callRunsWithoutDoAsWhenAuthUnset() throws Exception {
    String result = MaintenanceAuthDoAs.call(new Config() {}, () -> "ok");
    Assertions.assertEquals("ok", result);
  }

  @Test
  void callInstallsBasicPrincipalFromGravitinoAuth() {
    Config config =
        configWith(
            Map.of(
                "gravitino.maintenance.gravitinoAuth.authType", "basic",
                "gravitino.maintenance.gravitinoAuth.username", "tms-sa",
                "gravitino.maintenance.gravitinoAuth.password", "secret"));

    Optional<UserPrincipal> resolved = MaintenanceAuthDoAs.resolvePrincipal(config);
    Assertions.assertTrue(resolved.isPresent());
    Assertions.assertEquals("tms-sa", resolved.get().getName());
    Assertions.assertTrue(resolved.get().getAccessToken().orElseThrow().startsWith("Basic "));

    AtomicReference<String> seenUser = new AtomicReference<>();
    AtomicReference<String> seenAuth = new AtomicReference<>();
    String result =
        MaintenanceAuthDoAs.call(
            config,
            () -> {
              UserPrincipal current = (UserPrincipal) PrincipalUtils.getCurrentPrincipal();
              seenUser.set(current.getName());
              seenAuth.set(current.getAccessToken().orElse(null));
              return "done";
            });
    Assertions.assertEquals("done", result);
    Assertions.assertEquals("tms-sa", seenUser.get());
    Assertions.assertTrue(seenAuth.get().startsWith("Basic "));
  }

  private static Config configWith(Map<String, String> values) {
    Config config = new Config() {};
    config.loadFromMap(new HashMap<>(values), key -> true);
    return config;
  }
}
