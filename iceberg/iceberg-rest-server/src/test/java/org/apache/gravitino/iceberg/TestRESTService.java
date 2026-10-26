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
package org.apache.gravitino.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Test;

public class TestRESTService {

  /**
   * Entity bookkeeping for Iceberg REST drops and renames needs the entity store, which exists in
   * the auxiliary mode whether or not authorization is on.
   */
  @Test
  void testHookDispatchersAreInstalledWheneverAnEntityStoreExists() {
    assertTrue(
        RESTService.shouldInstallHookDispatchers(true, false),
        "Auxiliary mode without authorization must still delete and move entities");
    assertTrue(RESTService.shouldInstallHookDispatchers(true, true));
    assertTrue(RESTService.shouldInstallHookDispatchers(false, true));
    assertFalse(
        RESTService.shouldInstallHookDispatchers(false, false),
        "Standalone Iceberg REST has no entity store to keep in step");
  }

  /** The documented Iceberg REST key reaches the value the service passes to its error builder. */
  @Test
  public void testIncludeErrorStackTraceReadFromServiceConfig() {
    assertTrue(JettyServerConfig.fromConfig(new IcebergConfig()).isIncludeErrorStackTrace());
    assertFalse(
        JettyServerConfig.fromConfig(
                new IcebergConfig(Collections.singletonMap("includeErrorStackTrace", "false")))
            .isIncludeErrorStackTrace());
  }

  @Test
  public void testMetricsAreEnabledOnlyForConfiguredStandaloneMode() {
    IcebergConfig disabledConfig = new IcebergConfig();
    assertFalse(RESTService.shouldEnableMetrics(false, disabledConfig));
    assertFalse(RESTService.shouldEnableMetrics(true, disabledConfig));

    IcebergConfig enabledConfig =
        new IcebergConfig(Collections.singletonMap(IcebergConfig.METRICS_ENABLED.getKey(), "true"));
    assertTrue(RESTService.shouldEnableMetrics(false, enabledConfig));
    assertFalse(RESTService.shouldEnableMetrics(true, enabledConfig));
  }
}
