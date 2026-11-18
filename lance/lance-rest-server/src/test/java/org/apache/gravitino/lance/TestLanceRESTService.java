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
package org.apache.gravitino.lance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Test;

public class TestLanceRESTService {

  /** The documented Lance REST key reaches the value the service passes to its error builder. */
  @Test
  public void testIncludeErrorStackTraceReadFromServiceConfig() {
    assertTrue(JettyServerConfig.fromConfig(new LanceConfig()).isIncludeErrorStackTrace());
    assertFalse(
        JettyServerConfig.fromConfig(
                new LanceConfig(Collections.singletonMap("includeErrorStackTrace", "false")))
            .isIncludeErrorStackTrace());
  }

  @Test
  public void testMetricsAreEnabledOnlyForConfiguredStandaloneMode() {
    LanceConfig disabledConfig = new LanceConfig();
    assertFalse(LanceRESTService.shouldEnableMetrics(false, disabledConfig));
    assertFalse(LanceRESTService.shouldEnableMetrics(true, disabledConfig));

    LanceConfig enabledConfig =
        new LanceConfig(Collections.singletonMap(LanceConfig.METRICS_ENABLED.getKey(), "true"));
    assertTrue(LanceRESTService.shouldEnableMetrics(false, enabledConfig));
    assertFalse(LanceRESTService.shouldEnableMetrics(true, enabledConfig));
  }
}
