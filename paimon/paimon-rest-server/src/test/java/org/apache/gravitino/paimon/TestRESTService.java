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
package org.apache.gravitino.paimon;

import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Verifies the auxiliary service cannot start when Gravitino requires authorization. */
public class TestRESTService {
  @Test
  void testAuxiliaryServiceRejectsAuthorizationEnabled() {
    GravitinoEnv environment = Mockito.mock(GravitinoEnv.class);
    Config config = Mockito.mock(Config.class);
    Mockito.when(environment.config()).thenReturn(config);
    Mockito.when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
    try (MockedStatic<GravitinoEnv> environments = Mockito.mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(environment);
      IllegalStateException failure =
          Assertions.assertThrows(
              IllegalStateException.class, () -> new RESTService().serviceInit(Map.of(), true));
      Assertions.assertTrue(failure.getMessage().contains("authorization"));
      Mockito.verify(environment, Mockito.never()).metricsSystem();
    }
  }
}
