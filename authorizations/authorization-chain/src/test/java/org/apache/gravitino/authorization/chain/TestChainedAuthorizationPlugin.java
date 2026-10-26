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
package org.apache.gravitino.authorization.chain;

import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestChainedAuthorizationPlugin {

  @Test
  public void testRequiresLocationsWhenAnyChainedPluginDoes() throws Exception {
    Assertions.assertTrue(
        chainOf(pluginRequiringLocations(false), pluginRequiringLocations(true))
            .requiresLocations());
  }

  @Test
  public void testDoesNotRequireLocationsWhenNoChainedPluginDoes() throws Exception {
    Assertions.assertFalse(chainOf(pluginRequiringLocations(false)).requiresLocations());
    Assertions.assertFalse(chainOf().requiresLocations());
  }

  private static ChainedAuthorizationPlugin chainOf(AuthorizationPlugin... plugins)
      throws IllegalAccessException {
    // The constructor loads plugin jars, so only the tested method runs for real.
    ChainedAuthorizationPlugin chain =
        Mockito.mock(ChainedAuthorizationPlugin.class, Mockito.CALLS_REAL_METHODS);
    FieldUtils.writeField(chain, "plugins", List.of(plugins), true);
    return chain;
  }

  private static AuthorizationPlugin pluginRequiringLocations(boolean requiresLocations) {
    AuthorizationPlugin plugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(plugin.requiresLocations()).thenReturn(requiresLocations);
    return plugin;
  }
}
