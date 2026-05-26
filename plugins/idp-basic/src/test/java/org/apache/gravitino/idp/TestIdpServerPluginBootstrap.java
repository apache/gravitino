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

package org.apache.gravitino.idp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ServiceLoader;
import org.apache.gravitino.server.plugin.ServerPluginBootstrap;
import org.junit.jupiter.api.Test;

class TestIdpServerPluginBootstrap {

  @Test
  void testRegisteredViaServiceLoader() {
    ServerPluginBootstrap bootstrap =
        ServiceLoader.load(ServerPluginBootstrap.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(provider -> "idp-basic".equals(provider.name()))
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "IdpServerPluginBootstrap should be registered via META-INF/services"));
    assertEquals("idp-basic", bootstrap.name());
    assertTrue(bootstrap instanceof IdpServerPluginBootstrap);
  }
}
