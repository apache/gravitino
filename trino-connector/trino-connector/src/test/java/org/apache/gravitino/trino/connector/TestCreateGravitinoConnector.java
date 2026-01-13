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
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorFactory;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.catalog.CatalogRegister;
import org.junit.jupiter.api.Test;

public class TestCreateGravitinoConnector {

  @Test
  public void testSingleMetalakeCatalogNaming() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "true"));

    assertEquals("memory", manager.getTrinoCatalogName("test", "memory"));
  }

  @Test
  public void testMultiMetalakeCatalogNaming() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "false"));

    assertEquals("\"test.memory\"", manager.getTrinoCatalogName("test", "memory"));
  }

  private CatalogConnectorManager createManager(ImmutableMap<String, String> configMap) {
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    CatalogConnectorFactory catalogFactory = mock(CatalogConnectorFactory.class);
    CatalogConnectorManager manager = new CatalogConnectorManager(catalogRegister, catalogFactory);
    manager.config(new GravitinoConfig(configMap), mock(GravitinoAdminClient.class));
    return manager;
  }
}
