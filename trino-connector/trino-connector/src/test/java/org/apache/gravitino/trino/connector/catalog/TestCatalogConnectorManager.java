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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

public class TestCatalogConnectorManager {

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

  @Test
  public void testSingleMetalakeRejectsDifferentMetalakeConnector() throws Exception {
    CatalogConnectorFactory catalogFactory = createCatalogConnectorFactory();
    CatalogConnectorManager manager =
        createManager(
            catalogFactory,
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "true"));

    GravitinoConfig connectorConfig = createConnectorConfig(catalogConfigJson("test", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test0", connectorConfig, mockContext()));

    GravitinoConfig otherConnectorConfig =
        createConnectorConfig(catalogConfigJson("test2", "memory"));
    TrinoException error =
        assertThrows(
            TrinoException.class,
            () ->
                manager.createCatalogConnectorContext(
                    "test1", otherConnectorConfig, mockContext()));
    assertEquals(GravitinoErrorCode.GRAVITINO_OPERATION_FAILED.toErrorCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("Failed to create connector"));
    assertTrue(error.getCause().getMessage().contains("Multiple metalakes are not supported"));
  }

  @Test
  public void testMultiMetalakeAllowsDifferentMetalakeConnector() throws Exception {
    CatalogConnectorFactory catalogFactory = createCatalogConnectorFactory();
    CatalogConnectorManager manager =
        createManager(
            catalogFactory,
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "false"));

    GravitinoConfig connectorConfig = createConnectorConfig(catalogConfigJson("test", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test0", connectorConfig, mockContext()));

    GravitinoConfig otherConnectorConfig =
        createConnectorConfig(catalogConfigJson("test2", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test1", otherConnectorConfig, mockContext()));
  }

  @Test
  public void testSkipCatalogPatterns() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test1",
                "gravitino.trino.skip-catalog-patterns",
                "a.*, b1"));

    assertTrue(manager.skipCatalog("a1"));
    assertTrue(manager.skipCatalog("b1"));
    assertFalse(manager.skipCatalog("b2"));
  }

  private CatalogConnectorManager createManager(ImmutableMap<String, String> configMap)
      throws Exception {
    return createManager(createCatalogConnectorFactory(), configMap);
  }

  private CatalogConnectorManager createManager(
      CatalogConnectorFactory catalogFactory, ImmutableMap<String, String> configMap) {
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    CatalogConnectorManager manager = new CatalogConnectorManager(catalogRegister, catalogFactory);
    manager.config(new GravitinoConfig(configMap), mock(GravitinoAdminClient.class));
    return manager;
  }

  private CatalogConnectorFactory createCatalogConnectorFactory() throws Exception {
    CatalogConnectorFactory catalogFactory = mock(CatalogConnectorFactory.class);
    CatalogConnectorContext.Builder builder = mock(CatalogConnectorContext.Builder.class);
    when(catalogFactory.createCatalogConnectorContextBuilder(any())).thenReturn(builder);
    when(builder.withMetalake(any())).thenReturn(builder);
    when(builder.withContext(any())).thenReturn(builder);
    when(builder.build()).thenReturn(mock(CatalogConnectorContext.class));
    return catalogFactory;
  }

  private static GravitinoConfig createConnectorConfig(String catalogConfigJson) {
    return new GravitinoConfig(
        ImmutableMap.of(
            "gravitino.uri",
            "http://127.0.0.1:8090",
            "gravitino.metalake",
            "test",
            GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR,
            "true",
            GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG,
            catalogConfigJson));
  }

  private static String catalogConfigJson(String metalake, String name) throws Exception {
    GravitinoCatalog catalog =
        new GravitinoCatalog(metalake, "memory", name, ImmutableMap.of(), 0L);
    return GravitinoCatalog.toJson(catalog);
  }

  private static ConnectorContext mockContext() {
    return mock(ConnectorContext.class);
  }
}
