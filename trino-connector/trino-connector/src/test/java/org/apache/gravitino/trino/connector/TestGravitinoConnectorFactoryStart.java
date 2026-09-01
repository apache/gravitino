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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorContext;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that the catalog connector manager is only started from the static connector.
 *
 * <p>The `trino.jdbc.*` settings are not propagated to the dynamic catalogs, so starting the
 * manager from a dynamic connector would make the internal JDBC connection lose its credentials and
 * TLS settings.
 */
public class TestGravitinoConnectorFactoryStart {

  // Nothing listens on this port, but the Trino driver connects lazily, so the register still
  // initializes successfully.
  private static final String COORDINATOR_URI = "http://127.0.0.1:1";

  @TempDir private static Path catalogStoreDir;

  private static Map<String, String> staticConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.metalake", "test");
    config.put("discovery.uri", COORDINATOR_URI);
    config.put("catalog.config-dir", catalogStoreDir.toString());
    config.put("trino.jdbc.user", "gravitino");
    return config;
  }

  private static Map<String, String> dynamicConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("gravitino.metalake", "test");
    // A dynamic catalog config carries neither `discovery.uri` nor any `trino.jdbc.*` setting.
    config.put(GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR, "true");
    config.put(GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG, "{}");
    return config;
  }

  private static ConnectorContext mockContext() {
    ConnectorContext context = mock(ConnectorContext.class);
    when(context.getSpiVersion()).thenReturn("478");
    return context;
  }

  /** A factory that always reports itself as running on the coordinator. */
  private static class CoordinatorFactory extends GravitinoConnectorFactory {
    CoordinatorFactory(GravitinoAdminClient client) {
      super(client);
    }

    @Override
    protected boolean isCoordinator(ConnectorContext connectorContext) {
      return true;
    }

    @Override
    protected HostAddress getCurrentNodeAddress(ConnectorContext connectorContext) {
      return HostAddress.fromParts("127.0.0.1", 1);
    }
  }

  private static CoordinatorFactory newFactory() {
    return new CoordinatorFactory(mock(GravitinoAdminClient.class));
  }

  @Test
  public void testStaticConnectorStartsTheManager() {
    CoordinatorFactory factory = newFactory();

    assertNotNull(factory.create("gravitino", staticConfig(), mockContext()));
    assertTrue(factory.isCatalogConnectorManagerStartTriggered());
  }

  @Test
  public void testDynamicConnectorDoesNotStartTheManager() {
    CoordinatorFactory factory = newFactory();

    Exception e =
        assertThrows(
            Exception.class, () -> factory.create("catalog", dynamicConfig(), mockContext()));

    // It fails later while building the dynamic catalog, never by connecting back to Trino.
    assertFalse(
        e.getMessage().contains("jdbc:trino://"),
        "The manager must not be started from a dynamic connector, got: " + e.getMessage());
    assertNotNull(factory.getCatalogConnectorManager());
    assertFalse(factory.isCatalogConnectorManagerStartTriggered());
  }

  @Test
  public void testStaticConnectorStartsTheManagerAfterADynamicOne() {
    CoordinatorFactory factory = newFactory();

    // The coordinator restarted and Trino loaded a Gravitino-created catalog first.
    assertThrows(Exception.class, () -> factory.create("catalog", dynamicConfig(), mockContext()));

    // The static catalog is loaded afterwards and must start the manager with its own config.
    // Had the dynamic config been used instead, `discovery.uri` would have been missing and the
    // register would have failed to build the JDBC URI.
    assertNotNull(factory.create("gravitino", staticConfig(), mockContext()));
    assertTrue(factory.isCatalogConnectorManagerStartTriggered());
  }

  @Test
  public void testFailedClientInitializationDoesNotPublishManager() {
    CoordinatorFactory factory = new CoordinatorFactory(null);
    Map<String, String> brokenDynamicConfig = dynamicConfig();
    brokenDynamicConfig.put("gravitino.client.authType", "oauth2");

    assertThrows(
        Exception.class,
        () -> factory.create("broken_catalog", brokenDynamicConfig, mockContext()));

    assertNull(factory.getCatalogConnectorManager());
    assertNotNull(factory.create("gravitino", staticConfig(), mockContext()));
    assertTrue(factory.isCatalogConnectorManagerStartTriggered());
  }

  @Test
  public void testStartIsAttemptedOnlyOnce() {
    CoordinatorFactory factory = newFactory();

    Map<String, String> brokenConfig = staticConfig();
    brokenConfig.put("catalog.config-dir", "/not/exists/catalog");
    assertThrows(Exception.class, () -> factory.create("gravitino", brokenConfig, mockContext()));

    // Everything that makes start() fail is a configuration error, so the next create() must not
    // try again: a second init() would open another connection and abandon the first one.
    assertNotNull(factory.create("gravitino", brokenConfig, mockContext()));
    // The retry must not just avoid throwing: the load loop actually has to start this time, or a
    // catalog whose init previously failed would stay unregistered forever.
    assertTrue(factory.isCatalogConnectorManagerStartTriggered());
  }

  @Test
  public void testStaticConnectorPinsSplitsToTheCoordinator() {
    CoordinatorFactory factory = newFactory();

    assertNotNull(factory.create("gravitino", staticConfig(), mockContext()));

    // The registration state the system tables report lives only on the coordinator, so its
    // splits must be pinned to the address CoordinatorFactory reports for this node.
    assertEquals(
        HostAddress.fromParts("127.0.0.1", 1),
        GravitinoSystemConnector.Split.getCurrentCoordinatorAddress());
  }

  @Test
  public void testDynamicConfigCarriesNoJdbcSettings() {
    GravitinoConfig config = new GravitinoConfig(ImmutableMap.copyOf(staticConfig()));

    // What a dynamic catalog would receive: no `trino.jdbc.*`, no `discovery.uri`.
    String catalogConfig = config.toCatalogConfig();
    assertFalse(catalogConfig.contains("trino.jdbc."));
    assertFalse(catalogConfig.contains("discovery.uri"));
  }
}
