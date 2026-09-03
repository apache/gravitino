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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoConnectorFactoryInit {

  @TempDir private static Path catalogStoreDir;

  private static Map<String, String> config(String catalogStoreDirectory) {
    return ImmutableMap.of(
        "gravitino.uri", "http://127.0.0.1:8090",
        "gravitino.metalake", "test",
        "gravitino.trino.skip-version-validation", "true",
        "catalog.config-dir", catalogStoreDirectory,
        "discovery.uri", "http://127.0.0.1:8080");
  }

  @Test
  public void testFailedStartIsDiscardedAndCanRecover() {
    GravitinoConnectorFactory factory =
        new GravitinoConnectorFactory(mock(GravitinoAdminClient.class));

    // A catalog store directory that does not exist makes CatalogRegister.init() fail, so the
    // whole initialization fails.
    assertThrows(
        TrinoException.class,
        () ->
            factory.create(
                "gravitino", config("/nonexistent-gravitino-catalog-dir"), mockContext()));

    // The half-started manager must not stay published: a connector handed out afterwards would
    // run against a load loop that never started.
    assertNull(factory.getCatalogConnectorManager());
    assertFalse(factory.isCatalogConnectorManagerStartTriggered());

    // Once the configuration is fixed, the next create() starts over and succeeds.
    assertNotNull(factory.create("gravitino", config(catalogStoreDir.toString()), mockContext()));
    assertTrue(factory.isCatalogConnectorManagerStartTriggered());
  }

  @SuppressWarnings("deprecation")
  private static ConnectorContext mockContext() {
    ConnectorContext context = mock(ConnectorContext.class);
    when(context.getSpiVersion()).thenReturn("435");
    Node node = mock(Node.class);
    when(node.isCoordinator()).thenReturn(true);
    when(node.getHostAndPort()).thenReturn(HostAddress.fromParts("127.0.0.1", 8080));
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getCurrentNode()).thenReturn(node);
    when(context.getNodeManager()).thenReturn(nodeManager);
    return context;
  }
}
