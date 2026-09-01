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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import java.util.Map;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.junit.jupiter.api.Test;

public class TestGravitinoConnectorFactoryInit {

  @Test
  public void testFailedStartIsNotRetriedOnTheNextCreate() {
    GravitinoConnectorFactory factory =
        new GravitinoConnectorFactory(mock(GravitinoAdminClient.class));
    Map<String, String> config =
        ImmutableMap.of(
            "gravitino.uri", "http://127.0.0.1:8090",
            "gravitino.metalake", "test",
            "gravitino.trino.skip-version-validation", "true",
            // Makes CatalogRegister.init() fail, so the whole initialization fails.
            "catalog.config-dir", "/nonexistent-gravitino-catalog-dir",
            "discovery.uri", "http://127.0.0.1:8080");

    assertThrows(TrinoException.class, () -> factory.create("gravitino", config, mockContext()));

    // Everything that makes start() fail is a configuration error, so the next create() must not
    // try again: a second init() would open another connection and abandon the first one.
    assertDoesNotThrow(() -> factory.create("gravitino", config, mockContext()));
    // Not throwing is not enough: the load loop must actually start this time, or a catalog whose
    // init previously failed would stay unregistered forever.
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
