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
package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.client.GravitinoClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Verifies GravitinoCatalogManager's lazy caching of the Iceberg REST endpoint discovery. */
public class TestGravitinoCatalogManager {

  private static final String METALAKE_NAME = "test_metalake";

  @AfterEach
  void closeManager() {
    try {
      GravitinoCatalogManager.get().close();
    } catch (IllegalStateException e) {
      // The test closed the manager itself.
    }
  }

  @Test
  void testIcebergRestUriIsCachedAfterSuccessfulDiscovery() {
    AtomicInteger discoveryCalls = new AtomicInteger();
    GravitinoCatalogManager manager =
        createManager(
            invocation -> {
              discoveryCalls.incrementAndGet();
              return Optional.of("http://irc:9001/iceberg");
            });

    Optional<String> first = manager.getIcebergRestUri();
    Optional<String> second = manager.getIcebergRestUri();

    assertEquals(Optional.of("http://irc:9001/iceberg"), first);
    assertSame(first, second, "A cached discovery result must not be recomputed");
    assertEquals(1, discoveryCalls.get());
  }

  @Test
  void testNoIcebergRestUriDiscoveredIsAlsoCached() {
    AtomicInteger discoveryCalls = new AtomicInteger();
    GravitinoCatalogManager manager =
        createManager(
            invocation -> {
              discoveryCalls.incrementAndGet();
              return Optional.empty();
            });

    manager.getIcebergRestUri();
    manager.getIcebergRestUri();

    assertEquals(1, discoveryCalls.get(), "A negative discovery result must be cached too");
  }

  @Test
  void testIcebergRestUriDiscoveryFailurePropagatesAndIsNotCached() {
    AtomicInteger discoveryCalls = new AtomicInteger();
    GravitinoCatalogManager manager =
        createManager(
            invocation -> {
              if (discoveryCalls.incrementAndGet() == 1) {
                throw new RuntimeException("Gravitino server unreachable");
              }
              return Optional.of("http://irc:9001/iceberg");
            });

    assertThrows(RuntimeException.class, manager::getIcebergRestUri);
    Optional<String> second = manager.getIcebergRestUri();

    assertEquals(Optional.of("http://irc:9001/iceberg"), second);
    assertEquals(2, discoveryCalls.get(), "A failed discovery must be retried on the next call");
  }

  private GravitinoCatalogManager createManager(
      org.mockito.stubbing.Answer<Optional<String>> answer) {
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.icebergRestServiceUri(anyString())).thenAnswer(answer);
    return GravitinoCatalogManager.create(METALAKE_NAME, () -> client);
  }
}
