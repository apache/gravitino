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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Verifies that GravitinoCatalogManager partitions its client and catalog caches by the identity
 * carried in the bearer token, and that no other auth type changes behaviour.
 */
public class TestGravitinoCatalogManager {

  private static final String CATALOG_NAME = "test_catalog";

  private ClientFactory clientFactory;

  @AfterEach
  void closeManager() {
    try {
      GravitinoCatalogManager.get().close();
    } catch (IllegalStateException e) {
      // The test closed the manager itself.
    }
  }

  @Test
  void testDifferentSubjectsDoNotShareCatalogCache() {
    SparkConf sparkConf = tokenConf();
    GravitinoCatalogManager manager = createManager(sparkConf);

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice"));
    Catalog aliceCatalog = manager.getGravitinoCatalogInfo(CATALOG_NAME);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("bob"));
    Catalog bobCatalog = manager.getGravitinoCatalogInfo(CATALOG_NAME);

    assertEquals(2, clientFactory.clientCount());
    assertEquals(2, clientFactory.loadCount());
    assertNotSame(aliceCatalog, bobCatalog);
  }

  @Test
  void testSameSubjectSharesClientAndCatalogCache() {
    SparkConf sparkConf = tokenConf();
    GravitinoCatalogManager manager = createManager(sparkConf);

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice", "first"));
    Catalog first = manager.getGravitinoCatalogInfo(CATALOG_NAME);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice", "second"));
    Catalog second = manager.getGravitinoCatalogInfo(CATALOG_NAME);

    assertEquals(1, clientFactory.clientCount());
    assertEquals(1, clientFactory.loadCount());
    assertSame(first, second);
  }

  @Test
  void testOpaqueTokensArePartitionedByTokenValue() {
    SparkConf sparkConf = tokenConf();
    GravitinoCatalogManager manager = createManager(sparkConf);

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "opaque-token-one");
    manager.getGravitinoCatalogInfo(CATALOG_NAME);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "opaque-token-one");
    manager.getGravitinoCatalogInfo(CATALOG_NAME);

    assertEquals(1, clientFactory.clientCount());
    assertEquals(1, clientFactory.loadCount());

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "opaque-token-two");
    manager.getGravitinoCatalogInfo(CATALOG_NAME);

    assertEquals(2, clientFactory.clientCount());
    assertEquals(2, clientFactory.loadCount());
  }

  @Test
  void testSimpleAuthKeepsOneApplicationIdentity() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    GravitinoCatalogManager manager = createManager(sparkConf);

    // Tokens are irrelevant outside token mode: both sessions must resolve to one identity.
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice"));
    Catalog first = manager.getGravitinoCatalogInfo(CATALOG_NAME);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("bob"));
    Catalog second = manager.getGravitinoCatalogInfo(CATALOG_NAME);

    assertEquals(1, clientFactory.clientCount());
    assertEquals(1, clientFactory.loadCount());
    assertSame(first, second);
  }

  @Test
  void testCloseClosesEveryCachedClient() {
    SparkConf sparkConf = tokenConf();
    GravitinoCatalogManager manager = createManager(sparkConf);

    for (String user : new String[] {"alice", "bob", "carol"}) {
      sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt(user));
      manager.getGravitinoCatalogInfo(CATALOG_NAME);
    }
    assertEquals(3, clientFactory.clientCount());

    manager.close();

    assertEquals(3, clientFactory.closedCount());
  }

  @Test
  void testClientCacheEvictsAndClosesEvictedClient() {
    SparkConf sparkConf = tokenConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_CLIENT_CACHE_MAX_SIZE, "2");
    GravitinoCatalogManager manager = createManager(sparkConf);

    for (String user : new String[] {"alice", "bob", "carol"}) {
      sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt(user));
      manager.getGravitinoCatalogInfo(CATALOG_NAME);
    }

    assertEquals(3, clientFactory.clientCount());
    // Caffeine evicts and dispatches the removal listener asynchronously, and a read keeps the
    // cache draining its buffers while we wait.
    assertTrue(
        await(
            () -> {
              manager.getGravitinoCatalogInfo(CATALOG_NAME);
              return clientFactory.closedCount() >= 1;
            }),
        "Exceeding the client cache size should evict and close a client");
  }

  @Test
  void testCatalogCacheEntryExpires() throws InterruptedException {
    SparkConf sparkConf = tokenConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_CATALOG_CACHE_TTL_SEC, "1");
    GravitinoCatalogManager manager = createManager(sparkConf);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, jwt("alice"));

    manager.getGravitinoCatalogInfo(CATALOG_NAME);
    assertEquals(1, clientFactory.loadCount());

    Thread.sleep(1500);
    manager.getGravitinoCatalogInfo(CATALOG_NAME);

    assertEquals(2, clientFactory.loadCount(), "A stale catalog entry must be reloaded");
    // The client is cached independently of the catalog entry.
    assertEquals(1, clientFactory.clientCount());
  }

  @Test
  void testIcebergRestUriIsCachedAfterSuccessfulDiscovery() {
    AtomicInteger discoveryCalls = new AtomicInteger();
    GravitinoCatalogManager manager =
        createManagerWithIcebergRestUri(
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
        createManagerWithIcebergRestUri(
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
        createManagerWithIcebergRestUri(
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

  private GravitinoCatalogManager createManagerWithIcebergRestUri(Answer<Optional<String>> answer) {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_METALAKE, "test_metalake");
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.icebergRestServiceUri(anyString())).thenAnswer(answer);
    return GravitinoCatalogManager.create(sparkConf, "spark-user", identity -> client);
  }

  private GravitinoCatalogManager createManager(SparkConf sparkConf) {
    clientFactory = new ClientFactory();
    return GravitinoCatalogManager.create(sparkConf, "spark-user", clientFactory);
  }

  private static SparkConf tokenConf() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.TOKEN_AUTH_TYPE);
    return sparkConf;
  }

  /** Builds an unsigned three part JWT. Nothing verifies it, so no signing key is needed. */
  private static String jwt(String subject) {
    return jwt(subject, "unused");
  }

  private static String jwt(String subject, String jwtId) {
    return base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}")
        + "."
        + base64Url(String.format("{\"sub\":\"%s\",\"jti\":\"%s\"}", subject, jwtId))
        + ".signature";
  }

  private static String base64Url(String value) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static boolean await(BooleanSupplier condition) {
    for (int i = 0; i < 100; i++) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }

  /** Hands out a distinct mock client per identity and counts what the manager asks of it. */
  private static class ClientFactory implements Function<GravitinoIdentity, GravitinoClient> {

    private final List<AtomicBoolean> closedFlags = new ArrayList<>();
    private final AtomicInteger clients = new AtomicInteger();
    private final AtomicInteger loads = new AtomicInteger();

    @Override
    public GravitinoClient apply(GravitinoIdentity identity) {
      clients.incrementAndGet();
      GravitinoClient client = mock(GravitinoClient.class);
      when(client.loadCatalog(anyString()))
          .thenAnswer(
              invocation -> {
                loads.incrementAndGet();
                Catalog catalog = mock(Catalog.class);
                when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
                when(catalog.name()).thenReturn(invocation.getArgument(0));
                return catalog;
              });
      // Closing twice must not be counted twice: the shutdown path closes explicitly and the
      // removal listener may then fire for the same client.
      AtomicBoolean closed = new AtomicBoolean(false);
      synchronized (closedFlags) {
        closedFlags.add(closed);
      }
      doAnswer(
              invocation -> {
                closed.set(true);
                return null;
              })
          .when(client)
          .close();
      return client;
    }

    int clientCount() {
      return clients.get();
    }

    int loadCount() {
      return loads.get();
    }

    int closedCount() {
      synchronized (closedFlags) {
        return (int) closedFlags.stream().filter(AtomicBoolean::get).count();
      }
    }
  }
}
