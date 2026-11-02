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

package org.apache.gravitino.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

/** Tests for when entity caching is disabled. */
public class TestNoOpsCache {

  @Test
  void testWithCacheLockDoesNotSerializeUnrelatedReads() throws Exception {
    NoOpsCache cache = new NoOpsCache(new Config(false) {});
    EntityCacheKey userKey =
        EntityCacheKey.of(NameIdentifier.of("metalake", "user-a"), Entity.EntityType.USER);
    EntityCacheKey tableKey =
        EntityCacheKey.of(
            NameIdentifier.of("metalake", "catalog", "schema", "table-b"), Entity.EntityType.TABLE);

    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> userRead =
          executor.submit(
              () ->
                  cache.withCacheLock(
                      userKey,
                      () -> holdAndReturn(entered, release, inFlight, maxInFlight, "user")));
      Future<String> tableRead =
          executor.submit(
              () ->
                  cache.withCacheLock(
                      tableKey,
                      () -> holdAndReturn(entered, release, inFlight, maxInFlight, "table")));

      assertTrue(
          entered.await(2, TimeUnit.SECONDS),
          "Cache-disabled reads of unrelated entities must overlap. A single lock serialized them.");
      release.countDown();
      assertEquals("user", userRead.get(2, TimeUnit.SECONDS));
      assertEquals("table", tableRead.get(2, TimeUnit.SECONDS));
      assertEquals(2, maxInFlight.get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void testWithCacheLockPropagatesActionResultAndException() throws Exception {
    NoOpsCache cache = new NoOpsCache(new Config(false) {});
    EntityCacheKey key =
        EntityCacheKey.of(NameIdentifier.of("metalake", "user"), Entity.EntityType.USER);

    assertEquals("loaded", cache.withCacheLock(key, () -> "loaded"));
    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> cache.withCacheLock(key, this::fail));
    assertEquals("backend failed", failure.getMessage());
  }

  @Test
  void testWithCacheLockRejectsNullArguments() throws Exception {
    NoOpsCache cache = new NoOpsCache(new Config(false) {});
    EntityCacheKey key =
        EntityCacheKey.of(NameIdentifier.of("metalake", "user"), Entity.EntityType.USER);

    assertThrows(IllegalArgumentException.class, () -> cache.withCacheLock(null, () -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> cache.withCacheLock(key, (EntityCache.ThrowingRunnable<Exception>) null));
    assertThrows(IllegalArgumentException.class, () -> cache.withCacheLock(null, () -> "loaded"));
    assertThrows(
        IllegalArgumentException.class,
        () -> cache.withCacheLock(key, (EntityCache.ThrowingSupplier<String, Exception>) null));
  }

  private void fail() {
    throw new IllegalStateException("backend failed");
  }

  private static String holdAndReturn(
      CountDownLatch entered,
      CountDownLatch release,
      AtomicInteger inFlight,
      AtomicInteger maxInFlight,
      String value)
      throws InterruptedException {
    int current = inFlight.incrementAndGet();
    maxInFlight.accumulateAndGet(current, Math::max);
    entered.countDown();
    try {
      if (!release.await(2, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for the other read to enter");
      }
      return value;
    } finally {
      inFlight.decrementAndGet();
    }
  }
}
