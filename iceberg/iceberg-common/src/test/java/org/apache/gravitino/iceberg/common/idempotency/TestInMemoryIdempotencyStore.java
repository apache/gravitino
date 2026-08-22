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
package org.apache.gravitino.iceberg.common.idempotency;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore.ReserveResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestInMemoryIdempotencyStore {

  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String OTHER_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073990";
  private static final String BINDING = "POST v1/cat1/namespaces/ns1/tables";
  private static final long FAR_FUTURE_MS = Long.MAX_VALUE;

  private InMemoryIdempotencyStore store;

  @BeforeEach
  void setUp() {
    store = newStore(ImmutableMap.of());
  }

  @AfterEach
  void tearDown() {
    store.close();
  }

  @Test
  void testReserveClaimsKeyOnlyOnce() {
    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertEquals(ReserveResult.DUPLICATE, store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertEquals(
        ReserveResult.RESERVED, store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testLoadReturnsEmptyUntilFinalized() {
    store.reserve(KEY, BINDING, FAR_FUTURE_MS);
    Assertions.assertEquals(Optional.empty(), store.load(KEY));

    store.finalizeRecord(KEY, 200, "{\"a\":1}");

    IdempotencyRecord record = store.load(KEY).orElseThrow(AssertionError::new);
    Assertions.assertTrue(record.isFinalized());
    Assertions.assertEquals(KEY, record.idempotencyKey());
    Assertions.assertEquals(BINDING, record.operationBinding());
    Assertions.assertEquals(200, record.httpStatus().intValue());
    Assertions.assertEquals("{\"a\":1}", record.responseSummary());
  }

  @Test
  void testLoadUnknownKeyReturnsEmpty() {
    Assertions.assertEquals(Optional.empty(), store.load(KEY));
  }

  @Test
  void testFinalizeWithoutBodyIsReplayable() {
    store.reserve(KEY, BINDING, FAR_FUTURE_MS);
    store.finalizeRecord(KEY, 204, null);

    IdempotencyRecord record = store.load(KEY).orElseThrow(AssertionError::new);
    Assertions.assertEquals(204, record.httpStatus().intValue());
    Assertions.assertNull(record.responseSummary());
  }

  @Test
  void testFinalizeDoesNotResurrectAPurgedRecord() {
    store.finalizeRecord(KEY, 200, "{}");
    Assertions.assertEquals(Optional.empty(), store.load(KEY));
    Assertions.assertEquals(0, store.size());
  }

  @Test
  void testReleaseFreesTheKeyForRetry() {
    store.reserve(KEY, BINDING, FAR_FUTURE_MS);
    store.release(KEY);

    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testReleaseKeepsAFinalizedRecord() {
    store.reserve(KEY, BINDING, FAR_FUTURE_MS);
    store.finalizeRecord(KEY, 200, "{}");
    store.release(KEY);

    Assertions.assertTrue(store.load(KEY).isPresent());
    Assertions.assertEquals(ReserveResult.DUPLICATE, store.reserve(KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testPurgeExpiredRemovesOnlyElapsedRecords() {
    long now = System.currentTimeMillis();
    store.reserve(KEY, BINDING, now - 1);
    store.reserve(OTHER_KEY, BINDING, now + 60_000);

    Assertions.assertEquals(1, store.purgeExpired(now));
    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertEquals(
        ReserveResult.DUPLICATE, store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testSizeBoundEvictsRecords() throws InterruptedException {
    InMemoryIdempotencyStore boundedStore =
        newStore(ImmutableMap.of(IcebergConstants.ICEBERG_IDEMPOTENCY_MAX_ENTRIES, "2"));
    try {
      for (int i = 0; i < 50; i++) {
        boundedStore.reserve(String.format("key-%d", i), BINDING, FAR_FUTURE_MS);
      }
      // Caffeine evicts during maintenance, so the bound is reached shortly after the writes
      // rather than synchronously with them.
      long retained = boundedStore.size();
      for (int attempt = 0; attempt < 100 && retained > 2; attempt++) {
        Thread.sleep(20);
        retained = boundedStore.size();
      }
      Assertions.assertTrue(retained <= 2, "expected at most 2 retained records, got " + retained);
    } finally {
      boundedStore.close();
    }
  }

  @Test
  void testConcurrentReserveHasASingleWinner() throws InterruptedException {
    int threads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    AtomicInteger reserved = new AtomicInteger();
    try {
      for (int i = 0; i < threads; i++) {
        executor.submit(
            () -> {
              try {
                start.await();
                if (store.reserve(KEY, BINDING, FAR_FUTURE_MS) == ReserveResult.RESERVED) {
                  reserved.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      Assertions.assertTrue(done.await(30, TimeUnit.SECONDS));
      Assertions.assertEquals(1, reserved.get());
    } finally {
      executor.shutdownNow();
    }
  }

  private static InMemoryIdempotencyStore newStore(Map<String, String> properties) {
    InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
    store.initialize(properties);
    return store;
  }
}
