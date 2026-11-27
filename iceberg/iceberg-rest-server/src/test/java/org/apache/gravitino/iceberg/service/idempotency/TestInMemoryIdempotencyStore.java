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
package org.apache.gravitino.iceberg.service.idempotency;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestInMemoryIdempotencyStore {

  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String KEY_UPPER = "017F22E2-79B0-7CC3-98C4-DC0C0C07398F";
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
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertFalse(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertTrue(store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testKeyCaseVariantsAddressTheSameRecord() {
    Assertions.assertTrue(store.reserve(KEY_UPPER, BINDING, FAR_FUTURE_MS));
    Assertions.assertFalse(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertEquals(1, store.size());

    // Finalizing under one casing has to be replayable under the other, otherwise a retry that
    // changes case re-executes the mutation.
    store.finalizeRecord(KEY_UPPER, 200, "{\"a\":1}");
    IdempotencyRecord record = store.load(KEY).orElseThrow(AssertionError::new);
    Assertions.assertEquals(KEY, record.idempotencyKey());
    Assertions.assertEquals(200, record.httpStatus().intValue());
  }

  @Test
  void testLoadReturnsEmptyUntilFinalized() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
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
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
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
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    store.release(KEY);

    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testReleaseKeepsAFinalizedRecord() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    store.finalizeRecord(KEY, 200, "{}");
    store.release(KEY);

    Assertions.assertTrue(store.load(KEY).isPresent());
    Assertions.assertFalse(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
  }

  @Test
  void testReReserveAfterReleaseReusesFixedClaim() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    store.release(KEY);
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));

    store.finalizeRecord(KEY, 201, "{\"ok\":true}");
    Assertions.assertEquals(
        "{\"ok\":true}", store.load(KEY).orElseThrow(AssertionError::new).responseSummary());
  }

  @Test
  void testDifferentBindingStillReturnsDuplicateWithoutReplacingOriginal() {
    String otherBinding = "DELETE v1/cat1/namespaces/ns1/tables/t1";
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));

    Assertions.assertFalse(store.reserve(KEY, otherBinding, FAR_FUTURE_MS));
    Assertions.assertEquals(Optional.empty(), store.load(KEY));

    store.finalizeRecord(KEY, 201, "{}");
    Assertions.assertFalse(store.reserve(KEY, otherBinding, FAR_FUTURE_MS));
    Assertions.assertEquals(
        BINDING, store.load(KEY).orElseThrow(AssertionError::new).operationBinding());
  }

  @Test
  void testPurgeExpiredRemovesOnlyElapsedRecords() {
    AtomicLong now = new AtomicLong(1_000_000L);
    InMemoryIdempotencyStore clockedStore = newStore(ImmutableMap.of(), now::get);
    try {
      clockedStore.reserve(KEY, BINDING, now.get() + 1_000L);
      clockedStore.reserve(OTHER_KEY, BINDING, now.get() + 60_000L);

      // Only the first record's window has elapsed.
      now.addAndGet(1_000L);
      Assertions.assertEquals(1, clockedStore.purgeExpired(now.get()));

      Assertions.assertTrue(clockedStore.reserve(KEY, BINDING, now.get() + 1_000L));
      Assertions.assertFalse(clockedStore.reserve(OTHER_KEY, BINDING, now.get() + 60_000L));
    } finally {
      clockedStore.close();
    }
  }

  @Test
  void testRecordExpiresAtItsDeadlineAndFinalizeDoesNotExtendIt() {
    AtomicLong now = new AtomicLong(1_000_000L);
    InMemoryIdempotencyStore clockedStore = newStore(ImmutableMap.of(), now::get);
    try {
      long deadline = now.get() + 1_000L;
      Assertions.assertTrue(clockedStore.reserve(KEY, BINDING, deadline));

      // Finalize halfway through the window. A cache expiring on last-write would restart the
      // clock here and keep the record past the lifetime advertised to clients.
      now.set(deadline - 500L);
      clockedStore.finalizeRecord(KEY, 200, "{}");
      Assertions.assertTrue(clockedStore.load(KEY).isPresent());

      now.set(deadline);
      Assertions.assertEquals(Optional.empty(), clockedStore.load(KEY));
      Assertions.assertTrue(clockedStore.reserve(KEY, BINDING, now.get() + 1_000L));
    } finally {
      clockedStore.close();
    }
  }

  @Test
  void testSizeBoundEvictsRecords() throws InterruptedException {
    InMemoryIdempotencyStore boundedStore =
        newStore(ImmutableMap.of(IcebergConstants.ICEBERG_IDEMPOTENCY_MAX_ENTRIES, "2"));
    try {
      for (int i = 0; i < 50; i++) {
        boundedStore.reserve(uuidKey(i), BINDING, FAR_FUTURE_MS);
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
    List<Boolean> results = Collections.synchronizedList(new ArrayList<>());
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try {
      for (int i = 0; i < threads; i++) {
        executor.submit(
            () -> {
              try {
                start.await();
                results.add(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
              } catch (Throwable t) {
                failure.compareAndSet(null, t);
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      Assertions.assertTrue(done.await(30, TimeUnit.SECONDS));
      Assertions.assertNull(failure.get(), "no reserve should fail");
      Assertions.assertEquals(threads, results.size(), "every caller should report a result");
      Assertions.assertEquals(1, results.stream().filter(Boolean::booleanValue).count());
    } finally {
      executor.shutdownNow();
    }
  }

  /** Builds distinct valid UUIDv7 keys, since the store rejects anything else. */
  private static String uuidKey(int index) {
    return String.format("017f22e2-79b0-7cc3-98c4-%012x", index);
  }

  private static InMemoryIdempotencyStore newStore(Map<String, String> properties) {
    return newStore(properties, System::currentTimeMillis);
  }

  private static InMemoryIdempotencyStore newStore(
      Map<String, String> properties, LongSupplier clock) {
    InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(clock);
    store.initialize(properties);
    return store;
  }
}
