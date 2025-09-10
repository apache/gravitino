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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Test class for SegmentedLock. */
public class TestSegmentedLock {

  @Test
  void testPowerOfTwoRounding() {
    // Test rounding up to power of 2
    assertEquals(4, new SegmentedLock(3).getNumSegments());
    assertEquals(8, new SegmentedLock(5).getNumSegments());
    assertEquals(8, new SegmentedLock(7).getNumSegments());
    assertEquals(16, new SegmentedLock(9).getNumSegments());
    assertEquals(16, new SegmentedLock(15).getNumSegments());
    assertEquals(32, new SegmentedLock(17).getNumSegments());
    assertEquals(32, new SegmentedLock(31).getNumSegments());
  }

  // ========== Basic Locking Tests ==========

  @Test
  void testBasicLocking() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    lock.withLock("key1", () -> counter.incrementAndGet());
    assertEquals(1, counter.get());

    lock.withLock("key2", () -> counter.incrementAndGet());
    assertEquals(2, counter.get());
  }

  @Test
  void testLockingWithReturnValue() {
    SegmentedLock lock = new SegmentedLock(4);

    String result = lock.withLock("key1", () -> "test result");
    assertEquals("test result", result);

    Integer number = lock.withLock("key2", () -> 42);
    assertEquals(42, number);
  }

  // ========== Concurrency Tests ==========

  @Test
  @Timeout(30)
  void testConcurrentSegmentAccess() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(8);
    AtomicInteger counter = new AtomicInteger(0);
    int numThreads = 16;
    int operationsPerThread = 50;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              for (int j = 0; j < operationsPerThread; j++) {
                String key = "key" + (threadId % 8);
                lock.withLock(key, () -> counter.incrementAndGet());
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              finishLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(finishLatch.await(30, TimeUnit.SECONDS));
    assertEquals(numThreads * operationsPerThread, counter.get());

    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  void testSegmentDistribution() {
    SegmentedLock lock = new SegmentedLock(4);

    // Test that different keys can be processed without exceptions
    for (int i = 0; i < 100; i++) {
      String key = "key" + i;
      assertDoesNotThrow(() -> lock.withLock(key, () -> {}));
    }
  }

  @Test
  void testNullKeyHandling() {
    SegmentedLock lock = new SegmentedLock(4);

    assertDoesNotThrow(() -> lock.withLock(null, () -> {}));
    assertDoesNotThrow(() -> lock.withLock(null, () -> "result"));
  }

  @Test
  void testDifferentKeyTypes() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    lock.withLock("stringKey", () -> counter.incrementAndGet());
    lock.withLock(123, () -> counter.incrementAndGet());
    lock.withLock(123L, () -> counter.incrementAndGet());
    lock.withLock(123.45, () -> counter.incrementAndGet());
    lock.withLock(new Object(), () -> counter.incrementAndGet());

    assertEquals(5, counter.get());
  }

  @Test
  void testInterruptedExceptionHandling() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    Thread testThread =
        new Thread(
            () -> {
              try {
                lock.withLock(
                    "testKey",
                    () -> {
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during operation", e);
                      }
                      counter.incrementAndGet();
                    });
              } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("interrupted"));
              }
            });

    testThread.start();
    Thread.sleep(100);
    testThread.interrupt();
    testThread.join(2000);

    assertEquals(0, counter.get());
  }

  @Test
  void testLockReentrancy() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    lock.withLock(
        "sameKey",
        () -> {
          counter.incrementAndGet();
          lock.withLock(
              "sameKey",
              () -> {
                counter.incrementAndGet();
                lock.withLock("sameKey", () -> counter.incrementAndGet());
              });
        });

    assertEquals(3, counter.get());
  }

  // ========== Performance and Stress Tests ==========

  @Test
  @Timeout(60)
  void testHighConcurrencyStress() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(16);
    AtomicInteger counter = new AtomicInteger(0);
    int numThreads = 32;
    int operationsPerThread = 1000;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              for (int j = 0; j < operationsPerThread; j++) {
                String key = "key" + (threadId % 16);
                lock.withLock(key, () -> counter.incrementAndGet());
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              finishLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(finishLatch.await(60, TimeUnit.SECONDS));
    assertEquals(numThreads * operationsPerThread, counter.get());

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  void testLargeNumberOfSegments() {
    SegmentedLock lock = new SegmentedLock(1024);
    assertEquals(1024, lock.getNumSegments());

    AtomicInteger counter = new AtomicInteger(0);
    for (int i = 0; i < 1000; i++) {
      lock.withLock("key" + i, () -> counter.incrementAndGet());
    }
    assertEquals(1000, counter.get());
  }

  @Test
  void testLongRunningOperations() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger completed = new AtomicInteger(0);
    int numThreads = 4;
    int operationDurationMs = 100;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              String key = "key" + threadId;
              lock.withLock(
                  key,
                  () -> {
                    try {
                      Thread.sleep(operationDurationMs);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    completed.incrementAndGet();
                  });
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              finishLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(finishLatch.await(30, TimeUnit.SECONDS));
    assertEquals(numThreads, completed.get());

    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  void testGlobalClearingBlocksOtherOperations() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch clearingStarted = new CountDownLatch(1);
    CountDownLatch clearingFinished = new CountDownLatch(1);
    CountDownLatch operationFinished = new CountDownLatch(1);

    // Start a long-running clearing operation
    Thread clearingThread =
        new Thread(
            () -> {
              try {
                lock.withGlobalLock(
                    () -> {
                      clearingStarted.countDown();
                      try {
                        Thread.sleep(100); // Simulate long clearing operation
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      counter.incrementAndGet();
                    });
              } finally {
                clearingFinished.countDown();
              }
            });

    // Start a regular operation that should wait
    Thread operationThread =
        new Thread(
            () -> {
              try {
                clearingStarted.await(); // Wait for clearing to start
                lock.withLock("key1", () -> counter.incrementAndGet());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                operationFinished.countDown();
              }
            });

    clearingThread.start();
    operationThread.start();

    // Wait for clearing to start
    assertTrue(clearingStarted.await(1, TimeUnit.SECONDS));

    // Check that clearing is in progress
    assertTrue(lock.isClearing());

    // Wait for clearing to finish
    assertTrue(clearingFinished.await(2, TimeUnit.SECONDS));

    // Wait for operation to finish
    assertTrue(operationFinished.await(2, TimeUnit.SECONDS));

    // Both operations should have completed
    assertEquals(2, counter.get());

    // Clearing should no longer be in progress
    assertFalse(lock.isClearing());

    clearingThread.join();
    operationThread.join();
  }

  @Test
  void testConcurrentGlobalClearing() {
    SegmentedLock lock = new SegmentedLock(4);

    // First clearing operation should succeed
    assertDoesNotThrow(() -> lock.withGlobalLock(() -> {}));

    // Second concurrent clearing operation should fail
    assertThrows(
        IllegalStateException.class,
        () -> {
          lock.withGlobalLock(
              () -> {
                // Try to start another clearing operation
                lock.withGlobalLock(() -> {});
              });
        });
  }
}
