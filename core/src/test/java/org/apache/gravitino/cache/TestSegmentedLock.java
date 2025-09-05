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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Test class for SegmentedLock functionality */
public class TestSegmentedLock {

  @Test
  void testConstructorWithInvalidSegments() {
    // Test with non-positive values
    assertThrows(IllegalArgumentException.class, () -> new SegmentedLock(0));
    assertThrows(IllegalArgumentException.class, () -> new SegmentedLock(-1));
  }

  @Test
  void testConstructorWithValidSegments() {
    // Test with valid positive values
    SegmentedLock lock1 = new SegmentedLock(1);
    assertEquals(1, lock1.getNumSegments());

    SegmentedLock lock2 = new SegmentedLock(2);
    assertEquals(2, lock2.getNumSegments());

    // Test rounding up to power of 2
    SegmentedLock lock3 = new SegmentedLock(3);
    assertEquals(4, lock3.getNumSegments()); // 3 -> 4

    SegmentedLock lock5 = new SegmentedLock(5);
    assertEquals(8, lock5.getNumSegments()); // 5 -> 8

    SegmentedLock lock7 = new SegmentedLock(7);
    assertEquals(8, lock7.getNumSegments()); // 7 -> 8

    SegmentedLock lock8 = new SegmentedLock(8);
    assertEquals(8, lock8.getNumSegments()); // Already power of 2
  }

  @Test
  void testWithLock() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    // Test basic locking functionality
    lock.withLock("key1", () -> counter.incrementAndGet());
    assertEquals(1, counter.get());

    lock.withLock("key2", () -> counter.incrementAndGet());
    assertEquals(2, counter.get());
  }

  @Test
  void testWithLockReturnValue() {
    SegmentedLock lock = new SegmentedLock(4);

    // Test with return value
    String result = lock.withLock("key1", () -> "test result");
    assertEquals("test result", result);

    Integer number = lock.withLock("key2", () -> 42);
    assertEquals(42, number);
  }

  @Test
  void testWithAllLocks() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    // Test global locking
    lock.withAllLocks(() -> counter.incrementAndGet());
    assertEquals(1, counter.get());

    lock.withAllLocks(() -> counter.addAndGet(10));
    assertEquals(11, counter.get());
  }

  @Test
  void testWithAllLocksReturnValue() {
    SegmentedLock lock = new SegmentedLock(4);

    // Test with return value
    String result = lock.withAllLocks(() -> "global result");
    assertEquals("global result", result);

    Integer number = lock.withAllLocks(() -> 100);
    assertEquals(100, number);
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);
    int numThreads = 8;
    int operationsPerThread = 100;

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
                String key = "key" + (threadId % 4); // Use different keys for different segments
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
  void testWithAllLocksAndThrow() throws Exception {
    SegmentedLock lock = new SegmentedLock(4);

    // Test with ThrowingRunnable
    AtomicInteger counter = new AtomicInteger(0);
    lock.withAllLocksAndThrow(
        (EntityCache.ThrowingRunnable<Exception>)
            () -> {
              counter.incrementAndGet();
            });
    assertEquals(1, counter.get());

    // Test with ThrowingSupplier
    String result =
        lock.withAllLocksAndThrow(
            (EntityCache.ThrowingSupplier<String, Exception>) () -> "throwing result");
    assertEquals("throwing result", result);
  }

  @Test
  void testWithAllLocksAndThrowException() {
    SegmentedLock lock = new SegmentedLock(4);

    // Test that exceptions are properly propagated
    assertThrows(
        RuntimeException.class,
        () ->
            lock.withAllLocksAndThrow(
                (EntityCache.ThrowingRunnable<Exception>)
                    () -> {
                      throw new RuntimeException("test exception");
                    }));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            lock.withAllLocksAndThrow(
                (EntityCache.ThrowingSupplier<String, Exception>)
                    () -> {
                      throw new IllegalArgumentException("test exception");
                    }));
  }

  @Test
  void testSegmentDistribution() {
    SegmentedLock lock = new SegmentedLock(4);

    // Test that different keys get different segment locks
    // This is a basic test - in practice, hash collisions could occur
    for (int i = 0; i < 100; i++) {
      String key = "key" + i;
      // This should not throw an exception
      lock.withLock(key, () -> {});
    }
  }

  @Test
  void testNullKeyHandling() {
    SegmentedLock lock = new SegmentedLock(4);

    // Test that null keys are handled gracefully
    assertDoesNotThrow(() -> lock.withLock(null, () -> {}));
    assertDoesNotThrow(() -> lock.withLock(null, () -> "result"));
  }

  @Test
  void testRoundingUpToPowerOf2() {
    // Test various non-power-of-2 values
    assertEquals(1, new SegmentedLock(1).getNumSegments());
    assertEquals(2, new SegmentedLock(2).getNumSegments());
    assertEquals(4, new SegmentedLock(3).getNumSegments());
    assertEquals(4, new SegmentedLock(4).getNumSegments());
    assertEquals(8, new SegmentedLock(5).getNumSegments());
    assertEquals(8, new SegmentedLock(6).getNumSegments());
    assertEquals(8, new SegmentedLock(7).getNumSegments());
    assertEquals(8, new SegmentedLock(8).getNumSegments());
    assertEquals(16, new SegmentedLock(9).getNumSegments());
    assertEquals(16, new SegmentedLock(10).getNumSegments());
    assertEquals(16, new SegmentedLock(15).getNumSegments());
    assertEquals(16, new SegmentedLock(16).getNumSegments());
    assertEquals(32, new SegmentedLock(17).getNumSegments());
    assertEquals(32, new SegmentedLock(31).getNumSegments());
    assertEquals(32, new SegmentedLock(32).getNumSegments());
  }

  @Test
  void testInterruptedExceptionHandling() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    // Create a thread that will be interrupted
    Thread testThread =
        new Thread(
            () -> {
              try {
                lock.withLock(
                    "testKey",
                    () -> {
                      // Simulate long-running operation
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during operation", e);
                      }
                      counter.incrementAndGet();
                    });
              } catch (RuntimeException e) {
                // Expected when interrupted
                assertTrue(e.getMessage().contains("interrupted"));
              }
            });

    testThread.start();
    Thread.sleep(100); // Let the thread start and acquire the lock
    testThread.interrupt();
    testThread.join(2000); // Wait for thread to finish

    // The counter should not be incremented because the operation was interrupted
    assertEquals(0, counter.get());
  }

  @Test
  void testLockReentrancy() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    // Test that the same thread can acquire the same lock multiple times
    lock.withLock(
        "sameKey",
        () -> {
          counter.incrementAndGet();
          lock.withLock(
              "sameKey",
              () -> {
                counter.incrementAndGet();
                lock.withLock(
                    "sameKey",
                    () -> {
                      counter.incrementAndGet();
                    });
              });
        });

    assertEquals(3, counter.get());
  }

  @Test
  void testDifferentKeyTypes() {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);

    // Test with different key types
    lock.withLock("stringKey", () -> counter.incrementAndGet());
    lock.withLock(123, () -> counter.incrementAndGet());
    lock.withLock(123L, () -> counter.incrementAndGet());
    lock.withLock(123.45, () -> counter.incrementAndGet());
    lock.withLock(new Object(), () -> counter.incrementAndGet());

    assertEquals(5, counter.get());
  }

  @Test
  void testLargeNumberOfSegments() {
    // Test with a large number of segments
    SegmentedLock lock = new SegmentedLock(1024);
    assertEquals(1024, lock.getNumSegments());

    // Test that it still works correctly
    AtomicInteger counter = new AtomicInteger(0);
    for (int i = 0; i < 1000; i++) {
      lock.withLock("key" + i, () -> counter.incrementAndGet());
    }
    assertEquals(1000, counter.get());
  }

  @Test
  void testConcurrentWithAllLocks() throws InterruptedException {
    SegmentedLock lock = new SegmentedLock(4);
    AtomicInteger counter = new AtomicInteger(0);
    int numThreads = 4;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(
          () -> {
            try {
              startLatch.await();
              lock.withAllLocks(
                  () -> {
                    int current = counter.get();
                    try {
                      Thread.sleep(10); // Simulate some work
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException("Interrupted during work simulation", e);
                    }
                    counter.set(current + 1);
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

    // All threads should have executed sequentially due to withAllLocks
    assertEquals(numThreads, counter.get());

    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }
}
