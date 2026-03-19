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
package org.apache.gravitino.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClassLoaderPool {

  private IsolatedClassLoader createDummyClassLoader() {
    return new IsolatedClassLoader(
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testAcquireFirstTimeCreatesNewEntry() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
      PooledClassLoaderEntry entry = pool.acquire(key, this::createDummyClassLoader);

      Assertions.assertNotNull(entry);
      Assertions.assertEquals(1, entry.refCount());
      Assertions.assertNotNull(entry.classLoader());
      Assertions.assertEquals(1, pool.size());
    }
  }

  @Test
  public void testAcquireSameKeyReusesEntry() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
      PooledClassLoaderEntry entry1 = pool.acquire(key, this::createDummyClassLoader);
      PooledClassLoaderEntry entry2 = pool.acquire(key, this::createDummyClassLoader);

      Assertions.assertSame(entry1, entry2);
      Assertions.assertEquals(2, entry1.refCount());
      Assertions.assertEquals(1, pool.size());
    }
  }

  @Test
  public void testDifferentKeysCreateDifferentEntries() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key1 = new ClassLoaderKey("iceberg", null, null, null, null);
      ClassLoaderKey key2 = new ClassLoaderKey("hive", null, null, null, null);

      PooledClassLoaderEntry entry1 = pool.acquire(key1, this::createDummyClassLoader);
      PooledClassLoaderEntry entry2 = pool.acquire(key2, this::createDummyClassLoader);

      Assertions.assertNotSame(entry1, entry2);
      Assertions.assertEquals(1, entry1.refCount());
      Assertions.assertEquals(1, entry2.refCount());
      Assertions.assertEquals(2, pool.size());
    }
  }

  @Test
  public void testReleaseDecrementsRefCount() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
      PooledClassLoaderEntry entry = pool.acquire(key, this::createDummyClassLoader);
      pool.acquire(key, this::createDummyClassLoader);
      Assertions.assertEquals(2, entry.refCount());

      pool.release(entry);
      Assertions.assertEquals(1, entry.refCount());
      Assertions.assertEquals(1, pool.size());
    }
  }

  @Test
  public void testReleaseToZeroRemovesEntry() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
      PooledClassLoaderEntry entry = pool.acquire(key, this::createDummyClassLoader);
      Assertions.assertEquals(1, pool.size());

      pool.release(entry);
      Assertions.assertEquals(0, pool.size());
    }
  }

  @Test
  public void testDoubleReleaseIsHandledGracefully() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
      PooledClassLoaderEntry entry = pool.acquire(key, this::createDummyClassLoader);

      pool.release(entry); // refCount 1 -> 0, entry removed
      Assertions.assertEquals(0, pool.size());

      // Second release: entry is already removed from the pool, should log a warning
      // but not throw or corrupt state
      Assertions.assertDoesNotThrow(() -> pool.release(entry));
      Assertions.assertEquals(0, pool.size());

      // Verify the key can be re-acquired cleanly with no corrupted state
      PooledClassLoaderEntry fresh = pool.acquire(key, this::createDummyClassLoader);
      Assertions.assertNotNull(fresh);
      Assertions.assertEquals(1, fresh.refCount());
      Assertions.assertEquals(1, pool.size());
    }
  }

  @Test
  public void testReleaseOneDoesNotAffectOtherSameType() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
      PooledClassLoaderEntry entryA = pool.acquire(key, this::createDummyClassLoader);
      pool.acquire(key, this::createDummyClassLoader); // entryB shares same entry
      Assertions.assertEquals(2, entryA.refCount());

      pool.release(entryA);
      Assertions.assertEquals(1, entryA.refCount());
      Assertions.assertEquals(1, pool.size());
    }
  }

  @Test
  public void testConcurrentAcquireAndRelease() throws Exception {
    ClassLoaderPool pool = new ClassLoaderPool();
    ClassLoaderKey key = new ClassLoaderKey("iceberg", null, null, null, null);
    int threadCount = 20;
    CyclicBarrier barrier = new CyclicBarrier(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    List<Future<PooledClassLoaderEntry>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      futures.add(
          executor.submit(
              () -> {
                barrier.await();
                return pool.acquire(key, this::createDummyClassLoader);
              }));
    }

    List<PooledClassLoaderEntry> entries = new ArrayList<>();
    for (Future<PooledClassLoaderEntry> future : futures) {
      entries.add(future.get(5, TimeUnit.SECONDS));
    }

    // All should reference the same entry
    PooledClassLoaderEntry first = entries.get(0);
    for (PooledClassLoaderEntry entry : entries) {
      Assertions.assertSame(first, entry);
    }
    Assertions.assertEquals(threadCount, first.refCount());
    Assertions.assertEquals(1, pool.size());

    // Now release all concurrently
    CyclicBarrier releaseBarrier = new CyclicBarrier(threadCount);
    List<Future<?>> releaseFutures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      releaseFutures.add(
          executor.submit(
              () -> {
                releaseBarrier.await();
                pool.release(first);
                return null;
              }));
    }

    for (Future<?> future : releaseFutures) {
      future.get(5, TimeUnit.SECONDS);
    }

    Assertions.assertEquals(0, pool.size());

    executor.shutdown();
    pool.close();
  }

  @Test
  public void testCloseCleanupAllEntries() {
    ClassLoaderPool pool = new ClassLoaderPool();
    ClassLoaderKey key1 = new ClassLoaderKey("iceberg", null, null, null, null);
    ClassLoaderKey key2 = new ClassLoaderKey("hive", null, null, null, null);
    pool.acquire(key1, this::createDummyClassLoader);
    pool.acquire(key2, this::createDummyClassLoader);
    Assertions.assertEquals(2, pool.size());

    pool.close();
    Assertions.assertEquals(0, pool.size());
  }

  @Test
  public void testKeyWithPackageProperty() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key1 = new ClassLoaderKey("iceberg", "/path/a", null, null, null);
      ClassLoaderKey key2 = new ClassLoaderKey("iceberg", "/path/b", null, null, null);

      PooledClassLoaderEntry entry1 = pool.acquire(key1, this::createDummyClassLoader);
      PooledClassLoaderEntry entry2 = pool.acquire(key2, this::createDummyClassLoader);

      Assertions.assertNotSame(entry1, entry2);
      Assertions.assertEquals(2, pool.size());
    }
  }

  @Test
  public void testKeyWithAuthorizationPath() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key1 = new ClassLoaderKey("iceberg", null, "/auth/ranger", null, null);
      ClassLoaderKey key2 = new ClassLoaderKey("iceberg", null, null, null, null);

      PooledClassLoaderEntry entry1 = pool.acquire(key1, this::createDummyClassLoader);
      PooledClassLoaderEntry entry2 = pool.acquire(key2, this::createDummyClassLoader);

      Assertions.assertNotSame(entry1, entry2);
      Assertions.assertEquals(2, pool.size());
    }
  }

  @Test
  public void testDifferentKerberosPrincipalsCreateDifferentEntries() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key1 =
          new ClassLoaderKey("iceberg", null, null, "user-A@REALM", "/keytabs/a.keytab");
      ClassLoaderKey key2 =
          new ClassLoaderKey("iceberg", null, null, "user-B@REALM", "/keytabs/b.keytab");
      ClassLoaderKey key3 = new ClassLoaderKey("iceberg", null, null, null, null);

      PooledClassLoaderEntry entry1 = pool.acquire(key1, this::createDummyClassLoader);
      PooledClassLoaderEntry entry2 = pool.acquire(key2, this::createDummyClassLoader);
      PooledClassLoaderEntry entry3 = pool.acquire(key3, this::createDummyClassLoader);

      Assertions.assertNotSame(entry1, entry2);
      Assertions.assertNotSame(entry1, entry3);
      Assertions.assertNotSame(entry2, entry3);
      Assertions.assertEquals(3, pool.size());
    }
  }

  @Test
  public void testAcquireAfterCloseThrows() {
    ClassLoaderPool pool = new ClassLoaderPool();
    pool.close();
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            pool.acquire(
                new ClassLoaderKey("iceberg", null, null, null, null),
                this::createDummyClassLoader));
  }

  @Test
  public void testConcurrentAcquireDuringClose() throws Exception {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    ClassLoaderPool pool = new ClassLoaderPool();

    // Pre-populate the pool with entries that have active refCounts > 0
    // to exercise the force-cleanup path in close()
    ClassLoaderKey heldKey = new ClassLoaderKey("held", null, null, null, null);
    pool.acquire(heldKey, this::createDummyClassLoader); // refCount=1, never released

    // Race: some threads try to acquire while another thread closes
    CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      final int idx = i;
      futures.add(
          executor.submit(
              () -> {
                barrier.await();
                ClassLoaderKey k = new ClassLoaderKey("type-" + idx, null, null, null, null);
                try {
                  PooledClassLoaderEntry e = pool.acquire(k, this::createDummyClassLoader);
                  // If acquire succeeded, release it
                  pool.release(e);
                } catch (IllegalStateException expected) {
                  // Pool was closed before or during acquire — expected
                }
                return null;
              }));
    }

    // Close from the main thread racing with acquires — this force-cleans
    // the held entry (refCount > 0) and drains any concurrent insertions
    barrier.await();
    pool.close();

    for (Future<?> future : futures) {
      future.get(5, TimeUnit.SECONDS);
    }

    // After close + all threads done, pool must be empty
    Assertions.assertEquals(0, pool.size());
    executor.shutdown();
  }

  @Test
  public void testSameKerberosPrincipalSharesEntry() {
    try (ClassLoaderPool pool = new ClassLoaderPool()) {
      ClassLoaderKey key1 =
          new ClassLoaderKey("iceberg", null, null, "user-A@REALM", "/keytabs/a.keytab");
      ClassLoaderKey key2 =
          new ClassLoaderKey("iceberg", null, null, "user-A@REALM", "/keytabs/a.keytab");

      PooledClassLoaderEntry entry1 = pool.acquire(key1, this::createDummyClassLoader);
      PooledClassLoaderEntry entry2 = pool.acquire(key2, this::createDummyClassLoader);

      Assertions.assertSame(entry1, entry2);
      Assertions.assertEquals(2, entry1.refCount());
      Assertions.assertEquals(1, pool.size());
    }
  }
}
