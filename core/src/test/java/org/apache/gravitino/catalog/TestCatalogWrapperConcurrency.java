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
package org.apache.gravitino.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Verifies that {@link CatalogManager.CatalogWrapper} correctly serializes {@code close()} against
 * in-flight {@code doWithXxxOps()} and {@code doWithCapabilityOps()} calls, preventing the {@link
 * IsolatedClassLoader} from being torn down while operations are still executing.
 */
public class TestCatalogWrapperConcurrency {

  /**
   * close() must block until all concurrent doWithCapabilityOps() calls finish, and subsequent
   * doWithCapabilityOps() calls after close() must throw IllegalStateException.
   */
  @Test
  public void testCloseBlocksUntilInFlightCapabilityOpsComplete() throws Exception {
    BaseCatalog mockCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(mockCatalog.capability()).thenReturn(Capability.DEFAULT);

    IsolatedClassLoader mockCl = Mockito.mock(IsolatedClassLoader.class);
    Mockito.when(mockCl.withClassLoader(Mockito.any()))
        .thenAnswer(
            inv ->
                ((org.apache.gravitino.utils.ThrowableFunction<ClassLoader, ?>) inv.getArgument(0))
                    .apply(Thread.currentThread().getContextClassLoader()));

    CatalogManager.CatalogWrapper wrapper = new CatalogManager.CatalogWrapper(mockCatalog, mockCl);

    CountDownLatch opsStarted = new CountDownLatch(2);
    CountDownLatch permitClose = new CountDownLatch(1);
    AtomicInteger opsCompleted = new AtomicInteger(0);

    ExecutorService exec = Executors.newFixedThreadPool(3);
    try {
      // Two reader threads that hold the lock while waiting for permitClose.
      Callable<Void> op =
          () -> {
            wrapper.doWithCapabilityOps(
                cap -> {
                  opsStarted.countDown();
                  permitClose.await(5, TimeUnit.SECONDS);
                  opsCompleted.incrementAndGet();
                  return null;
                });
            return null;
          };

      Future<Void> f1 = exec.submit(op);
      Future<Void> f2 = exec.submit(op);

      // Wait for both ops to acquire the read lock before we start close().
      Assertions.assertTrue(opsStarted.await(5, TimeUnit.SECONDS));

      // Start close() on a separate thread; it should block waiting for readers.
      Future<Void> closeFuture =
          exec.submit(
              () -> {
                wrapper.close();
                return null;
              });

      // close() should not complete while readers hold the lock.
      Assertions.assertFalse(
          closeFuture.isDone(), "close() must not complete while ops are active");

      // Release readers.
      permitClose.countDown();
      f1.get(5, TimeUnit.SECONDS);
      f2.get(5, TimeUnit.SECONDS);

      // Now close() should complete.
      closeFuture.get(5, TimeUnit.SECONDS);
      Assertions.assertTrue(closeFuture.isDone());

      // Both ops finished before close() completed.
      Assertions.assertEquals(2, opsCompleted.get());

      // Subsequent capability ops must be rejected.
      Assertions.assertThrows(
          IllegalStateException.class, () -> wrapper.doWithCapabilityOps(cap -> null));
    } finally {
      exec.shutdownNow();
    }
  }

  /**
   * close() is idempotent: calling it a second time must not throw and must not call
   * classLoader.close() again.
   */
  @Test
  public void testCloseIsIdempotent() throws Exception {
    BaseCatalog mockCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(mockCatalog.capability()).thenReturn(Capability.DEFAULT);

    IsolatedClassLoader mockCl = Mockito.mock(IsolatedClassLoader.class);
    Mockito.when(mockCl.withClassLoader(Mockito.any()))
        .thenAnswer(
            inv ->
                ((org.apache.gravitino.utils.ThrowableFunction<ClassLoader, ?>) inv.getArgument(0))
                    .apply(Thread.currentThread().getContextClassLoader()));

    CatalogManager.CatalogWrapper wrapper = new CatalogManager.CatalogWrapper(mockCatalog, mockCl);

    wrapper.close();
    wrapper.close(); // must not throw

    // classLoader.close() should be called exactly once.
    Mockito.verify(mockCl, Mockito.times(1)).close();
  }

  /**
   * doWithCapabilityOps() after close() throws IllegalStateException, not a cryptic
   * NullPointerException or NoClassDefFoundError.
   */
  @Test
  public void testOpsAfterCloseThrowIllegalStateException() throws Exception {
    BaseCatalog mockCatalog = Mockito.mock(BaseCatalog.class);
    IsolatedClassLoader mockCl = Mockito.mock(IsolatedClassLoader.class);

    CatalogManager.CatalogWrapper wrapper = new CatalogManager.CatalogWrapper(mockCatalog, mockCl);
    Mockito.when(mockCl.withClassLoader(Mockito.any())).thenReturn(null);
    wrapper.close();

    Assertions.assertThrows(
        IllegalStateException.class, () -> wrapper.doWithCapabilityOps(cap -> null));
    Assertions.assertThrows(IllegalStateException.class, () -> wrapper.doWithTableOps(t -> null));
    Assertions.assertThrows(IllegalStateException.class, () -> wrapper.doWithSchemaOps(s -> null));
  }

  /**
   * Multiple concurrent close() calls must not result in multiple classLoader.close() invocations
   * (write-lock idempotency under concurrency).
   */
  @Test
  public void testConcurrentCloseCallsAreIdempotent() throws Exception {
    BaseCatalog mockCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(mockCatalog.capability()).thenReturn(Capability.DEFAULT);

    IsolatedClassLoader mockCl = Mockito.mock(IsolatedClassLoader.class);
    Mockito.when(mockCl.withClassLoader(Mockito.any()))
        .thenAnswer(
            inv ->
                ((org.apache.gravitino.utils.ThrowableFunction<ClassLoader, ?>) inv.getArgument(0))
                    .apply(Thread.currentThread().getContextClassLoader()));

    CatalogManager.CatalogWrapper wrapper = new CatalogManager.CatalogWrapper(mockCatalog, mockCl);

    int threads = 8;
    ExecutorService exec = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      futures.add(
          exec.submit(
              () -> {
                ready.countDown();
                go.await();
                wrapper.close();
                return null;
              }));
    }

    ready.await(5, TimeUnit.SECONDS);
    go.countDown();

    for (Future<Void> f : futures) {
      f.get(5, TimeUnit.SECONDS);
    }

    exec.shutdownNow();
    // classLoader.close() called exactly once regardless of concurrency.
    Mockito.verify(mockCl, Mockito.times(1)).close();
  }
}
