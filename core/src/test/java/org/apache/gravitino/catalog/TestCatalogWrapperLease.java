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

import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogManager.CatalogWrapper;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore.InMemoryEntityStore;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.utils.ClassLoaderPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests that a catalog wrapper evicted from the catalog cache is not torn down while an operation
 * is still using it. Cache eviction (expiry, explicit invalidation, remote change-log invalidation,
 * and drop) must only retire the wrapper; the catalog and the ClassLoader are cleaned up when the
 * last lease is released, exactly once.
 */
public class TestCatalogWrapperLease {

  private static final String METALAKE = "metalake";
  private static final String PROVIDER = "test";
  private static final Map<String, String> PROPS =
      ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

  private static final BaseMetalake METALAKE_ENTITY =
      BaseMetalake.builder()
          .withId(1L)
          .withName(METALAKE)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  private static Config config;
  private static InMemoryEntityStore entityStore;

  private CatalogManager catalogManager;

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.put(METALAKE_ENTITY, true);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @BeforeEach
  public void beforeEach() {
    catalogManager =
        new CatalogManager(config, entityStore, new RandomIdGenerator(), new SecretManager(config));
  }

  @AfterEach
  public void afterEach() throws IOException {
    if (catalogManager != null) {
      catalogManager.close();
      catalogManager = null;
    }
    entityStore.clear();
    entityStore.put(METALAKE_ENTITY, true);
  }

  @Test
  public void testCacheInvalidationDefersCleanupUntilLeaseIsReleased() throws Exception {
    NameIdentifier ident = createCatalog("invalidate_with_lease");

    CatalogLease lease = catalogManager.acquireCatalogLease(ident);
    CatalogWrapper wrapper = lease.wrapper();
    Assertions.assertEquals(1, wrapper.activeOperations());

    catalogManager.getCatalogCache().invalidate(ident);
    // Caffeine runs the removal listener asynchronously, so wait for the retirement to land.
    await().atMost(Duration.ofSeconds(10)).until(wrapper::isRetired);

    Assertions.assertTrue(wrapper.isRetired(), "eviction must retire the wrapper");
    Assertions.assertNotNull(
        wrapper.catalog(), "a leased wrapper must not be closed by cache eviction");
    // The leased wrapper is still fully usable: this is the operation that used to fail with an
    // NPE (or NoClassDefFoundError) once the removal listener closed the wrapper underneath it.
    Assertions.assertDoesNotThrow(
        () ->
            wrapper.doWithSchemaOps(ops -> ops.listSchemas(Namespace.of(METALAKE, ident.name()))));

    lease.close();
    // Closing the same lease twice must not double-release the active-operation count.
    lease.close();

    Assertions.assertEquals(0, wrapper.activeOperations());
    Assertions.assertNull(wrapper.catalog(), "the last lease release must clean up the catalog");
  }

  @Test
  public void testCacheExpiryDefersCleanupUntilLeaseIsReleased() throws Exception {
    Config expiringConfig = new Config(false) {};
    expiringConfig.set(Configs.CATALOG_LOAD_ISOLATED, false);
    expiringConfig.set(Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS, 1L);

    CatalogManager expiringManager =
        new CatalogManager(
            expiringConfig,
            entityStore,
            new RandomIdGenerator(),
            new SecretManager(expiringConfig));
    try {
      NameIdentifier ident = NameIdentifier.of(METALAKE, "expiring_catalog");
      expiringManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", PROPS);

      try (CatalogLease lease = expiringManager.acquireCatalogLease(ident)) {
        CatalogWrapper wrapper = lease.wrapper();

        await().atMost(Duration.ofSeconds(10)).until(wrapper::isRetired);

        Assertions.assertNull(expiringManager.getCatalogCache().getIfPresent(ident));
        Assertions.assertNotNull(
            wrapper.catalog(), "a leased wrapper must survive cache expiration");
        Assertions.assertDoesNotThrow(
            () ->
                wrapper.doWithSchemaOps(
                    ops -> ops.listSchemas(Namespace.of(METALAKE, ident.name()))));
      }
    } finally {
      expiringManager.close();
    }
  }

  @Test
  public void testRemoteChangeLogInvalidationDefersCleanupUntilLeaseIsReleased() throws Exception {
    NameIdentifier ident = createCatalog("remote_invalidation");

    try (CatalogLease lease = catalogManager.acquireCatalogLease(ident)) {
      CatalogWrapper wrapper = lease.wrapper();

      new CatalogChangeLogListener(catalogManager)
          .onEntityChange(
              List.of(
                  new EntityChangeRecord(
                      1L,
                      METALAKE,
                      "CATALOG",
                      METALAKE + "." + ident.name(),
                      OperateType.ALTER,
                      0L)));

      Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
      await().atMost(Duration.ofSeconds(10)).until(wrapper::isRetired);
      Assertions.assertTrue(wrapper.isRetired());
      Assertions.assertNotNull(
          wrapper.catalog(), "a leased wrapper must survive remote change-log invalidation");
      Assertions.assertDoesNotThrow(
          () ->
              wrapper.doWithSchemaOps(
                  ops -> ops.listSchemas(Namespace.of(METALAKE, ident.name()))));
    }
  }

  @Test
  public void testDropCatalogDefersCleanupUntilLeaseIsReleased() throws Exception {
    NameIdentifier ident = createCatalog("drop_with_lease");

    CatalogLease lease = catalogManager.acquireCatalogLease(ident);
    CatalogWrapper wrapper = lease.wrapper();

    catalogManager.disableCatalog(ident);
    Assertions.assertTrue(catalogManager.dropCatalog(ident));

    Assertions.assertNotNull(wrapper.catalog(), "a leased wrapper must survive a concurrent drop");
    // Caffeine runs the removal listener asynchronously, so wait for the retirement to land before
    // releasing the lease that defers the cleanup.
    await().atMost(Duration.ofSeconds(10)).until(wrapper::isRetired);
    lease.close();
    Assertions.assertNull(wrapper.catalog());
  }

  @Test
  public void testAcquireLeaseReloadsRetiredWrapper() throws Exception {
    NameIdentifier ident = createCatalog("retired_reload");

    CatalogWrapper retiredWrapper = catalogManager.getCatalogCache().getIfPresent(ident);
    Assertions.assertNotNull(retiredWrapper);
    retiredWrapper.retire();

    try (CatalogLease lease = catalogManager.acquireCatalogLease(ident)) {
      Assertions.assertNotSame(
          retiredWrapper, lease.wrapper(), "a retired wrapper must not be leased again");
      Assertions.assertFalse(lease.wrapper().isRetired());
      Assertions.assertNotNull(lease.catalog());
    }
  }

  @Test
  public void testCleanupRunsExactlyOnceForRepeatedRetireAndRelease() throws Exception {
    // Two catalogs of the same provider share one pooled ClassLoader, so the pool entry survives
    // as long as exactly one reference is released per wrapper. A double cleanup would drop the
    // reference count to zero and destroy the ClassLoader the second catalog is still using.
    NameIdentifier ident1 = createCatalog("exactly_once_1");
    createCatalog("exactly_once_2");

    ClassLoaderPool pool =
        (ClassLoaderPool) FieldUtils.readField(catalogManager, "classLoaderPool", true);
    Assertions.assertEquals(1, pool.size(), "same-provider catalogs share one pooled entry");

    CatalogLease lease = catalogManager.acquireCatalogLease(ident1);
    CatalogWrapper wrapper = lease.wrapper();

    // Repeated retirements (eviction + explicit invalidation + close) and a lease release must
    // together release the pooled ClassLoader reference exactly once.
    wrapper.retire();
    wrapper.retire();
    wrapper.close();
    lease.close();
    wrapper.retire();

    Assertions.assertNull(wrapper.catalog());
    Assertions.assertEquals(
        1, pool.size(), "the pooled ClassLoader of the second catalog must stay alive");
  }

  @Test
  public void testConcurrentEvictionDoesNotCloseCatalogUnderRunningOperation() throws Exception {
    NameIdentifier ident = createCatalog("concurrent_eviction");

    CountDownLatch leaseAcquired = new CountDownLatch(1);
    CountDownLatch evicted = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> operation =
          executor.submit(
              () -> {
                try (CatalogLease lease = catalogManager.acquireCatalogLease(ident)) {
                  leaseAcquired.countDown();
                  Assertions.assertTrue(evicted.await(10, TimeUnit.SECONDS));
                  // Runs after the wrapper has been evicted from the cache; without a lease the
                  // wrapper's catalog would already be closed here.
                  lease
                      .wrapper()
                      .doWithSchemaOps(
                          ops -> ops.listSchemas(Namespace.of(METALAKE, ident.name())));
                  return true;
                }
              });

      Assertions.assertTrue(leaseAcquired.await(10, TimeUnit.SECONDS));
      CatalogWrapper leasedWrapper = catalogManager.getCatalogCache().getIfPresent(ident);
      Assertions.assertNotNull(leasedWrapper);
      catalogManager.getCatalogCache().invalidate(ident);
      await().atMost(Duration.ofSeconds(10)).until(leasedWrapper::isRetired);
      evicted.countDown();

      Assertions.assertTrue(operation.get(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testDoWithCatalogKeepsLeaseForEntireCallback() throws Exception {
    NameIdentifier ident = createCatalog("callback_with_lease");
    CountDownLatch callbackStarted = new CountDownLatch(1);
    CountDownLatch continueCallback = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> operation =
          executor.submit(
              () ->
                  catalogManager.doWithCatalog(
                      ident,
                      catalog -> {
                        callbackStarted.countDown();
                        Assertions.assertTrue(continueCallback.await(10, TimeUnit.SECONDS));
                        catalog.ops();
                        return true;
                      }));

      Assertions.assertTrue(callbackStarted.await(10, TimeUnit.SECONDS));
      CatalogWrapper wrapper = catalogManager.getCatalogCache().getIfPresent(ident);
      Assertions.assertNotNull(wrapper);

      catalogManager.getCatalogCache().invalidate(ident);
      await().atMost(Duration.ofSeconds(10)).until(wrapper::isRetired);

      Assertions.assertNotNull(
          wrapper.catalog(), "the callback lease must survive a concurrent invalidation");
      continueCallback.countDown();
      Assertions.assertTrue(operation.get(10, TimeUnit.SECONDS));
      Assertions.assertNull(wrapper.catalog());
    } finally {
      continueCallback.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testManagerCloseDefersCleanupUntilLeaseIsReleased() throws Exception {
    NameIdentifier ident = createCatalog("manager_close_with_lease");

    CatalogLease lease = catalogManager.acquireCatalogLease(ident);
    CatalogWrapper wrapper = lease.wrapper();
    ClassLoaderPool pool =
        (ClassLoaderPool) FieldUtils.readField(catalogManager, "classLoaderPool", true);

    catalogManager.close();

    Assertions.assertTrue(wrapper.isRetired());
    Assertions.assertNotNull(
        wrapper.catalog(), "manager shutdown must not close a catalog with an active lease");
    Assertions.assertEquals(
        1, pool.size(), "manager shutdown must retain an actively leased ClassLoader");
    Assertions.assertDoesNotThrow(
        () ->
            wrapper.doWithSchemaOps(ops -> ops.listSchemas(Namespace.of(METALAKE, ident.name()))));
    Assertions.assertThrows(
        IllegalStateException.class, () -> catalogManager.acquireCatalogLease(ident));

    lease.close();

    Assertions.assertNull(wrapper.catalog());
    Assertions.assertEquals(0, pool.size());
  }

  @Test
  public void testRemovalListenerFailureDoesNotSkipWrapperRetirement() {
    NameIdentifier ident = createCatalog("failing_removal_listener");
    CatalogWrapper wrapper = catalogManager.getCatalogCache().getIfPresent(ident);
    Assertions.assertNotNull(wrapper);
    catalogManager.addCatalogCacheRemoveListener(
        ignored -> {
          throw new RuntimeException("listener failed");
        });

    catalogManager.getCatalogCache().invalidate(ident);

    await().atMost(Duration.ofSeconds(10)).until(wrapper::isRetired);
    Assertions.assertNull(
        wrapper.catalog(), "listener failures must not prevent wrapper resource cleanup");
  }

  @Test
  public void testReleaseWithoutAcquireIsRejected() throws Exception {
    NameIdentifier ident = createCatalog("release_without_acquire");
    CatalogWrapper wrapper = catalogManager.getCatalogCache().getIfPresent(ident);
    Assertions.assertNotNull(wrapper);

    Assertions.assertThrows(IllegalStateException.class, wrapper::release);
  }

  @Test
  public void testCleanupClearsCatalogEvenWhenCloseFails() throws Exception {
    NameIdentifier ident = createCatalog("failing_close");

    ClassLoaderPool pool =
        (ClassLoaderPool) FieldUtils.readField(catalogManager, "classLoaderPool", true);
    Assertions.assertEquals(1, pool.size());

    CatalogWrapper wrapper = catalogManager.getCatalogCache().getIfPresent(ident);
    Assertions.assertNotNull(wrapper);

    BaseCatalog<?> failingCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.doThrow(new IOException("close failed")).when(failingCatalog).close();
    FieldUtils.writeField(wrapper, "catalog", failingCatalog, true);

    // Cleanup runs exactly once, so a close() failure must not leave the reference behind: there
    // is no second chance to clear it.
    wrapper.retire();

    Mockito.verify(failingCatalog).close();
    Assertions.assertNull(
        wrapper.catalog(), "a failing close must still drop the catalog reference");
    Assertions.assertEquals(
        0, pool.size(), "a failing close must still release the pooled ClassLoader");
  }

  @Test
  public void testManagerCloseWaitsForConcurrentCatalogLoad() throws Exception {
    NameIdentifier ident = createCatalog("load_while_closing");
    CatalogWrapper oldWrapper = catalogManager.getCatalogCache().getIfPresent(ident);
    catalogManager.getCatalogCache().invalidate(ident);
    await().atMost(Duration.ofSeconds(10)).until(oldWrapper::isRetired);

    catalogManager = Mockito.spy(catalogManager);
    CountDownLatch loadStarted = new CountDownLatch(1);
    CountDownLatch continueLoad = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              loadStarted.countDown();
              Assertions.assertTrue(continueLoad.await(10, TimeUnit.SECONDS));
              return invocation.callRealMethod();
            })
        .when(catalogManager)
        .createCatalogWrapper(Mockito.any(), Mockito.isNull());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<CatalogLease> acquiredLease =
          executor.submit(() -> catalogManager.acquireCatalogLease(ident));
      Assertions.assertTrue(loadStarted.await(10, TimeUnit.SECONDS));

      Future<?> closeFuture = submitCloseAndAssertBlocked(executor);

      continueLoad.countDown();
      CatalogLease lease = acquiredLease.get(10, TimeUnit.SECONDS);
      closeFuture.get(10, TimeUnit.SECONDS);

      CatalogWrapper loadedWrapper = lease.wrapper();
      Assertions.assertTrue(loadedWrapper.isRetired());
      Assertions.assertNotNull(
          loadedWrapper.catalog(), "close must preserve a concurrently acquired lease");
      Assertions.assertThrows(
          IllegalStateException.class, () -> catalogManager.acquireCatalogLease(ident));

      lease.close();
      Assertions.assertNull(loadedWrapper.catalog());
    } finally {
      continueLoad.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testManagerCloseWaitsForConcurrentCatalogPublication() throws Exception {
    NameIdentifier ident = NameIdentifier.of(METALAKE, "publish_while_closing");
    catalogManager = Mockito.spy(catalogManager);
    CountDownLatch creationStarted = new CountDownLatch(1);
    CountDownLatch continueCreation = new CountDownLatch(1);
    AtomicReference<CatalogWrapper> createdWrapper = new AtomicReference<>();
    Mockito.doAnswer(
            invocation -> {
              creationStarted.countDown();
              Assertions.assertTrue(continueCreation.await(10, TimeUnit.SECONDS));
              CatalogWrapper wrapper = (CatalogWrapper) invocation.callRealMethod();
              createdWrapper.set(wrapper);
              return wrapper;
            })
        .when(catalogManager)
        .createCatalogWrapper(Mockito.any(), Mockito.any());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Catalog> createdCatalog =
          executor.submit(
              () ->
                  catalogManager.createCatalog(
                      ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", PROPS));
      Assertions.assertTrue(creationStarted.await(10, TimeUnit.SECONDS));

      Future<?> closeFuture = submitCloseAndAssertBlocked(executor);

      continueCreation.countDown();
      Assertions.assertNotNull(createdCatalog.get(10, TimeUnit.SECONDS));
      closeFuture.get(10, TimeUnit.SECONDS);

      Assertions.assertTrue(createdWrapper.get().isRetired());
      Assertions.assertNull(createdWrapper.get().catalog());
      Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
      Assertions.assertThrows(
          IllegalStateException.class, () -> catalogManager.acquireCatalogLease(ident));
    } finally {
      continueCreation.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testManagerCloseReleasesThePoolEvenIfRetiringAWrapperFails() throws Exception {
    createCatalog("failing_retire");
    ClassLoaderPool pool =
        (ClassLoaderPool) FieldUtils.readField(catalogManager, "classLoaderPool", true);

    CatalogWrapper failingWrapper = Mockito.mock(CatalogWrapper.class);
    Mockito.doThrow(new RuntimeException("retire failed")).when(failingWrapper).retire();
    catalogManager.getCatalogCache().put(NameIdentifier.of(METALAKE, "failing"), failingWrapper);

    Assertions.assertDoesNotThrow(() -> catalogManager.close());

    // The cache's removal listener retires the wrapper as well, asynchronously.
    Mockito.verify(failingWrapper, Mockito.atLeastOnce()).retire();
    Assertions.assertEquals(
        0, pool.size(), "a failing retirement must not keep the ClassLoader pool open");
  }

  private Future<?> submitCloseAndAssertBlocked(ExecutorService executor)
      throws InterruptedException {
    CountDownLatch closeStarted = new CountDownLatch(1);
    Future<?> closeFuture =
        executor.submit(
            () -> {
              closeStarted.countDown();
              catalogManager.close();
            });
    Assertions.assertTrue(closeStarted.await(10, TimeUnit.SECONDS));
    await()
        .during(Duration.ofMillis(200))
        .atMost(Duration.ofSeconds(1))
        .until(() -> !closeFuture.isDone());
    return closeFuture;
  }

  @Test
  public void testManagerCloseWaitsForConcurrentCatalogAlter() throws Exception {
    NameIdentifier ident = createCatalog("alter_while_closing");

    catalogManager = Mockito.spy(catalogManager);
    CountDownLatch publicationStarted = new CountDownLatch(1);
    CountDownLatch continuePublication = new CountDownLatch(1);
    // The only createCatalogWrapper(entity, null) call of alterCatalog happens after the entity
    // has been persisted and before the refreshed wrapper is published, i.e. exactly in the window
    // where a concurrent close() used to fail the alter that had already taken effect.
    Mockito.doAnswer(
            invocation -> {
              publicationStarted.countDown();
              Assertions.assertTrue(continuePublication.await(10, TimeUnit.SECONDS));
              return invocation.callRealMethod();
            })
        .when(catalogManager)
        .createCatalogWrapper(Mockito.any(), Mockito.isNull());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Catalog> alteredCatalog =
          executor.submit(
              () -> catalogManager.alterCatalog(ident, CatalogChange.updateComment("altered")));
      Assertions.assertTrue(publicationStarted.await(10, TimeUnit.SECONDS));

      Future<?> closeFuture = submitCloseAndAssertBlocked(executor);

      continuePublication.countDown();
      Catalog altered = alteredCatalog.get(10, TimeUnit.SECONDS);
      Assertions.assertEquals(
          "altered", altered.comment(), "the alter must succeed instead of racing with close()");
      closeFuture.get(10, TimeUnit.SECONDS);

      Assertions.assertThrows(
          IllegalStateException.class, () -> catalogManager.acquireCatalogLease(ident));
    } finally {
      continuePublication.countDown();
      executor.shutdownNow();
    }
  }

  private NameIdentifier createCatalog(String name) {
    NameIdentifier ident = NameIdentifier.of(METALAKE, name);
    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, PROVIDER, "comment", PROPS);
    return ident;
  }
}
