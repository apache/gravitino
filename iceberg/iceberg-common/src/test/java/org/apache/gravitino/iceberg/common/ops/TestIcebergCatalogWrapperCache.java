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
package org.apache.gravitino.iceberg.common.ops;

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogWrapperCache {

  @Test
  void getReusesCachedWrapper() {
    IcebergCatalogWrapperCache cache = new IcebergCatalogWrapperCache(TimeUnit.HOURS.toMillis(1));
    AtomicInteger creates = new AtomicInteger();
    IcebergCatalogWrapper first = cache.get("c1", name -> newMemoryWrapper(creates));
    IcebergCatalogWrapper second = cache.get("c1", name -> newMemoryWrapper(creates));
    Assertions.assertSame(first, second);
    Assertions.assertEquals(1, creates.get());
    cache.close();
  }

  @Test
  void sameCatalogVersionReusesWrapper() {
    IcebergCatalogWrapperCache cache = new IcebergCatalogWrapperCache(TimeUnit.HOURS.toMillis(1));
    AtomicInteger creates = new AtomicInteger();
    IcebergCatalogWrapper first = cache.get("c1", 1L, name -> newMemoryWrapper(creates));
    IcebergCatalogWrapper second = cache.get("c1", 1L, name -> newMemoryWrapper(creates));
    Assertions.assertSame(first, second);
    Assertions.assertEquals(1, creates.get());
    cache.close();
  }

  @Test
  void newerCatalogVersionKeepsPreviousEntryUntilClosed() {
    IcebergCatalogWrapperCache cache = new IcebergCatalogWrapperCache(TimeUnit.HOURS.toMillis(1));
    AtomicInteger creates = new AtomicInteger();
    IcebergCatalogWrapper first = cache.get("c1", 1L, name -> newMemoryWrapper(creates));
    IcebergCatalogWrapper second = cache.get("c1", 2L, name -> newMemoryWrapper(creates));

    Assertions.assertNotSame(first, second);
    Assertions.assertEquals(2, creates.get());
    Assertions.assertFalse(first.isClosed());
    Assertions.assertFalse(second.isClosed());
    cache.close();
    Assertions.assertTrue(first.isClosed());
    Assertions.assertTrue(second.isClosed());
  }

  @Test
  void invalidateRemovesAllVersionsForCatalogName() {
    IcebergCatalogWrapperCache cache = new IcebergCatalogWrapperCache(TimeUnit.HOURS.toMillis(1));
    IcebergCatalogWrapper v1 = cache.get("c1", 1L, name -> newMemoryWrapper(new AtomicInteger()));
    IcebergCatalogWrapper v2 = cache.get("c1", 2L, name -> newMemoryWrapper(new AtomicInteger()));
    cache.invalidate("c1");
    Assertions.assertTrue(v1.isClosed());
    Assertions.assertTrue(v2.isClosed());
    cache.close();
  }

  @Test
  void computeCacheDurationNanosWithoutTokenExpiryUsesAccessEviction() {
    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    long accessEvictionNanos = TimeUnit.HOURS.toNanos(1);
    Assertions.assertEquals(
        accessEvictionNanos,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(
            config, accessEvictionNanos, System.currentTimeMillis()));
  }

  @Test
  void computeCacheDurationNanosCapsByGcsTokenExpiry() {
    long now = 1_700_000_000_000L;
    long expiresAt = now + TimeUnit.HOURS.toMillis(1);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
                String.valueOf(expiresAt)));
    long accessEvictionNanos = TimeUnit.HOURS.toNanos(2);
    long expected =
        TimeUnit.MILLISECONDS.toNanos(
            TimeUnit.HOURS.toMillis(1) - IcebergCatalogWrapperCache.GCS_TOKEN_REFRESH_BUFFER_MS);
    Assertions.assertEquals(
        expected,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(config, accessEvictionNanos, now));
  }

  @Test
  void computeCacheDurationNanosExpiresImmediatelyWhenPastRefreshDeadline() {
    long now = 1_700_000_000_000L;
    long expiresAt = now + TimeUnit.MINUTES.toMillis(2);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
                String.valueOf(expiresAt)));
    Assertions.assertEquals(
        0L,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(
            config, TimeUnit.HOURS.toNanos(1), now));
  }

  @Test
  void computeCacheDurationNanosIgnoresInvalidExpiresAt() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
                "not-a-number"));
    long accessEvictionNanos = TimeUnit.MINUTES.toNanos(30);
    Assertions.assertEquals(
        accessEvictionNanos,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(
            config, accessEvictionNanos, System.currentTimeMillis()));
  }

  private static IcebergCatalogWrapper newMemoryWrapper(AtomicInteger creates) {
    creates.incrementAndGet();
    return new IcebergCatalogWrapper(
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory")));
  }
}
