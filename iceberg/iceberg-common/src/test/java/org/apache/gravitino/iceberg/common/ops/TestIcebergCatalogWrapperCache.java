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
import java.util.function.Consumer;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogWrapperCache {

  private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);
  private static final long HOUR_NS = TimeUnit.HOURS.toNanos(1);
  private static final long NOW = 1_700_000_000_000L;

  @Test
  void getReusesCachedWrapper() {
    withCache(
        cache -> {
          AtomicInteger creates = new AtomicInteger();
          IcebergCatalogWrapper first = cache.get("c1", name -> wrapper(creates));
          Assertions.assertSame(first, cache.get("c1", name -> wrapper(creates)));
          Assertions.assertEquals(1, creates.get());
        });
  }

  @Test
  void sameCatalogVersionReusesWrapper() {
    withCache(
        cache -> {
          AtomicInteger creates = new AtomicInteger();
          IcebergCatalogWrapper first = cache.get("c1", 1L, name -> wrapper(creates));
          Assertions.assertSame(first, cache.get("c1", 1L, name -> wrapper(creates)));
          Assertions.assertEquals(1, creates.get());
        });
  }

  @Test
  void newerCatalogVersionKeepsPreviousEntryUntilClosed() {
    IcebergCatalogWrapperCache cache = new IcebergCatalogWrapperCache(HOUR_MS);
    AtomicInteger creates = new AtomicInteger();
    IcebergCatalogWrapper first = cache.get("c1", 1L, name -> wrapper(creates));
    IcebergCatalogWrapper second = cache.get("c1", 2L, name -> wrapper(creates));
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
    withCache(
        cache -> {
          IcebergCatalogWrapper v1 = cache.get("c1", 1L, name -> wrapper(new AtomicInteger()));
          IcebergCatalogWrapper v2 = cache.get("c1", 2L, name -> wrapper(new AtomicInteger()));
          cache.invalidate("c1");
          Assertions.assertTrue(v1.isClosed());
          Assertions.assertTrue(v2.isClosed());
        });
  }

  @Test
  void computeCacheDurationNanosWithoutTokenExpiryUsesAccessEviction() {
    Assertions.assertEquals(
        HOUR_NS, IcebergCatalogWrapperCache.computeCacheDurationNanos(memory(), HOUR_NS, NOW));
  }

  @Test
  void computeCacheDurationNanosCapsByGcsTokenExpiry() {
    long expiresAt = NOW + HOUR_MS;
    long expected =
        TimeUnit.MILLISECONDS.toNanos(
            HOUR_MS - IcebergCatalogWrapperCache.GCS_TOKEN_REFRESH_BUFFER_MS);
    Assertions.assertEquals(
        expected,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(
            memoryWithExpires(String.valueOf(expiresAt)), TimeUnit.HOURS.toNanos(2), NOW));
  }

  @Test
  void computeCacheDurationNanosExpiresImmediatelyWhenPastRefreshDeadline() {
    Assertions.assertEquals(
        0L,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(
            memoryWithExpires(String.valueOf(NOW + TimeUnit.MINUTES.toMillis(2))), HOUR_NS, NOW));
  }

  @Test
  void computeCacheDurationNanosIgnoresInvalidExpiresAt() {
    long accessNs = TimeUnit.MINUTES.toNanos(30);
    Assertions.assertEquals(
        accessNs,
        IcebergCatalogWrapperCache.computeCacheDurationNanos(
            memoryWithExpires("not-a-number"), accessNs, NOW));
  }

  private static void withCache(Consumer<IcebergCatalogWrapperCache> body) {
    IcebergCatalogWrapperCache cache = new IcebergCatalogWrapperCache(HOUR_MS);
    try {
      body.accept(cache);
    } finally {
      cache.close();
    }
  }

  private static IcebergCatalogWrapper wrapper(AtomicInteger creates) {
    creates.incrementAndGet();
    return new IcebergCatalogWrapper(memory());
  }

  private static IcebergConfig memory() {
    return new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
  }

  private static IcebergConfig memoryWithExpires(String expiresAt) {
    return new IcebergConfig(
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND,
            "memory",
            IcebergConstants.ICEBERG_GCS_OAUTH2_TOKEN_EXPIRES_AT,
            expiresAt));
  }
}
