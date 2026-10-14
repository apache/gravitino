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
package org.apache.gravitino.job.k8s;

import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNamespacedResourceCache {

  private final AtomicLong now = new AtomicLong(1_000L);

  private final List<String> listed = new ArrayList<>();

  @Test
  public void testCachedWithinTtl() {
    NamespacedResourceCache<ConfigMap> cache =
        new NamespacedResourceCache<>(
            100L,
            namespace -> {
              listed.add(namespace);
              return ImmutableList.of(resource(namespace, "a"), resource(namespace, "b"));
            },
            e -> false,
            now::get);

    Assertions.assertEquals("a", cache.find("ns1", "a").get().getMetadata().getName());
    Assertions.assertEquals("b", cache.find("ns1", "b").get().getMetadata().getName());
    Assertions.assertFalse(cache.find("ns1", "c").isPresent());
    Assertions.assertTrue(cache.find("ns2", "a").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1", "ns2"), listed);

    now.addAndGet(100L);
    Assertions.assertTrue(cache.find("ns1", "a").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1", "ns2", "ns1"), listed);

    cache.invalidate("ns1");
    Assertions.assertTrue(cache.find("ns1", "a").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1", "ns2", "ns1", "ns1"), listed);
  }

  @Test
  public void testDisabled() {
    NamespacedResourceCache<ConfigMap> cache =
        new NamespacedResourceCache<>(
            0L,
            namespace -> {
              listed.add(namespace);
              return ImmutableList.of(resource(namespace, "a"));
            },
            e -> false,
            now::get);

    Assertions.assertFalse(cache.find("ns1", "a").isPresent());
    Assertions.assertTrue(listed.isEmpty());
  }

  @Test
  public void testListFailureIsCached() {
    NamespacedResourceCache<ConfigMap> cache =
        new NamespacedResourceCache<>(
            100L,
            namespace -> {
              listed.add(namespace);
              throw new IllegalStateException("forbidden");
            },
            e -> false,
            now::get);

    Assertions.assertFalse(cache.find("ns1", "a").isPresent());
    // The namespace isn't listed again until the failure expires.
    Assertions.assertFalse(cache.find("ns1", "b").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1"), listed);

    now.addAndGet(100L);
    Assertions.assertFalse(cache.find("ns1", "a").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1", "ns1"), listed);
  }

  @Test
  public void testInvalidatedWhileListing() {
    AtomicReference<NamespacedResourceCache<ConfigMap>> cache = new AtomicReference<>();
    cache.set(
        new NamespacedResourceCache<>(
            100L,
            namespace -> {
              listed.add(namespace);
              // A resource changes while the namespace is being listed.
              cache.get().invalidate(namespace);
              return ImmutableList.of(resource(namespace, "a"));
            },
            e -> false,
            now::get));

    Assertions.assertTrue(cache.get().find("ns1", "a").isPresent());
    // The listing may predate the change, so it isn't cached.
    Assertions.assertTrue(cache.get().find("ns1", "a").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1", "ns1"), listed);
  }

  @Test
  public void testFatalListFailureIsThrown() {
    NamespacedResourceCache<ConfigMap> cache =
        new NamespacedResourceCache<>(
            100L,
            namespace -> {
              listed.add(namespace);
              throw new IllegalStateException("unreachable");
            },
            e -> e instanceof IllegalStateException,
            now::get);

    Assertions.assertThrows(IllegalStateException.class, () -> cache.find("ns1", "a"));
    // Not cached as an empty listing, so the next query fails the same way.
    Assertions.assertThrows(IllegalStateException.class, () -> cache.find("ns1", "a"));
    Assertions.assertEquals(ImmutableList.of("ns1", "ns1"), listed);
  }

  @Test
  public void testInvalidationOfAnotherNamespace() {
    AtomicReference<NamespacedResourceCache<ConfigMap>> cache = new AtomicReference<>();
    cache.set(
        new NamespacedResourceCache<>(
            100L,
            namespace -> {
              listed.add(namespace);
              // Another namespace changes while this one is being listed.
              cache.get().invalidate("other");
              return ImmutableList.of(resource(namespace, "a"));
            },
            e -> false,
            now::get));

    Assertions.assertTrue(cache.get().find("ns1", "a").isPresent());
    // The listing of ns1 is still cached.
    Assertions.assertTrue(cache.get().find("ns1", "a").isPresent());
    Assertions.assertEquals(ImmutableList.of("ns1"), listed);
  }

  @Test
  public void testConcurrentFindsListOnce() throws Exception {
    CountDownLatch listing = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger lists = new AtomicInteger();
    NamespacedResourceCache<ConfigMap> cache =
        new NamespacedResourceCache<>(
            100L,
            namespace -> {
              lists.incrementAndGet();
              // Only the listing of ns1 takes a while.
              if ("ns1".equals(namespace)) {
                listing.countDown();
                try {
                  release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
              return ImmutableList.of(resource(namespace, "a"));
            },
            e -> false,
            now::get);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<Optional<ConfigMap>> first = pool.submit(() -> cache.find("ns1", "a"));
      Assertions.assertTrue(listing.await(10, TimeUnit.SECONDS));
      // The second find waits for the listing being made instead of listing again.
      Future<Optional<ConfigMap>> second = pool.submit(() -> cache.find("ns1", "a"));
      // Another namespace isn't held up by it.
      Assertions.assertFalse(cache.find("ns2", "b").isPresent());
      release.countDown();

      Assertions.assertTrue(first.get(10, TimeUnit.SECONDS).isPresent());
      Assertions.assertTrue(second.get(10, TimeUnit.SECONDS).isPresent());
      Assertions.assertEquals(2, lists.get());
    } finally {
      pool.shutdownNow();
    }
  }

  private static ConfigMap resource(String namespace, String name) {
    return new ConfigMapBuilder()
        .withNewMetadata()
        .withNamespace(namespace)
        .withName(name)
        .endMetadata()
        .build();
  }
}
