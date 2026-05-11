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

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link CaffeineGravitinoCache} and {@link NoOpsGravitinoCache}. */
public class TestGravitinoCache {

  @Test
  void testCaffeinePutAndGet() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("key1", 100L);
      cache.put("key2", 200L);

      Optional<Long> val1 = cache.getIfPresent("key1");
      Assertions.assertTrue(val1.isPresent());
      Assertions.assertEquals(100L, val1.get());

      Optional<Long> val2 = cache.getIfPresent("key2");
      Assertions.assertTrue(val2.isPresent());
      Assertions.assertEquals(200L, val2.get());

      Optional<Long> missing = cache.getIfPresent("nonexistent");
      Assertions.assertFalse(missing.isPresent());

      Assertions.assertEquals(2, cache.size());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidate() {
    CaffeineGravitinoCache<String, String> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("a", "val-a");
      cache.put("b", "val-b");

      cache.invalidate("a");
      Assertions.assertFalse(cache.getIfPresent("a").isPresent());
      Assertions.assertTrue(cache.getIfPresent("b").isPresent());

      Assertions.assertEquals(1, cache.size());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidateAll() {
    CaffeineGravitinoCache<String, Integer> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("x", 1);
      cache.put("y", 2);
      cache.put("z", 3);

      cache.invalidateAll();
      Assertions.assertEquals(0, cache.size());
      Assertions.assertFalse(cache.getIfPresent("x").isPresent());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidateByPrefix() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      // Simulate hierarchical keys: metalake::catalog::schema::
      cache.put("lake1::cat1::", 1L);
      cache.put("lake1::cat1::s1::", 2L);
      cache.put("lake1::cat1::s1::t1::TABLE", 3L);
      cache.put("lake1::cat1::s1::t2::TABLE", 4L);
      cache.put("lake1::cat1::s2::", 5L);
      cache.put("lake1::cat2::", 6L);
      cache.put("lake2::cat3::", 7L);

      Assertions.assertEquals(7, cache.size());

      // Drop catalog cat1 — should invalidate cat1 and all children
      cache.invalidateByPrefix("lake1::cat1::");

      Assertions.assertEquals(2, cache.size());
      Assertions.assertFalse(cache.getIfPresent("lake1::cat1::").isPresent());
      Assertions.assertFalse(cache.getIfPresent("lake1::cat1::s1::").isPresent());
      Assertions.assertFalse(cache.getIfPresent("lake1::cat1::s1::t1::TABLE").isPresent());
      Assertions.assertFalse(cache.getIfPresent("lake1::cat1::s1::t2::TABLE").isPresent());
      Assertions.assertFalse(cache.getIfPresent("lake1::cat1::s2::").isPresent());

      // cat2 and lake2 should be unaffected
      Assertions.assertTrue(cache.getIfPresent("lake1::cat2::").isPresent());
      Assertions.assertTrue(cache.getIfPresent("lake2::cat3::").isPresent());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidateByPrefixLeaf() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("lake1::cat1::s1::t1::TABLE", 1L);
      cache.put("lake1::cat1::s1::t2::TABLE", 2L);
      cache.put("lake1::cat1::s1::f1::FILESET", 3L);

      // Drop specific table — only t1 should be invalidated
      cache.invalidateByPrefix("lake1::cat1::s1::t1::TABLE");

      Assertions.assertEquals(2, cache.size());
      Assertions.assertFalse(cache.getIfPresent("lake1::cat1::s1::t1::TABLE").isPresent());
      Assertions.assertTrue(cache.getIfPresent("lake1::cat1::s1::t2::TABLE").isPresent());
      Assertions.assertTrue(cache.getIfPresent("lake1::cat1::s1::f1::FILESET").isPresent());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineOverwrite() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("k", 1L);
      Assertions.assertEquals(1L, cache.getIfPresent("k").get());

      cache.put("k", 2L);
      Assertions.assertEquals(2L, cache.getIfPresent("k").get());

      Assertions.assertEquals(1, cache.size());
    } finally {
      cache.close();
    }
  }

  @Test
  void testNoOpsCache() {
    NoOpsGravitinoCache<String, Long> cache = new NoOpsGravitinoCache<>();
    try {
      cache.put("key1", 100L);
      Assertions.assertFalse(cache.getIfPresent("key1").isPresent());
      Assertions.assertEquals(0, cache.size());

      // All operations are no-ops, should not throw
      cache.invalidate("key1");
      cache.invalidateAll();
      cache.invalidateByPrefix("any");
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineWithNonStringKeys() {
    CaffeineGravitinoCache<Long, String> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put(1L, "role1");
      cache.put(2L, "role2");
      cache.put(3L, "role3");

      Assertions.assertEquals("role1", cache.getIfPresent(1L).get());
      Assertions.assertEquals(3, cache.size());

      cache.invalidate(2L);
      Assertions.assertFalse(cache.getIfPresent(2L).isPresent());
      Assertions.assertEquals(2, cache.size());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineExpiresAfterAccessTtl() throws InterruptedException {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(50L, 1000L);
    try {
      cache.put("k", 1L);
      Assertions.assertTrue(cache.getIfPresent("k").isPresent());

      Thread.sleep(120L);
      // Caffeine cleanup is lazy; force it by accessing again then waiting briefly
      Optional<Long> afterTtl = cache.getIfPresent("k");
      Assertions.assertFalse(
          afterTtl.isPresent(), "Entry should have expired after TTL elapsed beyond grace");
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineEvictsBeyondMaxSize() {
    CaffeineGravitinoCache<Long, Long> cache = new CaffeineGravitinoCache<>(60_000L, 5L);
    try {
      for (long i = 0; i < 50L; i++) {
        cache.put(i, i);
      }
      // Caffeine eviction is asynchronous but bounded; size must respect maxSize within slack
      Assertions.assertTrue(
          cache.size() <= 50L,
          "Cache size must not exceed inserted count, but eviction should kick in");
      Assertions.assertTrue(
          cache.size() <= 10L,
          "Eviction should trim entries close to maxSize=5; observed: " + cache.size());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidateByPrefixWithEmptyPrefixDropsAll() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("a", 1L);
      cache.put("b", 2L);
      cache.put("c", 3L);

      cache.invalidateByPrefix("");
      Assertions.assertEquals(0, cache.size());
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidateByPrefixNoMatch() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("lake1::cat1::", 1L);
      cache.put("lake2::cat2::", 2L);

      cache.invalidateByPrefix("missing-prefix::");

      Assertions.assertEquals(
          2, cache.size(), "No keys should be removed when prefix has no match");
    } finally {
      cache.close();
    }
  }

  @Test
  void testCaffeineInvalidateNonExistentKey() {
    CaffeineGravitinoCache<String, Long> cache = new CaffeineGravitinoCache<>(60_000L, 1000L);
    try {
      cache.put("present", 1L);
      // Should not throw or affect existing entries
      cache.invalidate("absent");
      Assertions.assertEquals(1, cache.size());
      Assertions.assertTrue(cache.getIfPresent("present").isPresent());
    } finally {
      cache.close();
    }
  }

  @Test
  void testNoOpsCacheSizeAlwaysZero() {
    NoOpsGravitinoCache<String, Long> cache = new NoOpsGravitinoCache<>();
    try {
      for (long i = 0; i < 100; i++) {
        cache.put("k" + i, i);
      }
      Assertions.assertEquals(0, cache.size());
    } finally {
      cache.close();
    }
  }
}
