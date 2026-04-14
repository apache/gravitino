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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CaffeineGravitinoCache} and {@link NoOpsGravitinoCache}. */
public class TestGravitinoCache {

  @Test
  public void testCaffeinePutAndGet() throws IOException {
    try (GravitinoCache<String, Integer> cache = new CaffeineGravitinoCache<>(100, 3600)) {
      cache.put("key1", 42);
      Optional<Integer> result = cache.getIfPresent("key1");
      assertTrue(result.isPresent());
      assertEquals(42, result.get());
    }
  }

  @Test
  public void testCaffeineGetMissReturnsEmpty() throws IOException {
    try (GravitinoCache<String, Integer> cache = new CaffeineGravitinoCache<>(100, 3600)) {
      assertFalse(cache.getIfPresent("missing").isPresent());
    }
  }

  @Test
  public void testCaffeineInvalidate() throws IOException {
    try (GravitinoCache<Long, String> cache = new CaffeineGravitinoCache<>(100, 3600)) {
      cache.put(1L, "value");
      assertTrue(cache.getIfPresent(1L).isPresent());

      cache.invalidate(1L);
      assertFalse(cache.getIfPresent(1L).isPresent());
    }
  }

  @Test
  public void testCaffeineInvalidateAll() throws IOException {
    try (GravitinoCache<Long, String> cache = new CaffeineGravitinoCache<>(100, 3600)) {
      cache.put(1L, "a");
      cache.put(2L, "b");
      cache.put(3L, "c");

      cache.invalidateAll();

      assertFalse(cache.getIfPresent(1L).isPresent());
      assertFalse(cache.getIfPresent(2L).isPresent());
      assertFalse(cache.getIfPresent(3L).isPresent());
    }
  }

  @Test
  public void testCaffeineSize() throws IOException {
    try (GravitinoCache<Integer, String> cache = new CaffeineGravitinoCache<>(100, 3600)) {
      assertEquals(0, cache.size());
      cache.put(1, "a");
      cache.put(2, "b");
      // Caffeine size is eventually consistent; force cleanup
      ((CaffeineGravitinoCache<Integer, String>) cache).cleanUp();
      assertEquals(2, cache.size());
    }
  }

  @Test
  public void testCaffeineRemovalListenerFires() throws IOException {
    List<Long> evicted = new ArrayList<>();
    try (GravitinoCache<Long, Integer> cache =
        new CaffeineGravitinoCache<>(100, 3600, (key, value, cause) -> evicted.add(key))) {
      cache.put(10L, 1);
      cache.invalidate(10L);
      // Force Caffeine to run pending removal tasks synchronously
      ((CaffeineGravitinoCache<Long, Integer>) cache).cleanUp();
      assertTrue(evicted.contains(10L), "Removal listener should have been called for key 10");
    }
  }

  @Test
  public void testCaffeineOverwrite() throws IOException {
    try (GravitinoCache<String, Integer> cache = new CaffeineGravitinoCache<>(100, 3600)) {
      cache.put("k", 1);
      cache.put("k", 2);
      assertEquals(2, cache.getIfPresent("k").get());
    }
  }

  // -----------------------------------------------------------------------
  // NoOpsGravitinoCache
  // -----------------------------------------------------------------------

  @Test
  public void testNoOpsCacheAlwaysEmpty() throws IOException {
    try (GravitinoCache<String, Integer> cache = new NoOpsGravitinoCache<>()) {
      cache.put("key", 99);
      assertFalse(cache.getIfPresent("key").isPresent());
      assertEquals(0, cache.size());
    }
  }

  @Test
  public void testNoOpsCacheInvalidateIsNoOp() throws IOException {
    try (GravitinoCache<String, Integer> cache = new NoOpsGravitinoCache<>()) {
      // Should not throw
      cache.invalidate("anything");
      cache.invalidateAll();
    }
  }
}
