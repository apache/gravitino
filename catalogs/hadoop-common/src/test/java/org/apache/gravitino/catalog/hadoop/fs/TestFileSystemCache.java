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
package org.apache.gravitino.catalog.hadoop.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FileSystemCache}. */
public class TestFileSystemCache {

  private FileSystemCache cache;

  @BeforeEach
  public void setUp() {
    cache =
        FileSystemCache.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .maximumSize(100)
            .withCleanerScheduler("test-cache-cleaner-%d")
            .build();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void testGetOrCreate() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    FileSystemCacheKey key = new FileSystemCacheKey("file", null, ugi);

    AtomicInteger loaderCallCount = new AtomicInteger(0);

    FileSystem fs1 =
        cache.get(
            key,
            k -> {
              loaderCallCount.incrementAndGet();
              return new LocalFileSystem();
            });

    assertNotNull(fs1);
    assertEquals(1, loaderCallCount.get());

    // Second call should return cached instance
    FileSystem fs2 =
        cache.get(
            key,
            k -> {
              loaderCallCount.incrementAndGet();
              return new LocalFileSystem();
            });

    assertSame(fs1, fs2);
    assertEquals(1, loaderCallCount.get()); // Loader should not be called again
  }

  @Test
  public void testGetIfPresent() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    FileSystemCacheKey key = new FileSystemCacheKey("file", null, ugi);

    // Initially should be null
    assertNull(cache.getIfPresent(key));

    // Add to cache
    FileSystem fs =
        cache.get(
            key,
            k -> new LocalFileSystem());

    // Now should be present
    assertSame(fs, cache.getIfPresent(key));
  }

  @Test
  public void testInvalidate() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    FileSystemCacheKey key = new FileSystemCacheKey("file", null, ugi);

    cache.get(key, k -> new LocalFileSystem());
    assertNotNull(cache.getIfPresent(key));

    cache.invalidate(key);
    assertNull(cache.getIfPresent(key));
  }

  @Test
  public void testInvalidateAll() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    FileSystemCacheKey key1 = new FileSystemCacheKey("file", null, ugi);
    FileSystemCacheKey key2 = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);

    cache.get(key1, k -> new LocalFileSystem());
    cache.get(key2, k -> new LocalFileSystem());

    assertEquals(2, cache.estimatedSize());

    cache.invalidateAll();

    assertEquals(0, cache.estimatedSize());
  }

  @Test
  public void testEstimatedSize() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    assertEquals(0, cache.estimatedSize());

    FileSystemCacheKey key1 = new FileSystemCacheKey("file", null, ugi);
    cache.get(key1, k -> new LocalFileSystem());

    assertEquals(1, cache.estimatedSize());

    FileSystemCacheKey key2 = new FileSystemCacheKey("hdfs", "namenode:8020", ugi);
    cache.get(key2, k -> new LocalFileSystem());

    assertEquals(2, cache.estimatedSize());
  }

  @Test
  public void testAsMap() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    FileSystemCacheKey key = new FileSystemCacheKey("file", null, ugi);

    FileSystem fs = cache.get(key, k -> new LocalFileSystem());

    assertTrue(cache.asMap().containsKey(key));
    assertSame(fs, cache.asMap().get(key));
  }

  @Test
  public void testClose() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    FileSystemCacheKey key = new FileSystemCacheKey("file", null, ugi);

    LocalFileSystem fs = new LocalFileSystem();
    fs.initialize(fs.getUri(), new Configuration());
    cache.get(key, k -> fs);

    cache.close();

    // After close, the cache should be empty
    assertEquals(0, cache.estimatedSize());
  }

  @Test
  public void testBuilderDefaults() throws IOException {
    // Test that builder with default values works
    FileSystemCache defaultCache = FileSystemCache.newBuilder().build();
    assertNotNull(defaultCache);
    defaultCache.close();
  }

  @Test
  public void testBuilderWithMaximumSize() throws IOException {
    FileSystemCache smallCache =
        FileSystemCache.newBuilder()
            .maximumSize(2)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build();

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    smallCache.get(new FileSystemCacheKey("file", null, ugi), k -> new LocalFileSystem());
    smallCache.get(new FileSystemCacheKey("hdfs", "host1:8020", ugi), k -> new LocalFileSystem());
    smallCache.get(new FileSystemCacheKey("hdfs", "host2:8020", ugi), k -> new LocalFileSystem());

    // Force cleanup
    smallCache.cleanUp();

    // Cache should not exceed maximum size
    assertTrue(smallCache.estimatedSize() <= 2);

    smallCache.close();
  }
}
