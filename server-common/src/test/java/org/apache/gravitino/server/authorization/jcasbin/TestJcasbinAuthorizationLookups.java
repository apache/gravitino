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
package org.apache.gravitino.server.authorization.jcasbin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests for {@link JcasbinAuthorizationLookups}. */
public class TestJcasbinAuthorizationLookups {

  @Test
  void testResolveMetadataIdUsesAtomicSharedCacheAndRequestDedup() {
    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>();
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    Assertions.assertEquals(
        Optional.of(100L), lookups.resolveMetadataId(table, "ml1", requestContext));
    Assertions.assertEquals(
        Optional.of(100L), lookups.resolveMetadataId(table, "ml1", requestContext));

    Assertions.assertEquals(1, metadataIdCache.getCount);
    Assertions.assertEquals(0, metadataIdCache.getIfPresentCount);
    Assertions.assertEquals(0, metadataIdCache.putCount);
  }

  @Test
  void testResolveOwnerIdCachesPositiveOwnerInSharedCache() {
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>();
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    OwnerMetaMapper ownerMetaMapper = mock(OwnerMetaMapper.class);
    OwnerInfo ownerInfo = new OwnerInfo(10L, "USER");
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(
            100L, MetadataObject.Type.TABLE.name()))
        .thenReturn(ownerInfo);

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<OwnerMetaMapper, OwnerInfo> function = invocation.getArgument(1);
                return function.apply(ownerMetaMapper);
              });

      Assertions.assertEquals(
          Optional.of(ownerInfo),
          lookups.resolveOwnerId(
              100L, MetadataObject.Type.TABLE, new AuthorizationRequestContext()));
      Assertions.assertEquals(
          Optional.of(ownerInfo),
          lookups.resolveOwnerId(
              100L, MetadataObject.Type.TABLE, new AuthorizationRequestContext()));
    }

    Assertions.assertEquals(2, ownerRelCache.getCount);
    Assertions.assertEquals(0, ownerRelCache.getIfPresentCount);
    Assertions.assertEquals(1, ownerRelCache.putCount);
    verify(ownerMetaMapper)
        .selectOwnerByMetadataObjectIdAndType(100L, MetadataObject.Type.TABLE.name());
  }

  @Test
  void testResolveOwnerIdCachesMissingOwnerInSharedCacheWithSameContext() {
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>();
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    try (MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils.when(() -> SessionUtils.getWithoutCommit(any(), any())).thenReturn(null);

      Assertions.assertFalse(
          lookups.resolveOwnerId(100L, MetadataObject.Type.TABLE, requestContext).isPresent());
      Assertions.assertFalse(
          lookups.resolveOwnerId(100L, MetadataObject.Type.TABLE, requestContext).isPresent());
    }

    // Shared cache consulted once; second call hits per-request cache.
    Assertions.assertEquals(1, ownerRelCache.getCount);
    Assertions.assertEquals(0, ownerRelCache.getIfPresentCount);
    // Absent result is now stored in the shared cache (putCount=1) so later requests skip the DB.
    Assertions.assertEquals(1, ownerRelCache.putCount);
  }

  @Test
  void testResolveOwnerIdCachesMissingOwnerInSharedCache() {
    OwnerMetaMapper ownerMetaMapper = mock(OwnerMetaMapper.class);
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(100L, "TABLE")).thenReturn(null);

    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    try (CaffeineGravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        MockedStatic<SessionUtils> sessionUtils = mockStatic(SessionUtils.class)) {
      sessionUtils
          .when(() -> SessionUtils.getWithoutCommit(any(), any()))
          .thenAnswer(
              invocation -> {
                Function<Object, Object> func = invocation.getArgument(1);
                return func.apply(ownerMetaMapper);
              });
      JcasbinAuthorizationLookups lookups =
          new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);

      Assertions.assertFalse(
          lookups
              .resolveOwnerId(100L, MetadataObject.Type.TABLE, new AuthorizationRequestContext())
              .isPresent());
      Assertions.assertFalse(
          lookups
              .resolveOwnerId(100L, MetadataObject.Type.TABLE, new AuthorizationRequestContext())
              .isPresent());
    }

    verify(ownerMetaMapper, times(1)).selectOwnerByMetadataObjectIdAndType(100L, "TABLE");
  }

  private static class CountingCache<K, V> implements GravitinoCache<K, V> {
    private final V value;
    private Optional<V> cachedValue = Optional.empty();
    private int getCount;
    private int getIfPresentCount;
    private int putCount;

    private CountingCache() {
      this.value = null;
    }

    private CountingCache(V value) {
      this.value = value;
    }

    @Override
    public Optional<V> getIfPresent(K key) {
      getIfPresentCount++;
      return cachedValue;
    }

    @Override
    public V get(K key, Function<K, V> loader) {
      getCount++;
      if (cachedValue.isPresent()) {
        return cachedValue.get();
      }
      if (value != null) {
        cachedValue = Optional.of(value);
        return value;
      }
      V loaded = loader.apply(key);
      putCount++;
      cachedValue = Optional.of(loaded);
      return loaded;
    }

    @Override
    public void put(K key, V value) {
      putCount++;
      cachedValue = Optional.of(value);
    }

    @Override
    public void invalidate(K key) {}

    @Override
    public void invalidateAll() {}

    @Override
    public void invalidateByPrefix(String prefix) {}

    @Override
    public long size() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
