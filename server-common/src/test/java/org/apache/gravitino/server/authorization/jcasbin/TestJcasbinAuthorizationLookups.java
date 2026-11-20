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

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.CaffeineGravitinoCache;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.tag.TagAssignmentAuthorization;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests for {@link JcasbinAuthorizationLookups}. */
public class TestJcasbinAuthorizationLookups {

  @Test
  void testTableIdColdWarmAndInvalidatedCacheAvoidFullReads() throws Exception {
    RelationalEntityStore store = mock(RelationalEntityStore.class);
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.entityStore()).thenReturn(store);
    NameIdentifier ident = NameIdentifier.of("ml", "cat", "sch", "tbl");
    MetadataObject table = MetadataObjects.parse("cat.sch.tbl", MetadataObject.Type.TABLE);
    when(store.getTableId(ident)).thenReturn(42L, 43L);
    try (CaffeineGravitinoCache<String, Long> ids = new CaffeineGravitinoCache<>(60_000L, 100L);
        CaffeineGravitinoCache<Long, Optional<OwnerInfo>> owners =
            new CaffeineGravitinoCache<>(60_000L, 100L);
        MockedStatic<GravitinoEnv> mockedEnv = mockStatic(GravitinoEnv.class);
        MockedStatic<MetadataIdConverter> converter =
            mockStatic(MetadataIdConverter.class, CALLS_REAL_METHODS)) {
      mockedEnv.when(GravitinoEnv::getInstance).thenReturn(env);
      converter
          .when(() -> MetadataIdConverter.normalizeMetadataObject(table, "ml"))
          .thenReturn(table);
      JcasbinAuthorizationLookups lookups = new JcasbinAuthorizationLookups(ids, owners);
      AuthorizationRequestContext first = new AuthorizationRequestContext();
      Assertions.assertEquals(Optional.of(42L), lookups.resolveMetadataId(table, "ml", first));
      Assertions.assertEquals(Optional.of(42L), lookups.resolveMetadataId(table, "ml", first));
      Assertions.assertEquals(
          Optional.of(42L),
          lookups.resolveMetadataId(table, "ml", new AuthorizationRequestContext()));
      verify(store, times(1)).getTableId(ident);
      ids.invalidateAll();
      Assertions.assertEquals(
          Optional.of(43L),
          lookups.resolveMetadataId(table, "ml", new AuthorizationRequestContext()));
      verify(store, times(2)).getTableId(ident);
      verifyNoMoreInteractions(store);
    }
  }

  @Test
  void testScopedSchemaAncestorIdentity() {
    MetadataObject schema = MetadataObjects.parse("cat1.outer", MetadataObject.Type.SCHEMA);
    NameIdentifier schemaIdentifier = NameIdentifier.of("ml1", "cat1", "outer");
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, new CountingCache<>());
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(Capability.DEFAULT);
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.catalogManager()).thenReturn(catalogs);
    try (MockedStatic<GravitinoEnv> envMock = mockStatic(GravitinoEnv.class);
        TagAssignmentAuthorization scope = TagAssignmentAuthorization.open()) {
      envMock.when(GravitinoEnv::getInstance).thenReturn(env);
      TagAssignmentAuthorization.recordTarget(
          NameIdentifier.of("ml1", "cat1", "outer:inner"), Entity.EntityType.SCHEMA, null);
      TagAssignmentAuthorization.recordSchemaAncestor(
          schemaIdentifier, new NamespacedEntityId(100L, 1L, 2L));
      Assertions.assertEquals(
          Optional.of(100L),
          lookups.resolveMetadataId(schema, "ml1", new AuthorizationRequestContext()));
      assertScopedSchemaLookupFails(
          lookups, schema, schemaIdentifier, new NamespacedEntityId(101L, 1L, 2L));
      assertScopedSchemaLookupFails(lookups, schema, schemaIdentifier, null);
    }
  }

  @Test
  void testResolveMetadataIdUsesAtomicSharedCacheAndRequestDedup() {
    MetadataObject catalog = MetadataObjects.of(Arrays.asList("cat1"), MetadataObject.Type.CATALOG);
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>();
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    Assertions.assertEquals(
        Optional.of(100L), lookups.resolveMetadataId(catalog, "ml1", requestContext));
    Assertions.assertEquals(
        Optional.of(100L), lookups.resolveMetadataId(catalog, "ml1", requestContext));

    Assertions.assertEquals(1, metadataIdCache.getCount);
    Assertions.assertEquals(0, metadataIdCache.getIfPresentCount);
    Assertions.assertEquals(0, metadataIdCache.putCount);
  }

  @Test
  void testNormalizedTableUsesAtomicSharedCacheAndRequestDedup() {
    MetadataObject alias = MetadataObjects.parse("cat1.SCH1.TBL1", MetadataObject.Type.TABLE);
    MetadataObject canonical = MetadataObjects.parse("cat1.sch1.tbl1", MetadataObject.Type.TABLE);
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>();
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    Capability capability =
        new Capability() {
          @Override
          public CapabilityResult caseSensitiveOnName(Scope scope) {
            return CapabilityResult.unsupported("case insensitive");
          }
        };
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(capability);
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.catalogManager()).thenReturn(catalogs);
    try (MockedStatic<GravitinoEnv> envMock = mockStatic(GravitinoEnv.class)) {
      envMock.when(GravitinoEnv::getInstance).thenReturn(env);
      Assertions.assertEquals(Optional.of(100L), lookups.resolveMetadataId(alias, "ml1", context));
      Assertions.assertEquals(Optional.of(100L), lookups.resolveMetadataId(alias, "ml1", context));
      Assertions.assertEquals(
          Optional.of(100L), lookups.resolveMetadataId(canonical, "ml1", context));
    }
    // Each distinct spelling resolves capabilities and the physical table name once.
    verify(catalogs, times(4)).doWithCatalog(any(), any());
    verify(catalog, times(2)).capability();
    verify(catalog, times(2)).ops();
    Assertions.assertEquals(
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", canonical),
        metadataIdCache.lastKey);
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

  private static void assertScopedSchemaLookupFails(
      JcasbinAuthorizationLookups lookups,
      MetadataObject schema,
      NameIdentifier schemaIdentifier,
      @Nullable NamespacedEntityId recordedAncestor) {
    try (TagAssignmentAuthorization nested = TagAssignmentAuthorization.open()) {
      TagAssignmentAuthorization.recordTarget(
          NameIdentifier.of("ml1", "cat1", "outer:inner"), Entity.EntityType.SCHEMA, null);
      TagAssignmentAuthorization.recordSchemaAncestor(schemaIdentifier, recordedAncestor);
      JcasbinAuthorizationLookups.MetadataIdResolution resolution =
          lookups.resolveMetadataIdResult(schema, "ml1", new AuthorizationRequestContext());
      Assertions.assertEquals(Optional.empty(), resolution.metadataId());
      Assertions.assertTrue(resolution.normalizationFailed());
    }
  }

  private static class CountingCache<K, V> implements GravitinoCache<K, V> {
    private final V value;
    private Optional<V> cachedValue = Optional.empty();
    private K lastKey;
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
      lastKey = key;
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
