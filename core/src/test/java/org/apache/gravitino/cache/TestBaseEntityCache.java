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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the cacheable entity type allowlist that {@link BaseEntityCache} enforces on behalf of
 * every implementation.
 */
public class TestBaseEntityCache {

  private static final Set<Entity.EntityType> CACHEABLE_TYPES =
      ImmutableSet.of(
          Entity.EntityType.METALAKE,
          Entity.EntityType.CATALOG,
          Entity.EntityType.SCHEMA,
          Entity.EntityType.TABLE,
          Entity.EntityType.TOPIC,
          Entity.EntityType.VIEW,
          Entity.EntityType.FILESET,
          Entity.EntityType.TAG,
          Entity.EntityType.POLICY,
          Entity.EntityType.JOB,
          Entity.EntityType.USER,
          Entity.EntityType.GROUP);

  private RecordingCache cache;

  @BeforeEach
  void setUp() {
    cache = new RecordingCache(new Config() {});
  }

  @Test
  void testIsCacheableAcceptsOnlyApprovedTypes() {
    for (Entity.EntityType type : Entity.EntityType.values()) {
      Assertions.assertEquals(
          CACHEABLE_TYPES.contains(type),
          BaseEntityCache.isCacheable(type),
          "Unexpected cacheability for " + type);
    }
  }

  @Test
  void testIsCacheableRejectsNullType() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BaseEntityCache.isCacheable(null));
  }

  @Test
  void testPutSkipsNonCacheableTypesForSubclasses() {
    cache.put(TestUtil.getTestRoleEntity());

    Assertions.assertTrue(
        cache.cached.isEmpty(), "Subclasses must never see non-cacheable entities in doPut");
  }

  @Test
  void testPutCachesUserAndGroup() {
    cache.put(TestUtil.getTestUserEntity());
    cache.put(TestUtil.getTestGroupEntity());

    Assertions.assertEquals(2, cache.cached.size());
  }

  @Test
  void testPutDelegatesCacheableTypesToSubclass() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    cache.put(catalog);

    Assertions.assertEquals(1, cache.cached.size());
    Assertions.assertEquals(catalog, cache.cached.get(0));
  }

  @Test
  void testPutInvalidatesRelatedKeysEvenForNonCacheableTypes() {
    cache.put(TestUtil.getTestRoleEntity());

    Assertions.assertEquals(
        1,
        cache.keyChanges.size(),
        "invalidateOnKeyChange must run for non-cacheable entities as well");
  }

  @Test
  void testPutRejectsNullEntity() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.put(null));
  }

  /** A minimal {@link BaseEntityCache} that records everything handed down to it. */
  private static class RecordingCache extends BaseEntityCache {
    private final List<Entity> cached = new ArrayList<>();
    private final List<Entity> keyChanges = new ArrayList<>();

    RecordingCache(Config config) {
      super(config);
    }

    @Override
    protected <E extends Entity & HasIdentifier> void doPut(E entity) {
      cached.add(entity);
    }

    @Override
    public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {
      keyChanges.add(entity);
    }

    @Override
    protected void invalidateExpiredItem(EntityCacheKey key) {}

    @Override
    public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
        NameIdentifier ident, Entity.EntityType type) {
      return Optional.empty();
    }

    @Override
    public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
      return false;
    }

    @Override
    public boolean contains(NameIdentifier ident, Entity.EntityType type) {
      return false;
    }

    @Override
    public void clear() {}

    @Override
    public long size() {
      return cached.size();
    }

    @Override
    public <E extends Exception> void withCacheLock(EntityCacheKey key, ThrowingRunnable<E> action)
        throws E {
      action.run();
    }

    @Override
    public <T, E extends Exception> T withCacheLock(
        EntityCacheKey key, ThrowingSupplier<T, E> action) throws E {
      return action.get();
    }
  }
}
