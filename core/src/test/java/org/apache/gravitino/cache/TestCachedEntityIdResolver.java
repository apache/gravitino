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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestCachedEntityIdResolver {

  private static final NameIdentifier TABLE = NameIdentifier.of("m1", "c1", "s1", "t1");

  @Test
  @SuppressWarnings("unchecked")
  void testEveryLevelCachedIsResolvedFromOneLookup() {
    EntityCache cache = mock(EntityCache.class);
    EntityIdResolver underlying = mock(EntityIdResolver.class);
    List<Optional<HasIdentifier>> cached = ids(4L, 3L, 2L, 1L);
    when(cache.peekAll(anyList())).thenReturn(cached);

    NamespacedEntityId ids =
        new CachedEntityIdResolver(cache, underlying).getEntityIds(TABLE, Entity.EntityType.TABLE);

    Assertions.assertArrayEquals(new long[] {1L, 2L, 3L, 4L}, ids.fullIds());
    ArgumentCaptor<List<EntityCacheKey>> chain = ArgumentCaptor.forClass(List.class);
    verify(cache).peekAll(chain.capture());
    Assertions.assertEquals(
        ImmutableList.of(
            EntityCacheKey.of(TABLE, Entity.EntityType.TABLE),
            EntityCacheKey.of(NameIdentifier.of("m1", "c1", "s1"), Entity.EntityType.SCHEMA),
            EntityCacheKey.of(NameIdentifier.of("m1", "c1"), Entity.EntityType.CATALOG),
            EntityCacheKey.of(NameIdentifier.of("m1"), Entity.EntityType.METALAKE)),
        chain.getValue());
    verify(underlying, never()).getEntityIds(any(), any());
  }

  @Test
  void testTheFirstUncachedLevelAndAboveComeFromTheUnderlyingResolver() {
    EntityCache cache = mock(EntityCache.class);
    EntityIdResolver underlying = mock(EntityIdResolver.class);
    // The table and schema are cached, the catalog is not; the metalake hit is not used.
    List<Optional<HasIdentifier>> cached = ids(4L, 3L, null, 1L);
    when(cache.peekAll(anyList())).thenReturn(cached);
    NameIdentifier catalog = NameIdentifier.of("m1", "c1");
    when(underlying.getEntityIds(catalog, Entity.EntityType.CATALOG))
        .thenReturn(new NamespacedEntityId(2L, 1L));

    NamespacedEntityId ids =
        new CachedEntityIdResolver(cache, underlying).getEntityIds(TABLE, Entity.EntityType.TABLE);

    Assertions.assertArrayEquals(new long[] {1L, 2L, 3L, 4L}, ids.fullIds());
  }

  @Test
  void testAnUncachedIdentifierIsResolvedEntirelyByTheUnderlyingResolver() {
    EntityCache cache = mock(EntityCache.class);
    EntityIdResolver underlying = mock(EntityIdResolver.class);
    List<Optional<HasIdentifier>> cached = ids(null, 3L, 2L, 1L);
    when(cache.peekAll(anyList())).thenReturn(cached);
    NamespacedEntityId expected = new NamespacedEntityId(4L, 1L, 2L, 3L);
    when(underlying.getEntityIds(TABLE, Entity.EntityType.TABLE)).thenReturn(expected);

    Assertions.assertSame(
        expected,
        new CachedEntityIdResolver(cache, underlying).getEntityIds(TABLE, Entity.EntityType.TABLE));
  }

  @Test
  void testANonCacheableTypeSkipsTheCache() {
    EntityCache cache = mock(EntityCache.class);
    EntityIdResolver underlying = mock(EntityIdResolver.class);
    NameIdentifier user = NameIdentifier.of("m1", "system", "user", "u1");

    new CachedEntityIdResolver(cache, underlying).getEntityIds(user, Entity.EntityType.USER);

    verify(cache, never()).peekAll(anyList());
    verify(underlying).getEntityIds(user, Entity.EntityType.USER);
  }

  private static List<Optional<HasIdentifier>> ids(Long... ids) {
    ImmutableList.Builder<Optional<HasIdentifier>> result = ImmutableList.builder();
    for (Long id : ids) {
      if (id == null) {
        result.add(Optional.empty());
      } else {
        HasIdentifier entity = mock(HasIdentifier.class);
        when(entity.id()).thenReturn(id);
        result.add(Optional.of(entity));
      }
    }
    return result.build();
  }
}
