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

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link JcasbinAuthorizationLookups} static helpers. */
public class TestJcasbinAuthorizationLookups {

  // ---------- buildCacheKey ----------

  @Test
  void testBuildCacheKeyMetalake() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("ml1"), MetadataObject.Type.METALAKE);
    Assertions.assertEquals(key("ml1", ""), JcasbinAuthorizationLookups.buildCacheKey("ml1", obj));
  }

  @Test
  void testBuildCacheKeyCatalog() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    Assertions.assertEquals(
        key("ml1", "cat1", ""), JcasbinAuthorizationLookups.buildCacheKey("ml1", obj));
  }

  @Test
  void testBuildCacheKeySchema() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", ""), JcasbinAuthorizationLookups.buildCacheKey("ml1", obj));
  }

  @Test
  void testBuildCacheKeyLeafTypesGetTypeSuffix() {
    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "tbl1", "TABLE"),
        JcasbinAuthorizationLookups.buildCacheKey("ml1", table));

    MetadataObject view =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "v1"), MetadataObject.Type.VIEW);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "v1", "VIEW"),
        JcasbinAuthorizationLookups.buildCacheKey("ml1", view));

    MetadataObject fileset =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "fs1"), MetadataObject.Type.FILESET);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "fs1", "FILESET"),
        JcasbinAuthorizationLookups.buildCacheKey("ml1", fileset));
  }

  // ---------- isContainerType ----------

  @Test
  void testIsContainerTypeContainerTypes() {
    Assertions.assertTrue(
        JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.SCHEMA));
  }

  @Test
  void testIsContainerTypeLeafTypes() {
    Assertions.assertFalse(JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.VIEW));
    Assertions.assertFalse(
        JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(JcasbinAuthorizationLookups.isContainerType(MetadataObject.Type.TOPIC));
  }

  // ---------- Prefix invalidation ----------

  @Test
  void testPrefixInvalidationCoversContainerPath() {
    // Dropping a catalog should use a prefix that covers all schemas and tables below it.
    MetadataObject catalog =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    String catalogKey = JcasbinAuthorizationLookups.buildCacheKey("ml1", catalog);

    MetadataObject schema =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    String schemaKey = JcasbinAuthorizationLookups.buildCacheKey("ml1", schema);

    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    String tableKey = JcasbinAuthorizationLookups.buildCacheKey("ml1", table);

    Assertions.assertTrue(schemaKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(schemaKey));
  }

  @Test
  void testResolveMetadataIdUsesAtomicSharedCacheAndRequestDedup() {
    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>(Optional.empty());
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    Assertions.assertEquals(100L, lookups.resolveMetadataId(table, "ml1", requestContext));
    Assertions.assertEquals(100L, lookups.resolveMetadataId(table, "ml1", requestContext));

    Assertions.assertEquals(1, metadataIdCache.getCount);
    Assertions.assertEquals(0, metadataIdCache.getIfPresentCount);
    Assertions.assertEquals(0, metadataIdCache.putCount);
  }

  @Test
  void testResolveOwnerIdUsesAtomicSharedCacheAndRequestDedup() {
    CountingCache<String, Long> metadataIdCache = new CountingCache<>(100L);
    CountingCache<Long, Optional<OwnerInfo>> ownerRelCache = new CountingCache<>(Optional.empty());
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(metadataIdCache, ownerRelCache);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    Assertions.assertFalse(
        lookups.resolveOwnerId(100L, MetadataObject.Type.TABLE, requestContext).isPresent());
    Assertions.assertFalse(
        lookups.resolveOwnerId(100L, MetadataObject.Type.TABLE, requestContext).isPresent());

    Assertions.assertEquals(1, ownerRelCache.getCount);
    Assertions.assertEquals(0, ownerRelCache.getIfPresentCount);
    Assertions.assertEquals(0, ownerRelCache.putCount);
  }

  private static String key(String... parts) {
    return String.join(JcasbinAuthorizationLookups.KEY_SEP, parts);
  }

  private static class CountingCache<K, V> implements GravitinoCache<K, V> {
    private final V value;
    private int getCount;
    private int getIfPresentCount;
    private int putCount;

    private CountingCache(V value) {
      this.value = value;
    }

    @Override
    public Optional<V> getIfPresent(K key) {
      getIfPresentCount++;
      return Optional.empty();
    }

    @Override
    public V get(K key, Function<K, V> loader) {
      getCount++;
      return value;
    }

    @Override
    public void put(K key, V value) {
      putCount++;
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
