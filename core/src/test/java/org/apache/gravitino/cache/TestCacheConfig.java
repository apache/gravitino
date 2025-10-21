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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCacheConfig {
  @Test
  void testDefaultCacheConfig() {
    Config config = new Config(false) {};
    Assertions.assertFalse(config.get(Configs.CACHE_STATS_ENABLED));
    Assertions.assertTrue(config.get(Configs.CACHE_ENABLED));
    Assertions.assertTrue(config.get(Configs.CACHE_WEIGHER_ENABLED));
    Assertions.assertEquals(10_000, config.get(Configs.CACHE_MAX_ENTRIES));
    Assertions.assertEquals(3_600_000L, config.get(Configs.CACHE_EXPIRATION_TIME));
    Assertions.assertEquals(9_000_000L, EntityCacheWeigher.getMaxWeight());
    Assertions.assertEquals("caffeine", config.get(Configs.CACHE_IMPLEMENTATION));
  }

  @Test
  void testPolicyAndTagCacheWeigher() throws InterruptedException {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumWeight(2000);
    builder.weigher(EntityCacheWeigher.getInstance());
    Cache<EntityCacheRelationKey, List<Entity>> cache = builder.build();

    BaseMetalake baseMetalake =
        BaseMetalake.builder()
            .withName("metalake1")
            .withId(1L)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    cache.put(
        EntityCacheRelationKey.of(NameIdentifier.of("metalake1"), Entity.EntityType.METALAKE),
        List.of(baseMetalake));
    CatalogEntity catalogEntity =
        CatalogEntity.builder()
            .withNamespace(Namespace.of("metalake1"))
            .withName("catalog1")
            .withProvider("provider")
            .withAuditInfo(AuditInfo.EMPTY)
            .withId(100L)
            .withType(Catalog.Type.RELATIONAL)
            .build();
    cache.put(
        EntityCacheRelationKey.of(
            NameIdentifier.of(new String[] {"metalake1", "catalog1"}), Entity.EntityType.CATALOG),
        List.of(catalogEntity));

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withNamespace(Namespace.of("metalake1", "catalog1"))
            .withName("schema1")
            .withAuditInfo(AuditInfo.EMPTY)
            .withId(1000L)
            .build();
    cache.put(
        EntityCacheRelationKey.of(
            NameIdentifier.of(new String[] {"metalake1", "catalog1", "schema1"}),
            Entity.EntityType.SCHEMA),
        List.of(schemaEntity));

    for (int i = 0; i < 5; i++) {
      String filesetName = "fileset" + i;
      FilesetEntity fileset =
          FilesetEntity.builder()
              .withNamespace(Namespace.of("metalake1", "catalog1", "schema1"))
              .withName(filesetName)
              .withAuditInfo(AuditInfo.EMPTY)
              .withStorageLocations(ImmutableMap.of("default", "s3://bucket/path"))
              .withId((long) (i + 1) * 10_000)
              .withFilesetType(Fileset.Type.MANAGED)
              .build();
      cache.put(
          EntityCacheRelationKey.of(
              NameIdentifier.of(new String[] {"metalake1", "catalog1", "schema1", filesetName}),
              Entity.EntityType.FILESET),
          List.of(fileset));
    }

    for (int i = 0; i < 10; i++) {
      String tagName = "tag" + i;
      NameIdentifier tagNameIdent = NameIdentifierUtil.ofTag("metalake", tagName);
      TagEntity tagEntity =
          TagEntity.builder()
              .withNamespace(tagNameIdent.namespace())
              .withName(tagName)
              .withAuditInfo(AuditInfo.EMPTY)
              .withId((long) (i + 1) * 100_000)
              .build();
      cache.put(EntityCacheRelationKey.of(tagNameIdent, Entity.EntityType.TAG), List.of(tagEntity));
    }

    // The weight of the cache has exceeded 2000, some entities will be evicted if we continue to
    // add fileset entities.
    for (int i = 5; i < 15; i++) {
      String filesetName = "fileset" + i;
      FilesetEntity fileset =
          FilesetEntity.builder()
              .withNamespace(Namespace.of("metalake1", "catalog1", "schema1"))
              .withName(filesetName)
              .withAuditInfo(AuditInfo.EMPTY)
              .withStorageLocations(ImmutableMap.of("default", "s3://bucket/path"))
              .withId((long) (i + 1) * 10_000)
              .withFilesetType(Fileset.Type.MANAGED)
              .build();
      cache.put(
          EntityCacheRelationKey.of(
              NameIdentifier.of(new String[] {"metalake1", "catalog1", "schema1", filesetName}),
              Entity.EntityType.FILESET),
          List.of(fileset));
    }

    // Access all filesets to make them more likely to be retained
    for (int i = 5; i < 15; i++) {
      String filesetName = "fileset" + i;
      cache.getIfPresent(
          EntityCacheRelationKey.of(
              NameIdentifier.of(new String[] {"metalake1", "catalog1", "schema1", filesetName}),
              Entity.EntityType.FILESET));
    }

    cache.cleanUp(); // Force synchronous eviction

    // Count how many tags are still in cache - expect some to be evicted
    long remainingTags =
        IntStream.range(0, 10)
            .mapToObj(i -> NameIdentifierUtil.ofTag("metalake", "tag" + i))
            .filter(
                tagNameIdent ->
                    cache.getIfPresent(
                            EntityCacheRelationKey.of(tagNameIdent, Entity.EntityType.TAG))
                        != null)
            .count();

    Assertions.assertTrue(
        remainingTags < 10,
        "Expected some tags to be evicted, but found " + remainingTags + " tags still in cache");
  }

  @Test
  void testCaffeineCacheWithWeight() throws Exception {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumWeight(5000);
    builder.weigher(EntityCacheWeigher.getInstance());
    Cache<EntityCacheRelationKey, List<Entity>> cache = builder.build();

    // Insert 3 metalakes
    for (int i = 0; i < 3; i++) {
      BaseMetalake baseMetalake =
          BaseMetalake.builder()
              .withName("metalake" + 1)
              .withId((long) i)
              .withVersion(SchemaVersion.V_0_1)
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
      cache.put(
          EntityCacheRelationKey.of(NameIdentifier.of("metalake" + i), Entity.EntityType.METALAKE),
          List.of(baseMetalake));
    }

    // Insert 10 catalogs
    for (int i = 0; i < 10; i++) {
      CatalogEntity catalogEntity =
          CatalogEntity.builder()
              .withNamespace(Namespace.of("metalake1"))
              .withName("catalog" + i)
              .withProvider("provider")
              .withAuditInfo(AuditInfo.EMPTY)
              .withId((long) ((i + 1) * 100))
              .withType(Catalog.Type.RELATIONAL)
              .build();
      cache.put(
          EntityCacheRelationKey.of(
              NameIdentifier.of("metalake1.catalog" + i), Entity.EntityType.CATALOG),
          List.of(catalogEntity));
    }

    // insert 100 schemas
    for (int i = 0; i < 100; i++) {
      SchemaEntity schemaEntity =
          SchemaEntity.builder()
              .withNamespace(Namespace.of("metalake1", "catalog1"))
              .withName("schema" + i)
              .withAuditInfo(AuditInfo.EMPTY)
              .withId((long) ((i + 1) * 1000))
              .build();

      cache.put(
          EntityCacheRelationKey.of(
              NameIdentifier.of("metalake1.catalog1.schema" + i), Entity.EntityType.SCHEMA),
          List.of(schemaEntity));
    }

    // Three 3 metalakes still in cache.
    for (int i = 0; i < 3; i++) {
      Assertions.assertNotNull(
          cache.getIfPresent(
              EntityCacheRelationKey.of(
                  NameIdentifier.of("metalake" + 1), Entity.EntityType.METALAKE)));
    }

    // 10 catalogs still in cache.
    for (int i = 0; i < 10; i++) {
      Assertions.assertNotNull(
          cache.getIfPresent(
              EntityCacheRelationKey.of(
                  NameIdentifier.of("metalake1.catalog" + i), Entity.EntityType.CATALOG)));
    }

    // Only some of the 100 schemas are still in the cache, to be exact, 5000 / 500 = 10 schemas.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(10))
        .until(() -> cache.asMap().size() == 10 + 3 + 5000 / 500);
  }

  @Test
  void testSetConfigValues() {
    Config config = new Config(false) {};
    config.set(Configs.CACHE_ENABLED, false);
    config.set(Configs.CACHE_STATS_ENABLED, true);
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);
    config.set(Configs.CACHE_MAX_ENTRIES, 5000);
    config.set(Configs.CACHE_EXPIRATION_TIME, 600_000L);

    Assertions.assertFalse(config.get(Configs.CACHE_ENABLED));
    Assertions.assertTrue(config.get(Configs.CACHE_STATS_ENABLED));
    Assertions.assertFalse(config.get(Configs.CACHE_WEIGHER_ENABLED));
    Assertions.assertEquals(5000, config.get(Configs.CACHE_MAX_ENTRIES));
    Assertions.assertEquals(600_000L, config.get(Configs.CACHE_EXPIRATION_TIME));
  }
}
