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
import java.time.Duration;
import java.util.List;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
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
    Assertions.assertEquals(40_000_000L, EntityCacheWeigher.getMaxWeight());
    Assertions.assertEquals("caffeine", config.get(Configs.CACHE_IMPLEMENTATION));
  }

  @Test
  void testCaffeineCacheWithWeight() throws Exception {
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumWeight(500);
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

    // Only some of the 100 schemas are still in the cache, to be exact, 500 / 10 = 50 schemas.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(10))
        .until(() -> cache.asMap().size() == 10 + 3 + 500 / 10);
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
