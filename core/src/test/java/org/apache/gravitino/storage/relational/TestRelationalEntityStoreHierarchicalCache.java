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

package org.apache.gravitino.storage.relational;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Verifies that dropping a hierarchical schema through a real {@link RelationalEntityStore} also
 * drops its nested descendants from the entity cache, for the default and a non-default schema
 * separator.
 */
public class TestRelationalEntityStoreHierarchicalCache {

  private static final String METALAKE = "metalake_hs";
  private static final String CATALOG = "catalog_hs";
  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private RelationalEntityStore store;
  private String dbPath;
  private Object previousConfig;

  @AfterEach
  void tearDown() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
    if (dbPath != null) {
      FileUtils.deleteQuietly(new File(dbPath));
      dbPath = null;
    }
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {":", "|"})
  void testDropHierarchicalSchemaEvictsNestedDescendantsFromCache(String separator)
      throws Exception {
    initStore(separator);

    String parentName = String.join(separator, "raw", "events");
    String childName = String.join(separator, "raw", "events", "2024");
    String siblingName = String.join(separator, "raw", "events2");

    store.put(metalake(), false);
    store.put(catalog(), false);
    SchemaEntity parent = schema(parentName);
    SchemaEntity child = schema(childName);
    SchemaEntity sibling = schema(siblingName);
    store.put(parent, false);
    store.put(child, false);
    store.put(sibling, false);
    TableEntity tableInChild = table("t_child", childName);
    store.put(tableInChild, false);

    // Read everything back so the cache is populated with the names the store actually returns.
    store.get(parent.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class);
    store.get(child.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class);
    store.get(sibling.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class);
    store.get(tableInChild.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);
    Assertions.assertTrue(
        store.getCache().contains(child.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(
        store.getCache().contains(tableInChild.nameIdentifier(), Entity.EntityType.TABLE));

    // The cache is keyed by the identifier that reaches the store, which still carries the
    // configured external separator; the physical separator only exists in the backend rows.
    Set<String> cacheKeys =
        ((CaffeineEntityCache) store.getCache())
            .getCacheData().asMap().keySet().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
    Assertions.assertTrue(
        cacheKeys.stream().anyMatch(key -> key.contains(childName)),
        "nested schema must be cached under its logical name, keys: " + cacheKeys);
    Assertions.assertTrue(
        cacheKeys.stream()
            .noneMatch(key -> key.contains(HierarchicalSchemaUtil.physicalSeparator())),
        "no cache key may carry the physical separator, keys: " + cacheKeys);

    store.delete(parent.nameIdentifier(), Entity.EntityType.SCHEMA, true);

    Assertions.assertFalse(
        store.getCache().contains(parent.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(
        store.getCache().contains(child.nameIdentifier(), Entity.EntityType.SCHEMA),
        "nested schema must not survive the drop of its parent");
    Assertions.assertFalse(
        store.getCache().contains(tableInChild.nameIdentifier(), Entity.EntityType.TABLE),
        "table of a nested schema must not survive the drop of the parent schema");
    Assertions.assertTrue(
        store.getCache().contains(sibling.nameIdentifier(), Entity.EntityType.SCHEMA),
        "a sibling sharing a name prefix must not be invalidated");
  }

  private void initStore(String separator) throws Exception {
    dbPath = "/tmp/gravitino_hs_cache_test_" + UUID.randomUUID().toString().replace("-", "");
    File dir = new File(dbPath);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Failed to create test directory " + dbPath);
    }

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
        .thenReturn(Configs.DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", dbPath));
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123456");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
        .thenReturn("org.h2.Driver");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS))
        .thenReturn(Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
        .thenReturn(Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_MAX_WAIT_MILLISECONDS);
    Mockito.when(config.get(Configs.STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(Configs.VERSION_RETENTION_COUNT)).thenReturn(1L);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)).thenReturn(3L);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_RETENTION_SECS)).thenReturn(24 * 60 * 60L);
    Mockito.when(config.get(Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)).thenReturn(60 * 60L);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_LOCK_SEGMENTS)).thenReturn(16);
    Mockito.when(config.get(Configs.SCHEMA_SEPARATOR)).thenReturn(separator);

    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "idGenerator", RandomIdGenerator.INSTANCE, true);
    Assertions.assertEquals(separator, HierarchicalSchemaUtil.schemaSeparator());

    store = new RelationalEntityStore();
    store.initialize(config);
  }

  private static BaseMetalake metalake() {
    return BaseMetalake.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(METALAKE)
        .withAuditInfo(AUDIT_INFO)
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static CatalogEntity catalog() {
    return CatalogEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(CATALOG)
        .withNamespace(Namespace.of(METALAKE))
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment("")
        .withProperties(null)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static SchemaEntity schema(String name) {
    return SchemaEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(Namespace.of(METALAKE, CATALOG))
        .withComment("")
        .withProperties(null)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static TableEntity table(String name, String schemaName) {
    return TableEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(Namespace.of(METALAKE, CATALOG, schemaName))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
