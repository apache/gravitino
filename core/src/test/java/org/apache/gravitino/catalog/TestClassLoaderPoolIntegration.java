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
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore.InMemoryEntityStore;
import org.apache.gravitino.utils.ClassLoaderPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for ClassLoaderPool with CatalogManager. Tests that same-type catalogs share a
 * ClassLoader and that closing one catalog does not affect others of the same type.
 */
public class TestClassLoaderPoolIntegration {

  private static CatalogManager catalogManager;
  private static InMemoryEntityStore entityStore;
  private static Config config;
  private static final String METALAKE = "metalake";
  private static final String PROVIDER = "test";

  private static BaseMetalake metalakeEntity =
      BaseMetalake.builder()
          .withId(1L)
          .withName(METALAKE)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.put(metalakeEntity, true);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (catalogManager != null) {
      catalogManager.close();
    }
    if (entityStore != null) {
      entityStore.close();
    }
  }

  @BeforeEach
  public void beforeEach() throws IOException {
    catalogManager = new CatalogManager(config, entityStore, new RandomIdGenerator());
  }

  @AfterEach
  public void afterEach() throws IOException {
    if (catalogManager != null) {
      catalogManager.close();
    }
    entityStore.clear();
    entityStore.put(metalakeEntity, true);
  }

  @Test
  public void testSameTypeCatalogsShareClassLoader() throws IllegalAccessException {
    Map<String, String> props =
        ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

    Catalog catalog1 =
        catalogManager.createCatalog(
            NameIdentifier.of(METALAKE, "catalog1"),
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "test catalog 1",
            props);
    Catalog catalog2 =
        catalogManager.createCatalog(
            NameIdentifier.of(METALAKE, "catalog2"),
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "test catalog 2",
            props);

    Assertions.assertNotNull(catalog1);
    Assertions.assertNotNull(catalog2);

    // Both catalogs should use the same provider which means they share ClassLoader
    CatalogManager.CatalogWrapper wrapper1 =
        catalogManager.getCatalogCache().getIfPresent(NameIdentifier.of(METALAKE, "catalog1"));
    CatalogManager.CatalogWrapper wrapper2 =
        catalogManager.getCatalogCache().getIfPresent(NameIdentifier.of(METALAKE, "catalog2"));

    Assertions.assertNotNull(wrapper1);
    Assertions.assertNotNull(wrapper2);

    // Verify they actually share the same ClassLoader instance
    Object classLoader1 = FieldUtils.readField(wrapper1, "classLoader", true);
    Object classLoader2 = FieldUtils.readField(wrapper2, "classLoader", true);
    Assertions.assertSame(
        classLoader1, classLoader2, "Same-type catalogs should share a ClassLoader");
  }

  @Test
  public void testClosingOneCatalogDoesNotAffectOthers() {
    Map<String, String> props =
        ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

    catalogManager.createCatalog(
        NameIdentifier.of(METALAKE, "catalog1"),
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "test catalog 1",
        props);
    catalogManager.createCatalog(
        NameIdentifier.of(METALAKE, "catalog2"),
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "test catalog 2",
        props);

    // Drop catalog1
    catalogManager.disableCatalog(NameIdentifier.of(METALAKE, "catalog1"));
    catalogManager.dropCatalog(NameIdentifier.of(METALAKE, "catalog1"));

    // catalog2 should still be loadable
    Catalog loaded =
        Assertions.assertDoesNotThrow(
            () -> catalogManager.loadCatalog(NameIdentifier.of(METALAKE, "catalog2")));
    Assertions.assertNotNull(loaded);
  }

  @Test
  public void testSharingDisabledEachCatalogGetsOwnClassLoader() throws IllegalAccessException {
    Config noSharingConfig = new Config(false) {};
    noSharingConfig.set(Configs.CATALOG_LOAD_ISOLATED, false);
    noSharingConfig.set(Configs.CATALOG_CLASSLOADER_SHARING_ENABLED, false);

    CatalogManager noSharingManager =
        new CatalogManager(noSharingConfig, entityStore, new RandomIdGenerator());
    try {
      Map<String, String> props =
          ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

      noSharingManager.createCatalog(
          NameIdentifier.of(METALAKE, "catalog1"),
          Catalog.Type.RELATIONAL,
          PROVIDER,
          "test catalog 1",
          props);
      noSharingManager.createCatalog(
          NameIdentifier.of(METALAKE, "catalog2"),
          Catalog.Type.RELATIONAL,
          PROVIDER,
          "test catalog 2",
          props);

      CatalogManager.CatalogWrapper wrapper1 =
          noSharingManager.getCatalogCache().getIfPresent(NameIdentifier.of(METALAKE, "catalog1"));
      CatalogManager.CatalogWrapper wrapper2 =
          noSharingManager.getCatalogCache().getIfPresent(NameIdentifier.of(METALAKE, "catalog2"));

      Assertions.assertNotNull(wrapper1);
      Assertions.assertNotNull(wrapper2);

      // With sharing disabled, each catalog should have its own ClassLoader instance
      Object classLoader1 = FieldUtils.readField(wrapper1, "classLoader", true);
      Object classLoader2 = FieldUtils.readField(wrapper2, "classLoader", true);
      Assertions.assertNotSame(
          classLoader1,
          classLoader2,
          "With sharing disabled, catalogs should NOT share a ClassLoader");

      // Non-pooled wrappers should not hold a pool reference
      Object pool1 = FieldUtils.readField(wrapper1, "pool", true);
      Assertions.assertNull(pool1, "Non-pooled wrapper should not reference the pool");
    } finally {
      noSharingManager.close();
    }
  }

  @Test
  public void testSharingDisabledDropOneCatalogDoesNotAffectOther() {
    Config noSharingConfig = new Config(false) {};
    noSharingConfig.set(Configs.CATALOG_LOAD_ISOLATED, false);
    noSharingConfig.set(Configs.CATALOG_CLASSLOADER_SHARING_ENABLED, false);

    CatalogManager noSharingManager =
        new CatalogManager(noSharingConfig, entityStore, new RandomIdGenerator());
    try {
      Map<String, String> props =
          ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

      noSharingManager.createCatalog(
          NameIdentifier.of(METALAKE, "catalog1"),
          Catalog.Type.RELATIONAL,
          PROVIDER,
          "test catalog 1",
          props);
      noSharingManager.createCatalog(
          NameIdentifier.of(METALAKE, "catalog2"),
          Catalog.Type.RELATIONAL,
          PROVIDER,
          "test catalog 2",
          props);

      // Drop catalog1
      noSharingManager.disableCatalog(NameIdentifier.of(METALAKE, "catalog1"));
      noSharingManager.dropCatalog(NameIdentifier.of(METALAKE, "catalog1"));

      // catalog2 should still be loadable
      Catalog loaded =
          Assertions.assertDoesNotThrow(
              () -> noSharingManager.loadCatalog(NameIdentifier.of(METALAKE, "catalog2")));
      Assertions.assertNotNull(loaded);
    } finally {
      noSharingManager.close();
    }
  }

  @Test
  public void testClassLoaderPoolCleanupOnManagerClose() throws IllegalAccessException {
    Map<String, String> props =
        ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

    catalogManager.createCatalog(
        NameIdentifier.of(METALAKE, "catalog1"),
        Catalog.Type.RELATIONAL,
        PROVIDER,
        "test catalog 1",
        props);

    ClassLoaderPool pool =
        (ClassLoaderPool) FieldUtils.readField(catalogManager, "classLoaderPool", true);
    Assertions.assertNotNull(pool);

    catalogManager.close();

    // After close, the pool should be empty
    Assertions.assertEquals(0, pool.size());
    catalogManager = null;
  }
}
