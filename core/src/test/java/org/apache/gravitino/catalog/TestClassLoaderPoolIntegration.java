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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
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

    // Drop catalog1 (shares the pooled ClassLoader with catalog2). Releasing catalog1 must not
    // trigger the final-release cleanup while catalog2 still holds a reference.
    catalogManager.disableCatalog(NameIdentifier.of(METALAKE, "catalog1"));
    catalogManager.dropCatalog(NameIdentifier.of(METALAKE, "catalog1"));

    // catalog2 must still be functionally usable after catalog1's drop. Note: with
    // CATALOG_LOAD_ISOLATED=false the ClassLoader is an empty in-process loader, so this is an
    // end-to-end smoke check that the shared wrapper is not broken by the drop. The real proof that
    // the shared ClassLoader/driver is not prematurely cleaned up (which only matters with real
    // provider jars) lives in the docker-tagged IcebergClassLoaderPoolIT.
    NameIdentifier catalog2 = NameIdentifier.of(METALAKE, "catalog2");
    Schema schema =
        Assertions.assertDoesNotThrow(
            () ->
                catalogManager
                    .loadCatalogAndWrap(catalog2)
                    .doWithSchemaOps(
                        schemaOps ->
                            schemaOps.createSchema(
                                NameIdentifier.of(METALAKE, "catalog2", "schema1"),
                                "comment",
                                ImmutableMap.of())));
    Assertions.assertNotNull(schema);
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager
                .loadCatalogAndWrap(catalog2)
                .doWithSchemaOps(
                    schemaOps -> schemaOps.listSchemas(Namespace.of(METALAKE, "catalog2"))));
  }

  @Test
  public void testTestConnectionDoesNotBreakLiveCatalogSharingSameKey()
      throws IllegalAccessException {
    Map<String, String> props =
        ImmutableMap.of("key1", "value1", "key2", "value2", "key5-1", "value3");

    // A live catalog holds a reference to the pooled ClassLoader for this key.
    NameIdentifier liveIdent = NameIdentifier.of(METALAKE, "live_catalog");
    catalogManager.createCatalog(
        liveIdent, Catalog.Type.RELATIONAL, PROVIDER, "live catalog", props);

    CatalogManager.CatalogWrapper liveWrapper =
        catalogManager.getCatalogCache().getIfPresent(liveIdent);
    Assertions.assertNotNull(liveWrapper);

    ClassLoaderPool pool =
        (ClassLoaderPool) FieldUtils.readField(catalogManager, "classLoaderPool", true);
    Assertions.assertEquals(1, pool.size(), "one pooled entry for the shared key");

    // testConnection creates a throwaway wrapper that acquires and then releases the same key
    // synchronously (acquire on create, release in its finally). The release must only decrement
    // the reference count, not run final cleanup, because the live catalog still shares the key.
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.testConnection(
                NameIdentifier.of(METALAKE, "probe_catalog"),
                Catalog.Type.RELATIONAL,
                PROVIDER,
                "probe",
                props));

    // Strong, non-racy check: testConnection's acquire+release is synchronous, so the pooled entry
    // for the live catalog's key must survive (size stays 1). A refCount that wrongly hit 0 would
    // have removed the entry (size 0) and destroyed the live catalog's ClassLoader.
    Assertions.assertEquals(
        1, pool.size(), "shared-key entry must survive testConnection's acquire/release");

    // The live catalog must remain usable after testConnection's acquire/release cycle.
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager
                .loadCatalogAndWrap(liveIdent)
                .doWithSchemaOps(
                    schemaOps -> schemaOps.listSchemas(Namespace.of(METALAKE, "live_catalog"))));
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
