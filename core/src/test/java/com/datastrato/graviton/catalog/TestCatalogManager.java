/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.StringIdentifier;
import com.datastrato.graviton.TestEntityStore;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.SchemaVersion;
import com.datastrato.graviton.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestCatalogManager {

  private static CatalogManager catalogManager;

  private static EntityStore entityStore;

  private static Config config;

  private static String metalake = "metalake";

  private static String provider = "test";

  @BeforeAll
  public static void setUp() throws IOException {
    config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = new TestEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    BaseMetalake metalakeEntity =
        new BaseMetalake.Builder()
            .withId(1L)
            .withName(metalake)
            .withAuditInfo(
                new AuditInfo.Builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();
    entityStore.put(metalakeEntity, true);

    catalogManager = new CatalogManager(config, entityStore, new RandomIdGenerator());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }

    if (catalogManager != null) {
      catalogManager.close();
      catalogManager = null;
    }
  }

  @Test
  public void testCreateCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test1");
    Map<String, String> props = ImmutableMap.of();

    Catalog testCatalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    Assertions.assertEquals("test1", testCatalog.name());
    Assertions.assertEquals("comment", testCatalog.comment());
    testProperties(props, testCatalog.properties());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, testCatalog.type());

    // Test create under non-existed metalake
    NameIdentifier ident2 = NameIdentifier.of("metalake1", "test1");
    Throwable exception1 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                catalogManager.createCatalog(
                    ident2, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(exception1.getMessage().contains("Metalake metalake1 does not exist"));

    // Test create with duplicated name
    Throwable exception2 =
        Assertions.assertThrows(
            CatalogAlreadyExistsException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(
        exception2.getMessage().contains("Catalog metalake.test1 already exists"));

    // Test if the catalog is already cached
    CatalogManager.CatalogWrapper cached = catalogManager.catalogCache.getIfPresent(ident);
    Assertions.assertNotNull(cached);
  }

  @Test
  public void testListCatalogs() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test11");
    NameIdentifier ident1 = NameIdentifier.of("metalake", "test12");
    Map<String, String> props = ImmutableMap.of("provider", "test");

    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    catalogManager.createCatalog(ident1, Catalog.Type.RELATIONAL, provider, "comment", props);

    Set<NameIdentifier> idents = Sets.newHashSet(catalogManager.listCatalogs(ident.namespace()));
    Assertions.assertEquals(2, idents.size());
    Assertions.assertEquals(Sets.newHashSet(ident, ident1), idents);

    // Test list under non-existed metalake
    NameIdentifier ident2 = NameIdentifier.of("metalake1", "test1");
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> catalogManager.listCatalogs(ident2.namespace()));
    Assertions.assertTrue(exception.getMessage().contains("Metalake metalake1 does not exist"));
  }

  @Test
  public void testLoadCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test21");
    Map<String, String> props = ImmutableMap.of("provider", "test");

    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);

    Catalog catalog = catalogManager.loadCatalog(ident);
    Assertions.assertEquals("test21", catalog.name());
    Assertions.assertEquals("comment", catalog.comment());
    testProperties(props, catalog.properties());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test load non-existed catalog
    NameIdentifier ident1 = NameIdentifier.of("metalake", "test22");
    Throwable exception =
        Assertions.assertThrows(
            NoSuchCatalogException.class, () -> catalogManager.loadCatalog(ident1));
    Assertions.assertTrue(
        exception.getMessage().contains("Catalog metalake.test22 does not exist"));

    // Load operation will cache the catalog
    Assertions.assertNotNull(catalogManager.catalogCache.getIfPresent(ident));
  }

  @Test
  public void testAlterCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test31");
    Map<String, String> props = ImmutableMap.of("provider", "test");
    String comment = "comment";

    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);

    // Test alter name;
    CatalogChange change = CatalogChange.rename("test32");
    catalogManager.alterCatalog(ident, change);
    Catalog catalog = catalogManager.loadCatalog(NameIdentifier.of(ident.namespace(), "test32"));
    Assertions.assertEquals("test32", catalog.name());

    // Test alter comment;
    NameIdentifier ident1 = NameIdentifier.of(ident.namespace(), "test32");
    CatalogChange change1 = CatalogChange.updateComment("comment1");
    catalogManager.alterCatalog(ident1, change1);
    Catalog catalog1 = catalogManager.loadCatalog(ident1);
    Assertions.assertEquals("comment1", catalog1.comment());

    // Test alter properties;
    CatalogChange change2 = CatalogChange.setProperty("key1", "value1");
    CatalogChange change3 = CatalogChange.setProperty("key2", "value2");
    CatalogChange change4 = CatalogChange.removeProperty("key2");

    catalogManager.alterCatalog(ident1, change2, change3, change4);
    Catalog catalog2 = catalogManager.loadCatalog(ident1);
    Map<String, String> expectedProps = ImmutableMap.of("provider", "test", "key1", "value1");
    testProperties(expectedProps, catalog2.properties());

    // Test Catalog does not exist
    NameIdentifier ident2 = NameIdentifier.of(ident.namespace(), "test33");
    CatalogChange change5 = CatalogChange.rename("test34");
    Throwable exception =
        Assertions.assertThrows(
            NoSuchCatalogException.class, () -> catalogManager.alterCatalog(ident2, change5));
    Assertions.assertTrue(
        exception.getMessage().contains("Catalog metalake.test33 does not exist"));

    // Alter operation will update the cache
    Assertions.assertNull(catalogManager.catalogCache.getIfPresent(ident));
    Assertions.assertNotNull(catalogManager.catalogCache.getIfPresent(ident1));
  }

  @Test
  public void testDropCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test41");
    Map<String, String> props = ImmutableMap.of("provider", "test");
    String comment = "comment";

    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);

    // Test drop catalog
    boolean dropped = catalogManager.dropCatalog(ident);
    Assertions.assertTrue(dropped);

    // Test drop non-existed catalog
    NameIdentifier ident1 = NameIdentifier.of("metalake", "test42");
    boolean dropped1 = catalogManager.dropCatalog(ident1);
    Assertions.assertFalse(dropped1);

    // Drop operation will update the cache
    Assertions.assertNull(catalogManager.catalogCache.getIfPresent(ident));
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });

    Assertions.assertTrue(testProps.containsKey(StringIdentifier.ID_KEY));
    StringIdentifier StringId = StringIdentifier.fromString(testProps.get(StringIdentifier.ID_KEY));
    Assertions.assertEquals(StringId.toString(), testProps.get(StringIdentifier.ID_KEY));
  }
}
