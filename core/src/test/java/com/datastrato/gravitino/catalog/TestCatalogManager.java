/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.TestEntityStore;
import com.datastrato.gravitino.TestEntityStore.InMemoryEntityStore;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCatalogManager {

  private static CatalogManager catalogManager;

  private static EntityStore entityStore;

  private static Config config;

  private static String metalake = "metalake";

  private static String provider = "test";

  private static BaseMetalake metalakeEntity =
      new BaseMetalake.Builder()
          .withId(1L)
          .withName(metalake)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  @BeforeAll
  public static void setUp() throws IOException {
    config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = new TestEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    entityStore.put(metalakeEntity, true);

    catalogManager = new CatalogManager(config, entityStore, new RandomIdGenerator());
    catalogManager = Mockito.spy(catalogManager);
  }

  @BeforeEach
  @AfterEach
  void reset() throws IOException {
    ((InMemoryEntityStore) entityStore).clear();
    entityStore.put(metalakeEntity, true);
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
  void testCreateWithHiveProperty() throws IOException {
    NameIdentifier ident = NameIdentifier.of("metalake", "test445");
    Map<String, String> props1 = ImmutableMap.<String, String>builder().put("hive", "hive").build();
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props1));
    // BUG here, In memory store does not support rollback operation, so the catalog is created in
    // entity store,
    // we need to remove it manually
    reset();
    Map<String, String> props2 =
        ImmutableMap.<String, String>builder()
            .put("hive", "hive")
            .put("hive.metastore.uris", "mock_url")
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props2));
    reset();

    Map<String, String> props3 =
        ImmutableMap.<String, String>builder()
            .put("hive", "hive")
            .put("hive.metastore.uris", "")
            .put("hive.metastore.sasl.enabled", "true")
            .put("hive.metastore.kerberos.principal", "mock_principal")
            .put("hive.metastore.kerberos.keytab.file", "mock_keytab")
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props3));
  }

  @Test
  void testLoadTable() throws IOException {
    NameIdentifier ident = NameIdentifier.of("metalake", "test444");
    // key1 is required;
    Map<String, String> props1 =
        ImmutableMap.<String, String>builder()
            .put("key2", "value2")
            .put("key1", "value1")
            .put("hidden_key", "hidden_value")
            .put("mock", "mock")
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props1));

    Map<String, String> properties = catalogManager.loadCatalog(ident).properties();
    Assertions.assertTrue(properties.containsKey("key2"));
    Assertions.assertTrue(properties.containsKey("key1"));
    Assertions.assertFalse(properties.containsKey("hidden_key"));
    Assertions.assertFalse(properties.containsKey(ID_KEY));
    reset();
  }

  @Test
  void testPropertyValidationInAlter() throws IOException {
    // key1 is required and immutable and do not have default value, is not hidden and not reserved
    // key2 is required and mutable and do not have default value, is not hidden and not reserved
    // key3 is optional and immutable and have default value, is not hidden and not reserved
    // key4 is optional and mutable and have default value, is not hidden and not reserved
    // reserved_key is optional and immutable and have default value, is not hidden and reserved
    // hidden_key is optional and mutable and have default value, is hidden and not reserved

    NameIdentifier ident = NameIdentifier.of("metalake", "test111");
    // key1 is required;
    Map<String, String> props1 =
        ImmutableMap.<String, String>builder()
            .put("key2", "value2")
            .put("key1", "value1")
            .put("mock", "mock")
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props1));

    NameIdentifier ident2 = NameIdentifier.of("metalake", "test222");
    // key1 is required;
    Map<String, String> props2 =
        ImmutableMap.<String, String>builder()
            .put("key2", "value2")
            .put("key1", "value1")
            .put("key3", "3")
            .put("key4", "value4")
            .put("mock", "mock")
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                ident2, Catalog.Type.RELATIONAL, provider, "comment", props2));

    CatalogChange change1 = CatalogChange.setProperty("key1", "value1");
    Exception e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalogManager.alterCatalog(ident, change1));
    Assertions.assertTrue(e1.getMessage().contains("Property key1 is immutable"));

    CatalogChange change2 = CatalogChange.setProperty("key3", "value2");
    Exception e2 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalogManager.alterCatalog(ident2, change2));
    Assertions.assertTrue(e2.getMessage().contains("Property key3 is immutable"));

    Assertions.assertDoesNotThrow(
        () -> catalogManager.alterCatalog(ident2, CatalogChange.setProperty("key4", "value4")));
    Assertions.assertDoesNotThrow(
        () -> catalogManager.alterCatalog(ident2, CatalogChange.setProperty("key2", "value2")));

    CatalogChange change3 = CatalogChange.setProperty("key4", "value4");
    CatalogChange change4 = CatalogChange.removeProperty("key1");
    Exception e3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> catalogManager.alterCatalog(ident2, change3, change4));
    Assertions.assertTrue(e3.getMessage().contains("Property key1 is immutable"));
    reset();
  }

  @Test
  void testPropertyValidationInCreate() throws IOException {
    // key1 is required and immutable and do not have default value, is not hidden and not reserved
    // key2 is required and mutable and do not have default value, is not hidden and not reserved
    // key3 is optional and immutable and have default value, is not hidden and not reserved
    // key4 is optional and mutable and have default value, is not hidden and not reserved
    // reserved_key is optional and immutable and have default value, is not hidden and reserved
    // hidden_key is optional and mutable and have default value, is hidden and not reserved
    NameIdentifier ident = NameIdentifier.of("metalake", "test111111");

    // key1 is required;
    Map<String, String> props1 =
        ImmutableMap.<String, String>builder().put("key2", "value2").put("mock", "mock").build();
    IllegalArgumentException e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props1));
    Assertions.assertTrue(
        e1.getMessage().contains("Properties are required and must be set: [key1]"));
    // BUG here, in memory does not support rollback
    reset();

    // key2 is required;
    Map<String, String> props2 =
        ImmutableMap.<String, String>builder().put("key1", "value1").put("mock", "mock").build();
    e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props2));
    Assertions.assertTrue(
        e1.getMessage().contains("Properties are required and must be set: [key2]"));
    reset();

    // key3 is optional, but we assign a wrong value format
    Map<String, String> props3 =
        ImmutableMap.<String, String>builder()
            .put("key1", "value1")
            .put("key2", "value2")
            .put("key3", "a12a1a")
            .put("mock", "mock")
            .build();
    e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props3));
    Assertions.assertTrue(e1.getMessage().contains("Invalid value: 'a12a1a' for property: 'key3'"));
    reset();
  }

  @Test
  public void testCreateCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test1");
    Map<String, String> props = Maps.newHashMap();

    Catalog testCatalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    Assertions.assertEquals("test1", testCatalog.name());
    Assertions.assertEquals("comment", testCatalog.comment());
    testProperties(props, testCatalog.properties());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, testCatalog.type());

    Assertions.assertNotNull(catalogManager.catalogCache.getIfPresent(ident));

    // Test create under non-existed metalake
    NameIdentifier ident2 = NameIdentifier.of("metalake1", "test1");
    Throwable exception1 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                catalogManager.createCatalog(
                    ident2, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(exception1.getMessage().contains("Metalake metalake1 does not exist"));
    Assertions.assertNull(catalogManager.catalogCache.getIfPresent(ident2));

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

    // Test failed creation
    NameIdentifier failedIdent = NameIdentifier.of("metalake", "test2");
    props.put("fail-create", "true");
    Throwable exception3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalogManager.createCatalog(
                    failedIdent, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(exception3.getMessage().contains("Failed to create Test catalog"));
    Assertions.assertNull(catalogManager.catalogCache.getIfPresent(failedIdent));
    // Test failed for the second time
    Throwable exception4 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalogManager.createCatalog(
                    failedIdent, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(exception4.getMessage().contains("Failed to create Test catalog"));
    Assertions.assertNull(catalogManager.catalogCache.getIfPresent(failedIdent));
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
    Namespace namespace = ident2.namespace();
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> catalogManager.listCatalogs(namespace));
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

    Assertions.assertTrue(testProps.containsKey(ID_KEY));
    StringIdentifier StringId = StringIdentifier.fromString(testProps.get(ID_KEY));
    Assertions.assertEquals(StringId.toString(), testProps.get(ID_KEY));
  }
}
