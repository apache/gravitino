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

import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY1;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY2;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY3;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY4;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY5_PREFIX;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY6_PREFIX;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore.InMemoryEntityStore;
import org.apache.gravitino.utils.PrincipalUtils;
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
      BaseMetalake.builder()
          .withId(1L)
          .withName(metalake)
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

    catalogManager = new CatalogManager(config, entityStore, new RandomIdGenerator());
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
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
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY5_PREFIX + "1", "value1")
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
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY3, "3")
            .put(PROPERTY_KEY4, "value4")
            .put(PROPERTY_KEY5_PREFIX + "1", "value1")
            .put(PROPERTY_KEY6_PREFIX + "1", "value1")
            .put("mock", "mock")
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                ident2, Catalog.Type.RELATIONAL, provider, "comment", props2));

    CatalogChange change1 = CatalogChange.setProperty(PROPERTY_KEY1, "value1");
    Exception e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalogManager.alterCatalog(ident, change1));
    Assertions.assertTrue(e1.getMessage().contains("Property key1 is immutable"));

    CatalogChange change2 = CatalogChange.setProperty(PROPERTY_KEY3, "value2");
    Exception e2 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalogManager.alterCatalog(ident2, change2));
    Assertions.assertTrue(e2.getMessage().contains("Property key3 is immutable"));

    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.alterCatalog(
                ident2, CatalogChange.setProperty(PROPERTY_KEY4, "value4")));
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.alterCatalog(
                ident2, CatalogChange.setProperty(PROPERTY_KEY2, "value2")));

    CatalogChange change3 = CatalogChange.setProperty(PROPERTY_KEY4, "value4");
    CatalogChange change4 = CatalogChange.removeProperty(PROPERTY_KEY1);
    Exception e3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> catalogManager.alterCatalog(ident2, change3, change4));
    Assertions.assertTrue(e3.getMessage().contains("Property key1 is immutable"));

    CatalogChange change5 = CatalogChange.setProperty(PROPERTY_KEY6_PREFIX + "1", "value1");
    e3 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> catalogManager.alterCatalog(ident2, change5));
    Assertions.assertTrue(
        e3.getMessage().contains("Property key6-1 is immutable"), e3.getMessage());
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
        ImmutableMap.<String, String>builder()
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY5_PREFIX + "1", "value1")
            .put("mock", "mock")
            .build();
    IllegalArgumentException e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props1));
    Assertions.assertEquals(
        "Properties or property prefixes are required and must be set: [key1]", e1.getMessage());
    // BUG here, in memory does not support rollback
    reset();

    // key2 is required;
    Map<String, String> props2 =
        ImmutableMap.<String, String>builder()
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY5_PREFIX + "1", "value2")
            .put("mock", "mock")
            .build();
    e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props2));
    Assertions.assertEquals(
        "Properties or property prefixes are required and must be set: [key2]", e1.getMessage());
    reset();

    // property with fixed prefix key5- is required;
    Map<String, String> props4 =
        ImmutableMap.<String, String>builder()
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY2, "value2")
            .put("mock", "mock")
            .build();
    e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props4));
    Assertions.assertEquals(
        "Properties or property prefixes are required and must be set: [key5-]", e1.getMessage());

    // key3 is optional, but we assign a wrong value format
    Map<String, String> props3 =
        ImmutableMap.<String, String>builder()
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY3, "a12a1a")
            .put(PROPERTY_KEY5_PREFIX + "1", "value1")
            .put("mock", "mock")
            .build();
    e1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props3));
    Assertions.assertEquals("Invalid value: 'a12a1a' for property: 'key3'", e1.getMessage());
    reset();
  }

  @Test
  public void testCreateCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test1");

    Map<String, String> props = Maps.newHashMap();
    props.put(PROPERTY_KEY1, "value1");
    props.put(PROPERTY_KEY2, "value2");
    props.put(PROPERTY_KEY5_PREFIX + "1", "value3");

    // test before creation
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.testConnection(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props));

    Catalog testCatalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    Assertions.assertEquals("test1", testCatalog.name());
    Assertions.assertEquals("comment", testCatalog.comment());
    testProperties(props, testCatalog.properties());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, testCatalog.type());

    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(ident));

    // test before creation
    NameIdentifier ident2 = NameIdentifier.of("metalake1", "test1");
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () ->
            catalogManager.testConnection(
                ident2, Catalog.Type.RELATIONAL, provider, "comment", props));

    // Test create under non-existed metalake
    Throwable exception1 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                catalogManager.createCatalog(
                    ident2, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(exception1.getMessage().contains("Metalake metalake1 does not exist"));
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident2));

    // test before creation
    Assertions.assertThrows(
        CatalogAlreadyExistsException.class,
        () ->
            catalogManager.testConnection(
                ident, Catalog.Type.RELATIONAL, provider, "comment", props));

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
    CatalogManager.CatalogWrapper cached = catalogManager.getCatalogCache().getIfPresent(ident);
    Assertions.assertNotNull(cached);

    // Test failed creation
    NameIdentifier failedIdent = NameIdentifier.of("metalake", "test2");
    props.put("reserved_key", "test");
    Throwable exception3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalogManager.createCatalog(
                    failedIdent, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(
        exception3
            .getMessage()
            .contains("Properties or property prefixes are reserved and cannot be set"),
        exception3.getMessage());
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(failedIdent));
    // Test failed for the second time
    Throwable exception4 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalogManager.createCatalog(
                    failedIdent, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(
        exception4
            .getMessage()
            .contains("Properties or property prefixes are reserved and cannot be set"),
        exception4.getMessage());
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(failedIdent));
  }

  @Test
  public void testListCatalogs() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test11");
    NameIdentifier ident1 = NameIdentifier.of("metalake", "test12");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");

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
  public void testListCatalogsInfo() {
    NameIdentifier relIdent = NameIdentifier.of("metalake", "catalog_rel");
    NameIdentifier fileIdent = NameIdentifier.of("metalake", "catalog_file");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");

    catalogManager.createCatalog(relIdent, Catalog.Type.RELATIONAL, provider, "comment", props);
    catalogManager.createCatalog(fileIdent, Catalog.Type.FILESET, provider, "comment", props);

    Catalog[] catalogs = catalogManager.listCatalogsInfo(relIdent.namespace());
    Assertions.assertEquals(2, catalogs.length);
    for (Catalog catalog : catalogs) {
      Assertions.assertTrue(
          catalog.name().equals("catalog_rel") || catalog.name().equals("catalog_file"));
      Assertions.assertEquals("comment", catalog.comment());
      testProperties(props, catalog.properties());

      if (catalog.name().equals("catalog_rel")) {
        Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
      } else {
        Assertions.assertEquals(Catalog.Type.FILESET, catalog.type());
      }
    }

    // Test list under non-existed metalake
    NameIdentifier ident2 = NameIdentifier.of("metalake1", "test1");
    Namespace namespace = ident2.namespace();
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> catalogManager.listCatalogsInfo(namespace));
    Assertions.assertTrue(exception.getMessage().contains("Metalake metalake1 does not exist"));
  }

  @Test
  public void testLoadCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test21");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");

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
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(ident));
  }

  @Test
  public void testAlterCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test31");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");
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
    CatalogChange change2 = CatalogChange.setProperty("key5", "value1");
    CatalogChange change3 = CatalogChange.setProperty("key6", "value2");
    CatalogChange change4 = CatalogChange.removeProperty("key6");

    catalogManager.alterCatalog(ident1, change2, change3, change4);
    Catalog catalog2 = catalogManager.loadCatalog(ident1);
    Map<String, String> expectedProps =
        ImmutableMap.of("provider", "test", "key1", "value1", "key2", "value2", "key5", "value1");
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
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(ident1));
  }

  @Test
  public void testDropCatalog() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test41");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");
    String comment = "comment";

    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);

    // Test drop catalog
    Exception exception =
        Assertions.assertThrows(
            CatalogInUseException.class, () -> catalogManager.dropCatalog(ident));
    Assertions.assertTrue(exception.getMessage().contains("Catalog metalake.test41 is in use"));

    Assertions.assertDoesNotThrow(() -> catalogManager.disableCatalog(ident));
    boolean dropped = catalogManager.dropCatalog(ident);
    Assertions.assertTrue(dropped);

    // Test drop non-existed catalog
    NameIdentifier ident1 = NameIdentifier.of("metalake", "test42");
    boolean dropped1 = catalogManager.dropCatalog(ident1);
    Assertions.assertFalse(dropped1);

    // Drop operation will update the cache
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
  }

  @Test
  public void testForceDropCatalog() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "test41");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");
    String comment = "comment";
    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);
    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("test_schema1")
            .withNamespace(Namespace.of("metalake", "test41"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    entityStore.put(schemaEntity);
    CatalogManager.CatalogWrapper catalogWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.doReturn(catalogWrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doThrow(new RuntimeException("Failed connect"))
        .when(catalogWrapper)
        .doWithSchemaOps(any());
    Assertions.assertTrue(catalogManager.dropCatalog(ident, true));
  }

  @Test
  void testAlterMutableProperties() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test41");
    Map<String, String> props =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");
    String comment = "comment";

    Catalog oldCatalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);
    Catalog newCatalog =
        catalogManager.alterCatalog(ident, CatalogChange.setProperty("key2", "value3"));
    Assertions.assertEquals("value2", oldCatalog.properties().get("key2"));
    Assertions.assertEquals("value3", newCatalog.properties().get("key2"));
    Assertions.assertNotEquals(oldCatalog, newCatalog);
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });

    Assertions.assertFalse(testProps.containsKey(ID_KEY), "`gravitino.identifier` is missing");
  }
}
