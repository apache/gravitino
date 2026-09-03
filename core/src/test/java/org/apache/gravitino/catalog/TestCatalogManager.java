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
import static org.apache.gravitino.TestCatalog.PROPERTY_HIDDEN_KEY;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY1;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY2;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY3;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY4;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY5_PREFIX;
import static org.apache.gravitino.TestCatalog.PROPERTY_KEY6_PREFIX;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretProvider;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SecretReference;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore.InMemoryEntityStore;
import org.apache.gravitino.storage.relational.EntityChangeLogListener;
import org.apache.gravitino.storage.relational.SupportsEntityChangeLog;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestCatalogManager {

  /** Test-only external-reference provider. */
  public static class TestReferenceSecretsProvider implements SecretProvider {

    private String providerName;

    @Override
    public void initialize(String name, Map<String, String> providerConfig) {
      providerName = name;
    }

    @Override
    public String type() {
      return "test-reference";
    }

    @Override
    public SecretUrn writeSecret(String plaintext, Map<String, String> attributes) {
      throw new UnsupportedOperationException("write-through is not supported");
    }

    @Override
    public String readSecret(SecretUrn urn) {
      return "resolved-" + urn.identifierSegments().get(0);
    }

    @Override
    public void deleteSecret(SecretUrn urn) {}

    @Override
    public SecretUrn buildReferenceUrn(String propertyKey, Map<String, String> attributes) {
      return SecretUrn.parse(
          String.format(
              "%s%s:%s:%s",
              SecretConstants.URN_PREFIX, providerName, attributes.get("path"), propertyKey));
    }

    @Override
    public void close() {}
  }

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

    catalogManager =
        new CatalogManager(config, entityStore, new RandomIdGenerator(), new SecretManager(config));
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    catalogManager = Mockito.spy(catalogManager);
  }

  @BeforeEach
  @AfterEach
  void reset() throws IOException {
    ((InMemoryEntityStore) entityStore).clear();
    entityStore.put(metalakeEntity, true);
    // The shared CatalogManager is created once in @BeforeAll, so its cache would otherwise keep
    // entries created by previously executed test methods and make tests order-dependent.
    catalogManager.getCatalogCache().invalidateAll();
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
  void testAlterCatalogRejectsMaskedHiddenProperty() throws IOException {
    NameIdentifier ident = NameIdentifier.of("metalake", "masked_hidden");
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY3, "3")
            .put(PROPERTY_KEY4, "value4")
            .put(PROPERTY_KEY5_PREFIX + "1", "value1")
            .put(PROPERTY_KEY6_PREFIX + "1", "value1")
            .put(PROPERTY_HIDDEN_KEY, "secret")
            .put("mock", "mock")
            .build();
    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.alterCatalog(
                    ident,
                    CatalogChange.setProperty(
                        PROPERTY_HIDDEN_KEY, HiddenPropertyMaskUtils.MASKED_VALUE)));

    Assertions.assertTrue(exception.getMessage().contains(PROPERTY_HIDDEN_KEY));
    CatalogEntity entity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals("secret", entity.getProperties().get(PROPERTY_HIDDEN_KEY));
    reset();
  }

  @Test
  void testCreateCatalogRejectsMaskedHiddenProperty() throws IOException {
    NameIdentifier ident = NameIdentifier.of("metalake", "masked_create");
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY3, "3")
            .put(PROPERTY_KEY4, "value4")
            .put(PROPERTY_KEY5_PREFIX + "1", "value1")
            .put(PROPERTY_KEY6_PREFIX + "1", "value1")
            .put(PROPERTY_HIDDEN_KEY, HiddenPropertyMaskUtils.MASKED_VALUE)
            .put("mock", "mock")
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    ident, Catalog.Type.RELATIONAL, provider, "comment", props));
    Assertions.assertTrue(exception.getMessage().contains(PROPERTY_HIDDEN_KEY));
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
  void testCreateCatalogReturnsNoSuchMetalakeWhenParentDisappears() throws Exception {
    InMemoryEntityStore store = Mockito.spy(new InMemoryEntityStore());
    store.initialize(config);
    store.put(metalakeEntity, true);

    NoSuchEntityException missingMetalake =
        new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            EntityType.METALAKE.name().toLowerCase(),
            metalake);
    Mockito.doThrow(missingMetalake).when(store).put(any(CatalogEntity.class), eq(false));

    CatalogManager manager =
        new CatalogManager(config, store, new RandomIdGenerator(), new SecretManager(config));
    NameIdentifier ident = NameIdentifier.of(metalake, "concurrent_parent_drop");
    Map<String, String> props =
        ImmutableMap.of(
            PROPERTY_KEY1, "value1", PROPERTY_KEY2, "value2", PROPERTY_KEY5_PREFIX + "1", "value3");

    try {
      NoSuchMetalakeException exception =
          Assertions.assertThrows(
              NoSuchMetalakeException.class,
              () ->
                  manager.createCatalog(
                      ident, Catalog.Type.RELATIONAL, provider, "comment", props));
      Assertions.assertSame(missingMetalake, exception.getCause());
      Mockito.verify(store, Mockito.never()).delete(ident, EntityType.CATALOG, true);
    } finally {
      manager.close();
      store.close();
    }
  }

  @Test
  public void testCreateCatalogValidatesBackendConnection() {
    Map<String, String> okProps =
        Maps.newHashMap(
            ImmutableMap.of(
                PROPERTY_KEY1,
                "value1",
                PROPERTY_KEY2,
                "value2",
                PROPERTY_KEY5_PREFIX + "1",
                "value3",
                TestCatalogOperations.VALIDATE_ON_CREATE,
                "true"));

    // Opted in + backend resolves: creation succeeds.
    NameIdentifier okIdent = NameIdentifier.of("metalake", "validate-ok");
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                okIdent, Catalog.Type.RELATIONAL, provider, "comment", okProps));
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(okIdent));

    // Opted in + backend cannot be resolved: creation fails fast and leaves nothing behind.
    NameIdentifier failIdent = NameIdentifier.of("metalake", "validate-fail");
    Map<String, String> failProps =
        Maps.newHashMap(
            ImmutableMap.of(
                PROPERTY_KEY1,
                "value1",
                PROPERTY_KEY2,
                "value2",
                PROPERTY_KEY5_PREFIX + "1",
                "value3",
                TestCatalogOperations.VALIDATE_ON_CREATE,
                "true",
                TestCatalogOperations.FAIL_INITIALIZE,
                "true"));
    Throwable failure =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogManager.createCatalog(
                    failIdent, Catalog.Type.RELATIONAL, provider, "comment", failProps));
    Assertions.assertTrue(
        failure.getMessage().contains("backend rejected catalog configuration"),
        failure.getMessage());
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(failIdent));
    // Rolled back, so the identifier is reusable.
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                failIdent, Catalog.Type.RELATIONAL, provider, "comment", okProps));

    // Not opted in: the connection is not validated at create even if it would fail.
    NameIdentifier skipIdent = NameIdentifier.of("metalake", "validate-skip");
    Map<String, String> skipProps =
        Maps.newHashMap(
            ImmutableMap.of(
                PROPERTY_KEY1,
                "value1",
                PROPERTY_KEY2,
                "value2",
                PROPERTY_KEY5_PREFIX + "1",
                "value3",
                TestCatalogOperations.FAIL_INITIALIZE,
                "true"));
    Assertions.assertDoesNotThrow(
        () ->
            catalogManager.createCatalog(
                skipIdent, Catalog.Type.RELATIONAL, provider, "comment", skipProps));
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(skipIdent));
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
    catalogManager.getCatalogCache().invalidateAll();

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

    CatalogManager.CatalogWrapper relWrapper =
        catalogManager.getCatalogCache().getIfPresent(relIdent);
    CatalogManager.CatalogWrapper fileWrapper =
        catalogManager.getCatalogCache().getIfPresent(fileIdent);
    Assertions.assertNotNull(relWrapper);
    Assertions.assertNotNull(fileWrapper);

    catalogManager.listCatalogsInfo(relIdent.namespace());
    Assertions.assertSame(relWrapper, catalogManager.getCatalogCache().getIfPresent(relIdent));
    Assertions.assertSame(fileWrapper, catalogManager.getCatalogCache().getIfPresent(fileIdent));

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
  void testAlterCatalogRefreshesCacheAfterStoreUpdate() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "cache_race_test");
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

    Catalog catalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    CatalogEntity originalEntity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    FieldUtils.writeField(catalog, "entity", originalEntity, true);

    CatalogManager.CatalogWrapper staleWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.doReturn(catalog).when(staleWrapper).catalog();

    CatalogManager.CatalogWrapper freshWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class, Mockito.RETURNS_DEEP_STUBS);
    BaseCatalog<?> freshCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.doReturn("cache_race_test_renamed").when(freshCatalog).name();
    Mockito.doReturn(freshCatalog).when(freshWrapper).catalog();

    AtomicBoolean staleInserted = new AtomicBoolean(false);
    Answer<CatalogManager.CatalogWrapper> insertStaleWrapper =
        invocation -> {
          if (staleInserted.compareAndSet(false, true)) {
            catalogManager
                .getCatalogCache()
                .put(NameIdentifier.of("metalake", "cache_race_test_renamed"), staleWrapper);
          }
          return freshWrapper;
        };
    Mockito.doAnswer(insertStaleWrapper)
        .when(catalogManager)
        .createCatalogWrapper(any(CatalogEntity.class), eq(null));

    Catalog alteredCatalog =
        catalogManager.alterCatalog(ident, CatalogChange.rename("cache_race_test_renamed"));

    Assertions.assertEquals("cache_race_test_renamed", alteredCatalog.name());
    CatalogManager.CatalogWrapper cachedWrapper =
        catalogManager
            .getCatalogCache()
            .getIfPresent(NameIdentifier.of("metalake", "cache_race_test_renamed"));
    Assertions.assertSame(freshWrapper, cachedWrapper);
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));

    // Restore real method so stub does not leak into subsequent tests.
    Mockito.doCallRealMethod()
        .when(catalogManager)
        .createCatalogWrapper(any(CatalogEntity.class), eq(null));
  }

  @Test
  void testCatalogChangeLogListenerInvalidatesCatalogCacheForRemoteChange() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "change_log_catalog");
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
    Assertions.assertNotNull(catalogManager.loadCatalogAndWrap(ident));
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(ident));

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);
    listener.onEntityChange(
        List.of(
            new EntityChangeRecord(
                1L, "metalake", "CATALOG", "metalake.change_log_catalog", OperateType.ALTER, 0L)));

    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
  }

  @Test
  void testCatalogChangeLogListenerSkipsInvalidationForLocalMutation() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "change_log_local");
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
    Assertions.assertNotNull(catalogManager.loadCatalogAndWrap(ident));
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(ident));

    // Enable local-mutation tracking, which a CatalogManager backed by a change-log-aware store
    // would have turned on in its constructor. The in-memory store used here does not support a
    // change log, so set it explicitly.
    catalogManager.setTrackLocalMutations(true);
    catalogManager.markLocalMutation(ident);

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);
    listener.onEntityChange(
        List.of(
            new EntityChangeRecord(
                1L, "metalake", "CATALOG", "metalake.change_log_local", OperateType.ALTER, 0L)));

    Assertions.assertNotNull(
        catalogManager.getCatalogCache().getIfPresent(ident),
        "Cache should NOT be invalidated for local mutations");
  }

  @Test
  void testCatalogChangeLogListenerSkipsBadRecordAndStillProcessesLaterValidChange()
      throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "change_log_batch");
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
    Assertions.assertNotNull(catalogManager.loadCatalogAndWrap(ident));
    Assertions.assertNotNull(catalogManager.getCatalogCache().getIfPresent(ident));

    CatalogChangeLogListener listener = new CatalogChangeLogListener(catalogManager);
    listener.onEntityChange(
        List.of(
            new EntityChangeRecord(
                1L, "metalake", null, "metalake.change_log_batch", OperateType.ALTER, 0L),
            new EntityChangeRecord(
                2L, "metalake", "CATALOG", "metalake.change_log_batch", OperateType.ALTER, 0L)));

    Assertions.assertNull(
        catalogManager.getCatalogCache().getIfPresent(ident),
        "Cache should still be invalidated by the later valid record");
  }

  @Test
  void testCloseUnregistersCatalogChangeLogListener() {
    ChangeLogAwareEntityStore store = new ChangeLogAwareEntityStore();
    CatalogManager manager =
        new CatalogManager(config, store, new RandomIdGenerator(), new SecretManager(config));

    EntityChangeLogListener registeredListener = store.listener.get();
    Assertions.assertNotNull(registeredListener);

    manager.close();

    Assertions.assertSame(registeredListener, store.unregisteredListener.get());
  }

  @Test
  void testDropCatalogDoesNotMarkLocalMutationWhenStoreReturnsFalse() throws Exception {
    ChangeLogAwareEntityStore store = new ChangeLogAwareEntityStore();
    store.initialize(config);
    store.put(metalakeEntity, true);

    CatalogManager manager =
        new CatalogManager(config, store, new RandomIdGenerator(), new SecretManager(config));
    NameIdentifier ident = NameIdentifier.of("metalake", "delete_returns_false");
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

    manager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    store.returnFalseForCatalogDelete = true;

    Assertions.assertFalse(manager.dropCatalog(ident, true));
    Assertions.assertNotNull(manager.loadCatalogAndWrap(ident));
    Assertions.assertNotNull(manager.getCatalogCache().getIfPresent(ident));

    store
        .listener
        .get()
        .onEntityChange(
            List.of(
                new EntityChangeRecord(
                    1L,
                    "metalake",
                    "CATALOG",
                    "metalake.delete_returns_false",
                    OperateType.ALTER,
                    0L)));

    Assertions.assertNull(manager.getCatalogCache().getIfPresent(ident));
    manager.close();
  }

  @Test
  void testDropCatalogReturnsFalseWhenConcurrentDeleteWins() throws Exception {
    ChangeLogAwareEntityStore store = new ChangeLogAwareEntityStore();
    store.initialize(config);
    store.put(metalakeEntity, true);

    CatalogManager manager =
        new CatalogManager(config, store, new RandomIdGenerator(), new SecretManager(config));
    NameIdentifier ident = NameIdentifier.of("metalake", "concurrently_deleted");
    Map<String, String> props =
        ImmutableMap.of(
            PROPERTY_KEY1, "value1", PROPERTY_KEY2, "value2", PROPERTY_KEY5_PREFIX + "1", "value3");
    manager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    store.throwMissingCatalogForSchemaList = true;

    Assertions.assertFalse(manager.dropCatalog(ident, true));
    Assertions.assertNull(manager.getCatalogCache().getIfPresent(ident));
    manager.close();
  }

  @Test
  void testFailedCreateCatalogCleanupMarksLocalMutation() throws Exception {
    ChangeLogAwareEntityStore store = new ChangeLogAwareEntityStore();
    store.initialize(config);
    store.put(metalakeEntity, true);

    CatalogManager manager =
        new CatalogManager(config, store, new RandomIdGenerator(), new SecretManager(config));
    NameIdentifier ident = NameIdentifier.of("metalake", "failed_create_cleanup");

    // A creation that fails validation (key1 is required but missing) stores the entity and then
    // rolls it back via store.delete(), which writes a DROP record to the entity change log.
    Map<String, String> invalidProps =
        ImmutableMap.of(
            "provider", "test", PROPERTY_KEY2, "value2", PROPERTY_KEY5_PREFIX + "1", "v");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.createCatalog(
                ident, Catalog.Type.RELATIONAL, provider, "comment", invalidProps));

    // Recreate the same catalog successfully and load it into the cache.
    Map<String, String> validProps =
        ImmutableMap.of(
            "provider",
            "test",
            PROPERTY_KEY1,
            "value1",
            PROPERTY_KEY2,
            "value2",
            PROPERTY_KEY5_PREFIX + "1",
            "value3");
    manager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", validProps);
    Assertions.assertNotNull(manager.loadCatalogAndWrap(ident));
    Assertions.assertNotNull(manager.getCatalogCache().getIfPresent(ident));

    // Simulate a subsequent local mutation (e.g. disableCatalog) that writes an ALTER record.
    manager.markLocalMutation(ident);

    // The poller delivers both records in one batch: the DROP from the failed-create cleanup and
    // the ALTER from the local mutation. The cleanup DROP must carry its own local-mutation token
    // (the fix); otherwise it consumes the ALTER's token, the ALTER is treated as remote, and the
    // in-use cached wrapper is spuriously invalidated and asynchronously closed.
    store
        .listener
        .get()
        .onEntityChange(
            List.of(
                new EntityChangeRecord(
                    1L,
                    "metalake",
                    "CATALOG",
                    "metalake.failed_create_cleanup",
                    OperateType.DROP,
                    0L),
                new EntityChangeRecord(
                    2L,
                    "metalake",
                    "CATALOG",
                    "metalake.failed_create_cleanup",
                    OperateType.ALTER,
                    0L)));

    Assertions.assertNotNull(
        manager.getCatalogCache().getIfPresent(ident),
        "Cache should NOT be invalidated: the failed-create cleanup DROP must be tracked as a "
            + "local mutation so it does not steal the token meant for the later ALTER");
    manager.close();
  }

  @Test
  public void testDropCatalogSkipsImportedSchemas() throws Exception {
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

    Catalog catalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);
    Mockito.doCallRealMethod().when(catalogManager).loadCatalogAndWrap(ident);
    Assertions.assertDoesNotThrow(() -> catalogManager.disableCatalog(ident));
    CatalogEntity catalogEntity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    FieldUtils.writeField(catalog, "entity", catalogEntity, true);

    SchemaEntity importedSchemaEntity =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("imported_schema")
            .withNamespace(Namespace.of("metalake", "test41"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    entityStore.put(importedSchemaEntity);

    Schema importedSchema = Mockito.mock(Schema.class);
    // Non-empty properties without StringIdentifier simulate an imported schema on a backend
    // that supports property storage (e.g., Hive, Iceberg) but did not create this schema
    // via Gravitino.
    Mockito.doReturn(ImmutableMap.of("owner", "external")).when(importedSchema).properties();
    CatalogManager.CatalogWrapper wrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Capability capability = Mockito.mock(Capability.class);
    CapabilityResult unsupportedResult = CapabilityResult.unsupported("Not managed");
    Mockito.doReturn(wrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doReturn(catalog).when(wrapper).catalog();
    Mockito.doReturn(capability).when(wrapper).capabilities();
    Mockito.doReturn(unsupportedResult).when(capability).managedStorage(any());
    Mockito.doReturn(
            new NameIdentifier[] {NameIdentifier.of("metalake", "test41", "imported_schema")})
        .doReturn(importedSchema)
        .when(wrapper)
        .doWithSchemaOps(any());

    // Imported schema (no StringIdentifier in external catalog properties) should not block drop.
    Assertions.assertTrue(catalogManager.dropCatalog(ident));
  }

  private static class ChangeLogAwareEntityStore extends InMemoryEntityStore
      implements SupportsEntityChangeLog {
    private final AtomicReference<EntityChangeLogListener> listener = new AtomicReference<>();
    private final AtomicReference<EntityChangeLogListener> unregisteredListener =
        new AtomicReference<>();
    private boolean returnFalseForCatalogDelete;
    private boolean throwMissingCatalogForSchemaList;

    @Override
    public boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade)
        throws IOException {
      if (returnFalseForCatalogDelete && entityType == EntityType.CATALOG) {
        return false;
      }
      return super.delete(ident, entityType, cascade);
    }

    @Override
    public <E extends Entity & HasIdentifier> List<E> list(
        Namespace namespace, Class<E> cl, EntityType entityType) throws IOException {
      // Mirrors the relational store: listing the schemas of a catalog that another server has
      // already deleted resolves the parent catalog id first and reports the catalog as missing.
      if (throwMissingCatalogForSchemaList && entityType == EntityType.SCHEMA) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            EntityType.CATALOG.name().toLowerCase(),
            namespace.level(namespace.length() - 1));
      }
      return super.list(namespace, cl, entityType);
    }

    @Override
    public void registerEntityChangeLogListener(EntityChangeLogListener listener) {
      this.listener.set(listener);
    }

    @Override
    public void unregisterEntityChangeLogListener(EntityChangeLogListener listener) {
      this.unregisteredListener.set(listener);
    }
  }

  @Test
  public void testDropCatalogIgnoresMissingSchema() throws Exception {
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

    Catalog catalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);
    Mockito.doCallRealMethod().when(catalogManager).loadCatalogAndWrap(ident);
    Assertions.assertDoesNotThrow(() -> catalogManager.disableCatalog(ident));
    CatalogEntity catalogEntity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    FieldUtils.writeField(catalog, "entity", catalogEntity, true);

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("default")
            .withNamespace(Namespace.of("metalake", "test41"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    entityStore.put(schemaEntity);

    CatalogManager.CatalogWrapper wrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Capability capability = Mockito.mock(Capability.class);
    CapabilityResult unsupportedResult = CapabilityResult.unsupported("Not managed");
    Mockito.doReturn(wrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doReturn(catalog).when(wrapper).catalog();
    Mockito.doReturn(capability).when(wrapper).capabilities();
    Mockito.doReturn(unsupportedResult).when(capability).managedStorage(any());
    Mockito.doReturn(new NameIdentifier[] {NameIdentifier.of("metalake", "test41", "default")})
        .doThrow(new NoSuchSchemaException("Schema not found"))
        .when(wrapper)
        .doWithSchemaOps(any());

    // Schema disappearing between listSchemas and loadSchema should not block drop.
    Assertions.assertTrue(catalogManager.dropCatalog(ident));
  }

  @Test
  public void testDropCatalogFailsOnSchemaClassificationError() throws Exception {
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

    Catalog catalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, comment, props);
    Mockito.doCallRealMethod().when(catalogManager).loadCatalogAndWrap(ident);
    Assertions.assertDoesNotThrow(() -> catalogManager.disableCatalog(ident));
    CatalogEntity catalogEntity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    FieldUtils.writeField(catalog, "entity", catalogEntity, true);

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

    CatalogManager.CatalogWrapper wrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Capability capability = Mockito.mock(Capability.class);
    CapabilityResult unsupportedResult = CapabilityResult.unsupported("Not managed");
    Mockito.doReturn(wrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doReturn(catalog).when(wrapper).catalog();
    Mockito.doReturn(capability).when(wrapper).capabilities();
    Mockito.doReturn(unsupportedResult).when(capability).managedStorage(any());
    Mockito.doReturn(new NameIdentifier[] {NameIdentifier.of("metalake", "test41", "test_schema1")})
        .doThrow(new RuntimeException("Failed connect"))
        .when(wrapper)
        .doWithSchemaOps(any());

    // Unexpected errors during schema classification should propagate (fail-closed).
    RuntimeException ex =
        Assertions.assertThrows(RuntimeException.class, () -> catalogManager.dropCatalog(ident));
    Assertions.assertTrue(ex.getCause().getMessage().contains("Failed connect"));
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
    Catalog catalog =
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
    Capability capability = Mockito.mock(Capability.class);
    CapabilityResult unsupportedResult = CapabilityResult.unsupported("Not managed");
    Mockito.doReturn(catalogWrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doReturn(capability).when(catalogWrapper).capabilities();
    Mockito.doReturn(unsupportedResult).when(capability).managedStorage(any());
    Mockito.doReturn(catalog).when(catalogWrapper).catalog();
    Mockito.doThrow(new RuntimeException("Failed connect"))
        .when(catalogWrapper)
        .doWithSchemaOps(any());
    Assertions.assertTrue(catalogManager.dropCatalog(ident, true));
  }

  @Test
  void testDropCatalogInvalidatesCacheAfterStoreDelete() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "cache_drop_test");
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

    Catalog catalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    Assertions.assertDoesNotThrow(() -> catalogManager.disableCatalog(ident));
    CatalogEntity entity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    FieldUtils.writeField(catalog, "entity", entity, true);

    CatalogManager.CatalogWrapper catalogWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class, Mockito.RETURNS_DEEP_STUBS);
    Capability capability = Mockito.mock(Capability.class);
    CapabilityResult unsupportedResult = CapabilityResult.unsupported("Not managed");
    Mockito.doReturn(catalogWrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doReturn(catalog).when(catalogWrapper).catalog();
    Mockito.doReturn(capability).when(catalogWrapper).capabilities();
    Mockito.doReturn(unsupportedResult).when(capability).managedStorage(any());

    catalogManager.getCatalogCache().put(ident, catalogWrapper);
    boolean dropped = catalogManager.dropCatalog(ident);

    Assertions.assertTrue(dropped);
    Assertions.assertFalse(entityStore.exists(ident, EntityType.CATALOG));
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
  }

  @Test
  void testDropCatalogReloadsClosedCachedWrapper() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "closed_cache_drop_test");
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

    Catalog catalog =
        catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);
    Assertions.assertDoesNotThrow(() -> catalogManager.disableCatalog(ident));
    CatalogEntity entity = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    FieldUtils.writeField(catalog, "entity", entity, true);

    CatalogManager.CatalogWrapper closedWrapper = catalogManager.loadCatalogAndWrap(ident);
    closedWrapper.close();
    Assertions.assertSame(closedWrapper, catalogManager.getCatalogCache().getIfPresent(ident));

    boolean dropped = catalogManager.dropCatalog(ident);

    Assertions.assertTrue(dropped);
    Assertions.assertFalse(entityStore.exists(ident, EntityType.CATALOG));
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
  }

  @Test
  void testLoadCatalogAndWrapDoesNotInvalidateConcurrentlyReloadedWrapper() {
    NameIdentifier ident = NameIdentifier.of("metalake", "concurrent_cache_reload_test");

    CatalogManager.CatalogWrapper closedWrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    CatalogManager.CatalogWrapper freshWrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    BaseCatalog<?> freshCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.doReturn(freshCatalog).when(freshWrapper).catalog();
    Mockito.doAnswer(
            invocation -> {
              catalogManager.getCatalogCache().put(ident, freshWrapper);
              return null;
            })
        .when(closedWrapper)
        .catalog();

    try {
      catalogManager.getCatalogCache().put(ident, closedWrapper);

      CatalogManager.CatalogWrapper loadedWrapper = catalogManager.loadCatalogAndWrap(ident);

      Assertions.assertSame(freshWrapper, loadedWrapper);
      Assertions.assertSame(freshWrapper, catalogManager.getCatalogCache().getIfPresent(ident));
      Mockito.verify(freshWrapper, Mockito.never()).close();
    } finally {
      catalogManager.getCatalogCache().invalidate(ident);
    }
  }

  @Test
  void testAlterMutableProperties() {
    NameIdentifier ident = NameIdentifier.of("metalake", "test51");
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

  @Test
  public void testEnableAndDisableCatalog() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "enable_disable");
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

    catalogManager.disableCatalog(ident);
    CatalogEntity disabled = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals("false", disabled.getProperties().get(Catalog.PROPERTY_IN_USE));
    Assertions.assertThrows(
        CatalogNotInUseException.class, () -> catalogManager.testConnection(ident));

    catalogManager.enableCatalog(ident);
    CatalogEntity enabled = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals("true", enabled.getProperties().get(Catalog.PROPERTY_IN_USE));
    Assertions.assertNull(catalogManager.getCatalogCache().getIfPresent(ident));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> catalogManager.testConnection(ident));
  }

  @Test
  void testExistingCatalogConnectionHoldsCatalogReadLock() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "connection_lock_test");
    CatalogManager.CatalogWrapper wrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    CountDownLatch connectionStarted = new CountDownLatch(1);
    CountDownLatch releaseConnection = new CountDownLatch(1);
    CountDownLatch writerStarted = new CountDownLatch(1);
    CountDownLatch writeLockAcquired = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Mockito.doReturn(wrapper).when(catalogManager).loadCatalogAndWrap(ident);
    Mockito.doReturn(catalog).when(wrapper).catalog();
    Mockito.doAnswer(
            invocation -> {
              connectionStarted.countDown();
              Assertions.assertTrue(releaseConnection.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(wrapper)
        .doWithCatalogOps(any());

    Future<?> connectionFuture = executor.submit(() -> catalogManager.testConnection(ident));
    Future<?> writerFuture = null;
    try {
      Assertions.assertTrue(connectionStarted.await(5, TimeUnit.SECONDS));
      writerFuture =
          executor.submit(
              () -> {
                writerStarted.countDown();
                TreeLockUtils.doWithTreeLock(
                    ident,
                    LockType.WRITE,
                    () -> {
                      writeLockAcquired.countDown();
                      return null;
                    });
              });
      Assertions.assertTrue(writerStarted.await(5, TimeUnit.SECONDS));
      Assertions.assertFalse(writeLockAcquired.await(200, TimeUnit.MILLISECONDS));

      releaseConnection.countDown();
      connectionFuture.get(5, TimeUnit.SECONDS);
      writerFuture.get(5, TimeUnit.SECONDS);
      Assertions.assertEquals(0, writeLockAcquired.getCount());
    } finally {
      releaseConnection.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void testExistingCatalogConnectionWithProposedChanges() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "connection_changes_test");
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("provider", "test")
            .put(PROPERTY_KEY1, "value1")
            .put(PROPERTY_KEY2, "value2")
            .put(PROPERTY_KEY5_PREFIX + "1", "value3")
            .put("removable", "stored")
            .build();
    catalogManager.createCatalog(
        ident, Catalog.Type.RELATIONAL, provider, "stored comment", properties);
    CatalogEntity storedBefore = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
    CatalogManager.CatalogWrapper cachedBefore =
        catalogManager.getCatalogCache().getIfPresent(ident);

    CatalogManager.CatalogWrapper temporaryWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    CatalogOperations temporaryOperations = Mockito.mock(CatalogOperations.class);
    AtomicReference<CatalogEntity> effectiveEntity = new AtomicReference<>();
    Mockito.doAnswer(
            invocation -> {
              effectiveEntity.set(invocation.getArgument(0));
              return temporaryWrapper;
            })
        .when(catalogManager)
        .createCatalogWrapper(any(CatalogEntity.class), eq(null));
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<CatalogOperations, Object> operation = invocation.getArgument(0);
              return operation.apply(temporaryOperations);
            })
        .when(temporaryWrapper)
        .doWithCatalogOps(any());

    NameIdentifier renamedIdent = NameIdentifier.of("metalake", "connection_changes_renamed");
    try {
      catalogManager.testConnection(
          ident,
          CatalogChange.rename(renamedIdent.name()),
          CatalogChange.updateComment("temporary comment"),
          CatalogChange.setProperty(PROPERTY_KEY2, "temporary value"),
          CatalogChange.removeProperty("removable"));

      CatalogEntity effective = effectiveEntity.get();
      Assertions.assertNotNull(effective);
      Assertions.assertEquals(renamedIdent.name(), effective.name());
      Assertions.assertEquals("temporary comment", effective.getComment());
      Assertions.assertEquals("temporary value", effective.getProperties().get(PROPERTY_KEY2));
      Assertions.assertFalse(effective.getProperties().containsKey("removable"));
      Mockito.verify(temporaryOperations).testConnection(renamedIdent);
      Mockito.verify(temporaryWrapper).close();

      CatalogEntity storedAfter = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
      Assertions.assertEquals(storedBefore.name(), storedAfter.name());
      Assertions.assertEquals(storedBefore.getComment(), storedAfter.getComment());
      Assertions.assertEquals(storedBefore.getProperties(), storedAfter.getProperties());
      Assertions.assertFalse(entityStore.exists(renamedIdent, EntityType.CATALOG));
      Assertions.assertSame(cachedBefore, catalogManager.getCatalogCache().getIfPresent(ident));

      Mockito.doThrow(new IOException("probe failed"))
          .when(temporaryOperations)
          .testConnection(any(NameIdentifier.class));
      RuntimeException failure =
          Assertions.assertThrows(
              RuntimeException.class,
              () ->
                  catalogManager.testConnection(
                      ident, CatalogChange.setProperty(PROPERTY_KEY2, "another value")));
      Assertions.assertInstanceOf(IOException.class, failure.getCause());
      Mockito.verify(temporaryWrapper, Mockito.times(2)).close();
    } finally {
      Mockito.doCallRealMethod()
          .when(catalogManager)
          .createCatalogWrapper(any(CatalogEntity.class), eq(null));
    }
  }

  @Test
  public void testCatalogCacheRemoveListener() throws IOException {
    NameIdentifier ident = NameIdentifier.of(metalake, "catalog");
    Map<String, String> props =
        ImmutableMap.of(
            PROPERTY_KEY1, "value1", PROPERTY_KEY2, "value2", PROPERTY_KEY5_PREFIX + "1", "value3");

    // Use a dedicated CatalogManager (and entity store) instead of the shared static one: the
    // shared instance keeps cache entries and cache removal listeners registered by other test
    // methods, which would make the assertions below depend on the test execution order.
    EntityStore store = new InMemoryEntityStore();
    store.initialize(config);
    store.put(metalakeEntity, true);

    try (CatalogManager manager =
        new CatalogManager(config, store, new RandomIdGenerator(), new SecretManager(config))) {
      // Create a catalog
      manager.createCatalog(ident, Catalog.Type.RELATIONAL, provider, "comment", props);

      // Load the catalog to add it to the cache
      manager.loadCatalog(ident);
      Assertions.assertNotNull(manager.getCatalogCache().getIfPresent(ident));

      // Add a listener to track removed catalogs
      Set<NameIdentifier> removedCatalogs = Sets.newConcurrentHashSet();
      manager.addCatalogCacheRemoveListener(removedCatalogs::add);

      // Invalidate the cache to trigger the removal listener
      manager.getCatalogCache().invalidate(ident);

      // Wait for the async eviction to complete
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                Assertions.assertTrue(
                    removedCatalogs.contains(ident),
                    "Listener should be notified of catalog removal");
                Assertions.assertEquals(
                    1, removedCatalogs.size(), "Only one catalog should be removed");
              });
    } finally {
      store.close();
    }
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });

    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        testProps.get(ID_KEY),
        "`gravitino.identifier` should be returned as a masked placeholder");
  }

  @Test
  void testSecrets() throws Exception {
    try (SecretManager secrets = memorySecretManager();
        CatalogManager manager =
            new CatalogManager(config, entityStore, new RandomIdGenerator(), secrets)) {
      NameIdentifier ident = NameIdentifier.of("metalake", "secret_ok");
      Catalog catalog =
          manager.createCatalog(
              ident,
              Catalog.Type.RELATIONAL,
              provider,
              "comment",
              catalogProps(),
              Map.of(PROPERTY_KEY4, new SecretBinding("memory", "s3cr3t")),
              Map.of());
      Assertions.assertEquals(
          HiddenPropertyMaskUtils.MASKED_VALUE, catalog.properties().get(PROPERTY_KEY4));
      String urn =
          entityStore
              .get(ident, EntityType.CATALOG, CatalogEntity.class)
              .getProperties()
              .get(PROPERTY_KEY4);
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty(PROPERTY_KEY4, urn));
      Assertions.assertEquals("s3cr3t", secrets.readSecret(SecretUrn.parse(urn)));

      manager.alterCatalog(ident, CatalogChange.removeProperty(PROPERTY_KEY4));
      Assertions.assertFalse(
          entityStore
              .get(ident, EntityType.CATALOG, CatalogEntity.class)
              .getProperties()
              .containsKey(PROPERTY_KEY4));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> secrets.readSecret(SecretUrn.parse(urn)));

      Assertions.assertTrue(manager.dropCatalog(ident, true));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> secrets.readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testConnectionChangesDoNotMutateSecrets() throws Exception {
    try (SecretManager secrets = memorySecretManager();
        CatalogManager manager =
            Mockito.spy(
                new CatalogManager(config, entityStore, new RandomIdGenerator(), secrets))) {
      NameIdentifier ident = NameIdentifier.of("metalake", "secret_connection_test");
      manager.createCatalog(
          ident,
          Catalog.Type.RELATIONAL,
          provider,
          "comment",
          catalogProps(),
          Map.of(PROPERTY_KEY4, new SecretBinding("memory", "stored-secret")),
          Map.of());
      CatalogEntity stored = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
      String storedUrn = stored.getProperties().get(PROPERTY_KEY4);
      Assertions.assertEquals("stored-secret", secrets.readSecret(SecretUrn.parse(storedUrn)));
      SecretUrn proposedUrn = writeThroughUrn("catalog", stored.id(), PROPERTY_KEY2);

      CatalogManager.CatalogWrapper temporaryWrapper =
          Mockito.mock(CatalogManager.CatalogWrapper.class);
      AtomicReference<CatalogEntity> effectiveEntity = new AtomicReference<>();
      Mockito.doAnswer(
              invocation -> {
                effectiveEntity.set(invocation.getArgument(0));
                return temporaryWrapper;
              })
          .when(manager)
          .createCatalogWrapper(any(CatalogEntity.class), eq(null));
      Mockito.doReturn(null).when(temporaryWrapper).doWithCatalogOps(any());

      manager.testConnection(
          ident,
          CatalogChange.setSecretBinding(
              PROPERTY_KEY4, new SecretBinding("memory", "temporary-secret")),
          CatalogChange.setSecretBinding(
              PROPERTY_KEY2, new SecretBinding("memory", "temporary-new-secret")));
      Assertions.assertEquals(
          "temporary-secret", effectiveEntity.get().getProperties().get(PROPERTY_KEY4));
      Assertions.assertEquals(
          "temporary-new-secret", effectiveEntity.get().getProperties().get(PROPERTY_KEY2));
      Assertions.assertEquals("stored-secret", secrets.readSecret(SecretUrn.parse(storedUrn)));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> secrets.readSecret(proposedUrn));

      manager.testConnection(ident, CatalogChange.removeProperty(PROPERTY_KEY4));
      Assertions.assertFalse(effectiveEntity.get().getProperties().containsKey(PROPERTY_KEY4));
      Assertions.assertEquals("stored-secret", secrets.readSecret(SecretUrn.parse(storedUrn)));

      manager.testConnection(
          ident,
          CatalogChange.setSecretReference(
              PROPERTY_KEY4, new SecretReference("reference", Map.of("path", "external-secret"))));
      String referenceUrn = effectiveEntity.get().getProperties().get(PROPERTY_KEY4);
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty(PROPERTY_KEY4, referenceUrn));
      Assertions.assertEquals(
          "resolved-external-secret",
          secrets.toPlaintextProperties(effectiveEntity.get().getProperties()).get(PROPERTY_KEY4));
      Assertions.assertEquals("stored-secret", secrets.readSecret(SecretUrn.parse(storedUrn)));

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> manager.testConnection(ident, CatalogChange.setProperty(PROPERTY_KEY4, storedUrn)));
      CatalogEntity storedAfter = entityStore.get(ident, EntityType.CATALOG, CatalogEntity.class);
      Assertions.assertEquals(stored.getProperties(), storedAfter.getProperties());
      Assertions.assertEquals("stored-secret", secrets.readSecret(SecretUrn.parse(storedUrn)));
      Mockito.verify(temporaryWrapper, Mockito.times(3)).close();
    }
  }

  @Test
  void testSecretRollback() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      IdGenerator ids = new AtomicLong(4242L)::getAndIncrement;
      CatalogManager manager = Mockito.spy(new CatalogManager(config, entityStore, ids, secrets));
      NameIdentifier ident = NameIdentifier.of("metalake", "secret_fail");
      Mockito.doThrow(new RuntimeException("init failed"))
          .when(manager)
          .createCatalogWrapper(any(CatalogEntity.class), any());
      SecretUrn urn = writeThroughUrn("catalog", 4242L, PROPERTY_KEY4);
      Assertions.assertThrows(
          RuntimeException.class,
          () ->
              manager.createCatalog(
                  ident,
                  Catalog.Type.RELATIONAL,
                  provider,
                  "comment",
                  catalogProps(),
                  Map.of(PROPERTY_KEY4, new SecretBinding("memory", "x")),
                  Map.of()));
      Assertions.assertFalse(entityStore.exists(ident, EntityType.CATALOG));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
      manager.close();
    }
  }

  @Test
  void testSecretNoMetalake() throws Exception {
    try (SecretManager secrets = memorySecretManager();
        CatalogManager manager =
            new CatalogManager(
                config, entityStore, new AtomicLong(4343L)::getAndIncrement, secrets)) {
      NameIdentifier ident = NameIdentifier.of("missing_metalake", "secret_catalog");
      SecretUrn urn = writeThroughUrn("catalog", 4343L, PROPERTY_KEY4);
      Assertions.assertThrows(
          NoSuchMetalakeException.class,
          () ->
              manager.createCatalog(
                  ident,
                  Catalog.Type.RELATIONAL,
                  provider,
                  "comment",
                  catalogProps(),
                  Map.of(PROPERTY_KEY4, new SecretBinding("memory", "x")),
                  Map.of()));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
    }
  }

  private static Map<String, String> catalogProps() {
    return ImmutableMap.of(
        "provider",
        "test",
        PROPERTY_KEY1,
        "value1",
        PROPERTY_KEY2,
        "value2",
        PROPERTY_KEY5_PREFIX + "1",
        "value3");
  }

  private static SecretUrn writeThroughUrn(String entityType, long entityId, String key) {
    return SecretUrn.buildWriteThrough(
        "memory",
        Map.of(
            SecretConstants.ATTR_ENTITY_TYPE, entityType,
            SecretConstants.ATTR_ENTITY_ID, String.valueOf(entityId),
            SecretConstants.ATTR_PROPERTY_KEY, key));
  }

  private static SecretManager memorySecretManager() {
    Config c = new Config(false) {};
    Properties p = new Properties();
    p.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory,reference");
    p.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    p.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "reference."
            + SecretProviderRegistry.CLASS_NAME,
        TestReferenceSecretsProvider.class.getName());
    c.loadFromProperties(p);
    return new SecretManager(c);
  }
}
