/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Catalog.Type.RELATIONAL;
import static com.datastrato.gravitino.Entity.SECURABLE_ENTITY_RESERVED_NAME;

import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.memory.TestMemoryEntityStore;
import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCatalogNormalizeDispatcher {
  private static CatalogNormalizeDispatcher catalogNormalizeDispatcher;
  private static CatalogManager catalogManager;
  private static EntityStore entityStore;
  private static final String metalake = "metalake";
  private static final BaseMetalake metalakeEntity =
      BaseMetalake.builder()
          .withId(1L)
          .withName(metalake)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  @BeforeAll
  public static void setUp() throws IOException {
    Config config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    entityStore.put(metalakeEntity, true);

    catalogManager = new CatalogManager(config, entityStore, new RandomIdGenerator());
    catalogManager = Mockito.spy(catalogManager);
    catalogNormalizeDispatcher = new CatalogNormalizeDispatcher(catalogManager);
  }

  @BeforeEach
  @AfterEach
  void reset() throws IOException {
    ((TestMemoryEntityStore.InMemoryEntityStore) entityStore).clear();
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
  public void testNameSpc() {
    NameIdentifier catalogIdent1 = NameIdentifier.of(metalake, SECURABLE_ENTITY_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogNormalizeDispatcher.createCatalog(
                    catalogIdent1, RELATIONAL, "test", null, null));
    Assertions.assertEquals("The catalog name '*' is reserved.", exception.getMessage());

    NameIdentifier catalogIdent2 = NameIdentifier.of(metalake, "a-b");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogNormalizeDispatcher.createCatalog(
                    catalogIdent2, RELATIONAL, "test", null, null));
    Assertions.assertEquals("The catalog name 'a-b' is illegal.", exception.getMessage());

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogNormalizeDispatcher.alterCatalog(
                    catalogIdent2, CatalogChange.rename("a-b")));
    Assertions.assertEquals("The catalog name 'a-b' is illegal.", exception.getMessage());
  }
}
