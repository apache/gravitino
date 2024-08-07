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

import static org.apache.gravitino.Catalog.Type.RELATIONAL;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
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
  public void testNameSpec() {
    // Test for valid names
    String[] legalNames = {"catalog", "_catalog", "1_catalog", "_", "1"};
    for (String legalName : legalNames) {
      NameIdentifier catalogIdent = NameIdentifier.of(metalake, legalName);
      Map<String, String> props = ImmutableMap.of("key1", "value1", "key2", "value2");
      Catalog catalog =
          catalogNormalizeDispatcher.createCatalog(catalogIdent, RELATIONAL, "test", null, props);
      Assertions.assertEquals(legalName, catalog.name());
    }

    // Test for illegal and reserved names
    NameIdentifier catalogIdent1 =
        NameIdentifier.of(metalake, MetadataObjects.METADATA_OBJECT_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalogNormalizeDispatcher.createCatalog(
                    catalogIdent1, RELATIONAL, "test", null, null));
    Assertions.assertEquals("The catalog name '*' is reserved.", exception.getMessage());

    String[] illegalNames = {
      "catalog-xxx",
      "catalog/xxx",
      "catalog.xxx",
      "catalog xxx",
      "catalog(xxx)",
      "catalog@xxx",
      "catalog#xxx",
      "catalog$xxx",
      "catalog%xxx",
      "catalog^xxx",
      "catalog&xxx",
      "catalog*xxx",
      "catalog+xxx",
      "catalog=xxx",
      "catalog|xxx",
      "catalog\\xxx",
      "catalog`xxx",
      "catalog~xxx",
      "catalog!xxx",
      "catalog\"xxx",
      "catalog'xxx",
      "catalog<xxx",
      "catalog>xxx",
      "catalog,xxx",
      "catalog?xxx",
      "catalog:xxx",
      "catalog;xxx",
      "catalog[xxx",
      "catalog]xxx",
      "catalog{xxx",
      "catalog}xxx"
    };
    for (String illegalName : illegalNames) {
      NameIdentifier catalogIdent2 = NameIdentifier.of(metalake, illegalName);
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  catalogNormalizeDispatcher.createCatalog(
                      catalogIdent2, RELATIONAL, "test", null, null));
      Assertions.assertEquals(
          "The catalog name '" + illegalName + "' is illegal.", exception.getMessage());
    }
  }
}
