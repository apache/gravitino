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

import static org.apache.gravitino.TestFilesetPropertiesMetadata.TEST_FILESET_HIDDEN_KEY;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public abstract class TestOperationDispatcher {

  protected static EntityStore entityStore;

  protected static final IdGenerator idGenerator = new RandomIdGenerator();

  protected static final String metalake = "metalake";

  protected static final String catalog = "catalog";

  protected static CatalogManager catalogManager;

  private static Config config;

  @BeforeAll
  public static void setUp() throws IOException {
    config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);

    entityStore = spy(new TestMemoryEntityStore.InMemoryEntityStore());
    entityStore.initialize(config);

    BaseMetalake metalakeEntity =
        BaseMetalake.builder()
            .withId(1L)
            .withName(metalake)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withVersion(SchemaVersion.V_0_1)
            .build();
    entityStore.put(metalakeEntity, true);

    catalogManager = new CatalogManager(config, entityStore, idGenerator);

    NameIdentifier ident = NameIdentifier.of(metalake, catalog);
    Map<String, String> props = ImmutableMap.of("key1", "value1", "key2", "value2");
    catalogManager.createCatalog(ident, Catalog.Type.RELATIONAL, "test", "comment", props);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }

    if (catalogManager != null) {
      catalogManager.close();
      catalogManager = null;
    }
  }

  @BeforeEach
  public void beforeStart() throws IOException {
    reset(entityStore);
  }

  @Test
  public void testGetCatalogIdentifier() {
    NameIdentifier id1 = NameIdentifier.of("a");
    assertThrows(IllegalNamespaceException.class, () -> getCatalogIdentifier(id1));

    NameIdentifier id2 = NameIdentifier.of("a", "b");
    assertEquals(getCatalogIdentifier(id2), NameIdentifier.of("a", "b"));

    NameIdentifier id3 = NameIdentifier.of("a", "b", "c");
    assertEquals(getCatalogIdentifier(id3), NameIdentifier.of("a", "b"));

    NameIdentifier id4 = NameIdentifier.of("a", "b", "c", "d");
    assertEquals(getCatalogIdentifier(id4), NameIdentifier.of("a", "b"));

    NameIdentifier id5 = NameIdentifier.of("a", "b", "c", "d", "e");
    assertEquals(getCatalogIdentifier(id5), NameIdentifier.of("a", "b"));
  }

  void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });
    Assertions.assertFalse(testProps.containsKey(StringIdentifier.ID_KEY));
    Assertions.assertFalse(testProps.containsKey(TEST_FILESET_HIDDEN_KEY));
  }

  void testPropertyException(Executable operation, String... errorMessage) {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, operation);
    for (String msg : errorMessage) {
      Assertions.assertTrue(exception.getMessage().contains(msg));
    }
  }
}
