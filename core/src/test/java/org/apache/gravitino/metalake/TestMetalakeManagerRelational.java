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
package org.apache.gravitino.metalake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.NonEmptyMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

/** Tests metalake deletion through the manager against each relational backend. */
public class TestMetalakeManagerRelational extends TestJDBCBackend {

  private static final String METALAKE = "metalake_drop_race";

  private LockManager previousLockManager;
  private RelationalEntityStore store;
  private MetalakeManager manager;

  @BeforeEach
  void setUpManager() throws IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.TREE_LOCK_MAX_NODE_IN_MEMORY)).thenReturn(100000L);
    Mockito.when(config.get(Configs.TREE_LOCK_MIN_NODE_IN_MEMORY)).thenReturn(1000L);
    Mockito.when(config.get(Configs.TREE_LOCK_CLEAN_INTERVAL)).thenReturn(36000L);
    previousLockManager = GravitinoEnv.getInstance().lockManager();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    store = Mockito.spy(new RelationalEntityStore());
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
    manager = new MetalakeManager(store, new RandomIdGenerator());
  }

  @AfterEach
  void restoreLockManager() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", previousLockManager, true);
  }

  @TestTemplate
  void testNonForceDropPreservesCatalogCreatedAfterEmptyCheck() throws Exception {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE);
    NameIdentifier ident = metalake.nameIdentifier();
    manager.disableMetalake(ident);
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE),
            "late_catalog",
            AUDIT_INFO);

    Mockito.doAnswer(
            invocation -> {
              List<?> observed = (List<?>) invocation.callRealMethod();
              assertTrue(observed.isEmpty());
              // Model a second server committing after the manager's empty snapshot. It does
              // not share the first server's tree lock or entity cache.
              backend.insert(catalog, false);
              return observed;
            })
        .when(store)
        .list(Namespace.of(METALAKE), CatalogEntity.class, EntityType.CATALOG);

    NonEmptyMetalakeException failure =
        assertThrows(NonEmptyMetalakeException.class, () -> manager.dropMetalake(ident, false));
    assertTrue(failure.getMessage().contains(METALAKE));
    assertTrue(failure.getMessage().contains("force"));
    assertInstanceOf(NonEmptyEntityException.class, failure.getCause());
    assertEquals(metalake.id(), backend.<BaseMetalake>get(ident, EntityType.METALAKE).id());
    assertEquals(catalog, backend.<CatalogEntity>get(catalog.nameIdentifier(), EntityType.CATALOG));
    assertFalse(MetalakeManager.metalakeInUse(store, ident));
  }

  @TestTemplate
  void testNonForceDropRejectsExistingCatalog() throws Exception {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE);
    manager.disableMetalake(metalake.nameIdentifier());
    CatalogEntity catalog = createAndInsertCatalog(METALAKE, "existing_catalog");

    assertThrows(
        NonEmptyMetalakeException.class,
        () -> manager.dropMetalake(metalake.nameIdentifier(), false));
    assertEquals(catalog, backend.<CatalogEntity>get(catalog.nameIdentifier(), EntityType.CATALOG));
    assertEquals(
        metalake.id(),
        backend.<BaseMetalake>get(metalake.nameIdentifier(), EntityType.METALAKE).id());
  }

  @TestTemplate
  void testNonForceDropEmptyMetalakeAndRepeat() throws Exception {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE);
    NameIdentifier ident = metalake.nameIdentifier();
    manager.disableMetalake(ident);

    assertTrue(manager.dropMetalake(ident, false));
    assertFalse(backend.exists(ident, EntityType.METALAKE));
    assertFalse(manager.dropMetalake(ident, false));
  }
}
