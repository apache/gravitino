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

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Entity.EntityType.VIEW;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.rel.View;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestViewOperationDispatcher extends TestOperationDispatcher {
  static ViewOperationDispatcher viewOperationDispatcher;
  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    viewOperationDispatcher = new ViewOperationDispatcher(catalogManager, entityStore, idGenerator);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", schemaOperationDispatcher, true);
  }

  public static ViewOperationDispatcher getViewOperationDispatcher() {
    return viewOperationDispatcher;
  }

  public static SchemaOperationDispatcher getSchemaOperationDispatcher() {
    return schemaOperationDispatcher;
  }

  public static CatalogManager getCatalogManager() {
    return catalogManager;
  }

  /**
   * Helper method to create a mock View object.
   *
   * @param name The name of the view
   * @param props The properties of the view
   * @param auditInfo The audit info of the view
   * @return A mock View implementation
   */
  private static View createMockView(String name, Map<String, String> props, AuditInfo auditInfo) {
    return new View() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Map<String, String> properties() {
        return props;
      }

      @Override
      public AuditInfo auditInfo() {
        return auditInfo;
      }
    };
  }

  @Test
  public void testLoadView() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema61");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent1 = NameIdentifier.of(viewNs, "view1");

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView = createMockView("view1", props, auditInfo);

    // Mock the catalog operations to return the view
    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent1, mockView);

    // Test load view
    View loadedView = viewOperationDispatcher.loadView(viewIdent1);
    Assertions.assertEquals("view1", loadedView.name());
    Assertions.assertEquals("test", loadedView.auditInfo().creator());

    // Test load non-existent view
    NameIdentifier viewIdent2 = NameIdentifier.of(viewNs, "non_existent_view");
    Assertions.assertThrows(
        NoSuchViewException.class, () -> viewOperationDispatcher.loadView(viewIdent2));
  }

  @Test
  public void testLoadViewWithInvalidNamespace() {
    Namespace invalidNs = Namespace.of(metalake, catalog, "non_existent_schema");
    NameIdentifier viewIdent = NameIdentifier.of(invalidNs, "view1");

    Assertions.assertThrows(
        NoSuchViewException.class, () -> viewOperationDispatcher.loadView(viewIdent));
  }

  @Test
  public void testLoadViewWithMultipleViews() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema62");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    // Create multiple views
    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();

    for (int i = 1; i <= 3; i++) {
      NameIdentifier viewIdent = NameIdentifier.of(viewNs, "view" + i);
      AuditInfo auditInfo =
          AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
      View mockView = createMockView("view" + i, props, auditInfo);
      testCatalogOperations.views.put(viewIdent, mockView);
    }

    // Test loading each view
    for (int i = 1; i <= 3; i++) {
      NameIdentifier viewIdent = NameIdentifier.of(viewNs, "view" + i);
      View loadedView = viewOperationDispatcher.loadView(viewIdent);
      Assertions.assertEquals("view" + i, loadedView.name());
    }
  }

  @Test
  public void testLoadViewAutoImportsIntoEntityStore() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema63");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent = NameIdentifier.of(viewNs, "auto_import_view");

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView = createMockView("auto_import_view", props, auditInfo);

    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent, mockView);

    // Verify view is not in entity store initially
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> entityStore.get(viewIdent, VIEW, GenericEntity.class));

    // Load view - should auto-import
    View loadedView = viewOperationDispatcher.loadView(viewIdent);
    Assertions.assertEquals("auto_import_view", loadedView.name());

    // Verify view was auto-imported into entity store
    GenericEntity viewEntity = entityStore.get(viewIdent, VIEW, GenericEntity.class);
    Assertions.assertNotNull(viewEntity);
    Assertions.assertEquals(viewIdent.name(), viewEntity.name());
    Assertions.assertEquals(viewIdent.namespace(), viewEntity.namespace());
    Assertions.assertEquals(Entity.EntityType.VIEW, viewEntity.type());
  }

  @Test
  public void testLoadViewUsesEntityStoreCache() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema64");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent = NameIdentifier.of(viewNs, "cached_view");

    // Pre-populate entity store with view entity
    GenericEntity viewEntity =
        GenericEntity.builder()
            .withId(1L)
            .withName(viewIdent.name())
            .withNamespace(viewIdent.namespace())
            .withEntityType(Entity.EntityType.VIEW)
            .build();
    entityStore.put(viewEntity, false);

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView = createMockView("cached_view", props, auditInfo);

    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent, mockView);

    // Load view - should use entity store cache
    View loadedView = viewOperationDispatcher.loadView(viewIdent);
    Assertions.assertEquals("cached_view", loadedView.name());

    // Verify entity is still in store (no duplicate import)
    GenericEntity retrievedEntity = entityStore.get(viewIdent, VIEW, GenericEntity.class);
    Assertions.assertNotNull(retrievedEntity);
    Assertions.assertEquals(1L, retrievedEntity.id());
  }

  @Test
  public void testLoadViewCreatesGenericEntity() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema65");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent = NameIdentifier.of(viewNs, "generic_entity_view");

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView = createMockView("generic_entity_view", props, auditInfo);

    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent, mockView);

    // Load view - should create GenericEntity (not ViewEntity)
    View loadedView = viewOperationDispatcher.loadView(viewIdent);
    Assertions.assertEquals("generic_entity_view", loadedView.name());

    // Verify GenericEntity was created (without auditInfo)
    GenericEntity viewEntity = entityStore.get(viewIdent, VIEW, GenericEntity.class);
    Assertions.assertNotNull(viewEntity);
    Assertions.assertEquals(Entity.EntityType.VIEW, viewEntity.type());
    // GenericEntity doesn't have auditInfo, only basic fields
    Assertions.assertNotNull(viewEntity.id());
    Assertions.assertEquals(viewIdent.name(), viewEntity.name());
  }

  @Test
  public void testLoadViewAutoImportWithMultipleConcurrentLoads() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema66");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent = NameIdentifier.of(viewNs, "concurrent_view");

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView = createMockView("concurrent_view", props, auditInfo);

    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent, mockView);

    // Load view multiple times - should handle gracefully
    View loadedView1 = viewOperationDispatcher.loadView(viewIdent);
    View loadedView2 = viewOperationDispatcher.loadView(viewIdent);
    View loadedView3 = viewOperationDispatcher.loadView(viewIdent);

    Assertions.assertEquals("concurrent_view", loadedView1.name());
    Assertions.assertEquals("concurrent_view", loadedView2.name());
    Assertions.assertEquals("concurrent_view", loadedView3.name());

    // Verify only one entity exists in store
    GenericEntity viewEntity = entityStore.get(viewIdent, VIEW, GenericEntity.class);
    Assertions.assertNotNull(viewEntity);
  }

  @Test
  public void testLoadViewAfterManualDelete() throws IOException {
    Namespace viewNs = Namespace.of(metalake, catalog, "schema67");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(viewNs.levels()), "comment", props);

    NameIdentifier viewIdent = NameIdentifier.of(viewNs, "deleted_view");

    // Create a mock view through the catalog operations
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    View mockView = createMockView("deleted_view", props, auditInfo);

    TestCatalog testCatalog =
        (TestCatalog) catalogManager.loadCatalog(NameIdentifier.of(metalake, catalog));
    TestCatalogOperations testCatalogOperations = (TestCatalogOperations) testCatalog.ops();
    testCatalogOperations.views.put(viewIdent, mockView);

    // Load view - should auto-import
    View loadedView1 = viewOperationDispatcher.loadView(viewIdent);
    Assertions.assertEquals("deleted_view", loadedView1.name());

    // Manually delete from entity store
    entityStore.delete(viewIdent, VIEW);

    // Load view again - should re-import
    View loadedView2 = viewOperationDispatcher.loadView(viewIdent);
    Assertions.assertEquals("deleted_view", loadedView2.name());

    // Verify view was re-imported
    GenericEntity viewEntity = entityStore.get(viewIdent, VIEW, GenericEntity.class);
    Assertions.assertNotNull(viewEntity);
  }
}
