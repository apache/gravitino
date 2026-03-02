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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestViewMetaService extends TestJDBCBackend {

  private final String metalakeName = "metalake_for_view_test";
  private final String catalogName = "catalog_for_view_test";
  private final String schemaName = "schema_for_view_test";

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertAndGetView() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "test_view");

    // Insert view
    backend.insert(view, false);

    // Get view
    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "test_view");
    GenericEntity retrievedView = backend.get(viewIdent, Entity.EntityType.VIEW);

    assertNotNull(retrievedView);
    assertEquals(view.id(), retrievedView.id());
    assertEquals(view.name(), retrievedView.name());
    assertEquals(viewNamespace, retrievedView.namespace());
    assertEquals(Entity.EntityType.VIEW, retrievedView.type());
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view");
    GenericEntity viewCopy =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view");

    backend.insert(view, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(viewCopy, false));
  }

  @TestTemplate
  public void testInsertWithOverwrite() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view_overwrite");

    backend.insert(view, false);

    // Insert with overwrite should succeed
    GenericEntity viewOverwrite = createViewEntity(view.id(), viewNamespace, "view_overwrite");
    backend.insert(viewOverwrite, true);

    // Verify the view still exists
    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "view_overwrite");
    GenericEntity retrievedView = backend.get(viewIdent, Entity.EntityType.VIEW);
    assertNotNull(retrievedView);
    assertEquals(view.id(), retrievedView.id());
    assertEquals(viewNamespace, retrievedView.namespace());
  }

  @TestTemplate
  public void testListViews() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view1 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view1");
    GenericEntity view2 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view2");
    GenericEntity view3 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view3");

    backend.insert(view1, false);
    backend.insert(view2, false);
    backend.insert(view3, false);

    List<GenericEntity> views = backend.list(viewNamespace, Entity.EntityType.VIEW, true);

    assertEquals(3, views.size());
    assertTrue(views.stream().anyMatch(v -> v.name().equals("view1")));
    assertTrue(views.stream().anyMatch(v -> v.name().equals("view2")));
    assertTrue(views.stream().anyMatch(v -> v.name().equals("view3")));
    // Verify all views have namespace set
    assertTrue(views.stream().allMatch(v -> viewNamespace.equals(v.namespace())));
  }

  @TestTemplate
  public void testUpdateView() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view_to_update");

    backend.insert(view, false);

    // Update view name
    Function<GenericEntity, GenericEntity> updater =
        oldView ->
            GenericEntity.builder()
                .withId(oldView.id())
                .withName("view_updated")
                .withNamespace(oldView.namespace())
                .withEntityType(Entity.EntityType.VIEW)
                .build();

    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "view_to_update");
    GenericEntity updatedView = backend.update(viewIdent, Entity.EntityType.VIEW, updater);

    assertNotNull(updatedView);
    assertEquals("view_updated", updatedView.name());
    assertEquals(view.id(), updatedView.id());

    // Verify old name no longer exists
    assertThrows(NoSuchEntityException.class, () -> backend.get(viewIdent, Entity.EntityType.VIEW));

    // Verify new name exists
    NameIdentifier newViewIdent = NameIdentifier.of(viewNamespace, "view_updated");
    GenericEntity retrievedView = backend.get(newViewIdent, Entity.EntityType.VIEW);
    assertNotNull(retrievedView);
    assertEquals("view_updated", retrievedView.name());
    assertEquals(viewNamespace, retrievedView.namespace());
  }

  @TestTemplate
  public void testUpdateViewCrossNamespace() throws IOException {
    // Create a second schema for cross-namespace rename
    String schemaName2 = "schema_for_view_test_2";
    createAndInsertSchema(metalakeName, catalogName, schemaName2);

    Namespace viewNamespace1 = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    Namespace viewNamespace2 = NamespaceUtil.ofView(metalakeName, catalogName, schemaName2);

    GenericEntity view =
        createViewEntity(
            RandomIdGenerator.INSTANCE.nextId(), viewNamespace1, "view_cross_namespace");

    backend.insert(view, false);

    // Update view to move to different namespace
    Function<GenericEntity, GenericEntity> updater =
        oldView ->
            GenericEntity.builder()
                .withId(oldView.id())
                .withName("view_cross_namespace_renamed")
                .withNamespace(viewNamespace2)
                .withEntityType(Entity.EntityType.VIEW)
                .build();

    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace1, "view_cross_namespace");
    GenericEntity updatedView = backend.update(viewIdent, Entity.EntityType.VIEW, updater);

    assertNotNull(updatedView);
    assertEquals("view_cross_namespace_renamed", updatedView.name());
    assertEquals(view.id(), updatedView.id());

    // Verify old namespace+name no longer exists
    assertThrows(NoSuchEntityException.class, () -> backend.get(viewIdent, Entity.EntityType.VIEW));

    // Verify new namespace+name exists
    NameIdentifier newViewIdent = NameIdentifier.of(viewNamespace2, "view_cross_namespace_renamed");
    GenericEntity retrievedView = backend.get(newViewIdent, Entity.EntityType.VIEW);
    assertNotNull(retrievedView);
    assertEquals("view_cross_namespace_renamed", retrievedView.name());
    assertEquals(viewNamespace2, retrievedView.namespace());
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view1 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view1");
    GenericEntity view2 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view2");

    backend.insert(view1, false);
    backend.insert(view2, false);

    // Try to update view2's name to view1 (which already exists)
    NameIdentifier view2Ident = NameIdentifier.of(viewNamespace, "view2");
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                view2Ident,
                Entity.EntityType.VIEW,
                e ->
                    GenericEntity.builder()
                        .withId(view2.id())
                        .withName("view1")
                        .withNamespace(viewNamespace)
                        .withEntityType(Entity.EntityType.VIEW)
                        .build()));
  }

  @TestTemplate
  public void testDeleteView() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "view_to_delete");

    backend.insert(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "view_to_delete");

    // Verify view exists
    assertTrue(backend.exists(viewIdent, Entity.EntityType.VIEW));

    // Delete view
    boolean deleted = backend.delete(viewIdent, Entity.EntityType.VIEW, false);
    assertTrue(deleted);

    // Verify view no longer exists
    assertFalse(backend.exists(viewIdent, Entity.EntityType.VIEW));
  }

  @TestTemplate
  public void testDeleteNonExistentView() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "non_existent_view");

    // Delete should throw exception for non-existent view
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.delete(viewIdent, Entity.EntityType.VIEW, false));
  }

  @TestTemplate
  public void testGetNonExistentView() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "non_existent_view");

    assertThrows(NoSuchEntityException.class, () -> backend.get(viewIdent, Entity.EntityType.VIEW));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    GenericEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), viewNamespace, "lifecycle_view");

    backend.insert(view, false);

    List<GenericEntity> views = backend.list(viewNamespace, Entity.EntityType.VIEW, true);
    assertTrue(views.stream().anyMatch(v -> v.name().equals("lifecycle_view")));

    GenericEntity viewEntity =
        backend.get(NameIdentifier.of(viewNamespace, "lifecycle_view"), Entity.EntityType.VIEW);
    assertEquals(view.id(), viewEntity.id());
    assertEquals(view.name(), viewEntity.name());
    assertEquals(viewNamespace, viewEntity.namespace());

    // meta data soft delete
    backend.delete(
        NameIdentifierUtil.ofView(metalakeName, catalogName, schemaName, "lifecycle_view"),
        Entity.EntityType.VIEW,
        true);

    // check existence after soft delete
    NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "lifecycle_view");
    assertFalse(backend.exists(viewIdent, Entity.EntityType.VIEW));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(view.id(), Entity.EntityType.VIEW));

    // meta data hard delete
    backend.hardDeleteLegacyData(Entity.EntityType.VIEW, Instant.now().toEpochMilli() + 3000);
    assertFalse(legacyRecordExistsInDB(view.id(), Entity.EntityType.VIEW));
  }

  @TestTemplate
  public void testViewIdBySchemaIdAndName() throws IOException {

    Namespace viewNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    long viewId = RandomIdGenerator.INSTANCE.nextId();
    GenericEntity view = createViewEntity(viewId, viewNamespace, "id_test_view");

    backend.insert(view, false);

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(metalakeName, catalogName, schemaName), Entity.EntityType.SCHEMA);
    Long retrievedViewId =
        ViewMetaService.getInstance().getViewIdBySchemaIdAndName(schemaId, "id_test_view");

    Assertions.assertEquals(viewId, retrievedViewId);
  }

  @TestTemplate
  public void testGetViewIdBySchemaIdAndNameNotFound() throws IOException {

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(metalakeName, catalogName, schemaName), Entity.EntityType.SCHEMA);

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewIdBySchemaIdAndName(schemaId, "non_existent"));
  }
}
