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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestViewMetaService extends TestJDBCBackend {

  private final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_view_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_view_schema");

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "test_view", AUDIT_INFO);
    ViewEntity viewCopy =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, "test_view", AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> ViewMetaService.getInstance().insertView(viewCopy, false));
  }

  @TestTemplate
  public void testInsertAndGetView() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity loaded = ViewMetaService.getInstance().getViewByIdentifier(viewIdent);

    assertNotNull(loaded);
    assertEquals(view.id(), loaded.id());
    assertEquals(view.name(), loaded.name());
    assertEquals(view.comment(), loaded.comment());
    assertEquals(view.defaultCatalog(), loaded.defaultCatalog());
    assertEquals(view.defaultSchema(), loaded.defaultSchema());
    assertEquals(view.columns().length, loaded.columns().length);
    assertEquals(view.representations().length, loaded.representations().length);
    assertEquals(view.auditInfo().creator(), loaded.auditInfo().creator());
  }

  @TestTemplate
  public void testListViews() throws IOException {
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);

    String viewName1 = GravitinoITUtils.genRandomName("test_view1");
    ViewEntity view1 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName1, AUDIT_INFO);

    String viewName2 = GravitinoITUtils.genRandomName("test_view2");
    ViewEntity view2 =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName2, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view1, false);
    ViewMetaService.getInstance().insertView(view2, false);

    List<ViewEntity> views = ViewMetaService.getInstance().listViewsByNamespace(ns);

    assertEquals(2, views.size());
    assertTrue(views.stream().anyMatch(v -> v.name().equals(viewName1)));
    assertTrue(views.stream().anyMatch(v -> v.name().equals(viewName2)));
  }

  @TestTemplate
  public void testUpdateView() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity updated =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("updated comment")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog("updated_catalog")
            .withDefaultSchema("updated_schema")
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();

    ViewEntity result = ViewMetaService.getInstance().updateView(viewIdent, e -> updated);
    assertEquals("updated comment", result.comment());
    assertEquals("updated_catalog", result.defaultCatalog());
    assertEquals("updated_schema", result.defaultSchema());

    Map<Integer, Long> versions = listViewVersions(view.id());
    assertEquals(2, versions.size());
    assertTrue(versions.containsKey(1));
    assertTrue(versions.containsKey(2));
  }

  @TestTemplate
  public void testCreateViewWaitsForConcurrentSchemaDelete() throws Exception {
    SchemaPO observedSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaByFullQualifiedName(metalakeName, catalogName, schemaName));
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            GravitinoITUtils.genRandomName("view_parent_delete_race"),
            AUDIT_INFO);

    CountDownLatch deleteWritten = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch createStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                SchemaMetaMapper.class,
                                mapper ->
                                    mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                        observedSchemaPO.getSchemaId(),
                                        observedSchemaPO.getCurrentVersion()))),
                    () -> {
                      deleteWritten.countDown();
                      await(allowDeleteCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(deleteWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> createResult =
          executor.submit(
              () -> {
                createStarted.countDown();
                try {
                  ViewMetaService.getInstance().insertView(view, false);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(createStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> createResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Throwable createFailure = createResult.get(30, TimeUnit.SECONDS);
      assertTrue(createFailure instanceof NoSuchEntityException, String.valueOf(createFailure));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()));
  }

  @TestTemplate
  public void testUpdateViewReportsNoSuchAfterConcurrentDelete() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity updated =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("updated comment")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog("updated_catalog")
            .withDefaultSchema("updated_schema")
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();

    assertThrows(
        NoSuchEntityException.class,
        () ->
            ViewMetaService.getInstance()
                .updateView(
                    viewIdent,
                    e -> {
                      ViewMetaService.getInstance().deleteView(viewIdent);
                      return updated;
                    }));

    Map<Integer, Long> versions = listViewVersions(view.id());
    assertEquals(1, versions.size());
    assertTrue(versions.containsKey(1));
    assertTrue(versions.get(1) > 0L);
  }

  @TestTemplate
  public void testAlterReportsOptimisticLockConflictAndKeepsWinnerVersion() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_alter_conflict");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);

    assertThrows(
        OptimisticLockException.class,
        () ->
            ViewMetaService.getInstance()
                .updateView(
                    view.nameIdentifier(),
                    entity -> {
                      try {
                        ViewMetaService.getInstance()
                            .updateView(
                                view.nameIdentifier(),
                                competing ->
                                    copyViewWithComment(
                                        (ViewEntity) competing, "competing update"));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return copyViewWithComment((ViewEntity) entity, "requested update");
                    }));

    ViewEntity current = ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier());
    assertEquals("competing update", current.comment());
    Map<Integer, Long> versions = listViewVersions(view.id());
    assertEquals(2, versions.size());
    assertTrue(versions.containsKey(2));
  }

  @TestTemplate
  public void testAlterRollsBackRootCasWhenVersionInsertFails() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_version_insert_failure");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    ViewEntity conflictingVersion = copyViewWithComment(view, "conflicting version");
    ViewPO conflictingPO =
        ViewPO.buildViewPO(
            conflictingVersion, ViewPO.builder().withCurrentVersion(2L).withLastVersion(2L), 2);
    SessionUtils.doWithCommit(
        ViewVersionInfoMapper.class,
        mapper -> mapper.insertViewVersionInfo(conflictingPO.getViewVersionInfoPO()));

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            ViewMetaService.getInstance()
                .updateView(
                    view.nameIdentifier(),
                    entity -> copyViewWithComment((ViewEntity) entity, "must roll back")));

    ViewPO currentPO = ViewMetaService.getInstance().getViewPOByIdentifier(view.nameIdentifier());
    assertEquals(1L, currentPO.getCurrentVersion());
    assertEquals(1L, currentPO.getLastVersion());
    assertEquals(
        view.comment(),
        ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()).comment());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenRenamedConcurrently() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_alter_renamed");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    NameIdentifier renamedIdentifier = NameIdentifier.of(namespace, viewName + "_winner");

    assertThrows(
        NoSuchEntityException.class,
        () ->
            ViewMetaService.getInstance()
                .updateView(
                    view.nameIdentifier(),
                    entity -> {
                      try {
                        ViewMetaService.getInstance()
                            .updateView(
                                view.nameIdentifier(),
                                competing ->
                                    copyView(
                                        (ViewEntity) competing,
                                        renamedIdentifier.name(),
                                        namespace,
                                        "renamed winner"));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return copyViewWithComment((ViewEntity) entity, "stale update");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()));
    assertEquals(
        "renamed winner",
        ViewMetaService.getInstance().getViewByIdentifier(renamedIdentifier).comment());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenMovedConcurrently() throws IOException {
    String targetCatalogName = GravitinoITUtils.genRandomName("view_target_catalog");
    String targetSchemaName = GravitinoITUtils.genRandomName("view_target_schema");
    createAndInsertCatalog(metalakeName, targetCatalogName);
    createAndInsertSchema(metalakeName, targetCatalogName, targetSchemaName);
    String viewName = GravitinoITUtils.genRandomName("view_alter_moved");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    Namespace movedNamespace =
        NamespaceUtil.ofView(metalakeName, targetCatalogName, targetSchemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    NameIdentifier movedIdentifier = NameIdentifier.of(movedNamespace, viewName);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            ViewMetaService.getInstance()
                .updateView(
                    view.nameIdentifier(),
                    entity -> {
                      try {
                        ViewMetaService.getInstance()
                            .updateView(
                                view.nameIdentifier(),
                                competing ->
                                    copyView(
                                        (ViewEntity) competing,
                                        viewName,
                                        movedNamespace,
                                        "moved winner"));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                      return copyViewWithComment((ViewEntity) entity, "stale update");
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()));
    assertEquals(
        "moved winner",
        ViewMetaService.getInstance().getViewByIdentifier(movedIdentifier).comment());
  }

  @TestTemplate
  public void testMoveWaitsForConcurrentTargetSchemaDelete() throws Exception {
    String targetCatalogName = GravitinoITUtils.genRandomName("view_deleted_target_catalog");
    String targetSchemaName = GravitinoITUtils.genRandomName("view_deleted_target_schema");
    createAndInsertCatalog(metalakeName, targetCatalogName);
    createAndInsertSchema(metalakeName, targetCatalogName, targetSchemaName);
    SchemaPO targetSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaByFullQualifiedName(
                    metalakeName, targetCatalogName, targetSchemaName));
    String viewName = GravitinoITUtils.genRandomName("view_target_delete_race");
    Namespace sourceNamespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    Namespace targetNamespace =
        NamespaceUtil.ofView(metalakeName, targetCatalogName, targetSchemaName);
    ViewEntity view =
        createViewEntity(
            RandomIdGenerator.INSTANCE.nextId(), sourceNamespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    ViewEntity moved = copyView(view, viewName, targetNamespace, "must not move");

    CountDownLatch deleteWritten = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch moveStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                SchemaMetaMapper.class,
                                mapper ->
                                    mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                        targetSchemaPO.getSchemaId(),
                                        targetSchemaPO.getCurrentVersion()))),
                    () -> {
                      deleteWritten.countDown();
                      await(allowDeleteCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(deleteWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> moveResult =
          executor.submit(
              () -> {
                moveStarted.countDown();
                try {
                  ViewMetaService.getInstance().updateView(view.nameIdentifier(), ignored -> moved);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(moveStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> moveResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Throwable moveFailure = moveResult.get(30, TimeUnit.SECONDS);
      assertTrue(moveFailure instanceof NoSuchEntityException, String.valueOf(moveFailure));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    assertEquals(
        view.comment(),
        ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()).comment());
    assertEquals(1, listViewVersions(view.id()).size());
  }

  @TestTemplate
  public void testDeleteRejectsStaleVersion() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_stale_delete");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("view_occ_tag")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            view.nameIdentifier(),
            view.type(),
            new NameIdentifier[] {tag.nameIdentifier()},
            new NameIdentifier[0]);
    ViewPO stalePO = ViewMetaService.getInstance().getViewPOByIdentifier(view.nameIdentifier());

    ViewMetaService.getInstance()
        .updateView(
            view.nameIdentifier(),
            entity -> copyViewWithComment((ViewEntity) entity, "winning update"));

    assertThrows(
        OptimisticLockException.class,
        () -> ViewMetaService.getInstance().deleteViewWithVersion(view.nameIdentifier(), stalePO));
    assertEquals(
        "winning update",
        ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()).comment());
    assertEquals(1, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
  }

  @TestTemplate
  public void testDeleteReportsNoSuchWhenDeletedConcurrently() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_delete_deleted");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    ViewPO stalePO = ViewMetaService.getInstance().getViewPOByIdentifier(view.nameIdentifier());

    ViewMetaService.getInstance().deleteView(view.nameIdentifier());

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().deleteViewWithVersion(view.nameIdentifier(), stalePO));
  }

  @TestTemplate
  public void testDeleteView() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    // Set up tag relation
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            view.nameIdentifier(),
            view.type(),
            new NameIdentifier[] {NameIdentifierUtil.ofTag(metalakeName, tag.name())},
            new NameIdentifier[0]);
    assertEquals(1, countActiveTagRelForMetadataObject(view.id(), "VIEW"));

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    assertTrue(ViewMetaService.getInstance().deleteView(viewIdent));

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(viewIdent));
    assertEquals(0, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
  }

  @TestTemplate
  public void testGetNonExistentView() {
    NameIdentifier viewIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "non_existent_view");
    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(viewIdent));
  }

  @TestTemplate
  public void testInsertViewWithOverwrite() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);

    ViewMetaService.getInstance().insertView(view, false);

    ViewEntity newView =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("overwritten comment")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog(view.defaultCatalog())
            .withDefaultSchema(view.defaultSchema())
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();

    ViewMetaService.getInstance().insertView(newView, true);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity loaded = ViewMetaService.getInstance().getViewByIdentifier(viewIdent);
    assertEquals("overwritten comment", loaded.comment());
    ViewPO overwrittenPO = ViewMetaService.getInstance().getViewPOByIdentifier(viewIdent);
    assertEquals(2L, overwrittenPO.getCurrentVersion());
    assertEquals(2L, overwrittenPO.getLastVersion());
    assertEquals(2, listViewVersions(view.id()).size());
  }

  @TestTemplate
  public void testNaturalKeyOverwriteUsesPersistedViewId() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_natural_key_overwrite");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity original =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(original, false);
    ViewEntity replacement =
        copyView(
            createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO),
            viewName,
            namespace,
            "replacement");

    ViewMetaService.getInstance().insertView(replacement, true);

    ViewEntity stored =
        ViewMetaService.getInstance().getViewByIdentifier(original.nameIdentifier());
    ViewPO storedPO =
        ViewMetaService.getInstance().getViewPOByIdentifier(original.nameIdentifier());
    assertEquals(original.id(), stored.id());
    assertEquals("replacement", stored.comment());
    assertEquals(2L, storedPO.getCurrentVersion());
    assertEquals(2, listViewVersions(original.id()).size());
    assertTrue(listViewVersions(replacement.id()).isEmpty());
  }

  @TestTemplate
  public void testNormalReadRequiresCurrentVersionRow() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("view_missing_current_version");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);

    SessionUtils.doWithCommit(
        ViewVersionInfoMapper.class, mapper -> mapper.softDeleteViewVersionsByViewId(view.id()));

    assertThrows(
        NoSuchEntityException.class,
        () -> ViewMetaService.getInstance().getViewByIdentifier(view.nameIdentifier()));
  }

  @TestTemplate
  public void testNaturalKeyOverwriteWaitsForConcurrentRename() throws Exception {
    String viewName = GravitinoITUtils.genRandomName("view_overwrite_rename_race");
    Namespace namespace = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity original =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(original, false);
    ViewPO observedPO =
        ViewMetaService.getInstance().getViewPOByIdentifier(original.nameIdentifier());
    ViewEntity renamed = copyView(original, viewName + "_winner", namespace, "rename winner");
    ViewPO renamedPO =
        ViewPO.buildViewPO(renamed, ViewPO.builder().withCurrentVersion(2L).withLastVersion(2L), 2);
    ViewEntity replacement =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, viewName, AUDIT_INFO);

    CountDownLatch renameWritten = new CountDownLatch(1);
    CountDownLatch allowRenameCommit = new CountDownLatch(1);
    CountDownLatch overwriteStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> renameResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        assertEquals(
                            Integer.valueOf(1),
                            SessionUtils.getWithoutCommit(
                                ViewMetaMapper.class,
                                mapper -> mapper.updateViewMeta(renamedPO, observedPO))),
                    () ->
                        SessionUtils.doWithoutCommit(
                            ViewVersionInfoMapper.class,
                            mapper ->
                                mapper.insertViewVersionInfo(renamedPO.getViewVersionInfoPO())),
                    () -> {
                      renameWritten.countDown();
                      await(allowRenameCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      assertTrue(renameWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> overwriteResult =
          executor.submit(
              () -> {
                overwriteStarted.countDown();
                try {
                  ViewMetaService.getInstance().insertView(replacement, true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(overwriteStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> overwriteResult.get(500, TimeUnit.MILLISECONDS));

      allowRenameCommit.countDown();
      assertNull(renameResult.get(30, TimeUnit.SECONDS));
      assertNull(overwriteResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowRenameCommit.countDown();
      executor.shutdownNow();
    }

    assertEquals(
        original.id(),
        ViewMetaService.getInstance().getViewByIdentifier(renamed.nameIdentifier()).id());
    assertEquals(
        replacement.id(),
        ViewMetaService.getInstance().getViewByIdentifier(replacement.nameIdentifier()).id());
  }

  @TestTemplate
  public void testViewLifeCycle() throws IOException {
    String viewName = GravitinoITUtils.genRandomName("test_view");
    Namespace ns = NamespaceUtil.ofView(metalakeName, catalogName, schemaName);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), ns, viewName, AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);

    NameIdentifier viewIdent = NameIdentifier.of(metalakeName, catalogName, schemaName, viewName);
    ViewEntity v2 =
        ViewEntity.builder()
            .withId(view.id())
            .withName(view.name())
            .withNamespace(ns)
            .withComment("v2")
            .withColumns(view.columns())
            .withRepresentations(view.representations())
            .withDefaultCatalog(view.defaultCatalog())
            .withDefaultSchema(view.defaultSchema())
            .withProperties(view.properties())
            .withAuditInfo(AUDIT_INFO)
            .build();
    ViewMetaService.getInstance().updateView(viewIdent, e -> v2);

    assertTrue(ViewMetaService.getInstance().deleteView(viewIdent));

    int deleted =
        ViewMetaService.getInstance()
            .deleteViewMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 100);
    assertTrue(deleted >= 2);
    assertEquals(0, listViewVersions(view.id()).size());
  }

  private ViewEntity createViewEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    Column[] columns =
        new Column[] {
          Column.of("c1", Types.IntegerType.get(), "first column"),
          Column.of("c2", Types.StringType.get(), "second column")
        };
    Representation[] reps =
        new Representation[] {
          SQLRepresentation.builder().withDialect("spark").withSql("SELECT c1, c2 FROM t").build(),
          SQLRepresentation.builder().withDialect("trino").withSql("SELECT c1, c2 FROM t").build()
        };
    return ViewEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test view comment")
        .withColumns(columns)
        .withRepresentations(reps)
        .withDefaultCatalog(null)
        .withDefaultSchema(null)
        .withProperties(ImmutableMap.of("k1", "v1"))
        .withAuditInfo(auditInfo)
        .build();
  }

  private ViewEntity copyViewWithComment(ViewEntity view, String comment) {
    return copyView(view, view.name(), view.namespace(), comment);
  }

  private ViewEntity copyView(ViewEntity view, String name, Namespace namespace, String comment) {
    return ViewEntity.builder()
        .withId(view.id())
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withColumns(view.columns())
        .withRepresentations(view.representations())
        .withDefaultCatalog(view.defaultCatalog())
        .withDefaultSchema(view.defaultSchema())
        .withProperties(view.properties())
        .withAuditInfo(view.auditInfo())
        .build();
  }

  private void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private Map<Integer, Long> listViewVersions(Long viewId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM view_version_info WHERE view_id = %d",
                    viewId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
  }

  private int countActiveTagRelForMetadataObject(Long metadataObjectId, String metadataObjectType) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM tag_relation_meta"
                        + " WHERE metadata_object_id = %d AND metadata_object_type = '%s'"
                        + " AND deleted_at = 0",
                    metadataObjectId, metadataObjectType))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("No result for countActiveTagRelForMetadataObject");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }
}
