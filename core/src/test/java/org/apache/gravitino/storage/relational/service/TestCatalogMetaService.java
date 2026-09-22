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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCatalogMetaService extends TestJDBCBackend {

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  private final String metalakeName = "metalake_for_catalog_test";

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(catalogCopy, false));
  }

  @TestTemplate
  public void testInsertCatalogLocksMetalakeWithoutChangingVersion() throws IOException {
    MetalakePO beforeInsert =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_fence",
            auditInfo);
    backend.insert(catalog, false);

    MetalakePO afterInsert =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
    assertEquals(beforeInsert.getCurrentVersion(), afterInsert.getCurrentVersion());
    assertEquals(beforeInsert.getLastVersion(), afterInsert.getLastVersion());

    CatalogEntity duplicate =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            catalog.name(),
            auditInfo);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(duplicate, false));

    MetalakePO afterFailure =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(metalakeName));
    assertEquals(afterInsert.getCurrentVersion(), afterFailure.getCurrentVersion());
    assertEquals(afterInsert.getLastVersion(), afterFailure.getLastVersion());
  }

  @TestTemplate
  public void testConcurrentSameNameCatalogCreateReportsAlreadyExists() throws Exception {
    CatalogEntity first =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "concurrent_catalog",
            auditInfo);
    CatalogEntity second =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            first.name(),
            auditInfo);

    List<Throwable> results = insertCatalogsConcurrently(first, second);
    assertEquals(1, results.stream().filter(Objects::isNull).count());
    Throwable failure = results.stream().filter(Objects::nonNull).findFirst().orElseThrow();
    Assertions.assertTrue(
        failure instanceof EntityAlreadyExistsException,
        () -> "Expected EntityAlreadyExistsException, but got " + failure);
  }

  @TestTemplate
  public void testConcurrentDifferentCatalogCreatesBothSucceed() throws Exception {
    CatalogEntity first =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "concurrent_catalog_1",
            auditInfo);
    CatalogEntity second =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "concurrent_catalog_2",
            auditInfo);

    List<Throwable> results = insertCatalogsConcurrently(first, second);
    Assertions.assertTrue(
        results.stream().allMatch(Objects::isNull),
        () -> "Concurrent catalog creates failed: " + results);
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog1",
            auditInfo);
    backend.insert(catalog, false);
    backend.insert(catalogCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                catalogCopy.nameIdentifier(),
                Entity.EntityType.CATALOG,
                e ->
                    createCatalog(
                        catalogCopy.id(), catalogCopy.namespace(), "catalog", auditInfo)));
  }

  @TestTemplate
  void testUpdateCatalogWithNullableComment() throws IOException {
    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withNamespace(NamespaceUtil.ofCatalog(metalakeName))
            .withName("catalog")
            .withAuditInfo(auditInfo)
            .withComment(null)
            .withProperties(null)
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .build();
    backend.insert(catalog, false);

    backend.update(
        catalog.nameIdentifier(),
        Entity.EntityType.CATALOG,
        e ->
            CatalogEntity.builder()
                .withId(catalog.id())
                .withNamespace(catalog.namespace())
                .withName(catalog.name())
                .withAuditInfo(auditInfo)
                .withComment("comment")
                .withProperties(catalog.getProperties())
                .withType(Catalog.Type.RELATIONAL)
                .withProvider("test")
                .build());

    CatalogEntity updatedCatalog = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertNotNull(updatedCatalog.getComment());
  }

  @TestTemplate
  public void testAlterAndDeleteUseCurrentVersion() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_occ",
            auditInfo);
    backend.insert(catalog, false);
    CatalogPO oldPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));
    CatalogEntity updatedCatalog =
        CatalogEntity.builder()
            .withId(catalog.id())
            .withName(catalog.name())
            .withNamespace(catalog.namespace())
            .withAuditInfo(auditInfo)
            .withComment("updated")
            .withProperties(catalog.getProperties())
            .withType(catalog.getType())
            .withProvider(catalog.getProvider())
            .build();
    CatalogPO newPO =
        POConverters.updateCatalogPOWithVersion(oldPO, updatedCatalog, oldPO.getMetalakeId());

    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class, mapper -> mapper.updateCatalogMeta(newPO, oldPO));
    int staleUpdate =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class, mapper -> mapper.updateCatalogMeta(newPO, oldPO));
    int staleDelete =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class,
            mapper ->
                mapper.softDeleteCatalogMetasByCatalogId(catalog.id(), oldPO.getCurrentVersion()));
    assertEquals(1, updated);
    assertEquals(0, staleUpdate);
    assertEquals(0, staleDelete);
    assertTrue(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    int deleted =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class,
            mapper ->
                mapper.softDeleteCatalogMetasByCatalogId(catalog.id(), newPO.getCurrentVersion()));
    assertEquals(1, deleted);
  }

  @TestTemplate
  public void testOverwriteInsertAdvancesCurrentVersion() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_overwrite_occ",
            auditInfo);
    backend.insert(catalog, false);
    CatalogPO initialPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));

    backend.insert(catalog, true);

    CatalogPO overwrittenPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));
    assertEquals(initialPO.getCurrentVersion() + 1, overwrittenPO.getCurrentVersion().longValue());
    assertEquals(
        overwrittenPO.getCurrentVersion().longValue(), overwrittenPO.getLastVersion().longValue());

    // A writer that observed the catalog before the overwrite must not pass its compare-and-set.
    int staleDelete =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class,
            mapper ->
                mapper.softDeleteCatalogMetasByCatalogId(
                    catalog.id(), initialPO.getCurrentVersion()));
    assertEquals(0, staleDelete);
  }

  @TestTemplate
  public void testAlterReportsOptimisticLockConflict() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_alter_conflict",
            auditInfo);
    backend.insert(catalog, false);

    assertThrows(
        OptimisticLockException.class,
        () ->
            CatalogMetaService.getInstance()
                .updateCatalog(
                    catalog.nameIdentifier(),
                    entity -> {
                      CatalogEntity current = (CatalogEntity) entity;
                      CatalogPO currentPO =
                          SessionUtils.getWithoutCommit(
                              CatalogMetaMapper.class,
                              mapper -> mapper.selectCatalogMetaById(current.id()));
                      CatalogEntity competingUpdate =
                          copyCatalogWithComment(current, "competing update");
                      CatalogPO competingPO =
                          POConverters.updateCatalogPOWithVersion(
                              currentPO, competingUpdate, currentPO.getMetalakeId());
                      SessionUtils.doWithCommitAndFetchResult(
                          CatalogMetaMapper.class,
                          mapper -> mapper.updateCatalogMeta(competingPO, currentPO));
                      return copyCatalogWithComment(current, "requested update");
                    }));
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenCatalogIsDeletedConcurrently() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_alter_deleted",
            auditInfo);
    backend.insert(catalog, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            CatalogMetaService.getInstance()
                .updateCatalog(
                    catalog.nameIdentifier(),
                    entity -> {
                      CatalogEntity current = (CatalogEntity) entity;
                      CatalogPO currentPO =
                          SessionUtils.getWithoutCommit(
                              CatalogMetaMapper.class,
                              mapper -> mapper.selectCatalogMetaById(current.id()));
                      SessionUtils.doWithCommitAndFetchResult(
                          CatalogMetaMapper.class,
                          mapper ->
                              mapper.softDeleteCatalogMetasByCatalogId(
                                  current.id(), currentPO.getCurrentVersion()));
                      return copyCatalogWithComment(current, "requested update");
                    }));
  }

  @TestTemplate
  public void testNonCascadeDeleteRollsBackCatalogFence() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_non_empty",
            auditInfo);
    backend.insert(catalog, false);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalog.name()),
            "schema",
            auditInfo);
    backend.insert(schema, false);
    CatalogPO beforeDelete =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));

    assertThrows(
        NonEmptyEntityException.class,
        () -> CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false));

    CatalogPO afterDelete =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));
    assertEquals(beforeDelete.getCurrentVersion(), afterDelete.getCurrentVersion());
    assertTrue(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);

    List<CatalogEntity> catalogs =
        backend.list(catalog.namespace(), Entity.EntityType.CATALOG, true);
    assertTrue(catalogs.contains(catalog));
    assertEquals(1, catalogs.size());

    CatalogEntity catalogEntity = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    assertEquals(catalog, catalogEntity);
    Assertions.assertNotNull(
        CatalogMetaService.getInstance()
            .getCatalogPOByName(catalogEntity.namespace().level(0), catalog.name()));
    assertEquals(
        catalog.id(),
        CatalogMetaService.getInstance()
            .getCatalogIdByName(catalog.namespace().level(0), catalog.name()));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);

    assertEquals(
        0,
        SessionUtils.doWithCommitAndFetchResult(
                CatalogMetaMapper.class,
                mapper -> mapper.listCatalogPOsByMetalakeName(metalakeName))
            .size());

    // check existence after soft delete
    assertFalse(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));

    // meta data hard delete
    backend.hardDeleteLegacyData(Entity.EntityType.CATALOG, Instant.now().toEpochMilli() + 3000);
    assertFalse(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));
  }

  @TestTemplate
  public void testDeleteCatalogCascadeRemovesTagRelations() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_with_tags",
            auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalog.name()),
            "schema_with_tags",
            AUDIT_INFO);
    backend.insert(schema, false);

    Namespace objectNamespace = Namespace.of(metalakeName, catalog.name(), schema.name());
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column_with_tag")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_with_tag")
            .withNamespace(objectNamespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "topic_with_tag", AUDIT_INFO);
    TopicMetaService.getInstance().insertTopic(topic, false);
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "fileset_with_tag", AUDIT_INFO);
    FilesetMetaService.getInstance().insertFileset(fileset, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            objectNamespace,
            "model_with_tag",
            "comment",
            1,
            null,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "view_with_tag");
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "function_with_tag", AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    FunctionMetaService.getInstance().insertFunction(function, false);

    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    associateTag(tag, catalog.nameIdentifier(), catalog.type());
    associateTag(tag, schema.nameIdentifier(), schema.type());
    associateTag(tag, table.nameIdentifier(), table.type());
    associateTag(
        tag,
        NameIdentifier.of(Namespace.fromString(table.nameIdentifier().toString()), column.name()),
        column.type());
    associateTag(tag, topic.nameIdentifier(), topic.type());
    associateTag(tag, fileset.nameIdentifier(), fileset.type());
    associateTag(tag, model.nameIdentifier(), model.type());
    associateTag(tag, view.nameIdentifier(), view.type());
    associateTag(tag, function.nameIdentifier(), function.type());

    assertEquals(1, countActiveTagRelForMetadataObject(catalog.id(), "CATALOG"));
    assertEquals(1, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    assertEquals(1, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    assertEquals(1, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    assertEquals(1, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    assertEquals(1, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    assertEquals(1, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    assertEquals(1, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    assertEquals(1, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));

    assertTrue(CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true));

    assertEquals(0, countActiveTagRelForMetadataObject(catalog.id(), "CATALOG"));
    assertEquals(0, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    assertEquals(0, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    assertEquals(0, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    assertEquals(0, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    assertEquals(0, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    assertEquals(0, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    assertEquals(0, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    assertEquals(0, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));
  }

  private List<Throwable> insertCatalogsConcurrently(CatalogEntity first, CatalogEntity second)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<Throwable> firstResult =
          executor.submit(
              () -> {
                ready.countDown();
                start.await();
                try {
                  CatalogMetaService.getInstance().insertCatalog(first, false);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      Future<Throwable> secondResult =
          executor.submit(
              () -> {
                ready.countDown();
                start.await();
                try {
                  CatalogMetaService.getInstance().insertCatalog(second, false);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      start.countDown();
      return Arrays.asList(
          firstResult.get(30, TimeUnit.SECONDS), secondResult.get(30, TimeUnit.SECONDS));
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private CatalogEntity copyCatalogWithComment(CatalogEntity catalog, String comment) {
    return CatalogEntity.builder()
        .withId(catalog.id())
        .withName(catalog.name())
        .withNamespace(catalog.namespace())
        .withType(catalog.getType())
        .withProvider(catalog.getProvider())
        .withComment(comment)
        .withProperties(catalog.getProperties())
        .withAuditInfo(auditInfo)
        .build();
  }

  private void associateTag(TagEntity tag, NameIdentifier ident, Entity.EntityType type)
      throws IOException {
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            ident,
            type,
            new NameIdentifier[] {NameIdentifierUtil.ofTag(metalakeName, tag.name())},
            new NameIdentifier[0]);
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
