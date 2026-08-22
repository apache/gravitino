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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
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
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

public class TestSchemaMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_catalog_test";
  private final String catalogName = "catalog_for_catalog_test";

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    backend.insert(schema, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(schemaCopy, false));
  }

  @TestTemplate
  public void testInsertSchemaLocksCatalogWithoutChangingVersion() throws IOException {
    createAndInsertMakeLake(metalakeName);
    CatalogEntity catalog = createAndInsertCatalog(metalakeName, catalogName);
    CatalogPO beforeInsert =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_fence",
            AUDIT_INFO);
    backend.insert(schema, false);

    CatalogPO afterInsert =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));
    Assertions.assertEquals(beforeInsert.getCurrentVersion(), afterInsert.getCurrentVersion());
    Assertions.assertEquals(beforeInsert.getLastVersion(), afterInsert.getLastVersion());

    SchemaEntity duplicate =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            schema.name(),
            AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(duplicate, false));

    CatalogPO afterFailure =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalog.id()));
    Assertions.assertEquals(afterInsert.getCurrentVersion(), afterFailure.getCurrentVersion());
    Assertions.assertEquals(afterInsert.getLastVersion(), afterFailure.getLastVersion());
  }

  @TestTemplate
  public void testEntityCreateWaitsForConcurrentSchemaDelete() throws Exception {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_for_entity_lock",
            AUDIT_INFO);
    backend.insert(schema, false);
    SchemaPO observedSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));

    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch entityCreateStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      int deleted =
                          SessionUtils.getWithoutCommit(
                              SchemaMetaMapper.class,
                              mapper ->
                                  mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                      observedSchemaPO.getSchemaId(),
                                      observedSchemaPO.getCurrentVersion()));
                      Assertions.assertEquals(1, deleted);
                      schemaDeleteLocked.countDown();
                      try {
                        assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });
    try {
      assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      NameIdentifier tableIdentifier =
          NameIdentifier.of(metalakeName, catalogName, schema.name(), "new_table");
      Future<Throwable> createResult =
          executor.submit(
              () -> {
                entityCreateStarted.countDown();
                try {
                  SessionUtils.doMultipleWithCommit(
                      () ->
                          SchemaMetaService.getInstance()
                              .lockSchemaForEntityWrite(
                                  tableIdentifier,
                                  observedSchemaPO.getSchemaId(),
                                  observedSchemaPO.getCatalogId(),
                                  observedSchemaPO.getMetalakeId()));
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(entityCreateStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> createResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(
          NoSuchEntityException.class, createResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testConcurrentSameNameSchemaCreateReportsAlreadyExists() throws Exception {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity first =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "concurrent_schema",
            AUDIT_INFO);
    SchemaEntity second =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            first.name(),
            AUDIT_INFO);

    List<Throwable> results = insertSchemasConcurrently(first, second);
    Assertions.assertEquals(1, results.stream().filter(Objects::isNull).count());
    Throwable failure = results.stream().filter(Objects::nonNull).findFirst().orElseThrow();
    Assertions.assertTrue(
        failure instanceof EntityAlreadyExistsException,
        () -> "Expected EntityAlreadyExistsException, but got " + failure);
  }

  @TestTemplate
  public void testConcurrentDifferentSchemaCreatesBothSucceed() throws Exception {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity first =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "concurrent_schema_1",
            AUDIT_INFO);
    SchemaEntity second =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "concurrent_schema_2",
            AUDIT_INFO);

    List<Throwable> results = insertSchemasConcurrently(first, second);
    Assertions.assertTrue(
        results.stream().allMatch(Objects::isNull),
        () -> "Concurrent schema creates failed: " + results);
  }

  @TestTemplate
  public void testConcurrentSameSchemaDeletesAreIdempotent() throws Exception {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "concurrent_delete_schema",
            AUDIT_INFO);
    backend.insert(schema, false);

    List<Throwable> results =
        deleteSchemasConcurrently(schema.nameIdentifier(), schema.nameIdentifier());
    Assertions.assertEquals(1, results.stream().filter(Objects::isNull).count());
    Throwable loser = results.stream().filter(Objects::nonNull).findFirst().orElseThrow();
    Assertions.assertTrue(
        loser instanceof NoSuchEntityException,
        () -> "Expected an idempotent missing result, but got " + loser);
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema1",
            AUDIT_INFO);
    backend.insert(schema, false);
    backend.insert(schemaCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                schemaCopy.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                e ->
                    createSchemaEntity(
                        schemaCopy.id(), schemaCopy.namespace(), "schema", AUDIT_INFO)));
  }

  @TestTemplate
  public void testUpdateSchemaCommentFromNull() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("schema_null_comment")
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(schemaEntity, false);

    schemaMetaService.updateSchema(
        schemaEntity.nameIdentifier(),
        entity -> {
          SchemaEntity schema = (SchemaEntity) entity;
          return SchemaEntity.builder()
              .withId(schema.id())
              .withName(schema.name())
              .withNamespace(schema.namespace())
              .withComment("schema comment updated")
              .withProperties(schema.properties())
              .withAuditInfo(schema.auditInfo())
              .build();
        });

    SchemaEntity updatedSchema =
        schemaMetaService.getSchemaByIdentifier(schemaEntity.nameIdentifier());
    Assertions.assertEquals("schema comment updated", updatedSchema.comment());
  }

  @TestTemplate
  public void testAlterAndDeleteUseCurrentVersion() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_occ",
            AUDIT_INFO);
    backend.insert(schema, false);
    SchemaPO oldPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));
    SchemaEntity updatedSchema =
        SchemaEntity.builder()
            .withId(schema.id())
            .withName(schema.name())
            .withNamespace(schema.namespace())
            .withAuditInfo(schema.auditInfo())
            .withComment("updated")
            .withProperties(schema.properties())
            .build();
    SchemaPO newPO = POConverters.updateSchemaPOWithVersion(oldPO, updatedSchema);

    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            SchemaMetaMapper.class, mapper -> mapper.updateSchemaMeta(newPO, oldPO));
    int staleUpdate =
        SessionUtils.doWithCommitAndFetchResult(
            SchemaMetaMapper.class, mapper -> mapper.updateSchemaMeta(newPO, oldPO));
    int staleDelete =
        SessionUtils.doWithCommitAndFetchResult(
            SchemaMetaMapper.class,
            mapper ->
                mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                    schema.id(), oldPO.getCurrentVersion()));
    Assertions.assertEquals(1, updated);
    Assertions.assertEquals(0, staleUpdate);
    Assertions.assertEquals(0, staleDelete);
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    int deleted =
        SessionUtils.doWithCommitAndFetchResult(
            SchemaMetaMapper.class,
            mapper ->
                mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                    schema.id(), newPO.getCurrentVersion()));
    Assertions.assertEquals(1, deleted);
  }

  @TestTemplate
  public void testAlterReportsOptimisticLockConflict() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_alter_conflict",
            AUDIT_INFO);
    backend.insert(schema, false);

    assertThrows(
        OptimisticLockException.class,
        () ->
            SchemaMetaService.getInstance()
                .updateSchema(
                    schema.nameIdentifier(),
                    entity -> {
                      SchemaEntity current = (SchemaEntity) entity;
                      SchemaPO currentPO =
                          SessionUtils.getWithoutCommit(
                              SchemaMetaMapper.class,
                              mapper -> mapper.selectSchemaMetaById(current.id()));
                      SchemaEntity competingUpdate =
                          copySchemaWithComment(current, "competing update");
                      SchemaPO competingPO =
                          POConverters.updateSchemaPOWithVersion(currentPO, competingUpdate);
                      SessionUtils.doWithCommitAndFetchResult(
                          SchemaMetaMapper.class,
                          mapper -> mapper.updateSchemaMeta(competingPO, currentPO));
                      return copySchemaWithComment(current, "requested update");
                    }));
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenSchemaIsDeletedConcurrently() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_alter_deleted",
            AUDIT_INFO);
    backend.insert(schema, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            SchemaMetaService.getInstance()
                .updateSchema(
                    schema.nameIdentifier(),
                    entity -> {
                      SchemaEntity current = (SchemaEntity) entity;
                      SchemaPO currentPO =
                          SessionUtils.getWithoutCommit(
                              SchemaMetaMapper.class,
                              mapper -> mapper.selectSchemaMetaById(current.id()));
                      SessionUtils.doWithCommitAndFetchResult(
                          SchemaMetaMapper.class,
                          mapper ->
                              mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                  current.id(), currentPO.getCurrentVersion()));
                      return copySchemaWithComment(current, "requested update");
                    }));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    backend.insert(schema, false);

    String anotherMetalakeName = "another-metalake";
    String anotherCatalogName = "another-catalog";
    createAndInsertMakeLake(anotherMetalakeName);
    createAndInsertCatalog(anotherMetalakeName, anotherCatalogName);
    SchemaEntity anotherSchema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(anotherMetalakeName, anotherCatalogName),
            "another-schema",
            AUDIT_INFO);
    backend.insert(anotherSchema, false);

    List<SchemaEntity> schemas = backend.list(schema.namespace(), Entity.EntityType.SCHEMA, true);
    assertTrue(schemas.contains(schema));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(anotherSchema.nameIdentifier(), Entity.EntityType.SCHEMA));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testDeleteSchemaNonCascadingFailsWhenTopicExists() throws IOException {

    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    TopicMetaService topicMetaService = TopicMetaService.getInstance();

    final String schemaName = "schema_with_topic";
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            schemaName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(schema, false);

    final String topicName = "test_topic_dependency";
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            topicName,
            AUDIT_INFO);
    topicMetaService.insertTopic(topic, false);
    SchemaPO beforeDelete =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));

    Assertions.assertThrows(
        NonEmptyEntityException.class,
        () -> schemaMetaService.deleteSchema(schema.nameIdentifier(), false),
        "Non-cascading delete must fail when dependent topics exist.");

    SchemaPO afterDelete =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));
    Assertions.assertEquals(beforeDelete.getCurrentVersion(), afterDelete.getCurrentVersion());
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(topic.nameIdentifier(), Entity.EntityType.TOPIC));

    topicMetaService.deleteTopic(topic.nameIdentifier());
    schemaMetaService.deleteSchema(schema.nameIdentifier(), false);
  }

  @TestTemplate
  public void testDeleteSchemaNonCascadingFailsWhenViewExists() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_with_view",
            AUDIT_INFO);
    SchemaMetaService.getInstance().insertSchema(schema, false);
    ViewEntity view =
        createViewEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofView(metalakeName, catalogName, schema.name()),
            "dependent_view");
    ViewMetaService.getInstance().insertView(view, false);

    assertThrows(
        NonEmptyEntityException.class,
        () -> SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false));
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(view.nameIdentifier(), Entity.EntityType.VIEW));
  }

  @TestTemplate
  public void testDeleteSchemaNonCascadingFailsWhenFunctionExists() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_with_function",
            AUDIT_INFO);
    SchemaMetaService.getInstance().insertSchema(schema, false);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(metalakeName, catalogName, schema.name()),
            "dependent_function",
            AUDIT_INFO);
    FunctionMetaService.getInstance().insertFunction(function, false);

    assertThrows(
        NonEmptyEntityException.class,
        () -> SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false));
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(function.nameIdentifier(), Entity.EntityType.FUNCTION));
  }

  @TestTemplate
  public void testInsertHierarchicalSchemaCreatesAncestorsAndLeaf() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String logicalLeaf = "ns_a:ns_b:leaf";
    SchemaEntity hierarchical =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(logicalLeaf)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("nested")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(hierarchical, false);

    List<SchemaEntity> schemas =
        schemaMetaService.listSchemasByNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName));
    Set<String> logicalNames = schemas.stream().map(SchemaEntity::name).collect(Collectors.toSet());

    Assertions.assertTrue(logicalNames.contains("ns_a"));
    Assertions.assertTrue(logicalNames.contains("ns_a:ns_b"));
    Assertions.assertTrue(logicalNames.contains(logicalLeaf));

    SchemaEntity loaded =
        schemaMetaService.getSchemaByIdentifier(
            NameIdentifier.of(metalakeName, catalogName, logicalLeaf));
    Assertions.assertEquals(logicalLeaf, loaded.name());
    Assertions.assertEquals("nested", loaded.comment());
  }

  @TestTemplate
  public void testDeleteHierarchicalSchemaCascadeRemovesDescendantsAndChildren()
      throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    TopicMetaService topicMetaService = TopicMetaService.getInstance();

    // Insert a leaf schema A:B:C; this auto-creates ancestor rows A and A:B.
    String leafName = "anc_a:anc_b:leaf_c";
    SchemaEntity leaf =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            leafName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(leaf, false);

    // Topic under the leaf, to verify child entities are also cascade-deleted.
    TopicEntity leafTopic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, leafName),
            "leaf_topic",
            AUDIT_INFO);
    topicMetaService.insertTopic(leafTopic, false);

    // Topic under the middle ancestor A:B.
    String middleName = "anc_a:anc_b";
    TopicEntity middleTopic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, middleName),
            "middle_topic",
            AUDIT_INFO);
    topicMetaService.insertTopic(middleTopic, false);

    // A sibling schema outside the deleted subtree to confirm it survives.
    String siblingName = "anc_a:sibling_d";
    SchemaEntity sibling =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            siblingName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(sibling, false);

    TopicEntity siblingTopic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, siblingName),
            "sibling_topic",
            AUDIT_INFO);
    topicMetaService.insertTopic(siblingTopic, false);

    // Cascade-delete the middle ancestor; both A:B and A:B:C (plus their topics) must go.
    schemaMetaService.deleteSchema(NameIdentifier.of(metalakeName, catalogName, middleName), true);

    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, middleName), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, leafName), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(backend.exists(leafTopic.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(backend.exists(middleTopic.nameIdentifier(), Entity.EntityType.TOPIC));

    // Sibling subtree must still exist.
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, siblingName), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(backend.exists(siblingTopic.nameIdentifier(), Entity.EntityType.TOPIC));

    // The shared top-level ancestor A is outside the deleted subtree and must remain too.
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, "anc_a"), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testOverlappingHierarchicalSchemaDeletesDoNotDeadlock() throws Exception {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String firstLeaf = "overlap_a:overlap_b:leaf_c";
    String secondLeaf = "overlap_a:overlap_b:leaf_d";
    schemaMetaService.insertSchema(
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            firstLeaf,
            AUDIT_INFO),
        false);
    schemaMetaService.insertSchema(
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            secondLeaf,
            AUDIT_INFO),
        false);

    NameIdentifier ancestor = NameIdentifier.of(metalakeName, catalogName, "overlap_a:overlap_b");
    NameIdentifier descendant = NameIdentifier.of(metalakeName, catalogName, firstLeaf);
    List<Throwable> results = deleteSchemasConcurrently(ancestor, descendant);
    Assertions.assertTrue(
        results.stream()
            .allMatch(result -> result == null || result instanceof NoSuchEntityException),
        () -> "Overlapping cascade deletes produced an unexpected failure: " + results);
    Assertions.assertFalse(backend.exists(ancestor, Entity.EntityType.SCHEMA));
    Assertions.assertFalse(backend.exists(descendant, Entity.EntityType.SCHEMA));
    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, secondLeaf), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testDeleteSchemaCascadeRemovesTagRelations() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_with_tags",
            AUDIT_INFO);
    SchemaMetaService.getInstance().insertSchema(schema, false);

    Namespace objectNamespace = Namespace.of(metalakeName, catalogName, schema.name());
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

    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));

    assertTrue(SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true));

    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));
  }

  @TestTemplate
  public void testDeleteHierarchicalSchemaCascadeEscapesLikeMetacharacters() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();

    // Deleted subtree under ancestor "pa_b". The '_' is a LIKE wildcard, so without escaping the
    // cascade prefix "pa_b<sep>%" would also match the unrelated "paxb<sep>..." subtree below.
    String targetLeaf = "pa_b:leaf";
    schemaMetaService.insertSchema(
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            targetLeaf,
            AUDIT_INFO),
        false);

    // Decoy subtree that the unescaped '_' wildcard would falsely match ('x' in place of '_').
    String decoyLeaf = "paxb:leaf";
    schemaMetaService.insertSchema(
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            decoyLeaf,
            AUDIT_INFO),
        false);

    // Cascade-delete the literal "pa_b" ancestor.
    schemaMetaService.deleteSchema(NameIdentifier.of(metalakeName, catalogName, "pa_b"), true);

    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, "pa_b"), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, targetLeaf), Entity.EntityType.SCHEMA));

    // The decoy subtree must survive: literal-prefix matching only escapes the deleted subtree.
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, "paxb"), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, decoyLeaf), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testInsertHierarchicalSecondLeafReusesAncestorsWithoutUpsert() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String leaf1 = "ns_a:ns_b:leaf1";
    String leaf2 = "ns_a:ns_b:leaf2";
    String ancestorA = "ns_a";
    String ancestorAB = "ns_a:ns_b";

    SchemaEntity first =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf1)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("first")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(first, false);

    SchemaPO ancestorAPOBefore =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaMetaById(
                    schemaMetaService
                        .getSchemaByIdentifier(
                            NameIdentifier.of(metalakeName, catalogName, ancestorA))
                        .id()));
    SchemaPO ancestorABPOBefore =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaMetaById(
                    schemaMetaService
                        .getSchemaByIdentifier(
                            NameIdentifier.of(metalakeName, catalogName, ancestorAB))
                        .id()));

    SchemaEntity second =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf2)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("second")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(second, false);

    SchemaPO ancestorAPOAfter =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaMetaById(
                    schemaMetaService
                        .getSchemaByIdentifier(
                            NameIdentifier.of(metalakeName, catalogName, ancestorA))
                        .id()));
    SchemaPO ancestorABPOAfter =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper ->
                mapper.selectSchemaMetaById(
                    schemaMetaService
                        .getSchemaByIdentifier(
                            NameIdentifier.of(metalakeName, catalogName, ancestorAB))
                        .id()));
    Assertions.assertEquals(ancestorAPOBefore.getSchemaId(), ancestorAPOAfter.getSchemaId());
    Assertions.assertEquals(ancestorABPOBefore.getSchemaId(), ancestorABPOAfter.getSchemaId());
    Assertions.assertEquals(
        ancestorAPOBefore.getCurrentVersion(), ancestorAPOAfter.getCurrentVersion());
    Assertions.assertEquals(
        ancestorABPOBefore.getCurrentVersion(), ancestorABPOAfter.getCurrentVersion());
  }

  @TestTemplate
  public void testConcurrentCatalogCascadeAndSchemaCreateLeavesNoOrphan() throws Exception {
    createAndInsertMakeLake(metalakeName);
    CatalogEntity catalog = createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_racing_catalog_drop",
            AUDIT_INFO);

    CountDownLatch catalogLocked = new CountDownLatch(1);
    CountDownLatch allowSchemaSnapshot = new CountDownLatch(1);
    CountDownLatch createStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CatalogMetaService service = Mockito.spy(CatalogMetaService.getInstance());
    try {
      // Pause the cascade right after it has soft-deleted the catalog row, which is the moment it
      // holds that row, and before it reads the schemas to delete.
      Mockito.doAnswer(
              invocation -> {
                catalogLocked.countDown();
                assertTrue(allowSchemaSnapshot.await(30, TimeUnit.SECONDS));
                return invocation.callRealMethod();
              })
          .when(service)
          .listSchemaPOsForCascade(catalog.id());

      Future<Throwable> deleteResult =
          executor.submit(
              () -> {
                try {
                  service.deleteCatalog(catalog.nameIdentifier(), true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(catalogLocked.await(30, TimeUnit.SECONDS));

      Future<Throwable> createResult =
          executor.submit(
              () -> {
                createStarted.countDown();
                try {
                  SchemaMetaService.getInstance().insertSchema(schema, false);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(createStarted.await(30, TimeUnit.SECONDS));
      // The create must not slip past the drop: it waits on the catalog row instead.
      assertThrows(TimeoutException.class, () -> createResult.get(500, TimeUnit.MILLISECONDS));

      allowSchemaSnapshot.countDown();
      Throwable createFailure = createResult.get(30, TimeUnit.SECONDS);
      Throwable deleteFailure = deleteResult.get(30, TimeUnit.SECONDS);
      Assertions.assertInstanceOf(NoSuchEntityException.class, createFailure);
      Assertions.assertNull(deleteFailure, () -> "Catalog cascade failed: " + deleteFailure);
    } finally {
      allowSchemaSnapshot.countDown();
      executor.shutdownNow();
    }

    assertFalse(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    assertFalse(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  private List<Throwable> insertSchemasConcurrently(SchemaEntity first, SchemaEntity second)
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
                  SchemaMetaService.getInstance().insertSchema(first, false);
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
                  SchemaMetaService.getInstance().insertSchema(second, false);
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

  private List<Throwable> deleteSchemasConcurrently(NameIdentifier first, NameIdentifier second)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<Throwable> firstResult =
          executor.submit(() -> deleteSchemaAfterStart(first, ready, start));
      Future<Throwable> secondResult =
          executor.submit(() -> deleteSchemaAfterStart(second, ready, start));
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      start.countDown();
      return Arrays.asList(
          firstResult.get(30, TimeUnit.SECONDS), secondResult.get(30, TimeUnit.SECONDS));
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private Throwable deleteSchemaAfterStart(
      NameIdentifier identifier, CountDownLatch ready, CountDownLatch start) {
    ready.countDown();
    try {
      start.await();
      SchemaMetaService.getInstance().deleteSchema(identifier, true);
      return null;
    } catch (Throwable throwable) {
      return throwable;
    }
  }

  private SchemaEntity copySchemaWithComment(SchemaEntity schema, String comment) {
    return SchemaEntity.builder()
        .withId(schema.id())
        .withName(schema.name())
        .withNamespace(schema.namespace())
        .withComment(comment)
        .withProperties(schema.properties())
        .withAuditInfo(schema.auditInfo())
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
