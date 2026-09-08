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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSemanticModelMetaService extends TestJDBCBackend {

  private final String metalakeName = GravitinoITUtils.genRandomName("tst_semantic_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_semantic_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_semantic_schema");

  private Namespace namespace;
  private long schemaId;

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema = createAndInsertSchema(metalakeName, catalogName, schemaName);
    schemaId = schema.id();
    namespace = NamespaceUtil.ofSemanticModel(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertLoadListOverwriteAndIdLookup() throws IOException {
    String semanticModelName = GravitinoITUtils.genRandomName("sales_model");
    SemanticModelEntity original =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            semanticModelName,
            "orders",
            "initial comment",
            "initial");

    SemanticModelMetaService.getInstance().insertSemanticModel(original, false);

    assertEquals(
        original.id(),
        SemanticModelMetaService.getInstance()
            .getSemanticModelIdBySchemaIdAndName(schemaId, semanticModelName));
    assertEquals(
        original,
        SemanticModelMetaService.getInstance()
            .getSemanticModelByIdentifier(original.nameIdentifier()));
    List<SemanticModelEntity> listed =
        SemanticModelMetaService.getInstance().listSemanticModelsByNamespace(namespace);
    assertEquals(1, listed.size());
    assertEquals(original, listed.get(0));

    SemanticModelEntity duplicate =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            semanticModelName,
            "duplicate_orders",
            "duplicate comment",
            "duplicate");
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> SemanticModelMetaService.getInstance().insertSemanticModel(duplicate, false));

    SemanticModelEntity overwritten =
        semanticModelEntity(
            original.id(),
            semanticModelName,
            "orders_overwritten",
            "overwritten comment",
            "overwritten");
    SemanticModelMetaService.getInstance().insertSemanticModel(overwritten, true);
    assertEquals(
        overwritten,
        SemanticModelMetaService.getInstance()
            .getSemanticModelByIdentifier(overwritten.nameIdentifier()));
    assertEquals(2, listSemanticModelVersions(original.id()).size());

    assertThrows(
        NoSuchEntityException.class,
        () ->
            SemanticModelMetaService.getInstance()
                .getSemanticModelIdBySchemaIdAndName(schemaId, "missing_model"));
  }

  @TestTemplate
  public void testUpdateCreatesFullSnapshotAndRenameChangeLog() throws IOException {
    String oldName = GravitinoITUtils.genRandomName("sales_model_old");
    String newName = GravitinoITUtils.genRandomName("sales_model_new");
    SemanticModelEntity original =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(), oldName, "orders_v1", "v1 comment", "v1");
    SemanticModelMetaService.getInstance().insertSemanticModel(original, false);
    long lastChangeId = maxEntityChangeId();

    SemanticModelEntity expected =
        semanticModelEntity(original.id(), newName, "orders_v2", "v2 comment", "v2");
    SemanticModelEntity updated =
        SemanticModelMetaService.getInstance()
            .updateSemanticModel(original.nameIdentifier(), ignored -> expected);

    assertEquals(expected, updated);
    assertThrows(
        NoSuchEntityException.class,
        () ->
            SemanticModelMetaService.getInstance()
                .getSemanticModelByIdentifier(original.nameIdentifier()));
    assertEquals(
        expected,
        SemanticModelMetaService.getInstance()
            .getSemanticModelByIdentifier(expected.nameIdentifier()));
    assertEquals(
        original.id(),
        SemanticModelMetaService.getInstance()
            .getSemanticModelIdBySchemaIdAndName(schemaId, newName));

    Map<Integer, VersionState> versions = listSemanticModelVersions(original.id());
    assertEquals(2, versions.size());
    assertEquals(oldName, versions.get(1).name);
    assertEquals("v1 comment", versions.get(1).comment);
    assertTrue(versions.get(1).definition.contains("orders_v1"));
    assertEquals(newName, versions.get(2).name);
    assertEquals("v2 comment", versions.get(2).comment);
    assertTrue(versions.get(2).definition.contains("orders_v2"));
    assertEquals(0L, versions.get(1).deletedAt);
    assertEquals(0L, versions.get(2).deletedAt);

    long[] identityVersions = semanticModelIdentityVersions(original.id());
    assertEquals(2L, identityVersions[0]);
    assertEquals(2L, identityVersions[1]);
    assertEntityChange(lastChangeId, oldName, OperateType.ALTER);
  }

  @TestTemplate
  public void testInsertWaitsForConcurrentSchemaCascadeDelete() throws Exception {
    String semanticModelName = GravitinoITUtils.genRandomName("concurrent_create_model");
    SemanticModelEntity semanticModel =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(), semanticModelName, "orders", "comment", "create");
    SchemaPO observedSchemaPO = getObservedSchemaPO();
    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch insertStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        startSchemaCascadeDelete(observedSchemaPO, schemaDeleteLocked, allowDeleteCommit, executor);

    try {
      assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> insertResult =
          executor.submit(
              () -> {
                insertStarted.countDown();
                try {
                  SemanticModelMetaService.getInstance().insertSemanticModel(semanticModel, false);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(insertStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> insertResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      assertInstanceOf(NoSuchEntityException.class, insertResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    assertEquals(0, countSemanticModelIdentities(semanticModel.id()));
    assertTrue(listSemanticModelVersions(semanticModel.id()).isEmpty());
  }

  @TestTemplate
  public void testUpdateWaitsForConcurrentSchemaCascadeDelete() throws Exception {
    String semanticModelName = GravitinoITUtils.genRandomName("concurrent_update_model");
    SemanticModelEntity original =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            semanticModelName,
            "orders_v1",
            "v1 comment",
            "v1");
    SemanticModelMetaService.getInstance().insertSemanticModel(original, false);
    SemanticModelEntity updated =
        semanticModelEntity(original.id(), semanticModelName, "orders_v2", "v2 comment", "v2");
    SchemaPO observedSchemaPO = getObservedSchemaPO();
    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch updateStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        startSchemaCascadeDelete(observedSchemaPO, schemaDeleteLocked, allowDeleteCommit, executor);

    try {
      assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> updateResult =
          executor.submit(
              () -> {
                updateStarted.countDown();
                try {
                  SemanticModelMetaService.getInstance()
                      .updateSemanticModel(original.nameIdentifier(), ignored -> updated);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      assertTrue(updateStarted.await(30, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> updateResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      assertInstanceOf(NoSuchEntityException.class, updateResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    assertThrows(
        NoSuchEntityException.class,
        () ->
            SemanticModelMetaService.getInstance()
                .getSemanticModelByIdentifier(original.nameIdentifier()));
    Map<Integer, VersionState> versions = listSemanticModelVersions(original.id());
    assertEquals(1, versions.size());
    assertTrue(versions.get(1).deletedAt > 0L);
  }

  @TestTemplate
  public void testOptimisticUpdateRollsBackInsertedSnapshot() throws IOException {
    String semanticModelName = GravitinoITUtils.genRandomName("optimistic_model");
    SemanticModelEntity original =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            semanticModelName,
            "orders_v1",
            "v1 comment",
            "v1");
    SemanticModelMetaService.getInstance().insertSemanticModel(original, false);
    SemanticModelEntity expected =
        semanticModelEntity(original.id(), semanticModelName, "orders_v2", "v2 comment", "v2");

    assertThrows(
        OptimisticLockException.class,
        () ->
            SemanticModelMetaService.getInstance()
                .updateSemanticModel(
                    original.nameIdentifier(),
                    ignored -> {
                      SemanticModelMetaService.getInstance()
                          .deleteSemanticModel(original.nameIdentifier());
                      return expected;
                    }));

    Map<Integer, VersionState> versions = listSemanticModelVersions(original.id());
    assertEquals(1, versions.size());
    assertTrue(versions.containsKey(1));
    assertTrue(versions.get(1).deletedAt > 0L);
  }

  @TestTemplate
  public void testOptimisticDropRejectsStaleInternalVersion() throws IOException {
    String semanticModelName = GravitinoITUtils.genRandomName("optimistic_drop_model");
    SemanticModelEntity original =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            semanticModelName,
            "orders_v1",
            "v1 comment",
            "v1");
    SemanticModelMetaService.getInstance().insertSemanticModel(original, false);
    SemanticModelEntity updated =
        semanticModelEntity(original.id(), semanticModelName, "orders_v2", "v2 comment", "v2");
    SemanticModelMetaService.getInstance()
        .updateSemanticModel(original.nameIdentifier(), ignored -> updated);

    assertThrows(
        OptimisticLockException.class,
        () ->
            SemanticModelMetaService.getInstance()
                .deleteSemanticModel(
                    original.id(), 1, metalakeName, original.nameIdentifier().toString()));

    assertEquals(
        updated,
        SemanticModelMetaService.getInstance()
            .getSemanticModelByIdentifier(updated.nameIdentifier()));
    Map<Integer, VersionState> versions = listSemanticModelVersions(original.id());
    assertEquals(2, versions.size());
    assertEquals(0L, versions.get(1).deletedAt);
    assertEquals(0L, versions.get(2).deletedAt);
  }

  @TestTemplate
  public void testSoftDropVersionRetentionAndLegacyGarbageCollection() throws IOException {
    String semanticModelName = GravitinoITUtils.genRandomName("retention_model");
    SemanticModelEntity current =
        semanticModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            semanticModelName,
            "orders_v1",
            "v1 comment",
            "v1");
    SemanticModelMetaService.getInstance().insertSemanticModel(current, false);
    for (int version = 2; version <= 3; version++) {
      SemanticModelEntity next =
          semanticModelEntity(
              current.id(),
              semanticModelName,
              "orders_v" + version,
              "v" + version + " comment",
              "v" + version);
      current =
          SemanticModelMetaService.getInstance()
              .updateSemanticModel(current.nameIdentifier(), ignored -> next);
    }

    assertEquals(
        2,
        SemanticModelMetaService.getInstance()
            .deleteSemanticModelVersionsByRetentionCount(1L, 100));
    Map<Integer, VersionState> retainedVersions = listSemanticModelVersions(current.id());
    assertTrue(retainedVersions.get(1).deletedAt > 0L);
    assertTrue(retainedVersions.get(2).deletedAt > 0L);
    assertEquals(0L, retainedVersions.get(3).deletedAt);

    SemanticModelEntity finalCurrent = current;
    long lastChangeId = maxEntityChangeId();
    assertTrue(
        SemanticModelMetaService.getInstance().deleteSemanticModel(finalCurrent.nameIdentifier()));
    assertThrows(
        NoSuchEntityException.class,
        () ->
            SemanticModelMetaService.getInstance()
                .getSemanticModelByIdentifier(finalCurrent.nameIdentifier()));
    assertTrue(listSemanticModelVersions(finalCurrent.id()).get(3).deletedAt > 0L);
    assertEntityChange(lastChangeId, semanticModelName, OperateType.DROP);

    int deleted =
        SemanticModelMetaService.getInstance()
            .deleteSemanticModelMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 100);
    assertEquals(4, deleted);
    assertEquals(0, listSemanticModelVersions(finalCurrent.id()).size());
    assertEquals(0, countSemanticModelIdentities(finalCurrent.id()));
  }

  private SemanticModelEntity semanticModelEntity(
      Long id, String name, String datasetName, String comment, String extensionData) {
    return SemanticModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withDefinition(
            SemanticModelDefinition.builder()
                .withAIContext(AIContext.of("AI context for " + datasetName))
                .withDatasets(
                    new Dataset[] {
                      Dataset.builder()
                          .withName(datasetName)
                          .withSource(NameIdentifier.of("sales", "mart", datasetName))
                          .build()
                    })
                .withCustomExtensions(
                    new CustomExtension[] {
                      CustomExtension.builder()
                          .withVendorName("test")
                          .withData(extensionData)
                          .build()
                    })
                .build())
        .withProperties(ImmutableMap.of("dataset", datasetName))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private SchemaPO getObservedSchemaPO() {
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schemaId));
  }

  private Future<Throwable> startSchemaCascadeDelete(
      SchemaPO observedSchemaPO,
      CountDownLatch schemaDeleteLocked,
      CountDownLatch allowDeleteCommit,
      ExecutorService executor) {
    return executor.submit(
        () -> {
          try {
            // Reproduce the schema-row lock and Semantic Model cleanup portion of
            // SchemaMetaService.deleteSchema(..., true) in one controllable transaction. The
            // public cascade has no test hook between those operations.
            SessionUtils.doMultipleWithCommit(
                () -> {
                  int deleted =
                      SessionUtils.getWithoutCommit(
                          SchemaMetaMapper.class,
                          mapper ->
                              mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                  observedSchemaPO.getSchemaId(),
                                  observedSchemaPO.getCurrentVersion()));
                  assertEquals(1, deleted);
                  schemaDeleteLocked.countDown();
                  try {
                    assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                },
                () ->
                    SessionUtils.doWithoutCommit(
                        SemanticModelMetaMapper.class,
                        mapper ->
                            mapper.softDeleteSemanticModelMetasBySchemaIds(
                                List.of(observedSchemaPO.getSchemaId()))),
                () ->
                    SessionUtils.doWithoutCommit(
                        SemanticModelVersionInfoMapper.class,
                        mapper ->
                            mapper.softDeleteSemanticModelVersionsBySchemaIds(
                                List.of(observedSchemaPO.getSchemaId()))));
            return null;
          } catch (Throwable throwable) {
            return throwable;
          }
        });
  }

  private Map<Integer, VersionState> listSemanticModelVersions(Long semanticModelId) {
    Map<Integer, VersionState> versions = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT version, semantic_model_name, semantic_model_comment,"
                        + " semantic_model_definition, deleted_at"
                        + " FROM semantic_model_version_info WHERE semantic_model_id = %d",
                    semanticModelId))) {
      while (resultSet.next()) {
        versions.put(
            resultSet.getInt("version"),
            new VersionState(
                resultSet.getString("semantic_model_name"),
                resultSet.getString("semantic_model_comment"),
                resultSet.getString("semantic_model_definition"),
                resultSet.getLong("deleted_at")));
      }
      return versions;
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private long[] semanticModelIdentityVersions(Long semanticModelId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT current_version, last_version FROM semantic_model_meta"
                        + " WHERE semantic_model_id = %d",
                    semanticModelId))) {
      assertTrue(resultSet.next());
      return new long[] {resultSet.getLong("current_version"), resultSet.getLong("last_version")};
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private int countSemanticModelIdentities(Long semanticModelId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM semantic_model_meta WHERE semantic_model_id = %d",
                    semanticModelId))) {
      assertTrue(resultSet.next());
      return resultSet.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private long maxEntityChangeId() {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
  }

  private void assertEntityChange(
      long lastConsumedId, String semanticModelName, OperateType operateType) {
    List<EntityChangeRecord> changes =
        SessionUtils.doWithCommitAndFetchResult(
            EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(lastConsumedId, 100));
    String fullName =
        NameIdentifierUtil.ofSemanticModel(metalakeName, catalogName, schemaName, semanticModelName)
            .toString();
    assertTrue(
        changes.stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(Entity.EntityType.SEMANTIC_MODEL.name())
                        && record.getFullName().equals(fullName)
                        && record.getOperateType() == operateType));
  }

  private static class VersionState {
    private final String name;
    private final String comment;
    private final String definition;
    private final long deletedAt;

    private VersionState(String name, String comment, String definition, long deletedAt) {
      this.name = name;
      this.comment = comment;
      this.definition = definition;
      this.deletedAt = deletedAt;
    }
  }
}
