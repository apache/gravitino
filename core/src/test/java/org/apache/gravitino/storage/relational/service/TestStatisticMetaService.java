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

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TableStatisticEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestStatisticMetaService extends TestJDBCBackend {
  private final StatisticMetaService statisticMetaService = StatisticMetaService.getInstance();

  @TestTemplate
  public void testTableStatisticWriteWaitsForConcurrentSchemaDelete() throws Exception {
    String metalakeName = "metalake_for_statistic_schema_fence";
    String catalogName = "catalog_for_statistic_schema_fence";
    String schemaName = "schema_for_statistic_schema_fence";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createParentEntities(metalakeName, catalogName, schemaName, auditInfo);
    SchemaPO observedSchemaPO = selectSchemaPO(metalakeName, catalogName, schemaName);

    assertTableStatisticWriteWaitsForConcurrentSchemaDelete(
        metalakeName,
        catalogName,
        schemaName,
        auditInfo,
        () -> {
          int deleted =
              SessionUtils.getWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper ->
                      mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                          observedSchemaPO.getSchemaId(), observedSchemaPO.getCurrentVersion()));
          Assertions.assertEquals(1, deleted);
        });
  }

  @TestTemplate
  public void testNestedSchemaTableStatisticWriteWaitsForAncestorCascadeDelete() throws Exception {
    String metalakeName = "metalake_for_nested_statistic_fence";
    String catalogName = "catalog_for_nested_statistic_fence";
    String ancestorName = "fence_anc_a";
    String nestedName = ancestorName + ":fence_anc_b";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    // Inserting the nested leaf also creates the ancestor row.
    SchemaMetaService.getInstance()
        .insertSchema(
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of(metalakeName, catalogName),
                nestedName,
                auditInfo),
            false);
    SchemaPO observedAncestorPO = selectSchemaPO(metalakeName, catalogName, ancestorName);
    SchemaPO observedNestedPO = selectSchemaPO(metalakeName, catalogName, nestedName);

    // The statistic only locks its direct parent, the nested schema. A cascade delete of the
    // ancestor soft-deletes every descendant schema row in the same transaction, so the write
    // must still wait for it and then fail once the nested schema is gone.
    assertTableStatisticWriteWaitsForConcurrentSchemaDelete(
        metalakeName,
        catalogName,
        nestedName,
        auditInfo,
        () -> {
          int deletedAncestor =
              SessionUtils.getWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper ->
                      mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                          observedAncestorPO.getSchemaId(),
                          observedAncestorPO.getCurrentVersion()));
          Assertions.assertEquals(1, deletedAncestor);
          int deletedDescendants =
              SessionUtils.getWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasWithVersion(List.of(observedNestedPO)));
          Assertions.assertEquals(1, deletedDescendants);
        });
  }

  private SchemaPO selectSchemaPO(String metalakeName, String catalogName, String schemaName) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(metalakeName, catalogName, schemaName), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schemaId));
  }

  /**
   * Runs {@code schemaDeleteStep} in an uncommitted transaction, then verifies that a statistic
   * upsert on a table below {@code schemaName} blocks until that transaction commits and fails with
   * {@link NoSuchEntityException} afterwards.
   */
  private void assertTableStatisticWriteWaitsForConcurrentSchemaDelete(
      String metalakeName,
      String catalogName,
      String schemaName,
      AuditInfo auditInfo,
      Runnable schemaDeleteStep)
      throws Exception {
    Long metalakeId =
        EntityIdService.getEntityId(NameIdentifier.of(metalakeName), Entity.EntityType.METALAKE);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName, schemaName),
            "table",
            auditInfo);
    backend.insert(table, false);
    StatisticEntity statistic =
        TableStatisticEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("test")
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName, table.name()))
            .withValue(StatisticValues.longValue(100L))
            .withAuditInfo(auditInfo)
            .build();

    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch statisticWriteStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      schemaDeleteStep.run();
                      schemaDeleteLocked.countDown();
                      try {
                        Assertions.assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
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
      Assertions.assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> statisticWriteResult =
          executor.submit(
              () -> {
                statisticWriteStarted.countDown();
                try {
                  backend.batchPut(List.of(statistic), true);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      Assertions.assertTrue(statisticWriteStarted.await(30, TimeUnit.SECONDS));
      Assertions.assertThrows(
          TimeoutException.class, () -> statisticWriteResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(
          NoSuchEntityException.class, statisticWriteResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertEquals(0, countActiveStats(metalakeId));
  }

  @TestTemplate
  public void testStatisticsLifeCycle() throws Exception {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    List<StatisticEntity> statisticEntities = Lists.newArrayList();
    StatisticEntity statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);

    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    List<StatisticEntity> listEntities =
        statisticMetaService.listStatisticsByEntity(
            table.nameIdentifier(), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, listEntities.size());
    Assertions.assertEquals("test", listEntities.get(0).name());
    Assertions.assertEquals(100L, listEntities.get(0).value().value());

    // Update the duplicated key
    statisticEntity = createStatisticEntity(auditInfo, 200L);
    statisticEntities.clear();
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    listEntities =
        statisticMetaService.listStatisticsByEntity(
            table.nameIdentifier(), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, listEntities.size());
    Assertions.assertEquals("test", listEntities.get(0).name());
    Assertions.assertEquals(200L, listEntities.get(0).value().value());

    List<String> names = Lists.newArrayList(statisticEntity.name());
    statisticMetaService.batchDeleteStatisticPOs(table.nameIdentifier(), table.type(), names);
    listEntities =
        statisticMetaService.listStatisticsByEntity(table.nameIdentifier(), table.type());
    Assertions.assertEquals(0, listEntities.size());
  }

  @TestTemplate
  public void testDeleteMetadataObject() throws Exception {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    // insert stats
    List<StatisticEntity> statisticEntities = Lists.newArrayList();
    StatisticEntity statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete model
    ModelMetaService.getInstance().deleteModel(model.nameIdentifier());

    // assert stats
    Assertions.assertEquals(3, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete table
    TableMetaService.getInstance().deleteTable(table.nameIdentifier());
    // assert stats
    Assertions.assertEquals(2, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete topic
    TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());
    // assert stats
    Assertions.assertEquals(1, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete fileset
    FilesetMetaService.getInstance().deleteFileset(fileset.nameIdentifier());
    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete schema
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false);
    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete catalog
    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false);
    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(4, countAllStats(metalake.id()));

    // Test to delete catalog with cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);
    // insert stats
    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(8, countAllStats(metalake.id()));

    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true);

    // assert stats
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(8, countAllStats(metalake.id()));

    // Test to delete schema with cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "fileset",
            auditInfo);
    backend.insert(fileset, false);
    table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);
    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "topic",
            auditInfo);
    backend.insert(topic, false);
    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "model",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    // insert stats
    statisticEntities = Lists.newArrayList();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo, 100L);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOsOnDuplicateKeyUpdate(
        statisticEntities, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats count
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(12, countAllStats(metalake.id()));

    // delete object
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true);

    // assert stats count
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(12, countAllStats(metalake.id()));
  }

  private StatisticEntity createStatisticEntity(AuditInfo auditInfo, long value) {
    return TableStatisticEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName("test")
        .withValue(StatisticValues.longValue(value))
        .withAuditInfo(auditInfo)
        .build();
  }

  private Integer countAllStats(Long metalakeId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM statistic_meta WHERE metalake_id = %d", metalakeId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  private Integer countActiveStats(Long metalakeId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM statistic_meta WHERE metalake_id = %d AND deleted_at = 0",
                    metalakeId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }
}
