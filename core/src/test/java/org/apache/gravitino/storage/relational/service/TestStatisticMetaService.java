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
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStatisticMetaService extends TestJDBCBackend {
  StatisticMetaService statisticMetaService = StatisticMetaService.getInstance();

  @Test
  public void testStatisticsLifeCycle() throws Exception {
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    List<StatisticEntity> statisticEntities = Lists.newArrayList();
    StatisticEntity statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);

    NameIdentifier nameIdentifier = NameIdentifierUtil.ofMetalake(metalakeName);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, nameIdentifier, Entity.EntityType.METALAKE);

    List<StatisticEntity> listEntities =
        statisticMetaService.listStatisticsByObject(nameIdentifier, Entity.EntityType.METALAKE);
    Assertions.assertEquals(1, listEntities.size());
    Assertions.assertEquals("test", listEntities.get(0).name());
    Assertions.assertEquals(100L, listEntities.get(0).value().value());

    List<String> names = Lists.newArrayList(statisticEntity.name());
    statisticMetaService.batchDeleteStatisticPOs(names);
    listEntities =
        statisticMetaService.listStatisticsByObject(nameIdentifier, Entity.EntityType.METALAKE);
    Assertions.assertEquals(0, listEntities.size());
  }

  @Test
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
    StatisticEntity statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, model.nameIdentifier(), Entity.EntityType.MODEL);

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
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, model.nameIdentifier(), Entity.EntityType.MODEL);

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
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, table.nameIdentifier(), Entity.EntityType.TABLE);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, topic.nameIdentifier(), Entity.EntityType.TOPIC);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, fileset.nameIdentifier(), Entity.EntityType.FILESET);

    statisticEntities.clear();
    statisticEntity = createStatisticEntity(auditInfo);
    statisticEntities.add(statisticEntity);
    statisticMetaService.batchInsertStatisticPOs(
        statisticEntities, metalakeName, model.nameIdentifier(), Entity.EntityType.MODEL);

    // assert stats count
    Assertions.assertEquals(4, countActiveStats(metalake.id()));
    Assertions.assertEquals(12, countAllStats(metalake.id()));

    // delete object
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true);

    // assert stats count
    Assertions.assertEquals(0, countActiveStats(metalake.id()));
    Assertions.assertEquals(12, countAllStats(metalake.id()));
  }

  private static StatisticEntity createStatisticEntity(AuditInfo auditInfo) {
    return StatisticEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName("test")
        .withValue(StatisticValues.longValue(100L))
        .withAuditInfo(auditInfo)
        .build();
  }
}
