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

package org.apache.gravitino.storage.relational.utils;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TableStatisticEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionAliasRelPO;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.po.PolicyVersionPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPOConverters {
  private static final LocalDateTime FIX_DATE_TIME = LocalDateTime.of(2024, 2, 6, 0, 0, 0);

  private static final Instant FIX_INSTANT = FIX_DATE_TIME.toInstant(ZoneOffset.UTC);

  @Test
  public void testFromMetalakePO() throws JsonProcessingException {
    MetalakePO metalakePO = createMetalakePO(1L, "test", "this is test");

    BaseMetalake expectedMetalake = createMetalake(1L, "test", "this is test");

    BaseMetalake convertedMetalake = POConverters.fromMetalakePO(metalakePO);

    // Assert
    assertEquals(expectedMetalake.id(), convertedMetalake.id());
    assertEquals(expectedMetalake.name(), convertedMetalake.name());
    assertEquals(expectedMetalake.comment(), convertedMetalake.comment());
    assertEquals(
        expectedMetalake.properties().get("key"), convertedMetalake.properties().get("key"));
    assertEquals(expectedMetalake.auditInfo().creator(), convertedMetalake.auditInfo().creator());
    assertEquals(expectedMetalake.getVersion(), convertedMetalake.getVersion());
  }

  @Test
  public void testFromCatalogPO() throws JsonProcessingException {
    CatalogPO catalogPO = createCatalogPO(1L, "test", 1L, "this is test");

    CatalogEntity expectedCatalog =
        createCatalog(1L, "test", NamespaceUtil.ofCatalog("test_metalake"), "this is test");

    CatalogEntity convertedCatalog =
        POConverters.fromCatalogPO(catalogPO, NamespaceUtil.ofCatalog("test_metalake"));

    // Assert
    assertEquals(expectedCatalog.id(), convertedCatalog.id());
    assertEquals(expectedCatalog.name(), convertedCatalog.name());
    assertEquals(expectedCatalog.getComment(), convertedCatalog.getComment());
    assertEquals(expectedCatalog.getType(), convertedCatalog.getType());
    assertEquals(expectedCatalog.getProvider(), convertedCatalog.getProvider());
    assertEquals(expectedCatalog.namespace(), convertedCatalog.namespace());
    assertEquals(
        expectedCatalog.getProperties().get("key"), convertedCatalog.getProperties().get("key"));
    assertEquals(expectedCatalog.auditInfo().creator(), convertedCatalog.auditInfo().creator());
  }

  @Test
  public void testFromSchemaPO() throws JsonProcessingException {
    SchemaPO schemaPO = createSchemaPO(1L, "test", 1L, 1L, "this is test");

    SchemaEntity expectedSchema =
        createSchema(
            1L, "test", NamespaceUtil.ofSchema("test_metalake", "test_catalog"), "this is test");

    SchemaEntity convertedSchema =
        POConverters.fromSchemaPO(
            schemaPO, NamespaceUtil.ofSchema("test_metalake", "test_catalog"));

    // Assert
    assertEquals(expectedSchema.id(), convertedSchema.id());
    assertEquals(expectedSchema.name(), convertedSchema.name());
    assertEquals(expectedSchema.comment(), convertedSchema.comment());
    assertEquals(expectedSchema.namespace(), convertedSchema.namespace());
    assertEquals(expectedSchema.properties().get("key"), convertedSchema.properties().get("key"));
    assertEquals(expectedSchema.auditInfo().creator(), convertedSchema.auditInfo().creator());
  }

  @Test
  public void testFromTablePO() throws JsonProcessingException {
    TablePO tablePO = createTablePO(1L, "test", 1L, 1L, 1L);

    TableEntity expectedTable =
        createTable(
            1L, "test", NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));

    TableEntity convertedTable =
        POConverters.fromTablePO(
            tablePO, NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));

    // Assert
    assertEquals(expectedTable.id(), convertedTable.id());
    assertEquals(expectedTable.name(), convertedTable.name());
    assertEquals(expectedTable.namespace(), convertedTable.namespace());
    assertEquals(expectedTable.auditInfo().creator(), convertedTable.auditInfo().creator());
  }

  @Test
  public void testFromColumnPO() throws JsonProcessingException {
    ColumnPO columnPO =
        createColumnPO(
            1L,
            "test",
            0,
            1L,
            1L,
            1L,
            1L,
            Types.IntegerType.get(),
            "test",
            true,
            true,
            Literals.integerLiteral(1),
            ColumnPO.ColumnOpType.CREATE);

    ColumnEntity expectedColumn =
        createColumn(
            1L, "test", 0, Types.IntegerType.get(), "test", true, true, Literals.integerLiteral(1));

    ColumnEntity convertedColumn = POConverters.fromColumnPO(columnPO);
    assertEquals(expectedColumn.id(), convertedColumn.id());
    assertEquals(expectedColumn.name(), convertedColumn.name());
    assertEquals(expectedColumn.dataType(), convertedColumn.dataType());
    assertEquals(expectedColumn.comment(), convertedColumn.comment());
    assertEquals(expectedColumn.nullable(), convertedColumn.nullable());
    assertEquals(expectedColumn.autoIncrement(), convertedColumn.autoIncrement());
    assertEquals(expectedColumn.defaultValue(), convertedColumn.defaultValue());

    // Test column comment is null
    ColumnPO columnPO1 =
        createColumnPO(
            1L,
            "test",
            0,
            1L,
            1L,
            1L,
            1L,
            Types.IntegerType.get(),
            null,
            true,
            true,
            Literals.integerLiteral(1),
            ColumnPO.ColumnOpType.CREATE);

    ColumnEntity expectedColumn1 =
        createColumn(
            1L, "test", 0, Types.IntegerType.get(), null, true, true, Literals.integerLiteral(1));

    ColumnEntity convertedColumn1 = POConverters.fromColumnPO(columnPO1);
    assertEquals(expectedColumn1.comment(), convertedColumn1.comment());
  }

  @Test
  public void testFromTableColumnPOs() throws JsonProcessingException {
    TablePO tablePO = createTablePO(1L, "test", 1L, 1L, 1L);
    ColumnPO columnPO1 =
        createColumnPO(
            1L,
            "test1",
            0,
            1L,
            1L,
            1L,
            1L,
            Types.IntegerType.get(),
            "test1",
            true,
            true,
            Literals.integerLiteral(1),
            ColumnPO.ColumnOpType.CREATE);

    ColumnPO columnPO2 =
        createColumnPO(
            2L,
            "test2",
            1,
            1L,
            1L,
            1L,
            1L,
            Types.StringType.get(),
            "test2",
            true,
            true,
            Literals.stringLiteral("1"),
            ColumnPO.ColumnOpType.CREATE);

    ColumnEntity expectedColumn1 =
        createColumn(
            1L,
            "test1",
            0,
            Types.IntegerType.get(),
            "test1",
            true,
            true,
            Literals.integerLiteral(1));

    ColumnEntity expectedColumn2 =
        createColumn(
            2L,
            "test2",
            1,
            Types.StringType.get(),
            "test2",
            true,
            true,
            Literals.stringLiteral("1"));

    TableEntity expectedTable =
        createTableWithColumns(
            1L,
            "test",
            NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"),
            Lists.newArrayList(expectedColumn1, expectedColumn2));

    TableEntity convertedTable =
        POConverters.fromTableAndColumnPOs(
            tablePO,
            Lists.newArrayList(columnPO1, columnPO2),
            NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));

    assertEquals(expectedTable.id(), convertedTable.id());
    assertEquals(expectedTable.name(), convertedTable.name());
    assertEquals(expectedTable.namespace(), convertedTable.namespace());
    assertEquals(expectedTable.auditInfo().creator(), convertedTable.auditInfo().creator());
    assertEquals(expectedTable.columns().size(), convertedTable.columns().size());
    assertEquals(expectedTable.columns(), convertedTable.columns());
  }

  @Test
  public void testFromFilesetPO() throws JsonProcessingException {
    FilesetVersionPO filesetVersionPO =
        createFilesetVersionPO(
            1L, 1L, 1L, 1L, 1L, "this is test", "hdfs://localhost/test", new HashMap<>());
    FilesetPO filesetPO = createFilesetPO(1L, "test", 1L, 1L, 1L, 1L, filesetVersionPO);

    FilesetEntity expectedFileset =
        createFileset(
            1L,
            "test",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test",
            "hdfs://localhost/test",
            new HashMap<>());

    FilesetEntity convertedFileset =
        POConverters.fromFilesetPO(
            filesetPO, NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"));

    // Assert
    assertEquals(expectedFileset.id(), convertedFileset.id());
    assertEquals(expectedFileset.name(), convertedFileset.name());
    assertEquals(expectedFileset.namespace(), convertedFileset.namespace());
    assertEquals(expectedFileset.auditInfo().creator(), convertedFileset.auditInfo().creator());
    assertEquals(expectedFileset.storageLocation(), convertedFileset.storageLocation());
  }

  @Test
  public void testFromTopicPO() throws JsonProcessingException {
    TopicPO topicPO =
        createTopicPO(1L, "test", 1L, 1L, 1L, "test comment", ImmutableMap.of("key", "value"));

    TopicEntity expectedTopic =
        createTopic(
            1L,
            "test",
            NamespaceUtil.ofTopic("test_metalake", "test_catalog", "test_schema"),
            "test comment",
            ImmutableMap.of("key", "value"));

    TopicEntity convertedTopic =
        POConverters.fromTopicPO(
            topicPO, NamespaceUtil.ofTopic("test_metalake", "test_catalog", "test_schema"));

    assertEquals(expectedTopic.id(), convertedTopic.id());
    assertEquals(expectedTopic.name(), convertedTopic.name());
    assertEquals(expectedTopic.namespace(), convertedTopic.namespace());
    assertEquals(expectedTopic.auditInfo().creator(), convertedTopic.auditInfo().creator());
    assertEquals(expectedTopic.comment(), convertedTopic.comment());
    assertEquals(expectedTopic.properties(), convertedTopic.properties());
  }

  @Test
  public void testFromMetalakePOs() throws JsonProcessingException {
    MetalakePO metalakePO1 = createMetalakePO(1L, "test", "this is test");
    MetalakePO metalakePO2 = createMetalakePO(2L, "test2", "this is test2");
    List<MetalakePO> metalakePOs = new ArrayList<>(Arrays.asList(metalakePO1, metalakePO2));
    List<BaseMetalake> convertedMetalakes = POConverters.fromMetalakePOs(metalakePOs);

    BaseMetalake expectedMetalake1 = createMetalake(1L, "test", "this is test");
    BaseMetalake expectedMetalake2 = createMetalake(2L, "test2", "this is test2");
    List<BaseMetalake> expectedMetalakes =
        new ArrayList<>(Arrays.asList(expectedMetalake1, expectedMetalake2));

    // Assert
    int index = 0;
    for (BaseMetalake metalake : convertedMetalakes) {
      assertEquals(expectedMetalakes.get(index).id(), metalake.id());
      assertEquals(expectedMetalakes.get(index).name(), metalake.name());
      assertEquals(expectedMetalakes.get(index).comment(), metalake.comment());
      assertEquals(
          expectedMetalakes.get(index).properties().get("key"), metalake.properties().get("key"));
      assertEquals(
          expectedMetalakes.get(index).auditInfo().creator(), metalake.auditInfo().creator());
      assertEquals(expectedMetalakes.get(index).getVersion(), metalake.getVersion());
      index++;
    }
  }

  @Test
  public void testFromCatalogPOs() throws JsonProcessingException {
    CatalogPO catalogPO1 = createCatalogPO(1L, "test", 1L, "this is test");
    CatalogPO catalogPO2 = createCatalogPO(2L, "test2", 1L, "this is test2");
    List<CatalogPO> catalogPOs = new ArrayList<>(Arrays.asList(catalogPO1, catalogPO2));
    List<CatalogEntity> convertedCatalogs =
        POConverters.fromCatalogPOs(catalogPOs, NamespaceUtil.ofCatalog("test_metalake"));

    CatalogEntity expectedCatalog1 =
        createCatalog(1L, "test", NamespaceUtil.ofCatalog("test_metalake"), "this is test");
    CatalogEntity expectedCatalog2 =
        createCatalog(2L, "test2", NamespaceUtil.ofCatalog("test_metalake"), "this is test2");
    List<CatalogEntity> expectedCatalogs =
        new ArrayList<>(Arrays.asList(expectedCatalog1, expectedCatalog2));

    // Assert
    int index = 0;
    for (CatalogEntity catalog : convertedCatalogs) {
      assertEquals(expectedCatalogs.get(index).id(), catalog.id());
      assertEquals(expectedCatalogs.get(index).name(), catalog.name());
      assertEquals(expectedCatalogs.get(index).getComment(), catalog.getComment());
      assertEquals(expectedCatalogs.get(index).getType(), catalog.getType());
      assertEquals(expectedCatalogs.get(index).getProvider(), catalog.getProvider());
      assertEquals(expectedCatalogs.get(index).namespace(), catalog.namespace());
      assertEquals(
          expectedCatalogs.get(index).getProperties().get("key"),
          catalog.getProperties().get("key"));
      assertEquals(
          expectedCatalogs.get(index).auditInfo().creator(), catalog.auditInfo().creator());
      index++;
    }
  }

  @Test
  public void testFromSchemaPOs() throws JsonProcessingException {
    SchemaPO schemaPO1 = createSchemaPO(1L, "test", 1L, 1L, "this is test");
    SchemaPO schemaPO2 = createSchemaPO(2L, "test2", 1L, 1L, "this is test2");
    List<SchemaPO> schemaPOs = new ArrayList<>(Arrays.asList(schemaPO1, schemaPO2));
    List<SchemaEntity> convertedSchemas =
        POConverters.fromSchemaPOs(
            schemaPOs, NamespaceUtil.ofSchema("test_metalake", "test_catalog"));

    SchemaEntity expectedSchema1 =
        createSchema(
            1L, "test", NamespaceUtil.ofSchema("test_metalake", "test_catalog"), "this is test");
    SchemaEntity expectedSchema2 =
        createSchema(
            2L, "test2", NamespaceUtil.ofSchema("test_metalake", "test_catalog"), "this is test2");
    List<SchemaEntity> expectedSchemas =
        new ArrayList<>(Arrays.asList(expectedSchema1, expectedSchema2));

    // Assert
    int index = 0;
    for (SchemaEntity schema : convertedSchemas) {
      assertEquals(expectedSchemas.get(index).id(), schema.id());
      assertEquals(expectedSchemas.get(index).name(), schema.name());
      assertEquals(expectedSchemas.get(index).comment(), schema.comment());
      assertEquals(expectedSchemas.get(index).namespace(), schema.namespace());
      assertEquals(
          expectedSchemas.get(index).properties().get("key"), schema.properties().get("key"));
      assertEquals(expectedSchemas.get(index).auditInfo().creator(), schema.auditInfo().creator());
      index++;
    }
  }

  @Test
  public void testFromTablePOs() throws JsonProcessingException {
    TablePO tablePO1 = createTablePO(1L, "test", 1L, 1L, 1L);
    TablePO tablePO2 = createTablePO(2L, "test2", 1L, 1L, 1L);
    List<TablePO> tablePOs = new ArrayList<>(Arrays.asList(tablePO1, tablePO2));
    List<TableEntity> convertedTables =
        POConverters.fromTablePOs(
            tablePOs, NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));

    TableEntity expectedTable1 =
        createTable(
            1L, "test", NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));
    TableEntity expectedTable2 =
        createTable(
            2L, "test2", NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));
    List<TableEntity> expectedTables =
        new ArrayList<>(Arrays.asList(expectedTable1, expectedTable2));

    // Assert
    int index = 0;
    for (TableEntity tableEntity : convertedTables) {
      assertEquals(expectedTables.get(index).id(), tableEntity.id());
      assertEquals(expectedTables.get(index).name(), tableEntity.name());
      assertEquals(expectedTables.get(index).namespace(), tableEntity.namespace());
      assertEquals(
          expectedTables.get(index).auditInfo().creator(), tableEntity.auditInfo().creator());
      index++;
    }
  }

  @Test
  public void testFromFilesetPOs() throws JsonProcessingException {
    FilesetVersionPO filesetVersionPO1 =
        createFilesetVersionPO(
            1L, 1L, 1L, 1L, 1L, "this is test1", "hdfs://localhost/test1", new HashMap<>());
    FilesetPO filesetPO1 = createFilesetPO(1L, "test1", 1L, 1L, 1L, 1L, filesetVersionPO1);

    FilesetVersionPO filesetVersionPO2 =
        createFilesetVersionPO(
            2L, 1L, 1L, 1L, 2L, "this is test2", "hdfs://localhost/test2", new HashMap<>());
    FilesetPO filesetPO2 = createFilesetPO(2L, "test2", 1L, 1L, 1L, 2L, filesetVersionPO2);

    List<FilesetPO> filesetPOs = new ArrayList<>(Arrays.asList(filesetPO1, filesetPO2));
    List<FilesetEntity> convertedFilesets =
        POConverters.fromFilesetPOs(
            filesetPOs, NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"));

    FilesetEntity expectedFileset1 =
        createFileset(
            1L,
            "test1",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test1",
            "hdfs://localhost/test1",
            new HashMap<>());
    FilesetEntity expectedFileset2 =
        createFileset(
            2L,
            "test2",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test2",
            "hdfs://localhost/test2",
            new HashMap<>());
    List<FilesetEntity> expectedFilesets =
        new ArrayList<>(Arrays.asList(expectedFileset1, expectedFileset2));

    // Assert
    int index = 0;
    for (FilesetEntity fileset : convertedFilesets) {
      assertEquals(expectedFilesets.get(index).id(), fileset.id());
      assertEquals(expectedFilesets.get(index).name(), fileset.name());
      assertEquals(expectedFilesets.get(index).namespace(), fileset.namespace());
      assertEquals(
          expectedFilesets.get(index).auditInfo().creator(), fileset.auditInfo().creator());
      assertEquals(expectedFilesets.get(index).storageLocation(), fileset.storageLocation());
      index++;
    }
  }

  @Test
  public void testFromTopicPOs() throws JsonProcessingException {
    TopicPO topicPO1 =
        createTopicPO(1L, "test1", 1L, 1L, 1L, "test comment1", ImmutableMap.of("key", "value"));
    TopicPO topicPO2 =
        createTopicPO(2L, "test2", 1L, 1L, 1L, "test comment2", ImmutableMap.of("key", "value"));
    List<TopicPO> topicPOs = new ArrayList<>(Arrays.asList(topicPO1, topicPO2));
    List<TopicEntity> convertedTopics =
        POConverters.fromTopicPOs(
            topicPOs, NamespaceUtil.ofTopic("test_metalake", "test_catalog", "test_schema"));

    TopicEntity expectedTopic1 =
        createTopic(
            1L,
            "test1",
            NamespaceUtil.ofTopic("test_metalake", "test_catalog", "test_schema"),
            "test comment1",
            ImmutableMap.of("key", "value"));
    TopicEntity expectedTopic2 =
        createTopic(
            2L,
            "test2",
            NamespaceUtil.ofTopic("test_metalake", "test_catalog", "test_schema"),
            "test comment2",
            ImmutableMap.of("key", "value"));
    List<TopicEntity> expectedTopics =
        new ArrayList<>(Arrays.asList(expectedTopic1, expectedTopic2));

    int index = 0;
    for (TopicEntity topic : convertedTopics) {
      assertEquals(expectedTopics.get(index).id(), topic.id());
      assertEquals(expectedTopics.get(index).name(), topic.name());
      assertEquals(expectedTopics.get(index).namespace(), topic.namespace());
      assertEquals(expectedTopics.get(index).auditInfo().creator(), topic.auditInfo().creator());
      assertEquals(expectedTopics.get(index).comment(), topic.comment());
      assertEquals(expectedTopics.get(index).properties(), topic.properties());
      index++;
    }
  }

  @Test
  public void testInitMetalakePOVersion() {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");
    MetalakePO initPO = POConverters.initializeMetalakePOWithVersion(metalake);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitCatalogPOVersion() {
    CatalogEntity catalog =
        createCatalog(1L, "test", NamespaceUtil.ofCatalog("test_metalake"), "this is test");
    CatalogPO initPO = POConverters.initializeCatalogPOWithVersion(catalog, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitSchemaPOVersion() {
    SchemaEntity schema =
        createSchema(
            1L, "test", NamespaceUtil.ofSchema("test_metalake", "test_catalog"), "this is test");
    SchemaPO.Builder builder = SchemaPO.builder().withMetalakeId(1L).withCatalogId(1L);
    SchemaPO initPO = POConverters.initializeSchemaPOWithVersion(schema, builder);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitTablePOVersion() {
    TableEntity tableEntity =
        createTable(
            1L, "test", NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));
    TablePO.Builder builder =
        TablePO.builder().withMetalakeId(1L).withCatalogId(1L).withSchemaId(1L);
    TablePO initPO = POConverters.initializeTablePOWithVersion(tableEntity, builder);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitFilesetPOVersion() {
    FilesetEntity filesetEntity =
        createFileset(
            1L,
            "test",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test",
            "hdfs://localhost/test",
            new HashMap<>());
    FilesetPO.Builder builder =
        FilesetPO.builder().withMetalakeId(1L).withCatalogId(1L).withSchemaId(1L);
    FilesetPO initPO = POConverters.initializeFilesetPOWithVersion(filesetEntity, builder);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals(1, initPO.getFilesetVersionPOs().get(0).getVersion());
    assertEquals(1, initPO.getFilesetVersionPOs().size());
    assertEquals(
        "hdfs://localhost/test", initPO.getFilesetVersionPOs().get(0).getStorageLocation());
  }

  @Test
  public void testUpdateMetalakePOVersion() {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");
    BaseMetalake updatedMetalake = createMetalake(1L, "test", "this is test2");
    MetalakePO initPO = POConverters.initializeMetalakePOWithVersion(metalake);
    MetalakePO updatePO = POConverters.updateMetalakePOWithVersion(initPO, updatedMetalake);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getMetalakeComment());
  }

  @Test
  public void testUpdateCatalogPOVersion() {
    CatalogEntity catalog =
        createCatalog(1L, "test", NamespaceUtil.ofCatalog("test_metalake"), "this is test");
    CatalogEntity updatedCatalog =
        createCatalog(1L, "test", NamespaceUtil.ofCatalog("test_metalake"), "this is test2");
    CatalogPO initPO = POConverters.initializeCatalogPOWithVersion(catalog, 1L);
    CatalogPO updatePO = POConverters.updateCatalogPOWithVersion(initPO, updatedCatalog, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getCatalogComment());
  }

  @Test
  public void testUpdateSchemaPOVersion() {
    SchemaEntity schema =
        createSchema(
            1L, "test", NamespaceUtil.ofSchema("test_metalake", "test_catalog"), "this is test");
    SchemaEntity updatedSchema =
        createSchema(
            1L, "test", NamespaceUtil.ofSchema("test_metalake", "test_catalog"), "this is test2");
    SchemaPO.Builder builder = SchemaPO.builder().withMetalakeId(1L).withCatalogId(1L);
    SchemaPO initPO = POConverters.initializeSchemaPOWithVersion(schema, builder);
    SchemaPO updatePO = POConverters.updateSchemaPOWithVersion(initPO, updatedSchema);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getSchemaComment());
  }

  @Test
  public void testUpdateTablePOVersion() {
    TableEntity tableEntity =
        createTable(
            1L, "test", NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));
    TableEntity updatedTable =
        createTable(
            1L, "test", NamespaceUtil.ofTable("test_metalake", "test_catalog", "test_schema"));
    TablePO.Builder builder =
        TablePO.builder().withMetalakeId(1L).withCatalogId(1L).withSchemaId(1L);
    TablePO initPO = POConverters.initializeTablePOWithVersion(tableEntity, builder);
    TablePO updatePO = POConverters.updateTablePOWithVersion(initPO, updatedTable, false);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("test", updatePO.getTableName());
  }

  @Test
  public void testUpdateFilesetPOVersion() throws JsonProcessingException {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    FilesetEntity filesetEntity =
        createFileset(
            1L,
            "test",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test",
            "hdfs://localhost/test",
            properties);

    Map<String, String> updateProperties = new HashMap<>();
    updateProperties.put("key", "value1");
    FilesetEntity updatedFileset =
        createFileset(
            1L,
            "test",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test",
            "hdfs://localhost/test",
            updateProperties);

    FilesetEntity updatedFileset1 =
        createFileset(
            1L,
            "test1",
            NamespaceUtil.ofFileset("test_metalake", "test_catalog", "test_schema"),
            "this is test",
            "hdfs://localhost/test",
            properties);

    FilesetPO.Builder builder =
        FilesetPO.builder().withMetalakeId(1L).withCatalogId(1L).withSchemaId(1L);
    FilesetPO initPO = POConverters.initializeFilesetPOWithVersion(filesetEntity, builder);

    // map has updated
    boolean checkNeedUpdate1 =
        POConverters.checkFilesetVersionNeedUpdate(initPO.getFilesetVersionPOs(), updatedFileset);
    FilesetPO updatePO1 =
        POConverters.updateFilesetPOWithVersion(initPO, updatedFileset, checkNeedUpdate1);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    Map<String, String> storageLocations =
        updatePO1.getFilesetVersionPOs().stream()
            .collect(
                Collectors.toMap(
                    FilesetVersionPO::getLocationName, FilesetVersionPO::getStorageLocation));
    assertEquals(updatedFileset.storageLocation(), storageLocations.get(LOCATION_NAME_UNKNOWN));
    assertEquals(updatedFileset.storageLocations(), storageLocations);
    assertEquals(2, updatePO1.getCurrentVersion());
    assertEquals(2, updatePO1.getLastVersion());
    assertEquals(2, updatePO1.getFilesetVersionPOs().get(0).getVersion());
    Map<String, String> updatedProperties =
        JsonUtils.anyFieldMapper()
            .readValue(updatePO1.getFilesetVersionPOs().get(0).getProperties(), Map.class);
    assertEquals("value1", updatedProperties.get("key"));

    // will not update version, but update the fileset name
    boolean checkNeedUpdate2 =
        POConverters.checkFilesetVersionNeedUpdate(initPO.getFilesetVersionPOs(), updatedFileset1);
    FilesetPO updatePO2 =
        POConverters.updateFilesetPOWithVersion(initPO, updatedFileset1, checkNeedUpdate2);
    Map<String, String> storageLocations2 =
        updatePO2.getFilesetVersionPOs().stream()
            .collect(
                Collectors.toMap(
                    FilesetVersionPO::getLocationName, FilesetVersionPO::getStorageLocation));
    assertEquals(filesetEntity.storageLocation(), storageLocations2.get(LOCATION_NAME_UNKNOWN));
    assertEquals(filesetEntity.storageLocations(), storageLocations2);
    assertEquals(1, updatePO2.getCurrentVersion());
    assertEquals(1, updatePO2.getLastVersion());
    assertEquals(1, updatePO2.getFilesetVersionPOs().get(0).getVersion());
    assertEquals("test1", updatePO2.getFilesetName());
  }

  @Test
  public void testFromPolicyPO() throws JsonProcessingException {
    ImmutableSet<MetadataObject.Type> supportedObjectTypes =
        ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.SCHEMA);
    PolicyContent content = PolicyContents.custom(null, supportedObjectTypes, null);
    PolicyVersionPO policyVersionPO =
        createPolicyVersionPO(1L, 1L, 1L, "test comment", true, content);
    PolicyPO policyPO = createPolicyPO(1L, "test", "custom", 1L, policyVersionPO);

    PolicyEntity expectedPolicy =
        createPolicy(
            1L,
            "test",
            NamespaceUtil.ofPolicy("test_metalake"),
            Policy.BuiltInType.CUSTOM,
            "test comment",
            true,
            content);

    PolicyEntity convertedPolicy =
        POConverters.fromPolicyPO(policyPO, NamespaceUtil.ofPolicy("test_metalake"));

    assertEquals(expectedPolicy.id(), convertedPolicy.id());
    assertEquals(expectedPolicy.name(), convertedPolicy.name());
    assertEquals(expectedPolicy.namespace(), convertedPolicy.namespace());
    assertEquals(expectedPolicy.auditInfo().creator(), convertedPolicy.auditInfo().creator());
    assertEquals(expectedPolicy.policyType(), convertedPolicy.policyType());
    assertEquals(expectedPolicy.comment(), convertedPolicy.comment());
    assertEquals(expectedPolicy.enabled(), convertedPolicy.enabled());
    assertEquals(expectedPolicy.content(), convertedPolicy.content());
  }

  @Test
  public void testFromTagPO() throws JsonProcessingException {
    TagPO tagPO = createTagPO(1L, "test", 1L, "this is test");
    Namespace tagNS =
        Namespace.of("test_metalake", Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);

    TagEntity expectedTag = createTag(1L, "test", tagNS, "this is test");
    TagEntity convertedTag = POConverters.fromTagPO(tagPO, tagNS);

    // Assert
    assertEquals(expectedTag.id(), convertedTag.id());
    assertEquals(expectedTag.name(), convertedTag.name());
    assertEquals(expectedTag.namespace(), convertedTag.namespace());
    assertEquals(expectedTag.auditInfo().creator(), convertedTag.auditInfo().creator());
    assertEquals(expectedTag.comment(), convertedTag.comment());

    TagPO tagPOWithNullComment = createTagPO(1L, "test", 1L, null);
    TagEntity expectedTagWithNullComment = createTag(1L, "test", tagNS, null);
    TagEntity convertedTagWithNullComment = POConverters.fromTagPO(tagPOWithNullComment, tagNS);
    assertEquals(expectedTagWithNullComment.id(), convertedTagWithNullComment.id());
    assertEquals(expectedTagWithNullComment.name(), convertedTagWithNullComment.name());
    assertEquals(expectedTagWithNullComment.namespace(), convertedTagWithNullComment.namespace());
    assertEquals(
        expectedTagWithNullComment.auditInfo().creator(),
        convertedTagWithNullComment.auditInfo().creator());
    assertNull(convertedTagWithNullComment.comment());
  }

  @Test
  public void testInitTagPOVersion() {
    Namespace tagNS =
        Namespace.of("test_metalake", Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);
    TagEntity tag = createTag(1L, "test", tagNS, "this is test");
    TagPO.Builder builder = TagPO.builder().withMetalakeId(1L);
    TagPO initPO = POConverters.initializeTagPOWithVersion(tag, builder);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testUpdateTagPOVersion() {
    Namespace tagNS =
        Namespace.of("test_metalake", Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);
    TagEntity tag = createTag(1L, "test", tagNS, "this is test");
    TagEntity updatedTag = createTag(1L, "test", tagNS, "this is test2");
    TagPO.Builder builder = TagPO.builder().withMetalakeId(1L);
    TagPO initPO = POConverters.initializeTagPOWithVersion(tag, builder);
    TagPO updatePO = POConverters.updateTagPOWithVersion(initPO, updatedTag);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getComment());
  }

  @Test
  public void testTagMetadataObjectRelPO() {
    TagMetadataObjectRelPO tagMetadataObjectRelPO =
        POConverters.initializeTagMetadataObjectRelPOWithVersion(
            1L, 1L, MetadataObject.Type.CATALOG.toString());
    assertEquals(1L, tagMetadataObjectRelPO.getTagId());
    assertEquals(1L, tagMetadataObjectRelPO.getMetadataObjectId());
    assertEquals(
        MetadataObject.Type.CATALOG.toString(), tagMetadataObjectRelPO.getMetadataObjectType());

    assertEquals(1, tagMetadataObjectRelPO.getCurrentVersion());
    assertEquals(1, tagMetadataObjectRelPO.getLastVersion());
    assertEquals(0, tagMetadataObjectRelPO.getDeletedAt());
  }

  @Test
  public void testOwnerRelPO() {
    OwnerRelPO ownerRelPO =
        POConverters.initializeOwnerRelPOsWithVersion(
            1L, Entity.EntityType.USER.name(), 1L, Entity.EntityType.METALAKE.name(), 1L);

    assertEquals(1L, ownerRelPO.getOwnerId());
    assertEquals(1L, ownerRelPO.getMetalakeId());
    assertEquals(1L, ownerRelPO.getMetadataObjectId());
    assertEquals(Entity.EntityType.METALAKE.name(), ownerRelPO.getMetadataObjectType());
    assertEquals(Entity.EntityType.USER.name(), ownerRelPO.getOwnerType());

    assertEquals(1, ownerRelPO.getCurrentVersion());
    assertEquals(1, ownerRelPO.getLastVersion());
    assertEquals(0, ownerRelPO.getDeletedAt());
  }

  @Test
  public void testInitModelPO() throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    ModelEntity modelEntity =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(Namespace.of("test_metalake", "test_catalog", "test_schema"))
            .withComment("this is test")
            .withProperties(ImmutableMap.of("key", "value"))
            .withLatestVersion(1)
            .withAuditInfo(auditInfo)
            .build();

    ModelPO.Builder builder =
        ModelPO.builder().withMetalakeId(1L).withCatalogId(1L).withSchemaId(1L);
    ModelPO modelPO = POConverters.initializeModelPO(modelEntity, builder);

    assertEquals(1, modelPO.getModelId());
    assertEquals("test", modelPO.getModelName());
    assertEquals(1, modelPO.getMetalakeId());
    assertEquals(1, modelPO.getCatalogId());
    assertEquals(1, modelPO.getSchemaId());
    assertEquals("this is test", modelPO.getModelComment());

    Map<String, String> resultProperties =
        JsonUtils.anyFieldMapper().readValue(modelPO.getModelProperties(), Map.class);
    assertEquals(ImmutableMap.of("key", "value"), resultProperties);

    AuditInfo resultAuditInfo =
        JsonUtils.anyFieldMapper().readValue(modelPO.getAuditInfo(), AuditInfo.class);
    assertEquals(auditInfo, resultAuditInfo);
    assertEquals(1, modelPO.getModelLatestVersion());
    assertEquals(0, modelPO.getDeletedAt());

    // Test with null fields
    ModelEntity modelEntityWithNull =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(Namespace.of("test_metalake", "test_catalog", "test_schema"))
            .withComment(null)
            .withProperties(null)
            .withLatestVersion(1)
            .withAuditInfo(auditInfo)
            .build();

    ModelPO.Builder builderWithNull =
        ModelPO.builder().withMetalakeId(1L).withCatalogId(1L).withSchemaId(1L);
    ModelPO modelPOWithNull = POConverters.initializeModelPO(modelEntityWithNull, builderWithNull);

    assertNull(modelPOWithNull.getModelComment());
    Map<String, String> resultPropertiesWithNull =
        JsonUtils.anyFieldMapper().readValue(modelPOWithNull.getModelProperties(), Map.class);
    assertNull(resultPropertiesWithNull);
  }

  @Test
  public void testFromModelPO() throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = ImmutableMap.of("key", "value");
    Map<String, String> emptyProperties = Collections.emptyMap();
    Namespace namespace = Namespace.of("test_metalake", "test_catalog", "test_schema");

    ModelPO modelPO =
        ModelPO.builder()
            .withModelId(1L)
            .withModelName("test")
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withModelComment("this is test")
            .withModelProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
            .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
            .withModelLatestVersion(1)
            .withDeletedAt(0L)
            .build();

    ModelEntity expectedModel =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(namespace)
            .withComment("this is test")
            .withProperties(properties)
            .withLatestVersion(1)
            .withAuditInfo(auditInfo)
            .build();

    ModelEntity convertedModel = POConverters.fromModelPO(modelPO, namespace);
    assertEquals(expectedModel, convertedModel);

    // test null fields
    ModelPO modelPOWithNull =
        ModelPO.builder()
            .withModelId(1L)
            .withModelName("test")
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withModelComment(null)
            .withModelProperties(JsonUtils.anyFieldMapper().writeValueAsString(null))
            .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
            .withModelLatestVersion(1)
            .withDeletedAt(0L)
            .build();

    ModelEntity expectedModelWithNull =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(namespace)
            .withComment(null)
            .withProperties(null)
            .withLatestVersion(1)
            .withAuditInfo(auditInfo)
            .build();

    ModelEntity convertedModelWithNull = POConverters.fromModelPO(modelPOWithNull, namespace);
    assertEquals(expectedModelWithNull, convertedModelWithNull);

    // Test with empty properties
    ModelPO modelPOWithEmptyProperties =
        ModelPO.builder()
            .withModelId(1L)
            .withModelName("test")
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withModelComment("this is test")
            .withModelProperties(JsonUtils.anyFieldMapper().writeValueAsString(emptyProperties))
            .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
            .withModelLatestVersion(1)
            .withDeletedAt(0L)
            .build();

    ModelEntity expectedModelWithEmptyProperties =
        ModelEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(namespace)
            .withComment("this is test")
            .withProperties(emptyProperties)
            .withLatestVersion(1)
            .withAuditInfo(auditInfo)
            .build();

    ModelEntity convertedModelWithEmptyProperties =
        POConverters.fromModelPO(modelPOWithEmptyProperties, namespace);
    assertEquals(expectedModelWithEmptyProperties, convertedModelWithEmptyProperties);
  }

  @Test
  public void testInitModelVersionPO() throws JsonProcessingException {
    NameIdentifier modelIdent = NameIdentifierUtil.ofModel("m", "c", "s", "model1");
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();

    ModelVersionEntity modelVersionEntity =
        ModelVersionEntity.builder()
            .withModelIdentifier(modelIdent)
            .withVersion(1)
            .withAliases(ImmutableList.of("alias1"))
            .withComment("this is test")
            .withProperties(ImmutableMap.of("key", "value"))
            .withUris(ImmutableMap.of("unknown", "hdfs://localhost/test"))
            .withAuditInfo(auditInfo)
            .build();

    List<ModelVersionPO> modelVersionPOs =
        POConverters.initializeModelVersionPO(modelVersionEntity, 1L);
    Assertions.assertEquals(1, modelVersionPOs.size());
    Assertions.assertEquals(1, modelVersionPOs.get(0).getModelVersion());
    Assertions.assertEquals(1L, modelVersionPOs.get(0).getModelId());
    Assertions.assertEquals("this is test", modelVersionPOs.get(0).getModelVersionComment());
    Assertions.assertEquals("unknown", modelVersionPOs.get(0).getModelVersionUriName());
    Assertions.assertEquals("hdfs://localhost/test", modelVersionPOs.get(0).getModelVersionUri());
    Assertions.assertEquals(0L, modelVersionPOs.get(0).getDeletedAt());

    Map<String, String> resultProperties =
        JsonUtils.anyFieldMapper()
            .readValue(modelVersionPOs.get(0).getModelVersionProperties(), Map.class);
    Assertions.assertEquals(ImmutableMap.of("key", "value"), resultProperties);

    AuditInfo resultAuditInfo =
        JsonUtils.anyFieldMapper()
            .readValue(modelVersionPOs.get(0).getAuditInfo(), AuditInfo.class);
    Assertions.assertEquals(auditInfo, resultAuditInfo);

    List<ModelVersionAliasRelPO> aliasPOs =
        POConverters.initializeModelVersionAliasRelPO(
            modelVersionEntity, modelVersionPOs.get(0).getModelId());
    Assertions.assertEquals(1, aliasPOs.size());
    Assertions.assertEquals(1, aliasPOs.get(0).getModelVersion());
    Assertions.assertEquals("alias1", aliasPOs.get(0).getModelVersionAlias());
    Assertions.assertEquals(1L, aliasPOs.get(0).getModelId());
    Assertions.assertEquals(0L, aliasPOs.get(0).getDeletedAt());

    // Test with null fields
    ModelVersionEntity modelVersionEntityWithNull =
        ModelVersionEntity.builder()
            .withModelIdentifier(modelIdent)
            .withVersion(1)
            .withAliases(null)
            .withComment(null)
            .withProperties(null)
            .withUris(ImmutableMap.of("unknown", "hdfs://localhost/test"))
            .withAuditInfo(auditInfo)
            .build();

    List<ModelVersionPO> modelVersionPOsWithNull =
        POConverters.initializeModelVersionPO(modelVersionEntityWithNull, 1L);
    Assertions.assertEquals(1, modelVersionPOsWithNull.size());
    Assertions.assertNull(modelVersionPOsWithNull.get(0).getModelVersionComment());

    Map<String, String> resultPropertiesWithNull =
        JsonUtils.anyFieldMapper()
            .readValue(modelVersionPOsWithNull.get(0).getModelVersionProperties(), Map.class);
    Assertions.assertNull(resultPropertiesWithNull);

    List<ModelVersionAliasRelPO> aliasPOsWithNull =
        POConverters.initializeModelVersionAliasRelPO(
            modelVersionEntityWithNull, modelVersionPOsWithNull.get(0).getModelId());
    Assertions.assertEquals(0, aliasPOsWithNull.size());
  }

  @Test
  public void testFromModelVersionPO() throws JsonProcessingException {
    NameIdentifier modelIdent = NameIdentifierUtil.ofModel("m", "c", "s", "model1");
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = ImmutableMap.of("key", "value");
    List<String> aliases = ImmutableList.of("alias1", "alias2");

    ModelVersionPO modelVersionPO =
        ModelVersionPO.builder()
            .withModelVersion(1)
            .withModelId(1L)
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withModelVersionComment("this is test")
            .withModelVersionProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
            .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
            .withModelVersionUriName("unknown")
            .withModelVersionUri("hdfs://localhost/test")
            .withDeletedAt(0L)
            .build();
    List<ModelVersionAliasRelPO> aliasPOs =
        aliases.stream()
            .map(
                a ->
                    ModelVersionAliasRelPO.builder()
                        .withModelVersionAlias(a)
                        .withModelVersion(1)
                        .withModelId(1L)
                        .withDeletedAt(0L)
                        .build())
            .collect(Collectors.toList());

    ModelVersionEntity expectedModelVersion =
        ModelVersionEntity.builder()
            .withModelIdentifier(modelIdent)
            .withVersion(1)
            .withAliases(aliases)
            .withComment("this is test")
            .withProperties(properties)
            .withUris(ImmutableMap.of("unknown", "hdfs://localhost/test"))
            .withAuditInfo(auditInfo)
            .build();

    ModelVersionEntity convertedModelVersion =
        POConverters.fromModelVersionPO(modelIdent, ImmutableList.of(modelVersionPO), aliasPOs);
    assertEquals(expectedModelVersion, convertedModelVersion);

    // test null fields
    ModelVersionPO modelVersionPOWithNull =
        ModelVersionPO.builder()
            .withModelVersion(1)
            .withModelId(1L)
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withModelVersionComment(null)
            .withModelVersionProperties(JsonUtils.anyFieldMapper().writeValueAsString(null))
            .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
            .withModelVersionUriName("unknown")
            .withModelVersionUri("hdfs://localhost/test")
            .withDeletedAt(0L)
            .build();
    List<ModelVersionAliasRelPO> aliasPOsWithNull = Collections.emptyList();

    ModelVersionEntity expectedModelVersionWithNull =
        ModelVersionEntity.builder()
            .withModelIdentifier(modelIdent)
            .withVersion(1)
            .withAliases(Collections.emptyList())
            .withComment(null)
            .withProperties(null)
            .withUris(ImmutableMap.of("unknown", "hdfs://localhost/test"))
            .withAuditInfo(auditInfo)
            .build();

    ModelVersionEntity convertedModelVersionWithNull =
        POConverters.fromModelVersionPO(
            modelIdent, ImmutableList.of(modelVersionPOWithNull), aliasPOsWithNull);
    assertEquals(expectedModelVersionWithNull, convertedModelVersionWithNull);
  }

  @Test
  public void testStatisticPO() throws JsonProcessingException {
    List<StatisticEntity> statisticEntities = Lists.newArrayList();
    statisticEntities.add(
        TableStatisticEntity.builder()
            .withId(1L)
            .withName("test_statistic")
            .withNamespace(
                NameIdentifierUtil.ofStatistic(
                        NameIdentifierUtil.ofTable("test", "test", "test", "test"), "test")
                    .namespace())
            .withValue(StatisticValues.stringValue("test"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build())
            .build());

    List<StatisticPO> statisticPOs =
        StatisticPO.initializeStatisticPOs(statisticEntities, 1L, 1L, MetadataObject.Type.CATALOG);

    assertEquals(1, statisticPOs.get(0).getCurrentVersion());
    assertEquals(1, statisticPOs.get(0).getLastVersion());
    assertEquals(0, statisticPOs.get(0).getDeletedAt());
    assertEquals("\"test\"", statisticPOs.get(0).getStatisticValue());
    assertEquals("test_statistic", statisticPOs.get(0).getStatisticName());

    StatisticPO statisticPO =
        StatisticPO.builder()
            .withStatisticId(1L)
            .withLastVersion(1L)
            .withCurrentVersion(1L)
            .withStatisticName("test")
            .withStatisticValue("\"test\"")
            .withMetadataObjectId(1L)
            .withMetadataObjectType("TABLE")
            .withDeletedAt(0L)
            .withMetalakeId(1L)
            .withAuditInfo(
                JsonUtils.anyFieldMapper()
                    .writeValueAsString(
                        AuditInfo.builder()
                            .withCreator("creator")
                            .withCreateTime(FIX_INSTANT)
                            .build()))
            .build();
    StatisticEntity entity = StatisticPO.fromStatisticPO(statisticPO);
    Assertions.assertEquals(1L, entity.id());
    Assertions.assertEquals("test", entity.name());
    Assertions.assertEquals("test", entity.value().value());
  }

  private static BaseMetalake createMetalake(Long id, String name, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return BaseMetalake.builder()
        .withId(id)
        .withName(name)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static MetalakePO createMetalakePO(Long id, String name, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return MetalakePO.builder()
        .withMetalakeId(id)
        .withMetalakeName(name)
        .withMetalakeComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withSchemaVersion(JsonUtils.anyFieldMapper().writeValueAsString(SchemaVersion.V_0_1))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static CatalogEntity createCatalog(
      Long id, String name, Namespace namespace, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static CatalogPO createCatalogPO(Long id, String name, Long metalakeId, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return CatalogPO.builder()
        .withCatalogId(id)
        .withCatalogName(name)
        .withMetalakeId(metalakeId)
        .withType(Catalog.Type.RELATIONAL.name())
        .withProvider("test")
        .withCatalogComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static SchemaEntity createSchema(
      Long id, String name, Namespace namespace, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static SchemaPO createSchemaPO(
      Long id, String name, Long metalakeId, Long catalogId, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return SchemaPO.builder()
        .withSchemaId(id)
        .withSchemaName(name)
        .withMetalakeId(metalakeId)
        .withCatalogId(catalogId)
        .withSchemaComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static TableEntity createTable(Long id, String name, Namespace namespace) {
    return createTableWithColumns(id, name, namespace, Collections.emptyList());
  }

  private static TableEntity createTableWithColumns(
      Long id, String name, Namespace namespace, List<ColumnEntity> columns) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withColumns(columns)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static TablePO createTablePO(
      Long id, String name, Long metalakeId, Long catalogId, Long schemaId)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return TablePO.builder()
        .withTableId(id)
        .withTableName(name)
        .withMetalakeId(metalakeId)
        .withCatalogId(catalogId)
        .withSchemaId(schemaId)
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static ColumnPO createColumnPO(
      Long id,
      String columnName,
      Integer columnPosition,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long tableId,
      Type columnType,
      String columnComment,
      boolean columnNullable,
      boolean columnAutoIncrement,
      Expression columnDefaultValue,
      ColumnPO.ColumnOpType columnOpType)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return ColumnPO.builder()
        .withColumnId(id)
        .withColumnName(columnName)
        .withColumnPosition(columnPosition)
        .withMetalakeId(metalakeId)
        .withCatalogId(catalogId)
        .withSchemaId(schemaId)
        .withTableId(tableId)
        .withTableVersion(1L)
        .withColumnType(JsonUtils.anyFieldMapper().writeValueAsString(columnType))
        .withColumnComment(columnComment)
        .withNullable(ColumnPO.Nullable.fromBoolean(columnNullable).value())
        .withAutoIncrement(ColumnPO.AutoIncrement.fromBoolean(columnAutoIncrement).value())
        .withDefaultValue(
            JsonUtils.anyFieldMapper()
                .writeValueAsString(DTOConverters.toFunctionArg(columnDefaultValue)))
        .withColumnOpType(columnOpType.value())
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withDeletedAt(0L)
        .build();
  }

  private static ColumnEntity createColumn(
      Long id,
      String columnName,
      Integer columnPosition,
      Type columnType,
      String columnComment,
      boolean columnNullable,
      boolean columnAutoIncrement,
      Expression columnDefaultValue) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return ColumnEntity.builder()
        .withId(id)
        .withName(columnName)
        .withPosition(columnPosition)
        .withDataType(columnType)
        .withComment(columnComment)
        .withNullable(columnNullable)
        .withAutoIncrement(columnAutoIncrement)
        .withDefaultValue(columnDefaultValue)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static TopicEntity createTopic(
      Long id, String name, Namespace namespace, String comment, Map<String, String> properties) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return TopicEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static TopicPO createTopicPO(
      Long id,
      String name,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      String comment,
      Map<String, String> properties)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return TopicPO.builder()
        .withTopicId(id)
        .withTopicName(name)
        .withMetalakeId(metalakeId)
        .withCatalogId(catalogId)
        .withSchemaId(schemaId)
        .withComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static FilesetEntity createFileset(
      Long id,
      String name,
      Namespace namespace,
      String comment,
      String storageLocation,
      Map<String, String> properties) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withFilesetType(Fileset.Type.MANAGED)
        .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, storageLocation))
        .withProperties(properties)
        .withComment(comment)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static FilesetPO createFilesetPO(
      Long id,
      String name,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long filesetId,
      FilesetVersionPO filesetVersionPO)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return FilesetPO.builder()
        .withFilesetId(id)
        .withFilesetName(name)
        .withMetalakeId(metalakeId)
        .withCatalogId(catalogId)
        .withSchemaId(schemaId)
        .withFilesetId(filesetId)
        .withType(Fileset.Type.MANAGED.name())
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .withFilesetVersionPOs(ImmutableList.of(filesetVersionPO))
        .build();
  }

  private static FilesetVersionPO createFilesetVersionPO(
      Long id,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long filesetId,
      String comment,
      String storageLocation,
      Map<String, String> properties)
      throws JsonProcessingException {
    return FilesetVersionPO.builder()
        .withId(id)
        .withMetalakeId(metalakeId)
        .withCatalogId(catalogId)
        .withSchemaId(schemaId)
        .withFilesetId(filesetId)
        .withFilesetComment(comment)
        .withLocationName(LOCATION_NAME_UNKNOWN)
        .withStorageLocation(storageLocation)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static PolicyEntity createPolicy(
      Long id,
      String name,
      Namespace namespace,
      Policy.BuiltInType type,
      String comment,
      boolean enabled,
      PolicyContent content) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return PolicyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withPolicyType(type)
        .withComment(comment)
        .withEnabled(enabled)
        .withContent(content)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static PolicyPO createPolicyPO(
      Long id, String name, String policyType, Long metalakeId, PolicyVersionPO policyVersionPO)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return PolicyPO.builder()
        .withPolicyId(id)
        .withPolicyName(name)
        .withPolicyType(policyType)
        .withMetalakeId(metalakeId)
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .withPolicyVersionPO(policyVersionPO)
        .build();
  }

  private static PolicyVersionPO createPolicyVersionPO(
      Long id,
      Long metalakeId,
      Long policyId,
      String comment,
      boolean enabled,
      PolicyContent content)
      throws JsonProcessingException {
    return PolicyVersionPO.builder()
        .withId(id)
        .withMetalakeId(metalakeId)
        .withPolicyId(policyId)
        .withVersion(1L)
        .withPolicyComment(comment)
        .withEnabled(enabled)
        .withContent(JsonUtils.anyFieldMapper().writeValueAsString(content))
        .withDeletedAt(0L)
        .build();
  }

  private static TagPO createTagPO(Long id, String name, Long metalakeId, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = ImmutableMap.of("key", "value");
    return TagPO.builder()
        .withTagId(id)
        .withTagName(name)
        .withMetalakeId(metalakeId)
        .withComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
  }

  private static TagEntity createTag(Long id, String name, Namespace namespace, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = ImmutableMap.of("key", "value");
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }
}
