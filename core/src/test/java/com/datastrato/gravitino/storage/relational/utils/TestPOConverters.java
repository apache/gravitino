/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.po.FilesetPO;
import com.datastrato.gravitino.storage.relational.po.FilesetVersionPO;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.po.SchemaPO;
import com.datastrato.gravitino.storage.relational.po.TablePO;
import com.datastrato.gravitino.storage.relational.po.TopicPO;
import com.datastrato.gravitino.utils.NamespaceUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    assertEquals(1, initPO.getFilesetVersionPO().getVersion());
    assertEquals("hdfs://localhost/test", initPO.getFilesetVersionPO().getStorageLocation());
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
    TablePO updatePO = POConverters.updateTablePOWithVersion(initPO, updatedTable);
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
        POConverters.checkFilesetVersionNeedUpdate(initPO.getFilesetVersionPO(), updatedFileset);
    FilesetPO updatePO1 =
        POConverters.updateFilesetPOWithVersion(initPO, updatedFileset, checkNeedUpdate1);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals(
        updatedFileset.storageLocation(), updatePO1.getFilesetVersionPO().getStorageLocation());
    assertEquals(2, updatePO1.getCurrentVersion());
    assertEquals(2, updatePO1.getLastVersion());
    assertEquals(2, updatePO1.getFilesetVersionPO().getVersion());
    Map<String, String> updatedProperties =
        JsonUtils.anyFieldMapper()
            .readValue(updatePO1.getFilesetVersionPO().getProperties(), Map.class);
    assertEquals("value1", updatedProperties.get("key"));

    // will not update version, but update the fileset name
    boolean checkNeedUpdate2 =
        POConverters.checkFilesetVersionNeedUpdate(initPO.getFilesetVersionPO(), updatedFileset1);
    FilesetPO updatePO2 =
        POConverters.updateFilesetPOWithVersion(initPO, updatedFileset1, checkNeedUpdate2);
    assertEquals(
        filesetEntity.storageLocation(), updatePO2.getFilesetVersionPO().getStorageLocation());
    assertEquals(1, updatePO2.getCurrentVersion());
    assertEquals(1, updatePO2.getLastVersion());
    assertEquals(1, updatePO2.getFilesetVersionPO().getVersion());
    assertEquals("test1", updatePO2.getFilesetName());
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
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
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
        .withFilesetType(com.datastrato.gravitino.file.Fileset.Type.MANAGED)
        .withStorageLocation(storageLocation)
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
        .withType(com.datastrato.gravitino.file.Fileset.Type.MANAGED.name())
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .withFilesetVersionPO(filesetVersionPO)
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
        .withStorageLocation(storageLocation)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withVersion(1L)
        .withDeletedAt(0L)
        .build();
  }
}
