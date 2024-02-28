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
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.po.SchemaPO;
import com.datastrato.gravitino.storage.relational.po.TablePO;
import com.fasterxml.jackson.core.JsonProcessingException;
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
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");

    CatalogEntity convertedCatalog =
        POConverters.fromCatalogPO(catalogPO, Namespace.ofCatalog("test_metalake"));

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
            1L, "test", Namespace.ofSchema("test_metalake", "test_catalog"), "this is test");

    SchemaEntity convertedSchema =
        POConverters.fromSchemaPO(schemaPO, Namespace.ofSchema("test_metalake", "test_catalog"));

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
        createTable(1L, "test", Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));

    TableEntity convertedTable =
        POConverters.fromTablePO(
            tablePO, Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));

    // Assert
    assertEquals(expectedTable.id(), convertedTable.id());
    assertEquals(expectedTable.name(), convertedTable.name());
    assertEquals(expectedTable.namespace(), convertedTable.namespace());
    assertEquals(expectedTable.auditInfo().creator(), convertedTable.auditInfo().creator());
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
        POConverters.fromCatalogPOs(catalogPOs, Namespace.ofCatalog("test_metalake"));

    CatalogEntity expectedCatalog1 =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");
    CatalogEntity expectedCatalog2 =
        createCatalog(2L, "test2", Namespace.ofCatalog("test_metalake"), "this is test2");
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
        POConverters.fromSchemaPOs(schemaPOs, Namespace.ofSchema("test_metalake", "test_catalog"));

    SchemaEntity expectedSchema1 =
        createSchema(
            1L, "test", Namespace.ofSchema("test_metalake", "test_catalog"), "this is test");
    SchemaEntity expectedSchema2 =
        createSchema(
            2L, "test2", Namespace.ofSchema("test_metalake", "test_catalog"), "this is test2");
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
            tablePOs, Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));

    TableEntity expectedTable1 =
        createTable(1L, "test", Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));
    TableEntity expectedTable2 =
        createTable(2L, "test2", Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));
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
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");
    CatalogPO initPO = POConverters.initializeCatalogPOWithVersion(catalog, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitSchemaPOVersion() {
    SchemaEntity schema =
        createSchema(
            1L, "test", Namespace.ofSchema("test_metalake", "test_catalog"), "this is test");
    SchemaPO initPO = POConverters.initializeSchemaPOWithVersion(schema, 1L, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitTablePOVersion() {
    TableEntity tableEntity =
        createTable(1L, "test", Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));
    TablePO initPO = POConverters.initializeTablePOWithVersion(tableEntity, 1L, 1L, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
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
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");
    CatalogEntity updatedCatalog =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test2");
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
            1L, "test", Namespace.ofSchema("test_metalake", "test_catalog"), "this is test");
    SchemaEntity updatedSchema =
        createSchema(
            1L, "test", Namespace.ofSchema("test_metalake", "test_catalog"), "this is test2");
    SchemaPO initPO = POConverters.initializeSchemaPOWithVersion(schema, 1L, 1L);
    SchemaPO updatePO = POConverters.updateSchemaPOWithVersion(initPO, updatedSchema, 1L, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getSchemaComment());
  }

  @Test
  public void testUpdateTablePOVersion() {
    TableEntity tableEntity =
        createTable(1L, "test", Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));
    TableEntity updatedTable =
        createTable(1L, "test", Namespace.ofTable("test_metalake", "test_catalog", "test_schema"));
    TablePO initPO = POConverters.initializeTablePOWithVersion(tableEntity, 1L, 1L, 1L);
    TablePO updatePO = POConverters.updateTablePOWithVersion(initPO, updatedTable, 1L, 1L, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("test", updatePO.getTableName());
  }

  private static BaseMetalake createMetalake(Long id, String name, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return new BaseMetalake.Builder()
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
    return new MetalakePO.Builder()
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
    return new CatalogPO.Builder()
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
    return new SchemaEntity.Builder()
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
    return new SchemaPO.Builder()
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
    return new TableEntity.Builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static TablePO createTablePO(
      Long id, String name, Long metalakeId, Long catalogId, Long schemaId)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    return new TablePO.Builder()
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
}
