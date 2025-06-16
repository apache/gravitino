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

package org.apache.gravitino.server.authorization;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.MockedStatic;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMetadataIdConverter {
  private EntityStore mockStore;
  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;
  private NameIdentifier ident7;

  // Test Entities
  private BaseMetalake entity1;
  private CatalogEntity entity2;
  private SchemaEntity entity3;
  private TableEntity entity4;
  private ModelEntity entity5;
  private FilesetEntity entity6;
  private TopicEntity entity7;

  @BeforeAll
  void initTest() throws IOException {
    initTestNameIdentifier();
    initTestEntities();
    initMockCache();
  }

  @Test
  void testConvert() throws IOException, IllegalAccessException {
    CatalogManager mockCatalogManager = mock(CatalogManager.class);
    Object originalCatalogManager =
        FieldUtils.readDeclaredField(GravitinoEnv.getInstance(), "catalogManager", true);
    Object originalEntityStore =
        FieldUtils.readDeclaredField(GravitinoEnv.getInstance(), "entityStore", true);

    FieldUtils.writeDeclaredField(
        GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
    FieldUtils.writeDeclaredField(GravitinoEnv.getInstance(), "entityStore", mockStore, true);

    try (MockedStatic<MetadataIdConverter> mockedStatic =
        mockStatic(MetadataIdConverter.class, CALLS_REAL_METHODS)) {
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident1), eq(null), eq(mockCatalogManager)))
          .thenReturn(ident1);
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident2), eq(null), eq(mockCatalogManager)))
          .thenReturn(ident2);
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident3), eq(Capability.Scope.SCHEMA), eq(mockCatalogManager)))
          .thenReturn(ident3);
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident4), eq(Capability.Scope.TABLE), eq(mockCatalogManager)))
          .thenReturn(ident4);
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident5), eq(Capability.Scope.MODEL), eq(mockCatalogManager)))
          .thenReturn(ident5);
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident6), eq(Capability.Scope.FILESET), eq(mockCatalogManager)))
          .thenReturn(ident6);
      mockedStatic
          .when(
              () ->
                  MetadataIdConverter.normalizeCaseSensitive(
                      eq(ident7), eq(Capability.Scope.TOPIC), eq(mockCatalogManager)))
          .thenReturn(ident7);

      Long metalakeConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(ImmutableList.of("metalake"), MetadataObject.Type.METALAKE),
              "metalake");
      Long catalogConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(ImmutableList.of("catalog"), MetadataObject.Type.CATALOG),
              "metalake");
      Long schemaConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(ImmutableList.of("catalog", "schema"), MetadataObject.Type.SCHEMA),
              "metalake");
      Long tableConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(
                  ImmutableList.of("catalog", "schema", "table"), MetadataObject.Type.TABLE),
              "metalake");
      Long modelConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(
                  ImmutableList.of("catalog", "schema", "model"), MetadataObject.Type.MODEL),
              "metalake");
      Long filesetConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(
                  ImmutableList.of("catalog", "schema", "fileset"), MetadataObject.Type.FILESET),
              "metalake");
      Long topicConvertedId =
          MetadataIdConverter.getID(
              MetadataObjects.of(
                  ImmutableList.of("catalog", "schema", "topic"), MetadataObject.Type.TOPIC),
              "metalake");

      Assertions.assertEquals(1L, metalakeConvertedId);
      Assertions.assertEquals(2L, catalogConvertedId);
      Assertions.assertEquals(3L, schemaConvertedId);
      Assertions.assertEquals(4L, tableConvertedId);
      Assertions.assertEquals(5L, modelConvertedId);
      Assertions.assertEquals(6L, filesetConvertedId);
      Assertions.assertEquals(7L, topicConvertedId);
    } finally {
      FieldUtils.writeDeclaredField(
          GravitinoEnv.getInstance(), "catalogManager", originalCatalogManager, true);
      FieldUtils.writeDeclaredField(
          GravitinoEnv.getInstance(), "entityStore", originalEntityStore, true);
    }
  }

  private void initTestNameIdentifier() {
    ident1 = NameIdentifier.of("metalake");
    ident2 = NameIdentifier.of("metalake", "catalog");
    ident3 = NameIdentifier.of("metalake", "catalog", "schema");
    ident4 = NameIdentifier.of("metalake", "catalog", "schema", "table");
    ident5 = NameIdentifier.of("metalake", "catalog", "schema", "model");
    ident6 = NameIdentifier.of("metalake", "catalog", "schema", "fileset");
    ident7 = NameIdentifier.of("metalake", "catalog", "schema", "topic");
  }

  private void initTestEntities() {
    entity1 = getTestMetalake(1L, "metalake", "test_metalake");
    entity2 = getTestCatalogEntity(2L, "catlaog", Namespace.of("metalake"), "hive", "test_catalog");
    entity3 = getTestSchemaEntity(3L, "schema", Namespace.of("metalake", "catalog"), "test_schema");
    entity4 = getTestTableEntity(4L, "table", Namespace.of("metalake", "catalog", "schema"));
    entity5 = getTestModelEntity(5L, "model", Namespace.of("metalake", "catalog", "schema"));
    entity6 =
        getTestFileSetEntity(
            6L,
            "fileset",
            "file:///tmp/fileset_test",
            Namespace.of("metalake", "catalog", "schema"),
            "test_fileset",
            Fileset.Type.EXTERNAL);
    entity7 =
        getTestTopicEntity(
            7L, "topic", Namespace.of("metalake", "catalog", "schema"), "test_topic");
  }

  private void initMockCache() throws IOException {
    mockStore = mock(EntityStore.class);

    when(mockStore.get(ident1, Entity.EntityType.METALAKE, BaseMetalake.class)).thenReturn(entity1);
    when(mockStore.get(ident2, Entity.EntityType.CATALOG, CatalogEntity.class)).thenReturn(entity2);
    when(mockStore.get(ident3, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(entity3);
    when(mockStore.get(ident4, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity4);
    when(mockStore.get(ident5, Entity.EntityType.MODEL, ModelEntity.class)).thenReturn(entity5);
    when(mockStore.get(ident6, Entity.EntityType.FILESET, FilesetEntity.class)).thenReturn(entity6);
    when(mockStore.get(ident7, Entity.EntityType.TOPIC, TopicEntity.class)).thenReturn(entity7);
  }

  private BaseMetalake getTestMetalake(long id, String name, String comment) {
    return BaseMetalake.builder()
        .withId(id)
        .withName(name)
        .withVersion(SchemaVersion.V_0_1)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  public CatalogEntity getTestCatalogEntity(
      long id, String name, Namespace namespace, String provider, String comment) {
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider(provider)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  private SchemaEntity getTestSchemaEntity(
      long id, String name, Namespace namespace, String comment) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  private TableEntity getTestTableEntity(long id, String name, Namespace namespace) {

    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(getTestAuditInfo())
        .withNamespace(namespace)
        .withColumns(ImmutableList.of(getMockColumnEntity()))
        .build();
  }

  private FilesetEntity getTestFileSetEntity(
      long id,
      String name,
      String storageLocation,
      Namespace namespace,
      String comment,
      Fileset.Type type) {
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withStorageLocations(ImmutableMap.of("location1", storageLocation))
        .withFilesetType(type)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  private ModelEntity getTestModelEntity(long id, String name, Namespace namespace) {
    return ModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withLatestVersion(1)
        .withAuditInfo(getTestAuditInfo())
        .build();
  }

  private AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  private ColumnEntity getMockColumnEntity() {
    ColumnEntity mockColumn = mock(ColumnEntity.class);
    when(mockColumn.name()).thenReturn("filed1");
    when(mockColumn.dataType()).thenReturn(Types.StringType.get());
    when(mockColumn.nullable()).thenReturn(false);
    when(mockColumn.auditInfo()).thenReturn(getTestAuditInfo());

    return mockColumn;
  }

  private TopicEntity getTestTopicEntity(
      long id, String name, Namespace namespace, String comment) {
    return TopicEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }
}
