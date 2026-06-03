/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestEntityChangeLogService extends TestJDBCBackend {
  private static final String METALAKE_NAME = "metalake_for_entity_change_log_test";
  private static final String CATALOG_NAME = "catalog_for_entity_change_log_test";
  private static final String SCHEMA_NAME = "schema_for_entity_change_log_test";

  private long maxEntityChangeId() {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
  }

  private List<EntityChangeRecord> listEntityChanges(long lastConsumedId) {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(lastConsumedId, 100));
  }

  private void assertEntityChange(
      long lastConsumedId,
      String metalakeName,
      Entity.EntityType entityType,
      String fullName,
      OperateType operateType) {
    Assertions.assertTrue(
        listEntityChanges(lastConsumedId).stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(entityType.name())
                        && record.getFullName().equals(fullName)
                        && record.getOperateType() == operateType),
        String.format("Missing %s %s changelog for %s", entityType, operateType, fullName));
  }

  @TestTemplate
  void testMetalakeChangeLogOnRenameAndDrop() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    long maxIdBeforeRename = maxEntityChangeId();
    BaseMetalake renamedMetalake =
        backend.update(
            metalake.nameIdentifier(),
            Entity.EntityType.METALAKE,
            entity ->
                createBaseMakeLake(
                    metalake.id(), METALAKE_NAME + "_renamed", metalake.auditInfo()));
    assertEntityChange(
        maxIdBeforeRename,
        METALAKE_NAME,
        Entity.EntityType.METALAKE,
        METALAKE_NAME,
        OperateType.ALTER);

    long maxIdBeforeDrop = maxEntityChangeId();
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(renamedMetalake.nameIdentifier(), false));
    assertEntityChange(
        maxIdBeforeDrop,
        renamedMetalake.name(),
        Entity.EntityType.METALAKE,
        renamedMetalake.name(),
        OperateType.DROP);
  }

  @TestTemplate
  void testCatalogAndSchemaChangeLogOnRenameAndDrop() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    long maxIdBeforeCatalogAlter = maxEntityChangeId();
    CatalogEntity alteredCatalog =
        backend.update(
            catalog.nameIdentifier(),
            Entity.EntityType.CATALOG,
            entity ->
                CatalogEntity.builder()
                    .withId(catalog.id())
                    .withNamespace(catalog.namespace())
                    .withName(CATALOG_NAME)
                    .withType(Catalog.Type.RELATIONAL)
                    .withProvider("test")
                    .withComment("updated comment")
                    .withProperties(null)
                    .withAuditInfo(AUDIT_INFO)
                    .build());
    assertEntityChange(
        maxIdBeforeCatalogAlter,
        METALAKE_NAME,
        Entity.EntityType.CATALOG,
        NameIdentifierUtil.ofCatalog(METALAKE_NAME, CATALOG_NAME).toString(),
        OperateType.ALTER);

    long maxIdBeforeCatalogRename = maxEntityChangeId();
    CatalogEntity renamedCatalog =
        backend.update(
            alteredCatalog.nameIdentifier(),
            Entity.EntityType.CATALOG,
            entity ->
                createCatalog(
                    alteredCatalog.id(),
                    alteredCatalog.namespace(),
                    CATALOG_NAME + "_renamed",
                    AUDIT_INFO));
    assertEntityChange(
        maxIdBeforeCatalogRename,
        METALAKE_NAME,
        Entity.EntityType.CATALOG,
        NameIdentifierUtil.ofCatalog(METALAKE_NAME, CATALOG_NAME).toString(),
        OperateType.ALTER);

    long maxIdBeforeCatalogDrop = maxEntityChangeId();
    Assertions.assertTrue(
        CatalogMetaService.getInstance().deleteCatalog(renamedCatalog.nameIdentifier(), false));
    assertEntityChange(
        maxIdBeforeCatalogDrop,
        METALAKE_NAME,
        Entity.EntityType.CATALOG,
        NameIdentifierUtil.ofCatalog(METALAKE_NAME, renamedCatalog.name()).toString(),
        OperateType.DROP);

    CatalogEntity schemaCatalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME + "_schema");
    SchemaEntity schema = createAndInsertSchema(METALAKE_NAME, schemaCatalog.name(), SCHEMA_NAME);
    long maxIdBeforeSchemaRename = maxEntityChangeId();
    SchemaEntity renamedSchema =
        backend.update(
            schema.nameIdentifier(),
            Entity.EntityType.SCHEMA,
            entity ->
                createSchemaEntity(
                    schema.id(), schema.namespace(), SCHEMA_NAME + "_renamed", AUDIT_INFO));
    assertEntityChange(
        maxIdBeforeSchemaRename,
        METALAKE_NAME,
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema(METALAKE_NAME, schemaCatalog.name(), SCHEMA_NAME).toString(),
        OperateType.ALTER);

    long maxIdBeforeSchemaDrop = maxEntityChangeId();
    Assertions.assertTrue(
        SchemaMetaService.getInstance().deleteSchema(renamedSchema.nameIdentifier(), false));
    assertEntityChange(
        maxIdBeforeSchemaDrop,
        METALAKE_NAME,
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema(METALAKE_NAME, schemaCatalog.name(), renamedSchema.name())
            .toString(),
        OperateType.DROP);
  }

  @TestTemplate
  void testLeafEntityChangeLogOnRenameAndDrop() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    TableEntity table =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "table1", AUDIT_INFO);
    backend.insert(table, false);
    long maxIdBeforeTableRename = maxEntityChangeId();
    TableEntity renamedTable =
        TableMetaService.getInstance()
            .updateTable(
                table.nameIdentifier(),
                entity ->
                    createTableEntity(table.id(), table.namespace(), "table2", table.auditInfo()));
    assertEntityChange(
        maxIdBeforeTableRename,
        METALAKE_NAME,
        Entity.EntityType.TABLE,
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table1").toString(),
        OperateType.ALTER);

    long maxIdBeforeTableDrop = maxEntityChangeId();
    Assertions.assertTrue(
        TableMetaService.getInstance().deleteTable(renamedTable.nameIdentifier()));
    assertEntityChange(
        maxIdBeforeTableDrop,
        METALAKE_NAME,
        Entity.EntityType.TABLE,
        NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "table2").toString(),
        OperateType.DROP);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);
    long maxIdBeforeTopicRename = maxEntityChangeId();
    TopicEntity renamedTopic =
        backend.update(
            topic.nameIdentifier(),
            Entity.EntityType.TOPIC,
            entity -> createTopicEntity(topic.id(), topic.namespace(), "topic2", AUDIT_INFO));
    assertEntityChange(
        maxIdBeforeTopicRename,
        METALAKE_NAME,
        Entity.EntityType.TOPIC,
        NameIdentifierUtil.ofTopic(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "topic1").toString(),
        OperateType.ALTER);

    long maxIdBeforeTopicDrop = maxEntityChangeId();
    Assertions.assertTrue(
        TopicMetaService.getInstance().deleteTopic(renamedTopic.nameIdentifier()));
    assertEntityChange(
        maxIdBeforeTopicDrop,
        METALAKE_NAME,
        Entity.EntityType.TOPIC,
        NameIdentifierUtil.ofTopic(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "topic2").toString(),
        OperateType.DROP);

    ViewEntity view =
        createViewEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofView(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "view1");
    ViewMetaService.getInstance().insertView(view, false);
    long maxIdBeforeViewRename = maxEntityChangeId();
    ViewEntity renamedView =
        ViewMetaService.getInstance()
            .updateView(
                view.nameIdentifier(),
                entity ->
                    ViewEntity.builder()
                        .withId(view.id())
                        .withName("view2")
                        .withNamespace(view.namespace())
                        .withColumns(view.columns())
                        .withRepresentations(view.representations())
                        .withAuditInfo(view.auditInfo())
                        .build());
    assertEntityChange(
        maxIdBeforeViewRename,
        METALAKE_NAME,
        Entity.EntityType.VIEW,
        NameIdentifierUtil.ofView(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "view1").toString(),
        OperateType.ALTER);

    long maxIdBeforeViewDrop = maxEntityChangeId();
    Assertions.assertTrue(ViewMetaService.getInstance().deleteView(renamedView.nameIdentifier()));
    assertEntityChange(
        maxIdBeforeViewDrop,
        METALAKE_NAME,
        Entity.EntityType.VIEW,
        NameIdentifierUtil.ofView(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "view2").toString(),
        OperateType.DROP);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "fileset1",
            AUDIT_INFO);
    FilesetMetaService.getInstance().insertFileset(fileset, false);
    long maxIdBeforeFilesetRename = maxEntityChangeId();
    FilesetEntity renamedFileset =
        FilesetMetaService.getInstance()
            .updateFileset(
                fileset.nameIdentifier(),
                entity ->
                    createFilesetEntity(fileset.id(), fileset.namespace(), "fileset2", AUDIT_INFO));
    assertEntityChange(
        maxIdBeforeFilesetRename,
        METALAKE_NAME,
        Entity.EntityType.FILESET,
        NameIdentifierUtil.ofFileset(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "fileset1")
            .toString(),
        OperateType.ALTER);

    long maxIdBeforeFilesetDrop = maxEntityChangeId();
    Assertions.assertTrue(
        FilesetMetaService.getInstance().deleteFileset(renamedFileset.nameIdentifier()));
    assertEntityChange(
        maxIdBeforeFilesetDrop,
        METALAKE_NAME,
        Entity.EntityType.FILESET,
        NameIdentifierUtil.ofFileset(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "fileset2")
            .toString(),
        OperateType.DROP);

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "model1",
            "model comment",
            0,
            Map.of("k1", "v1"),
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    long maxIdBeforeModelRename = maxEntityChangeId();
    ModelEntity renamedModel =
        ModelMetaService.getInstance()
            .updateModel(
                model.nameIdentifier(),
                entity ->
                    createModelEntity(
                        model.id(),
                        model.namespace(),
                        "model2",
                        model.comment(),
                        model.latestVersion(),
                        model.properties(),
                        AUDIT_INFO));
    assertEntityChange(
        maxIdBeforeModelRename,
        METALAKE_NAME,
        Entity.EntityType.MODEL,
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "model1").toString(),
        OperateType.ALTER);

    long maxIdBeforeModelDrop = maxEntityChangeId();
    Assertions.assertTrue(
        ModelMetaService.getInstance().deleteModel(renamedModel.nameIdentifier()));
    assertEntityChange(
        maxIdBeforeModelDrop,
        METALAKE_NAME,
        Entity.EntityType.MODEL,
        NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, "model2").toString(),
        OperateType.DROP);
  }
}
