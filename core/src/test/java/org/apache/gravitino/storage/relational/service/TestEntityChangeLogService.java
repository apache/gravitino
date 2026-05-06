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
import org.apache.gravitino.storage.relational.po.auth.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestEntityChangeLogService extends TestJDBCBackend {
  private static final String METALAKE_NAME = "metalake_for_entity_change_log_test";
  private static final String CATALOG_NAME = "catalog_for_entity_change_log_test";
  private static final String SCHEMA_NAME = "schema_for_entity_change_log_test";

  private List<EntityChangeRecord> listEntityChanges(long createdAtAfter) {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, mapper -> mapper.selectChanges(createdAtAfter, 100));
  }

  private void assertEntityChange(
      long createdAtAfter,
      String metalakeName,
      Entity.EntityType entityType,
      String fullName,
      OperateType operateType) {
    Assertions.assertTrue(
        listEntityChanges(createdAtAfter).stream()
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

    long beforeRename = System.currentTimeMillis() - 1;
    BaseMetalake renamedMetalake =
        backend.update(
            metalake.nameIdentifier(),
            Entity.EntityType.METALAKE,
            entity ->
                createBaseMakeLake(
                    metalake.id(), METALAKE_NAME + "_renamed", metalake.auditInfo()));
    assertEntityChange(
        beforeRename, METALAKE_NAME, Entity.EntityType.METALAKE, METALAKE_NAME, OperateType.ALTER);

    long beforeDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(renamedMetalake.nameIdentifier(), false));
    assertEntityChange(
        beforeDrop,
        renamedMetalake.name(),
        Entity.EntityType.METALAKE,
        renamedMetalake.name(),
        OperateType.DROP);
  }

  @TestTemplate
  void testCatalogAndSchemaChangeLogOnRenameAndDrop() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    long beforeCatalogRename = System.currentTimeMillis() - 1;
    CatalogEntity renamedCatalog =
        backend.update(
            catalog.nameIdentifier(),
            Entity.EntityType.CATALOG,
            entity ->
                createCatalog(
                    catalog.id(), catalog.namespace(), CATALOG_NAME + "_renamed", AUDIT_INFO));
    assertEntityChange(
        beforeCatalogRename,
        METALAKE_NAME,
        Entity.EntityType.CATALOG,
        CATALOG_NAME,
        OperateType.ALTER);

    long beforeCatalogDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        CatalogMetaService.getInstance().deleteCatalog(renamedCatalog.nameIdentifier(), false));
    assertEntityChange(
        beforeCatalogDrop,
        METALAKE_NAME,
        Entity.EntityType.CATALOG,
        renamedCatalog.name(),
        OperateType.DROP);

    CatalogEntity schemaCatalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME + "_schema");
    SchemaEntity schema = createAndInsertSchema(METALAKE_NAME, schemaCatalog.name(), SCHEMA_NAME);
    long beforeSchemaRename = System.currentTimeMillis() - 1;
    SchemaEntity renamedSchema =
        backend.update(
            schema.nameIdentifier(),
            Entity.EntityType.SCHEMA,
            entity ->
                createSchemaEntity(
                    schema.id(), schema.namespace(), SCHEMA_NAME + "_renamed", AUDIT_INFO));
    assertEntityChange(
        beforeSchemaRename,
        METALAKE_NAME,
        Entity.EntityType.SCHEMA,
        schemaCatalog.name() + "." + SCHEMA_NAME,
        OperateType.ALTER);

    long beforeSchemaDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        SchemaMetaService.getInstance().deleteSchema(renamedSchema.nameIdentifier(), false));
    assertEntityChange(
        beforeSchemaDrop,
        METALAKE_NAME,
        Entity.EntityType.SCHEMA,
        schemaCatalog.name() + "." + renamedSchema.name(),
        OperateType.DROP);
  }

  @TestTemplate
  void testLeafEntityChangeLogOnRenameAndDrop() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    TableEntity table =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "table1", AUDIT_INFO);
    backend.insert(table, false);
    long beforeTableRename = System.currentTimeMillis() - 1;
    TableEntity renamedTable =
        TableMetaService.getInstance()
            .updateTable(
                table.nameIdentifier(),
                entity ->
                    createTableEntity(table.id(), table.namespace(), "table2", table.auditInfo()));
    assertEntityChange(
        beforeTableRename,
        METALAKE_NAME,
        Entity.EntityType.TABLE,
        CATALOG_NAME + "." + SCHEMA_NAME + ".table1",
        OperateType.ALTER);

    long beforeTableDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        TableMetaService.getInstance().deleteTable(renamedTable.nameIdentifier()));
    assertEntityChange(
        beforeTableDrop,
        METALAKE_NAME,
        Entity.EntityType.TABLE,
        CATALOG_NAME + "." + SCHEMA_NAME + ".table2",
        OperateType.DROP);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);
    long beforeTopicRename = System.currentTimeMillis() - 1;
    TopicEntity renamedTopic =
        backend.update(
            topic.nameIdentifier(),
            Entity.EntityType.TOPIC,
            entity -> createTopicEntity(topic.id(), topic.namespace(), "topic2", AUDIT_INFO));
    assertEntityChange(
        beforeTopicRename,
        METALAKE_NAME,
        Entity.EntityType.TOPIC,
        CATALOG_NAME + "." + SCHEMA_NAME + ".topic1",
        OperateType.ALTER);

    long beforeTopicDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        TopicMetaService.getInstance().deleteTopic(renamedTopic.nameIdentifier()));
    assertEntityChange(
        beforeTopicDrop,
        METALAKE_NAME,
        Entity.EntityType.TOPIC,
        CATALOG_NAME + "." + SCHEMA_NAME + ".topic2",
        OperateType.DROP);

    ViewEntity view =
        createViewEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofView(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "view1");
    ViewMetaService.getInstance().insertView(view, false);
    long beforeViewRename = System.currentTimeMillis() - 1;
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
        beforeViewRename,
        METALAKE_NAME,
        Entity.EntityType.VIEW,
        CATALOG_NAME + "." + SCHEMA_NAME + ".view1",
        OperateType.ALTER);

    long beforeViewDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(ViewMetaService.getInstance().deleteView(renamedView.nameIdentifier()));
    assertEntityChange(
        beforeViewDrop,
        METALAKE_NAME,
        Entity.EntityType.VIEW,
        CATALOG_NAME + "." + SCHEMA_NAME + ".view2",
        OperateType.DROP);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "fileset1",
            AUDIT_INFO);
    FilesetMetaService.getInstance().insertFileset(fileset, false);
    long beforeFilesetRename = System.currentTimeMillis() - 1;
    FilesetEntity renamedFileset =
        FilesetMetaService.getInstance()
            .updateFileset(
                fileset.nameIdentifier(),
                entity ->
                    createFilesetEntity(fileset.id(), fileset.namespace(), "fileset2", AUDIT_INFO));
    assertEntityChange(
        beforeFilesetRename,
        METALAKE_NAME,
        Entity.EntityType.FILESET,
        CATALOG_NAME + "." + SCHEMA_NAME + ".fileset1",
        OperateType.ALTER);

    long beforeFilesetDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        FilesetMetaService.getInstance().deleteFileset(renamedFileset.nameIdentifier()));
    assertEntityChange(
        beforeFilesetDrop,
        METALAKE_NAME,
        Entity.EntityType.FILESET,
        CATALOG_NAME + "." + SCHEMA_NAME + ".fileset2",
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
    long beforeModelRename = System.currentTimeMillis() - 1;
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
        beforeModelRename,
        METALAKE_NAME,
        Entity.EntityType.MODEL,
        CATALOG_NAME + "." + SCHEMA_NAME + ".model1",
        OperateType.ALTER);

    long beforeModelDrop = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        ModelMetaService.getInstance().deleteModel(renamedModel.nameIdentifier()));
    assertEntityChange(
        beforeModelDrop,
        METALAKE_NAME,
        Entity.EntityType.MODEL,
        CATALOG_NAME + "." + SCHEMA_NAME + ".model2",
        OperateType.DROP);
  }
}
