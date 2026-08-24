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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
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
    long matchingChanges =
        listEntityChanges(lastConsumedId).stream()
            .filter(change -> metalakeName.equals(change.getMetalakeName()))
            .filter(change -> entityType.name().equals(change.getEntityType()))
            .filter(change -> fullName.equals(change.getFullName()))
            .filter(change -> operateType == change.getOperateType())
            .count();
    Assertions.assertEquals(
        1,
        matchingChanges,
        String.format(
            "Expected exactly one %s %s changelog for %s", entityType, operateType, fullName));
  }

  @TestTemplate
  void testMetalakeChangeLogOnRenameAndDrop() throws IOException {
    long maxIdBeforeCreate = maxEntityChangeId();
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    Assertions.assertEquals(maxIdBeforeCreate, maxEntityChangeId());

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
        backend.delete(renamedMetalake.nameIdentifier(), Entity.EntityType.METALAKE, false));
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

    long maxIdBeforeCatalogCreate = maxEntityChangeId();
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    Assertions.assertEquals(maxIdBeforeCatalogCreate, maxEntityChangeId());
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
        backend.delete(renamedCatalog.nameIdentifier(), Entity.EntityType.CATALOG, false));
    assertEntityChange(
        maxIdBeforeCatalogDrop,
        METALAKE_NAME,
        Entity.EntityType.CATALOG,
        NameIdentifierUtil.ofCatalog(METALAKE_NAME, renamedCatalog.name()).toString(),
        OperateType.DROP);

    CatalogEntity schemaCatalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME + "_schema");
    long maxIdBeforeSchemaCreate = maxEntityChangeId();
    SchemaEntity schema = createAndInsertSchema(METALAKE_NAME, schemaCatalog.name(), SCHEMA_NAME);
    Assertions.assertEquals(maxIdBeforeSchemaCreate, maxEntityChangeId());
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
        backend.delete(renamedSchema.nameIdentifier(), Entity.EntityType.SCHEMA, false));
    assertEntityChange(
        maxIdBeforeSchemaDrop,
        METALAKE_NAME,
        Entity.EntityType.SCHEMA,
        NameIdentifierUtil.ofSchema(METALAKE_NAME, schemaCatalog.name(), renamedSchema.name())
            .toString(),
        OperateType.DROP);
  }

  @TestTemplate
  void testEntityAndChangeLogRollbackTogether() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    long maxIdBeforeUpdate = maxEntityChangeId();

    SessionUtils.beginTransaction();
    try {
      backend.update(
          catalog.nameIdentifier(),
          Entity.EntityType.CATALOG,
          entity ->
              createCatalog(
                  catalog.id(), catalog.namespace(), CATALOG_NAME + "_rolled_back", AUDIT_INFO));
    } finally {
      SessionUtils.rollbackTransaction();
    }

    CatalogEntity persistedCatalog =
        backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertEquals(CATALOG_NAME, persistedCatalog.name());
    Assertions.assertEquals(maxIdBeforeUpdate, maxEntityChangeId());
  }

  @TestTemplate
  void testEntityUpdateRollsBackWhenChangeLogInsertFails() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    long maxIdBeforeUpdate = maxEntityChangeId();

    // Take the change-log table away so that the entity row is updated first and the change-log
    // insert then fails inside the same transaction. This is the only ordering the rollback in
    // JDBCBackend#update protects against, and the caller owns no outer transaction here.
    renameTable("entity_change_log", "entity_change_log_bak");
    try {
      Assertions.assertThrows(
          Exception.class,
          () ->
              backend.update(
                  catalog.nameIdentifier(),
                  Entity.EntityType.CATALOG,
                  entity ->
                      createCatalog(
                          catalog.id(),
                          catalog.namespace(),
                          CATALOG_NAME + "_rolled_back",
                          AUDIT_INFO)));
    } finally {
      renameTable("entity_change_log_bak", "entity_change_log");
    }

    // The entity mutation must not survive a failed change-log write.
    CatalogEntity persistedCatalog =
        backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertEquals(CATALOG_NAME, persistedCatalog.name());
    Assertions.assertEquals(maxIdBeforeUpdate, maxEntityChangeId());
  }

  private void renameTable(String from, String to) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("ALTER TABLE %s RENAME TO %s", from, to));
    }
  }

  @TestTemplate
  void testLeafEntityChangeLogOnRenameAndDrop() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Namespace namespace = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    TableEntity table =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "table1", AUDIT_INFO);
    long maxIdBeforeTableCreate = maxEntityChangeId();
    backend.insert(table, false);
    Assertions.assertEquals(maxIdBeforeTableCreate, maxEntityChangeId());
    long maxIdBeforeTableRename = maxEntityChangeId();
    TableEntity renamedTable =
        backend.update(
            table.nameIdentifier(),
            Entity.EntityType.TABLE,
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
        backend.delete(renamedTable.nameIdentifier(), Entity.EntityType.TABLE, false));
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
    long maxIdBeforeTopicCreate = maxEntityChangeId();
    backend.insert(topic, false);
    Assertions.assertEquals(maxIdBeforeTopicCreate, maxEntityChangeId());
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
        backend.delete(renamedTopic.nameIdentifier(), Entity.EntityType.TOPIC, false));
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
    long maxIdBeforeViewCreate = maxEntityChangeId();
    backend.insert(view, false);
    Assertions.assertEquals(maxIdBeforeViewCreate, maxEntityChangeId());
    long maxIdBeforeViewRename = maxEntityChangeId();
    ViewEntity renamedView =
        backend.update(
            view.nameIdentifier(),
            Entity.EntityType.VIEW,
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
    Assertions.assertTrue(
        backend.delete(renamedView.nameIdentifier(), Entity.EntityType.VIEW, false));
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
    long maxIdBeforeFilesetCreate = maxEntityChangeId();
    backend.insert(fileset, false);
    Assertions.assertEquals(maxIdBeforeFilesetCreate, maxEntityChangeId());
    long maxIdBeforeFilesetRename = maxEntityChangeId();
    FilesetEntity renamedFileset =
        backend.update(
            fileset.nameIdentifier(),
            Entity.EntityType.FILESET,
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
        backend.delete(renamedFileset.nameIdentifier(), Entity.EntityType.FILESET, false));
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

  @TestTemplate
  void testModelChangeLogEncodesNameLevelsContainingDots() throws IOException {
    String dottedSchema = "schema.with.dot";
    createParentEntities(METALAKE_NAME, CATALOG_NAME, dottedSchema, AUDIT_INFO);

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel(METALAKE_NAME, CATALOG_NAME, dottedSchema),
            "model1",
            "model comment",
            0,
            Map.of("k1", "v1"),
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);

    // Readers decode() every full_name, so the writer must encode() it. Asserting on the encoded
    // form (not toString()) is what pins the round trip: a raw "ml.cat.schema.with.dot.model1"
    // decodes to a six-level identifier under a schema named "schema", not this model.
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
        EntityChangeLogNameIdentifierCodec.encode(
            NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, dottedSchema, "model1")),
        OperateType.ALTER);

    long maxIdBeforeModelDrop = maxEntityChangeId();
    Assertions.assertTrue(
        ModelMetaService.getInstance().deleteModel(renamedModel.nameIdentifier()));
    assertEntityChange(
        maxIdBeforeModelDrop,
        METALAKE_NAME,
        Entity.EntityType.MODEL,
        EntityChangeLogNameIdentifierCodec.encode(
            NameIdentifierUtil.ofModel(METALAKE_NAME, CATALOG_NAME, dottedSchema, "model2")),
        OperateType.DROP);
  }

  @TestTemplate
  void testTagChangeLogOnAlterAndDrop() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    long maxIdBeforeCreate = maxEntityChangeId();
    TagEntity tag = createAndInsertTagEntity("tag1", "tag comment", METALAKE_NAME);
    Assertions.assertEquals(maxIdBeforeCreate, maxEntityChangeId());

    long maxIdBeforeTagAlter = maxEntityChangeId();
    backend.update(
        tag.nameIdentifier(),
        Entity.EntityType.TAG,
        entity ->
            TagEntity.builder()
                .withId(tag.id())
                .withName(tag.name())
                .withNamespace(tag.namespace())
                .withComment("tag comment updated")
                .withProperties(ImmutableMap.of())
                .withAuditInfo(AUDIT_INFO)
                .build());
    assertEntityChange(
        maxIdBeforeTagAlter,
        METALAKE_NAME,
        Entity.EntityType.TAG,
        NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1").toString(),
        OperateType.ALTER);

    long maxIdBeforeTagDrop = maxEntityChangeId();
    Assertions.assertTrue(backend.delete(tag.nameIdentifier(), Entity.EntityType.TAG, false));
    assertEntityChange(
        maxIdBeforeTagDrop,
        METALAKE_NAME,
        Entity.EntityType.TAG,
        NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1").toString(),
        OperateType.DROP);
  }

  @TestTemplate
  void testDottedNameUsesLosslessChangeLogEncoding() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    TagEntity tag = createAndInsertTagEntity("tag.with.dot", "tag comment", METALAKE_NAME);
    long maxIdBeforeAlter = maxEntityChangeId();

    backend.update(
        tag.nameIdentifier(),
        Entity.EntityType.TAG,
        entity ->
            TagEntity.builder()
                .withId(tag.id())
                .withName(tag.name())
                .withNamespace(tag.namespace())
                .withComment("updated comment")
                .withProperties(ImmutableMap.of())
                .withAuditInfo(AUDIT_INFO)
                .build());

    String encoded = EntityChangeLogNameIdentifierCodec.encode(tag.nameIdentifier());
    assertEntityChange(
        maxIdBeforeAlter, METALAKE_NAME, Entity.EntityType.TAG, encoded, OperateType.ALTER);
    Assertions.assertEquals(
        tag.nameIdentifier(), EntityChangeLogNameIdentifierCodec.decode(encoded));
  }

  @TestTemplate
  void testPolicyChangeLogOnAlterAndDrop() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("field1", 1), ImmutableSet.of(MetadataObject.Type.TABLE), null);
    long maxIdBeforeCreate = maxEntityChangeId();
    PolicyEntity policy =
        createAndInsertPolicyEntity("policy1", "policy comment", content, METALAKE_NAME);
    Assertions.assertEquals(maxIdBeforeCreate, maxEntityChangeId());

    long maxIdBeforePolicyAlter = maxEntityChangeId();
    backend.update(
        policy.nameIdentifier(),
        Entity.EntityType.POLICY,
        entity ->
            PolicyEntity.builder()
                .withId(policy.id())
                .withName(policy.name())
                .withNamespace(policy.namespace())
                .withPolicyType(Policy.BuiltInType.CUSTOM)
                .withComment("policy comment updated")
                .withEnabled(true)
                .withContent(content)
                .withAuditInfo(AUDIT_INFO)
                .build());
    assertEntityChange(
        maxIdBeforePolicyAlter,
        METALAKE_NAME,
        Entity.EntityType.POLICY,
        NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1").toString(),
        OperateType.ALTER);

    long maxIdBeforePolicyDrop = maxEntityChangeId();
    Assertions.assertTrue(backend.delete(policy.nameIdentifier(), Entity.EntityType.POLICY, false));
    assertEntityChange(
        maxIdBeforePolicyDrop,
        METALAKE_NAME,
        Entity.EntityType.POLICY,
        NameIdentifierUtil.ofPolicy(METALAKE_NAME, "policy1").toString(),
        OperateType.DROP);
  }

  @TestTemplate
  void testJobChangeLogOnOverwriteAndDrop() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    JobTemplateEntity jobTemplate =
        TestJobTemplateMetaService.newShellJobTemplateEntity(
            "job_template_for_change_log", "comment", METALAKE_NAME);
    JobTemplateMetaService.getInstance().insertJobTemplate(jobTemplate, false);

    JobEntity job =
        TestJobTemplateMetaService.newJobEntity(
            jobTemplate.name(), JobHandle.Status.QUEUED, METALAKE_NAME);

    // A plain insert (create) must not emit a change-log row: list bypasses the cache and there is
    // no negative caching.
    long maxIdBeforeCreate = maxEntityChangeId();
    backend.insert(job, false);
    Assertions.assertEquals(maxIdBeforeCreate, maxEntityChangeId());

    // An overwrite is an in-place status update, so it emits an ALTER row.
    long maxIdBeforeOverwrite = maxEntityChangeId();
    JobEntity runningJob =
        JobEntity.builder()
            .withId(job.id())
            .withJobExecutionId(job.jobExecutionId())
            .withNamespace(job.namespace())
            .withJobTemplateName(job.jobTemplateName())
            .withStatus(JobHandle.Status.STARTED)
            .withAuditInfo(AUDIT_INFO)
            .withStartedAt(System.currentTimeMillis())
            .withFinishedAt(0L)
            .build();
    backend.insert(runningJob, true);
    assertEntityChange(
        maxIdBeforeOverwrite,
        METALAKE_NAME,
        Entity.EntityType.JOB,
        job.nameIdentifier().toString(),
        OperateType.ALTER);

    long maxIdBeforeJobDrop = maxEntityChangeId();
    Assertions.assertTrue(backend.delete(job.nameIdentifier(), Entity.EntityType.JOB, false));
    assertEntityChange(
        maxIdBeforeJobDrop,
        METALAKE_NAME,
        Entity.EntityType.JOB,
        job.nameIdentifier().toString(),
        OperateType.DROP);
  }
}
