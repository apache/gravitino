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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestSchemaMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_catalog_test";
  private final String catalogName = "catalog_for_catalog_test";

  @TestTemplate
  void testDeleteSchemaAndGetRollsBackWhenCleanupFails() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createAndInsertSchema(metalakeName, catalogName, "schema_cleanup_failure");
    NameIdentifier ident = schema.nameIdentifier();
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(metalakeName, catalogName, schema.name()),
            "function",
            AUDIT_INFO);
    backend.insert(function, false);
    long lastChangeId =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
    NoSuchEntityException failure = new NoSuchEntityException("cleanup dependency is missing");

    assertThrows(
        NonEmptyEntityException.class,
        () ->
            backend.deleteSchemaAndGet(
                ident,
                false,
                ignored -> Assertions.fail("A non-cascading drop must preserve functions")));

    Assertions.assertSame(
        failure,
        assertThrows(
            NoSuchEntityException.class,
            () ->
                backend.deleteSchemaAndGet(
                    ident,
                    true,
                    deleted -> {
                      Assertions.assertEquals(schema.id(), deleted.id());
                      Assertions.assertEquals(schema.properties(), deleted.properties());
                      throw failure;
                    })));
    assertTrue(backend.exists(ident, Entity.EntityType.SCHEMA));
    Assertions.assertEquals(
        function.comment(),
        backend
            .<FunctionEntity>get(function.nameIdentifier(), Entity.EntityType.FUNCTION)
            .comment());
    assertTrue(
        SessionUtils.getWithoutCommit(
                EntityChangeLogMapper.class,
                mapper -> mapper.selectEntityChanges(lastChangeId, 100))
            .isEmpty());
    Assertions.assertEquals(
        schema.properties(),
        backend.<SchemaEntity>get(ident, Entity.EntityType.SCHEMA).properties());

    SchemaEntity deleted =
        backend
            .deleteSchemaAndGet(
                ident, true, snapshot -> Assertions.assertEquals(schema.id(), snapshot.id()))
            .orElseThrow();
    Assertions.assertEquals(schema.id(), deleted.id());
    assertFalse(backend.exists(ident, Entity.EntityType.SCHEMA));
    assertFalse(backend.exists(function.nameIdentifier(), Entity.EntityType.FUNCTION));
    List<EntityChangeRecord> changes =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(lastChangeId, 100));
    Assertions.assertEquals(2, changes.size());
    Assertions.assertEquals(
        Set.of(Entity.EntityType.SCHEMA.name(), Entity.EntityType.FUNCTION.name()),
        changes.stream().map(EntityChangeRecord::getEntityType).collect(Collectors.toSet()));
    assertTrue(changes.stream().allMatch(change -> change.getOperateType() == OperateType.DROP));
    assertFalse(
        backend
            .deleteSchemaAndGet(
                ident,
                true,
                ignored -> Assertions.fail("Cleanup must not run for a missing schema"))
            .isPresent());
  }

  @TestTemplate
  void testDeleteSchemaAndGetRejectsRemainingFilesetsBeforeCleanup() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema = createAndInsertSchema(metalakeName, catalogName, "schema_with_child");
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName, schema.name()),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(metalakeName, catalogName, schema.name()),
            "function",
            AUDIT_INFO);
    backend.insert(function, false);

    assertThrows(
        NonEmptyEntityException.class,
        () ->
            backend.deleteSchemaAndGet(
                schema.nameIdentifier(),
                true,
                ignored -> Assertions.fail("Cleanup must not run for a non-empty schema")));
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(fileset.nameIdentifier(), Entity.EntityType.FILESET));
    assertTrue(backend.exists(function.nameIdentifier(), Entity.EntityType.FUNCTION));
  }

  @TestTemplate
  void testDeleteSchemaAndGetPropagatesFunctionCleanupFailure() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    SchemaEntity schema =
        createAndInsertSchema(metalakeName, catalogName, "function_cleanup_failure");
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFunction(metalakeName, catalogName, schema.name()),
            "function",
            AUDIT_INFO);
    backend.insert(function, false);
    FunctionMetaService functionService = FunctionMetaService.getInstance();

    for (RuntimeException failure :
        List.of(
            new NoSuchEntityException("function disappeared"),
            new IllegalStateException("function cleanup failed"))) {
      FunctionMetaService failingService = Mockito.spy(functionService);
      Mockito.doThrow(failure).when(failingService).deleteFunction(function.nameIdentifier());
      try (MockedStatic<FunctionMetaService> mocked =
          Mockito.mockStatic(FunctionMetaService.class)) {
        mocked.when(FunctionMetaService::getInstance).thenReturn(failingService);
        RuntimeException actual =
            assertThrows(
                RuntimeException.class,
                () ->
                    backend.deleteSchemaAndGet(
                        schema.nameIdentifier(),
                        true,
                        ignored ->
                            Assertions.fail("Cleanup must not run after a function failure")));
        if (failure instanceof NoSuchEntityException) {
          Assertions.assertEquals(
              "Failed to delete functions under schema " + schema.nameIdentifier(),
              actual.getMessage());
          Assertions.assertSame(failure, actual.getCause());
        } else {
          Assertions.assertSame(failure, actual);
        }
      }
      assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
      assertTrue(backend.exists(function.nameIdentifier(), Entity.EntityType.FUNCTION));
    }

    FunctionMetaService failingService = Mockito.spy(functionService);
    Mockito.doReturn(false).when(failingService).deleteFunction(function.nameIdentifier());
    try (MockedStatic<FunctionMetaService> mocked = Mockito.mockStatic(FunctionMetaService.class)) {
      mocked.when(FunctionMetaService::getInstance).thenReturn(failingService);
      RuntimeException failure =
          assertThrows(
              RuntimeException.class,
              () ->
                  backend.deleteSchemaAndGet(
                      schema.nameIdentifier(),
                      true,
                      ignored -> Assertions.fail("Cleanup must not run when a function remains")));
      Assertions.assertInstanceOf(NoSuchEntityException.class, failure.getCause());
    }
    assertTrue(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(function.nameIdentifier(), Entity.EntityType.FUNCTION));
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    backend.insert(schema, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(schemaCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    SchemaEntity schemaCopy =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema1",
            AUDIT_INFO);
    backend.insert(schema, false);
    backend.insert(schemaCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                schemaCopy.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                e ->
                    createSchemaEntity(
                        schemaCopy.id(), schemaCopy.namespace(), "schema", AUDIT_INFO)));
  }

  @TestTemplate
  public void testUpdateSchemaCommentFromNull() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("schema_null_comment")
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(schemaEntity, false);

    schemaMetaService.updateSchema(
        schemaEntity.nameIdentifier(),
        entity -> {
          SchemaEntity schema = (SchemaEntity) entity;
          return SchemaEntity.builder()
              .withId(schema.id())
              .withName(schema.name())
              .withNamespace(schema.namespace())
              .withComment("schema comment updated")
              .withProperties(schema.properties())
              .withAuditInfo(schema.auditInfo())
              .build();
        });

    SchemaEntity updatedSchema =
        schemaMetaService.getSchemaByIdentifier(schemaEntity.nameIdentifier());
    Assertions.assertEquals("schema comment updated", updatedSchema.comment());
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema",
            AUDIT_INFO);
    backend.insert(schema, false);

    String anotherMetalakeName = "another-metalake";
    String anotherCatalogName = "another-catalog";
    createAndInsertMakeLake(anotherMetalakeName);
    createAndInsertCatalog(anotherMetalakeName, anotherCatalogName);
    SchemaEntity anotherSchema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(anotherMetalakeName, anotherCatalogName),
            "another-schema",
            AUDIT_INFO);
    backend.insert(anotherSchema, false);

    List<SchemaEntity> schemas = backend.list(schema.namespace(), Entity.EntityType.SCHEMA, true);
    assertTrue(schemas.contains(schema));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertTrue(backend.exists(anotherSchema.nameIdentifier(), Entity.EntityType.SCHEMA));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(schema.id(), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testDeleteSchemaNonCascadingFailsWhenTopicExists() throws IOException {

    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    TopicMetaService topicMetaService = TopicMetaService.getInstance();

    final String schemaName = "schema_with_topic";
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            schemaName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(schema, false);

    final String topicName = "test_topic_dependency";
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName),
            topicName,
            AUDIT_INFO);
    topicMetaService.insertTopic(topic, false);

    Assertions.assertThrows(
        NonEmptyEntityException.class,
        () -> schemaMetaService.deleteSchema(schema.nameIdentifier(), false),
        "Non-cascading delete must fail when dependent topics exist.");

    topicMetaService.deleteTopic(topic.nameIdentifier());
    schemaMetaService.deleteSchema(schema.nameIdentifier(), false);
  }

  @TestTemplate
  public void testInsertHierarchicalSchemaCreatesAncestorsAndLeaf() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String logicalLeaf = "ns_a:ns_b:leaf";
    SchemaEntity hierarchical =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(logicalLeaf)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("nested")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(hierarchical, false);

    List<SchemaEntity> schemas =
        schemaMetaService.listSchemasByNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName));
    Set<String> logicalNames = schemas.stream().map(SchemaEntity::name).collect(Collectors.toSet());

    Assertions.assertTrue(logicalNames.contains("ns_a"));
    Assertions.assertTrue(logicalNames.contains("ns_a:ns_b"));
    Assertions.assertTrue(logicalNames.contains(logicalLeaf));

    SchemaEntity loaded =
        schemaMetaService.getSchemaByIdentifier(
            NameIdentifier.of(metalakeName, catalogName, logicalLeaf));
    Assertions.assertEquals(logicalLeaf, loaded.name());
    Assertions.assertEquals("nested", loaded.comment());
  }

  @TestTemplate
  public void testDeleteHierarchicalSchemaCascadeRemovesDescendantsAndChildren()
      throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    TopicMetaService topicMetaService = TopicMetaService.getInstance();

    // Insert a leaf schema A:B:C; this auto-creates ancestor rows A and A:B.
    String leafName = "anc_a:anc_b:leaf_c";
    SchemaEntity leaf =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            leafName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(leaf, false);

    // Topic under the leaf, to verify child entities are also cascade-deleted.
    TopicEntity leafTopic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, leafName),
            "leaf_topic",
            AUDIT_INFO);
    topicMetaService.insertTopic(leafTopic, false);

    // Topic under the middle ancestor A:B.
    String middleName = "anc_a:anc_b";
    TopicEntity middleTopic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, middleName),
            "middle_topic",
            AUDIT_INFO);
    topicMetaService.insertTopic(middleTopic, false);

    // A sibling schema outside the deleted subtree to confirm it survives.
    String siblingName = "anc_a:sibling_d";
    SchemaEntity sibling =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            siblingName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(sibling, false);

    TopicEntity siblingTopic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTopic(metalakeName, catalogName, siblingName),
            "sibling_topic",
            AUDIT_INFO);
    topicMetaService.insertTopic(siblingTopic, false);

    // Cascade-delete the middle ancestor; both A:B and A:B:C (plus their topics) must go.
    schemaMetaService.deleteSchema(NameIdentifier.of(metalakeName, catalogName, middleName), true);

    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, middleName), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, leafName), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(backend.exists(leafTopic.nameIdentifier(), Entity.EntityType.TOPIC));
    Assertions.assertFalse(backend.exists(middleTopic.nameIdentifier(), Entity.EntityType.TOPIC));

    // Sibling subtree must still exist.
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, siblingName), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(backend.exists(siblingTopic.nameIdentifier(), Entity.EntityType.TOPIC));

    // The shared top-level ancestor A is outside the deleted subtree and must remain too.
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, "anc_a"), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testDeleteSchemaCascadeRemovesTagRelations() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "schema_with_tags",
            AUDIT_INFO);
    SchemaMetaService.getInstance().insertSchema(schema, false);

    Namespace objectNamespace = Namespace.of(metalakeName, catalogName, schema.name());
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column_with_tag")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_with_tag")
            .withNamespace(objectNamespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "topic_with_tag", AUDIT_INFO);
    TopicMetaService.getInstance().insertTopic(topic, false);
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "fileset_with_tag", AUDIT_INFO);
    FilesetMetaService.getInstance().insertFileset(fileset, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            objectNamespace,
            "model_with_tag",
            "comment",
            1,
            null,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "view_with_tag");
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "function_with_tag", AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    FunctionMetaService.getInstance().insertFunction(function, false);

    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    associateTag(tag, schema.nameIdentifier(), schema.type());
    associateTag(tag, table.nameIdentifier(), table.type());
    associateTag(
        tag,
        NameIdentifier.of(Namespace.fromString(table.nameIdentifier().toString()), column.name()),
        column.type());
    associateTag(tag, topic.nameIdentifier(), topic.type());
    associateTag(tag, fileset.nameIdentifier(), fileset.type());
    associateTag(tag, model.nameIdentifier(), model.type());
    associateTag(tag, view.nameIdentifier(), view.type());
    associateTag(tag, function.nameIdentifier(), function.type());

    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    Assertions.assertEquals(1, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));

    assertTrue(SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true));

    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    Assertions.assertEquals(0, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));
  }

  @TestTemplate
  public void testDeleteHierarchicalSchemaCascadeEscapesLikeMetacharacters() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();

    // Deleted subtree under ancestor "pa_b". The '_' is a LIKE wildcard, so without escaping the
    // cascade prefix "pa_b<sep>%" would also match the unrelated "paxb<sep>..." subtree below.
    String targetLeaf = "pa_b:leaf";
    schemaMetaService.insertSchema(
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            targetLeaf,
            AUDIT_INFO),
        false);

    // Decoy subtree that the unescaped '_' wildcard would falsely match ('x' in place of '_').
    String decoyLeaf = "paxb:leaf";
    schemaMetaService.insertSchema(
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            decoyLeaf,
            AUDIT_INFO),
        false);

    // Cascade-delete the literal "pa_b" ancestor.
    schemaMetaService.deleteSchema(NameIdentifier.of(metalakeName, catalogName, "pa_b"), true);

    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, "pa_b"), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, targetLeaf), Entity.EntityType.SCHEMA));

    // The decoy subtree must survive: literal-prefix matching only escapes the deleted subtree.
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, "paxb"), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(
        backend.exists(
            NameIdentifier.of(metalakeName, catalogName, decoyLeaf), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  public void testInsertHierarchicalSecondLeafReusesAncestorsWithoutUpsert() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String leaf1 = "ns_a:ns_b:leaf1";
    String leaf2 = "ns_a:ns_b:leaf2";
    String ancestorA = "ns_a";
    String ancestorAB = "ns_a:ns_b";

    SchemaEntity first =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf1)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("first")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(first, false);

    long idA =
        schemaMetaService
            .getSchemaByIdentifier(NameIdentifier.of(metalakeName, catalogName, ancestorA))
            .id();
    long idAB =
        schemaMetaService
            .getSchemaByIdentifier(NameIdentifier.of(metalakeName, catalogName, ancestorAB))
            .id();

    SchemaEntity second =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf2)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("second")
            .withProperties(Collections.emptyMap())
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(second, false);

    Assertions.assertEquals(
        idA,
        schemaMetaService
            .getSchemaByIdentifier(NameIdentifier.of(metalakeName, catalogName, ancestorA))
            .id());
    Assertions.assertEquals(
        idAB,
        schemaMetaService
            .getSchemaByIdentifier(NameIdentifier.of(metalakeName, catalogName, ancestorAB))
            .id());
  }

  private void associateTag(TagEntity tag, NameIdentifier ident, Entity.EntityType type)
      throws IOException {
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            ident,
            type,
            new NameIdentifier[] {NameIdentifierUtil.ofTag(metalakeName, tag.name())},
            new NameIdentifier[0]);
  }

  private int countActiveTagRelForMetadataObject(Long metadataObjectId, String metadataObjectType) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM tag_relation_meta"
                        + " WHERE metadata_object_id = %d AND metadata_object_type = '%s'"
                        + " AND deleted_at = 0",
                    metadataObjectId, metadataObjectType))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("No result for countActiveTagRelForMetadataObject");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }
}
