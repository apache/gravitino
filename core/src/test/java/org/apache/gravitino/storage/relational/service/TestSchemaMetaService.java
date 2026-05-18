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
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestSchemaMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_catalog_test";
  private final String catalogName = "catalog_for_catalog_test";

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
  public void testUpdateSchemaRenameAndProperties() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String originalName = "schema_to_rename";
    String renamedName = "schema_renamed";

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(originalName)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withComment("original comment")
            .withProperties(Collections.singletonMap("k1", "v1"))
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(schemaEntity, false);

    SchemaEntity updated =
        schemaMetaService.updateSchema(
            schemaEntity.nameIdentifier(),
            entity -> {
              SchemaEntity s = (SchemaEntity) entity;
              return SchemaEntity.builder()
                  .withId(s.id())
                  .withName(renamedName)
                  .withNamespace(s.namespace())
                  .withComment("updated comment")
                  .withProperties(Collections.singletonMap("k2", "v2"))
                  .withAuditInfo(s.auditInfo())
                  .build();
            });

    Assertions.assertEquals(renamedName, updated.name());
    Assertions.assertEquals("updated comment", updated.comment());
    Assertions.assertEquals(Map.of("k2", "v2"), updated.properties());

    SchemaEntity loaded =
        schemaMetaService.getSchemaByIdentifier(
            NameIdentifier.of(metalakeName, catalogName, renamedName));
    Assertions.assertEquals(schemaEntity.id(), loaded.id());
    Assertions.assertEquals(renamedName, loaded.name());
    Assertions.assertEquals("updated comment", loaded.comment());
    Assertions.assertEquals(Map.of("k2", "v2"), loaded.properties());

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            schemaMetaService.getSchemaByIdentifier(
                NameIdentifier.of(metalakeName, catalogName, originalName)),
        "Original name must no longer resolve after rename.");
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
  public void testDeleteSchemaNonCascadeFailsWhenNestedChildExists() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String leaf = "ns_a:ns_b";
    String ancestorA = "ns_a";
    SchemaEntity hierarchical =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(hierarchical, false);

    NameIdentifier ancestorIdent = NameIdentifier.of(metalakeName, catalogName, ancestorA);
    Assertions.assertThrows(
        NonEmptyEntityException.class,
        () -> schemaMetaService.deleteSchema(ancestorIdent, false),
        "Non-cascading delete must fail when nested descendant schemas exist.");

    // Ancestor row must still exist after the rejected delete.
    SchemaEntity stillThere = schemaMetaService.getSchemaByIdentifier(ancestorIdent);
    Assertions.assertEquals(ancestorA, stillThere.name());
  }

  @TestTemplate
  public void testDeleteSchemaCascadeRemovesNestedDescendants() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String leaf = "ns_a:ns_b:leaf";
    String ancestorA = "ns_a";
    String ancestorAB = "ns_a:ns_b";
    SchemaEntity hierarchical =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(leaf)
            .withNamespace(NamespaceUtil.ofSchema(metalakeName, catalogName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    schemaMetaService.insertSchema(hierarchical, false);

    // Insert a table under the deepest schema so we can verify it is also removed.
    Namespace leafNamespace = NamespaceUtil.ofTable(metalakeName, catalogName, leaf);
    TableEntity tableUnderLeaf =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(), leafNamespace, "table_under_leaf", AUDIT_INFO);
    backend.insert(tableUnderLeaf, false);

    NameIdentifier ancestorIdent = NameIdentifier.of(metalakeName, catalogName, ancestorA);
    Assertions.assertTrue(schemaMetaService.deleteSchema(ancestorIdent, true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> schemaMetaService.getSchemaByIdentifier(ancestorIdent),
        "Ancestor schema must be gone after cascade delete.");
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            schemaMetaService.getSchemaByIdentifier(
                NameIdentifier.of(metalakeName, catalogName, ancestorAB)),
        "Middle descendant schema must be gone after cascade delete.");
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            schemaMetaService.getSchemaByIdentifier(
                NameIdentifier.of(metalakeName, catalogName, leaf)),
        "Leaf descendant schema must be gone after cascade delete.");

    Assertions.assertFalse(
        backend.exists(tableUnderLeaf.nameIdentifier(), Entity.EntityType.TABLE),
        "Table under deepest descendant schema must be removed by cascade delete.");
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
}
