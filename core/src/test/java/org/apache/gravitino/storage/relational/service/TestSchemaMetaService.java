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
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
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
  public void testNestedSchemaInsertStoresPhysicalNameAndReturnsLogical() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String logicalName = "teamA:sales:reports";
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            logicalName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(schema, false);

    // Reading back via getSchemaByIdentifier must return the logical name.
    SchemaEntity loaded =
        schemaMetaService.getSchemaByIdentifier(
            NameIdentifierUtil.ofSchema(metalakeName, catalogName, logicalName));
    Assertions.assertEquals(logicalName, loaded.name());

    // The raw row stored in the DB must use the physical separator ".".
    String rawStoredName = readRawSchemaNameFromDb(schema.id());
    Assertions.assertEquals("teamA.sales.reports", rawStoredName);
  }

  @TestTemplate
  public void testFlatSchemaNameStoredAndReturnedUnchanged() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String flatName = "flat_schema";
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            flatName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(schema, false);

    SchemaEntity loaded =
        schemaMetaService.getSchemaByIdentifier(
            NameIdentifierUtil.ofSchema(metalakeName, catalogName, flatName));
    Assertions.assertEquals(flatName, loaded.name());

    // Flat name: raw DB value must equal the name as-is.
    Assertions.assertEquals(flatName, readRawSchemaNameFromDb(schema.id()));
  }

  @TestTemplate
  public void testListSchemasReturnsLogicalNames() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();

    SchemaEntity flat =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "flat",
            AUDIT_INFO);
    SchemaEntity nested2 =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "A:B",
            AUDIT_INFO);
    SchemaEntity nested3 =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            "A:B:C",
            AUDIT_INFO);
    schemaMetaService.insertSchema(flat, false);
    schemaMetaService.insertSchema(nested2, false);
    schemaMetaService.insertSchema(nested3, false);

    List<SchemaEntity> listed =
        schemaMetaService.listSchemasByNamespace(
            NamespaceUtil.ofSchema(metalakeName, catalogName));

    List<String> names = listed.stream().map(SchemaEntity::name).collect(java.util.stream.Collectors.toList());
    Assertions.assertTrue(names.contains("flat"), "flat name must be present");
    Assertions.assertTrue(names.contains("A:B"), "2-level logical name must be present");
    Assertions.assertTrue(names.contains("A:B:C"), "3-level logical name must be present");
    // Physical dot-separated names must NOT appear.
    Assertions.assertFalse(names.contains("A.B"), "physical name must not be exposed");
    Assertions.assertFalse(names.contains("A.B.C"), "physical name must not be exposed");
  }

  @TestTemplate
  public void testUpdateNestedSchemaPreservesLogicalName() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String logicalName = "dep:analytics";
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            logicalName,
            AUDIT_INFO);
    schemaMetaService.insertSchema(schema, false);

    NameIdentifier ident = NameIdentifierUtil.ofSchema(metalakeName, catalogName, logicalName);
    schemaMetaService.updateSchema(
        ident,
        entity -> {
          SchemaEntity s = (SchemaEntity) entity;
          return SchemaEntity.builder()
              .withId(s.id())
              .withName(s.name())
              .withNamespace(s.namespace())
              .withComment("updated comment")
              .withProperties(s.properties())
              .withAuditInfo(s.auditInfo())
              .build();
        });

    SchemaEntity updated = schemaMetaService.getSchemaByIdentifier(ident);
    // Name must still be logical after update.
    Assertions.assertEquals(logicalName, updated.name());
    Assertions.assertEquals("updated comment", updated.comment());
    // Physical storage must still use ".".
    Assertions.assertEquals("dep.analytics", readRawSchemaNameFromDb(schema.id()));
  }

  @TestTemplate
  public void testNestedSchemaInsertDuplicateThrows() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);

    SchemaMetaService schemaMetaService = SchemaMetaService.getInstance();
    String logicalName = "X:Y:Z";
    SchemaEntity first =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            logicalName,
            AUDIT_INFO);
    SchemaEntity duplicate =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            logicalName,
            AUDIT_INFO);

    schemaMetaService.insertSchema(first, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> schemaMetaService.insertSchema(duplicate, false),
        "Duplicate nested schema name must throw EntityAlreadyExistsException");
  }

  /**
   * Reads the raw {@code schema_name} value directly from the {@code schema_meta} table, bypassing
   * any logical conversion in {@link SchemaMetaService}. This lets us verify that the physical
   * (dot-separated) name is actually persisted in the database.
   */
  private String readRawSchemaNameFromDb(Long schemaId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT schema_name FROM schema_meta WHERE schema_id = %d", schemaId))) {
      if (rs.next()) {
        return rs.getString("schema_name");
      }
      throw new RuntimeException("No schema row found for id " + schemaId);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to read raw schema name", e);
    }
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
}
