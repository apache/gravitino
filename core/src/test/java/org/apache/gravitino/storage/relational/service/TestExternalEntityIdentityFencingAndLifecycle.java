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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.TestTemplate;

/** Tests identity-fencing (#13172), import collisions (#13173), and lifecycle checks (#13176). */
public class TestExternalEntityIdentityFencingAndLifecycle extends TestJDBCBackend {

  private static final String METALAKE = "fencing_metalake";
  private static final String CATALOG = "fencing_catalog";
  private static final String SCHEMA = "fencing_schema";

  /**
   * Sets up parent hierarchy for entity tests.
   *
   * @throws IOException if database insert fails.
   */
  private void setUpHierarchy() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
  }

  /**
   * Tests table identity-fenced update and delete.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testTableIdentityFencedDeleteAndUpdate() throws IOException {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String tableName = "table_fenced";
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, tableName);

    long idA = 1001L;
    TableEntity tableA = createTableEntity(idA, tableNs, tableName, AUDIT_INFO);
    backend.insert(tableA, false);

    long idB = 1002L;
    TableEntity tableB = createTableEntity(idB, tableNs, tableName, AUDIT_INFO);
    backend.insert(tableB, true);

    // Stale delete using idA must fail with OptimisticLockException
    assertThrows(
        OptimisticLockException.class,
        () -> backend.delete(tableIdent, EntityType.TABLE, false, idA, null));

    // Entity B survives
    TableEntity current = backend.get(tableIdent, EntityType.TABLE);
    assertNotNull(current);
    assertEquals(idB, current.id());

    // Stale update using idA must fail with OptimisticLockException
    assertThrows(
        OptimisticLockException.class,
        () ->
            backend.update(
                tableIdent,
                EntityType.TABLE,
                (TableEntity entity) ->
                    TableEntity.builder()
                        .withId(entity.id())
                        .withName(entity.name())
                        .withNamespace(entity.namespace())
                        .withComment("updated_comment")
                        .withAuditInfo(entity.auditInfo())
                        .build(),
                idA));

    // Matching update using idB succeeds
    TableEntity updated =
        backend.update(
            tableIdent,
            EntityType.TABLE,
            (TableEntity entity) ->
                TableEntity.builder()
                    .withId(entity.id())
                    .withName(entity.name())
                    .withNamespace(entity.namespace())
                    .withComment("updated_comment")
                    .withAuditInfo(entity.auditInfo())
                    .build(),
            idB);
    assertEquals("updated_comment", updated.comment());

    // Fenced delete using idB succeeds
    assertTrue(backend.delete(tableIdent, EntityType.TABLE, false, idB, null));
    assertThrows(NoSuchEntityException.class, () -> backend.get(tableIdent, EntityType.TABLE));
  }

  /**
   * Tests that if an entity was unobserved initially, concurrent registration under the same name
   * is preserved and not deleted.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testInitiallyUnobservedDropPreservesConcurrentRegistration() throws IOException {
    setUpHierarchy();
    Namespace tableNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String tableName = "table_unobserved_preserve";
    NameIdentifier tableIdent = NameIdentifier.of(tableNs, tableName);

    // Initial state: table does not exist in store
    assertThrows(NoSuchEntityException.class, () -> backend.get(tableIdent, EntityType.TABLE));

    // Concurrently, a new table is registered
    long idB = 8002L;
    TableEntity tableB = createTableEntity(idB, tableNs, tableName, AUDIT_INFO);
    backend.insert(tableB, false);

    // Stale/unobserved drop must not delete without verified identity.
    // An attempt to delete with stale/nonexistent idA (e.g. 8001L) must fail with
    // OptimisticLockException
    assertThrows(
        OptimisticLockException.class,
        () -> backend.delete(tableIdent, EntityType.TABLE, false, 8001L, null));

    // The newer table entity survives untouched
    TableEntity current = backend.get(tableIdent, EntityType.TABLE);
    assertNotNull(current);
    assertEquals(idB, current.id());
  }

  /**
   * Tests schema identity-fenced update and delete.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testSchemaIdentityFencedDeleteAndUpdate() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    Namespace schemaNs = Namespace.of(METALAKE, CATALOG);
    String schemaName = "schema_fenced_test";
    NameIdentifier schemaIdent = NameIdentifier.of(schemaNs, schemaName);

    long idA = 2001L;
    SchemaEntity schemaA = createSchemaEntity(idA, schemaNs, schemaName, AUDIT_INFO);
    backend.insert(schemaA, false);

    long idB = 2002L;
    SchemaEntity schemaB = createSchemaEntity(idB, schemaNs, schemaName, AUDIT_INFO);
    backend.delete(schemaIdent, EntityType.SCHEMA, false);
    backend.insert(schemaB, false);

    // Stale delete with idA must fail
    assertThrows(
        OptimisticLockException.class,
        () -> backend.delete(schemaIdent, EntityType.SCHEMA, false, idA, null));

    SchemaEntity current = backend.get(schemaIdent, EntityType.SCHEMA);
    assertEquals(idB, current.id());

    // Stale update with idA must fail
    assertThrows(
        OptimisticLockException.class,
        () ->
            backend.update(
                schemaIdent,
                EntityType.SCHEMA,
                (SchemaEntity entity) ->
                    SchemaEntity.builder()
                        .withId(entity.id())
                        .withName(entity.name())
                        .withNamespace(entity.namespace())
                        .withComment("updated_schema")
                        .withAuditInfo(entity.auditInfo())
                        .build(),
                idA));

    // Fenced delete with idB succeeds
    assertTrue(backend.delete(schemaIdent, EntityType.SCHEMA, false, idB, null));
    assertThrows(NoSuchEntityException.class, () -> backend.get(schemaIdent, EntityType.SCHEMA));
  }

  /**
   * Tests topic identity-fenced update and delete.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testTopicIdentityFencedDeleteAndUpdate() throws IOException {
    setUpHierarchy();
    Namespace topicNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String topicName = "topic_fenced";
    NameIdentifier topicIdent = NameIdentifier.of(topicNs, topicName);

    long idA = 3001L;
    TopicEntity topicA = createTopicEntity(idA, topicNs, topicName, AUDIT_INFO);
    backend.insert(topicA, false);

    long idB = 3002L;
    TopicEntity topicB = createTopicEntity(idB, topicNs, topicName, AUDIT_INFO);
    backend.delete(topicIdent, EntityType.TOPIC, false);
    backend.insert(topicB, false);

    assertThrows(
        OptimisticLockException.class,
        () -> backend.delete(topicIdent, EntityType.TOPIC, false, idA, null));

    TopicEntity current = backend.get(topicIdent, EntityType.TOPIC);
    assertEquals(idB, current.id());

    assertThrows(
        OptimisticLockException.class,
        () ->
            backend.update(
                topicIdent,
                EntityType.TOPIC,
                (TopicEntity entity) ->
                    TopicEntity.builder()
                        .withId(entity.id())
                        .withName(entity.name())
                        .withNamespace(entity.namespace())
                        .withComment("updated_topic")
                        .withAuditInfo(entity.auditInfo())
                        .build(),
                idA));

    assertTrue(backend.delete(topicIdent, EntityType.TOPIC, false, idB, null));
    assertThrows(NoSuchEntityException.class, () -> backend.get(topicIdent, EntityType.TOPIC));
  }

  /**
   * Tests view identity-fenced update and delete.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testViewIdentityFencedDeleteAndUpdate() throws IOException {
    setUpHierarchy();
    Namespace viewNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String viewName = "view_fenced";
    NameIdentifier viewIdent = NameIdentifier.of(viewNs, viewName);

    long idA = 4001L;
    ViewEntity viewA = createViewEntity(idA, viewNs, viewName);
    backend.insert(viewA, false);

    long idB = 4002L;
    ViewEntity viewB = createViewEntity(idB, viewNs, viewName);
    backend.delete(viewIdent, EntityType.VIEW, false);
    backend.insert(viewB, false);

    assertThrows(
        OptimisticLockException.class,
        () -> backend.delete(viewIdent, EntityType.VIEW, false, idA, null));

    ViewEntity current = backend.get(viewIdent, EntityType.VIEW);
    assertEquals(idB, current.id());

    assertThrows(
        OptimisticLockException.class,
        () ->
            backend.update(
                viewIdent,
                EntityType.VIEW,
                (ViewEntity entity) ->
                    ViewEntity.builder()
                        .withId(entity.id())
                        .withName(entity.name())
                        .withNamespace(entity.namespace())
                        .withComment("updated_view")
                        .withColumns(entity.columns())
                        .withRepresentations(entity.representations())
                        .withAuditInfo(entity.auditInfo())
                        .build(),
                idA));

    assertTrue(backend.delete(viewIdent, EntityType.VIEW, false, idB, null));
    assertThrows(NoSuchEntityException.class, () -> backend.get(viewIdent, EntityType.VIEW));
  }

  /**
   * Tests function identity-fenced update and delete.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testFunctionIdentityFencedDeleteAndUpdate() throws IOException {
    setUpHierarchy();
    Namespace funcNs = Namespace.of(METALAKE, CATALOG, SCHEMA);
    String funcName = "func_fenced";
    NameIdentifier funcIdent = NameIdentifier.of(funcNs, funcName);

    long idA = 5001L;
    FunctionEntity funcA = createFunctionEntity(idA, funcNs, funcName, AUDIT_INFO);
    backend.insert(funcA, false);

    long idB = 5002L;
    FunctionEntity funcB = createFunctionEntity(idB, funcNs, funcName, AUDIT_INFO);
    backend.delete(funcIdent, EntityType.FUNCTION, false);
    backend.insert(funcB, false);

    assertThrows(
        OptimisticLockException.class,
        () -> backend.delete(funcIdent, EntityType.FUNCTION, false, idA, null));

    FunctionEntity current = backend.get(funcIdent, EntityType.FUNCTION);
    assertEquals(idB, current.id());

    assertThrows(
        OptimisticLockException.class,
        () ->
            backend.update(
                funcIdent,
                EntityType.FUNCTION,
                (FunctionEntity entity) ->
                    FunctionEntity.builder()
                        .withId(entity.id())
                        .withName(entity.name())
                        .withNamespace(entity.namespace())
                        .withComment("updated_func")
                        .withFunctionType(entity.functionType())
                        .withDeterministic(entity.deterministic())
                        .withDefinitions(entity.definitions())
                        .withAuditInfo(entity.auditInfo())
                        .build(),
                idA));

    assertTrue(backend.delete(funcIdent, EntityType.FUNCTION, false, idB, null));
    assertThrows(NoSuchEntityException.class, () -> backend.get(funcIdent, EntityType.FUNCTION));
  }

  /**
   * Tests import collision checks for table, schema, topic, view, and function.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testImportCollisions() throws IOException {
    setUpHierarchy();
    Namespace ns = Namespace.of(METALAKE, CATALOG, SCHEMA);

    // 1. Table import collision
    long tableId = 1100L;
    TableEntity table1 = createTableEntity(tableId, ns, "imported_table_1", AUDIT_INFO);
    backend.insert(table1, false);

    // Attempting to import another table reusing the same ID tableId must throw
    // EntityAlreadyExistsException
    TableEntity table2 = createTableEntity(tableId, ns, "imported_table_2", AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(table2, false));

    // 2. Schema import collision
    Namespace schemaNs = Namespace.of(METALAKE, CATALOG);
    long schemaId = 1200L;
    SchemaEntity schema1 = createSchemaEntity(schemaId, schemaNs, "imported_schema_1", AUDIT_INFO);
    backend.insert(schema1, false);

    SchemaEntity schema2 = createSchemaEntity(schemaId, schemaNs, "imported_schema_2", AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(schema2, false));

    // 3. Topic import collision
    long topicId = 1300L;
    TopicEntity topic1 = createTopicEntity(topicId, ns, "imported_topic_1", AUDIT_INFO);
    backend.insert(topic1, false);

    TopicEntity topic2 = createTopicEntity(topicId, ns, "imported_topic_2", AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(topic2, false));

    // 4. View import collision
    long viewId = 1400L;
    ViewEntity view1 = createViewEntity(viewId, ns, "imported_view_1");
    backend.insert(view1, false);

    ViewEntity view2 = createViewEntity(viewId, ns, "imported_view_2");
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(view2, false));

    // 5. Function import collision
    long funcId = 1500L;
    FunctionEntity func1 = createFunctionEntity(funcId, ns, "imported_func_1", AUDIT_INFO);
    backend.insert(func1, false);

    FunctionEntity func2 = createFunctionEntity(funcId, ns, "imported_func_2", AUDIT_INFO);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(func2, false));
  }

  /**
   * Tests catalog and metalake lifecycle preconditions inside transactions.
   *
   * @throws IOException on error.
   */
  @TestTemplate
  public void testTransactionalLifecyclePreconditions() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    createAndInsertSchema(METALAKE, CATALOG, SCHEMA);

    NameIdentifier catalogIdent = NameIdentifier.of(METALAKE, CATALOG);
    NameIdentifier metalakeIdent = NameIdentifier.of(METALAKE);

    // Non-force drop of catalog with user schemas must fail
    assertThrows(
        NonEmptyEntityException.class,
        () -> backend.delete(catalogIdent, EntityType.CATALOG, false));

    // User schema still exists
    assertNotNull(backend.get(NameIdentifier.of(METALAKE, CATALOG, SCHEMA), EntityType.SCHEMA));

    // Force drop of catalog succeeds
    assertTrue(backend.delete(catalogIdent, EntityType.CATALOG, true));

    // Test catalog with only built-in schema for provider "kafka"
    String kafkaCatalogName = "kafka_catalog";
    NameIdentifier kafkaCatalogIdent = NameIdentifier.of(METALAKE, kafkaCatalogName);
    CatalogEntity kafkaCatalog =
        CatalogEntity.builder()
            .withId(9999L)
            .withName(kafkaCatalogName)
            .withNamespace(Namespace.of(METALAKE))
            .withType(org.apache.gravitino.Catalog.Type.MESSAGING)
            .withProvider("kafka")
            .withComment("")
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(kafkaCatalog, false);

    // Built-in schema "default"
    SchemaEntity builtInSchema =
        createSchemaEntity(8888L, Namespace.of(METALAKE, kafkaCatalogName), "default", AUDIT_INFO);
    backend.insert(builtInSchema, false);

    // Non-force drop of kafka catalog with only built-in schema "default" succeeds
    assertTrue(backend.delete(kafkaCatalogIdent, EntityType.CATALOG, false));

    // Re-create a catalog to test metalake drop
    createAndInsertCatalog(METALAKE, CATALOG);

    // Non-force drop of metalake with catalogs must fail
    assertThrows(
        NonEmptyEntityException.class,
        () -> backend.delete(metalakeIdent, EntityType.METALAKE, false));

    // Force drop of metalake succeeds
    assertTrue(backend.delete(metalakeIdent, EntityType.METALAKE, true));
    assertThrows(
        NoSuchEntityException.class, () -> backend.get(metalakeIdent, EntityType.METALAKE));
  }
}
