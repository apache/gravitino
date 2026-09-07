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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.TestBasePropertiesMetadata.COMMENT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSchemaOperationDispatcher extends TestOperationDispatcher {

  static SchemaOperationDispatcher dispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    dispatcher = new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(Configs.TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(Configs.TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(Configs.TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @Test
  public void testCreateAndListSchemas() throws IOException {
    Namespace ns = Namespace.of(metalake, catalog);

    NameIdentifier schemaIdent = NameIdentifier.of(ns, "schema1");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("schema1", schema.name());
    Assertions.assertEquals("comment", schema.comment());
    testProperties(props, schema.properties());

    // Test required schema properties exception
    Map<String, String> illegalSchemaProperties =
        new HashMap<String, String>() {
          {
            put("k2", "v2");
          }
        };

    testPropertyException(
        () -> dispatcher.createSchema(schemaIdent, "comment", illegalSchemaProperties),
        "Properties or property prefixes are required and must be set");

    // Test reserved table properties exception
    illegalSchemaProperties.put(COMMENT_KEY, "table comment");
    illegalSchemaProperties.put(ID_KEY, "gravitino.v1.uidfdsafdsa");
    testPropertyException(
        () -> dispatcher.createSchema(schemaIdent, "comment", illegalSchemaProperties),
        "Properties or property prefixes are reserved and cannot be set",
        "comment",
        "gravitino.identifier");

    // Check if the Schema entity is stored in the EntityStore
    SchemaEntity schemaEntity = entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class);
    Assertions.assertNotNull(schemaEntity);
    Assertions.assertEquals("schema1", schemaEntity.name());
    Assertions.assertNotNull(schemaEntity.id());

    Optional<NameIdentifier> ident1 =
        Arrays.stream(dispatcher.listSchemas(ns))
            .filter(s -> s.name().equals("schema1"))
            .findFirst();
    Assertions.assertTrue(ident1.isPresent());

    // Test when the entity store failed to put the schema entity
    doThrow(new IOException()).when(entityStore).put(any(), anyBoolean());
    NameIdentifier schemaIdent2 = NameIdentifier.of(ns, "schema2");
    Schema schema2 = dispatcher.createSchema(schemaIdent2, "comment", props);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("schema2", schema2.name());
    Assertions.assertEquals("comment", schema2.comment());
    testProperties(props, schema2.properties());

    // Check if the Schema entity is stored in the EntityStore
    Assertions.assertFalse(entityStore.exists(schemaIdent2, SCHEMA));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> entityStore.get(schemaIdent2, SCHEMA, SchemaEntity.class));

    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", schema2.auditInfo().creator());
  }

  @Test
  public void testCreateAndLoadSchema() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema20");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);
    Assertions.assertEquals("schema20", schema.name());
    Assertions.assertEquals("comment", schema.comment());
    testProperties(props, schema.properties());

    Schema loadedSchema = dispatcher.loadSchema(schemaIdent);
    Assertions.assertEquals(loadedSchema.name(), schema.name());
    Assertions.assertEquals(loadedSchema.comment(), schema.comment());
    testProperties(loadedSchema.properties(), schema.properties());
    // Audit info is gotten from the entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadedSchema.auditInfo().creator());

    // Case 2: Test if the schema is not found in entity store
    doThrow(new NoSuchEntityException("mock error"))
        .when(entityStore)
        .get(any(), eq(Entity.EntityType.SCHEMA), any());
    entityStore.delete(schemaIdent, Entity.EntityType.SCHEMA);
    Schema loadedSchema1 = dispatcher.loadSchema(schemaIdent);
    Assertions.assertEquals(schema.name(), loadedSchema1.name());
    Assertions.assertEquals(schema.comment(), loadedSchema1.comment());
    testProperties(props, loadedSchema1.properties());
    // Succeed to import the topic entity
    Assertions.assertTrue(entityStore.exists(schemaIdent, SCHEMA));

    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", loadedSchema1.auditInfo().creator());

    // Case 3: Test if entity store is failed to get the schema entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).get(any(), eq(Entity.EntityType.SCHEMA), any());
    entityStore.delete(schemaIdent, Entity.EntityType.SCHEMA);
    Schema loadedSchema2 = dispatcher.loadSchema(schemaIdent);
    // Succeed to import the topic entity
    Assertions.assertTrue(entityStore.exists(schemaIdent, SCHEMA));
    Assertions.assertEquals(schema.name(), loadedSchema2.name());
    Assertions.assertEquals(schema.comment(), loadedSchema2.comment());
    testProperties(props, loadedSchema2.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", loadedSchema2.auditInfo().creator());

    // Case 4: Test if the fetched schema entity is matched.
    reset(entityStore);
    SchemaEntity unmatchedEntity =
        SchemaEntity.builder()
            .withId(1L)
            .withName("schema21")
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(AuthConstants.ANONYMOUS_USER)
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).get(any(), eq(Entity.EntityType.SCHEMA), any());
    Schema loadedSchema3 = dispatcher.loadSchema(schemaIdent);
    // Succeed to import the schema entity
    reset(entityStore);
    SchemaEntity schemaEntity = entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class);
    Assertions.assertEquals("test", schemaEntity.auditInfo().creator());
    Assertions.assertEquals(schema.name(), loadedSchema3.name());
    Assertions.assertEquals(schema.comment(), loadedSchema3.comment());
    testProperties(props, loadedSchema3.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", loadedSchema3.auditInfo().creator());
  }

  @Test
  public void testConcurrentImportSchemaReusesExistingEntity() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schemaConcurrent");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(schemaIdent, "comment", props);
    SchemaEntity importedSchemaEntity = entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class);

    AuditInfo concurrentAudit =
        AuditInfo.builder().withCreator("concurrent").withCreateTime(Instant.now()).build();
    SchemaEntity concurrentSchemaEntity =
        SchemaEntity.builder()
            .withId(importedSchemaEntity.id())
            .withName(schemaIdent.name())
            .withNamespace(schemaIdent.namespace())
            .withAuditInfo(concurrentAudit)
            .build();

    // Simulate HA race: first two gets return not-found (so both the pre-import check and the
    // internalLoadSchema inside importSchema proceed to store.put), then put throws
    // EntityAlreadyExistsException, and the dispatcher-level retry sees the entity on the third
    // get.
    reset(entityStore);
    doThrow(new NoSuchEntityException("mock error"))
        .doThrow(new NoSuchEntityException("mock error"))
        .doReturn(concurrentSchemaEntity)
        .when(entityStore)
        .get(any(), eq(Entity.EntityType.SCHEMA), any());
    doThrow(new EntityAlreadyExistsException("mock conflict"))
        .when(entityStore)
        .put(any(), anyBoolean());

    Schema loadedSchema = Assertions.assertDoesNotThrow(() -> dispatcher.loadSchema(schemaIdent));
    Assertions.assertEquals(schemaIdent.name(), loadedSchema.name());
    Assertions.assertEquals("comment", loadedSchema.comment());
  }

  @Test
  public void testConcurrentImportSchemaFailsOnMismatchedIdentifier() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schemaConcurrentMismatch");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(schemaIdent, "comment", props);
    SchemaEntity importedSchemaEntity = entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class);

    AuditInfo concurrentAudit =
        AuditInfo.builder().withCreator("concurrent").withCreateTime(Instant.now()).build();
    SchemaEntity mismatchedSchemaEntity =
        SchemaEntity.builder()
            .withId(importedSchemaEntity.id() + 1)
            .withName(schemaIdent.name())
            .withNamespace(schemaIdent.namespace())
            .withAuditInfo(concurrentAudit)
            .build();

    // Simulate genuine multi-catalog conflict: put fails, and the dispatcher-level retry finds
    // an entity with a mismatched ID (operateOnEntity returns null → imported=false → error
    // thrown).
    reset(entityStore);
    doThrow(new NoSuchEntityException("mock error"))
        .doThrow(new NoSuchEntityException("mock error"))
        .doReturn(mismatchedSchemaEntity)
        .when(entityStore)
        .get(any(), eq(Entity.EntityType.SCHEMA), any());
    doThrow(new EntityAlreadyExistsException("mock conflict"))
        .when(entityStore)
        .put(any(), anyBoolean());

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> dispatcher.loadSchema(schemaIdent));
    Assertions.assertTrue(exception.getMessage().contains("Schema managed by multiple catalogs"));
  }

  @Test
  public void testCreateAndAlterSchema() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema21");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Schema schema = dispatcher.createSchema(schemaIdent, "comment", props);

    // Test immutable schema properties
    SchemaChange[] illegalChange =
        new SchemaChange[] {SchemaChange.setProperty(COMMENT_KEY, "new comment")};
    testPropertyException(
        () -> dispatcher.alterSchema(schemaIdent, illegalChange),
        "Property comment is immutable or reserved, cannot be set");

    SchemaChange[] changes =
        new SchemaChange[] {
          SchemaChange.setProperty("k3", "v3"), SchemaChange.removeProperty("k1")
        };

    Schema alteredSchema = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema.name());
    Assertions.assertEquals(schema.comment(), alteredSchema.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredSchema.properties());
    // Audit info is gotten from gravitino entity store.
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredSchema.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredSchema.auditInfo().lastModifier());

    // Case 2: Test if the schema is not found in entity store
    doThrow(new NoSuchEntityException("mock error"))
        .when(entityStore)
        .update(any(), any(), any(), any());
    Schema alteredSchema1 = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema1.name());
    Assertions.assertEquals(schema.comment(), alteredSchema1.comment());
    testProperties(expectedProps, alteredSchema1.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", alteredSchema1.auditInfo().creator());
    Assertions.assertEquals("test", alteredSchema1.auditInfo().lastModifier());

    // Case 3: Test if entity store is failed to get the schema entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).update(any(), any(), any(), any());
    Schema alteredSchema2 = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema2.name());
    Assertions.assertEquals(schema.comment(), alteredSchema2.comment());
    testProperties(expectedProps, alteredSchema2.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", alteredSchema2.auditInfo().creator());
    Assertions.assertEquals("test", alteredSchema1.auditInfo().lastModifier());

    // Case 4: Test if the fetched schema entity is matched.
    reset(entityStore);
    SchemaEntity unmatchedEntity =
        SchemaEntity.builder()
            .withId(1L)
            .withName("schema21")
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(AuthConstants.ANONYMOUS_USER)
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).update(any(), any(), any(), any());
    Schema alteredSchema3 = dispatcher.alterSchema(schemaIdent, changes);
    Assertions.assertEquals(schema.name(), alteredSchema3.name());
    Assertions.assertEquals(schema.comment(), alteredSchema3.comment());
    testProperties(expectedProps, alteredSchema3.properties());
    // Audit info is gotten from catalog, not from the entity store
    Assertions.assertEquals("test", alteredSchema3.auditInfo().creator());
    Assertions.assertEquals("test", alteredSchema1.auditInfo().lastModifier());
  }

  @Test
  public void testCreateAndDropSchema() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema31");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    dispatcher.createSchema(schemaIdent, "comment", props);

    boolean dropped = dispatcher.dropSchema(schemaIdent, false);
    Assertions.assertTrue(dropped);
    Assertions.assertFalse(dispatcher.dropSchema(schemaIdent, false));

    // Test if entity store is failed to drop the schema entity
    dispatcher.createSchema(schemaIdent, "comment", props);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(
        RuntimeException.class, () -> dispatcher.dropSchema(schemaIdent, false));
  }

  @Test
  public void testDropHierarchicalSchemaCleansUpOrphanedAncestors() throws IOException {
    // Clear any spy stubs leaked from other tests sharing the static entityStore.
    reset(entityStore);
    // Only the leaf "orphanA:orphanB:orphanC" is created in the catalog and the store. Names are
    // unique to this test because the catalog connector keeps its schemas in shared static state.
    NameIdentifier leaf = NameIdentifier.of(metalake, catalog, "orphanA:orphanB:orphanC");
    dispatcher.createSchema(leaf, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    // Simulate the ancestor entities the relational store auto-creates for a hierarchical name.
    NameIdentifier ancestorAb = NameIdentifier.of(metalake, catalog, "orphanA:orphanB");
    NameIdentifier ancestorA = NameIdentifier.of(metalake, catalog, "orphanA");
    putSchemaEntity(ancestorAb);
    putSchemaEntity(ancestorA);

    // The catalog only knows the leaf, so dropping it leaves the ancestors orphaned in the store;
    // since neither ancestor exists in the catalog, both entities must be cleaned up.
    Assertions.assertTrue(dispatcher.dropSchema(leaf, false));
    Assertions.assertFalse(entityStore.exists(ancestorAb, SCHEMA));
    Assertions.assertFalse(entityStore.exists(ancestorA, SCHEMA));
  }

  @Test
  public void testDropHierarchicalSchemaKeepsAncestorsThatStillExist() throws IOException {
    // Clear any spy stubs leaked from other tests sharing the static entityStore.
    reset(entityStore);
    // Both the parent "keepA:keepB" and the leaf "keepA:keepB:keepC" exist in the catalog and the
    // store. Names are unique to this test to avoid the connector's shared static schema state.
    NameIdentifier parentAb = NameIdentifier.of(metalake, catalog, "keepA:keepB");
    NameIdentifier leaf = NameIdentifier.of(metalake, catalog, "keepA:keepB:keepC");
    dispatcher.createSchema(parentAb, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));
    dispatcher.createSchema(leaf, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    // Simulate the top-level ancestor entity.
    NameIdentifier ancestorA = NameIdentifier.of(metalake, catalog, "keepA");
    putSchemaEntity(ancestorA);

    // "keepA:keepB" still exists in the catalog, so cleanup stops there and keeps it and "keepA".
    Assertions.assertTrue(dispatcher.dropSchema(leaf, false));
    Assertions.assertTrue(entityStore.exists(parentAb, SCHEMA));
    Assertions.assertTrue(entityStore.exists(ancestorA, SCHEMA));
  }

  private void putSchemaEntity(NameIdentifier ident) throws IOException {
    SchemaEntity entity =
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(entity, true);
  }
}
