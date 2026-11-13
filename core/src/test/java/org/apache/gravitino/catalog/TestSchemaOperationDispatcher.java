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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.TestSchema;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSchemaOperationDispatcher extends TestOperationDispatcher {

  static SchemaOperationDispatcher dispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    dispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(Configs.TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(Configs.TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(Configs.TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  /**
   * Imports write creation audit once and recover safely after competing imports or deletion.
   *
   * @param scenario the source audit and competing-import ordering to exercise
   */
  @ParameterizedTest
  @ValueSource(strings = {"missing", "reported", "modified", "load", "raced", "deleted"})
  public void testImportCreationAudit(String scenario) throws Exception {
    Instant importTime = Instant.EPOCH.plusSeconds(60);
    AuditInfo importAudit =
        AuditInfo.builder().withCreator("alice").withCreateTime(importTime).build();
    AuditInfo catalogAudit =
        "reported".equals(scenario)
            ? AuditInfo.builder().withCreator("catalog_user").withCreateTime(Instant.EPOCH).build()
            : "modified".equals(scenario)
                ? AuditInfo.builder()
                    .withCreateTime(Instant.EPOCH)
                    .withLastModifier("modifier")
                    .withLastModifiedTime(Instant.EPOCH)
                    .build()
                : AuditInfo.EMPTY;
    String expectedCreator =
        "reported".equals(scenario) ? "catalog_user" : "load".equals(scenario) ? null : "alice";
    Instant expectedTime =
        "reported".equals(scenario) || "modified".equals(scenario)
            ? Instant.EPOCH
            : "load".equals(scenario) ? null : importTime;
    NameIdentifier schemaIdent =
        NameIdentifier.of(metalake, catalog, "schema_import_audit_schema_" + scenario);
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(schemaIdent, "comment", props);
    TestCatalog testCatalog =
        (TestCatalog)
            catalogManager.loadCatalogAndWrap(NameIdentifier.of(metalake, catalog)).catalog();
    TestCatalogOperations operations = (TestCatalogOperations) testCatalog.ops();
    NameIdentifier identifier = schemaIdent;
    TestSchema catalogSchema = (TestSchema) operations.loadSchema(identifier);
    FieldUtils.writeField(catalogSchema, "auditInfo", catalogAudit, true);
    entityStore.delete(identifier, SCHEMA);
    clearInvocations(entityStore);

    if ("raced".equals(scenario) || "deleted".equals(scenario)) {
      dispatcher.loadSchema(identifier);
      clearInvocations(entityStore);
    }
    if ("deleted".equals(scenario)) {
      doAnswer(
              call -> {
                entityStore.delete(identifier, SCHEMA);
                throw new OptimisticLockException("Registration deleted during audit recovery");
              })
          .when(entityStore)
          .update(eq(identifier), eq(SchemaEntity.class), eq(SCHEMA), any());
    }
    Schema loaded =
        "load".equals(scenario)
            ? dispatcher.loadSchema(identifier)
            : dispatcher.importSchema(identifier, importAudit);

    if ("deleted".equals(scenario)) {
      EntityCombinedSchema combined = (EntityCombinedSchema) loaded;
      Assertions.assertFalse(combined.imported());
      Assertions.assertNull(combined.schemaEntity());
      Assertions.assertNull(loaded.auditInfo().creator());
      Assertions.assertEquals(identifier.name(), loaded.name());
      Assertions.assertEquals(props, loaded.properties());
      verify(entityStore, never()).put(any(SchemaEntity.class), anyBoolean());
      verify(entityStore).update(eq(identifier), eq(SchemaEntity.class), eq(SCHEMA), any());
      reset(entityStore);
      dispatcher.loadSchema(identifier);
      Assertions.assertNull(
          entityStore.get(identifier, SCHEMA, SchemaEntity.class).auditInfo().creator());
      return;
    }

    SchemaEntity imported = entityStore.get(identifier, SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(expectedCreator, imported.auditInfo().creator());
    Assertions.assertEquals(expectedCreator, loaded.auditInfo().creator());
    Assertions.assertEquals(expectedTime, loaded.auditInfo().createTime());
    Assertions.assertEquals(
        "modified".equals(scenario) ? "modifier" : null, imported.auditInfo().lastModifier());
    Assertions.assertEquals(
        "modified".equals(scenario) ? Instant.EPOCH : null,
        imported.auditInfo().lastModifiedTime());
    Assertions.assertEquals(expectedTime, imported.auditInfo().createTime());
    if ("raced".equals(scenario)) {
      verify(entityStore, never()).put(any(SchemaEntity.class), anyBoolean());
      verify(entityStore).update(eq(identifier), eq(SchemaEntity.class), eq(SCHEMA), any());
    } else {
      verify(entityStore).put(any(SchemaEntity.class), anyBoolean());
      verify(entityStore, never()).update(any(), any(), any(), any());
    }

    clearInvocations(entityStore);
    dispatcher.importSchema(
        identifier,
        AuditInfo.builder().withCreator("bob").withCreateTime(importTime.plusSeconds(60)).build());
    Assertions.assertEquals(
        "load".equals(scenario) ? "bob" : expectedCreator,
        entityStore.get(identifier, SCHEMA, SchemaEntity.class).auditInfo().creator());
    Assertions.assertEquals(
        "load".equals(scenario) ? importTime.plusSeconds(60) : expectedTime,
        entityStore.get(identifier, SCHEMA, SchemaEntity.class).auditInfo().createTime());
    verify(entityStore, never()).put(any(), anyBoolean());
    if ("load".equals(scenario)) {
      verify(entityStore).update(eq(identifier), eq(SchemaEntity.class), eq(SCHEMA), any());
    } else {
      verify(entityStore, never()).update(any(), any(), any(), any());
    }
  }

  @Test
  void testExternalRenamePreservesSchemaIdentity() throws Exception {
    NameIdentifier original = NameIdentifier.of(metalake, catalog, "external_rename_original");
    NameIdentifier renamed = NameIdentifier.of(metalake, catalog, "external_rename_new");
    dispatcher.createSchema(original, "original", ImmutableMap.of("k1", "v1", "k2", "v2"));
    SchemaEntity originalEntity = entityStore.get(original, SCHEMA, SchemaEntity.class);
    Map<String, String> properties = new HashMap<>(originalEntity.properties());
    properties.put("k1", "v1");
    properties.put("k2", "v2");
    properties.put(ID_KEY, StringIdentifier.fromId(originalEntity.id()).toString());
    catalogManager.doWithCatalogWrapper(
        NameIdentifier.of(metalake, catalog),
        c -> {
          c.doWithSchemaOps(
              ops -> {
                ops.dropSchema(original, false);
                return ops.createSchema(renamed, "renamed", properties);
              });
          return null;
        });
    SchemaAlreadyExistsException conflict =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () ->
                dispatcher.createSchema(
                    original, "replacement", ImmutableMap.of("k1", "v1", "k2", "v2")));
    Assertions.assertTrue(conflict.getMessage().contains(original.toString()));
    Assertions.assertTrue(conflict.getMessage().contains("loadSchema under its new name"));
    Assertions.assertEquals(
        originalEntity.id(), entityStore.get(original, SCHEMA, SchemaEntity.class).id());
    catalogManager.doWithCatalogWrapper(
        NameIdentifier.of(metalake, catalog),
        c -> {
          boolean originalExists = c.doWithSchemaOps(ops -> ops.schemaExists(original));
          boolean renamedExists = c.doWithSchemaOps(ops -> ops.schemaExists(renamed));
          Assertions.assertFalse(originalExists);
          Assertions.assertTrue(renamedExists);
          return null;
        });
    Assertions.assertEquals(renamed.name(), dispatcher.loadSchema(renamed).name());
    Assertions.assertEquals(
        originalEntity.id(), entityStore.get(renamed, SCHEMA, SchemaEntity.class).id());
    Assertions.assertFalse(entityStore.exists(original, SCHEMA));
    dispatcher.createSchema(original, "replacement", ImmutableMap.of("k1", "v1", "k2", "v2"));
    Assertions.assertNotEquals(
        originalEntity.id(), entityStore.get(original, SCHEMA, SchemaEntity.class).id());
    Assertions.assertEquals(
        originalEntity.id(), entityStore.get(renamed, SCHEMA, SchemaEntity.class).id());
  }

  @Test
  void testCreateSchemaStopsBeforeSourceWhenRegistrationLookupFails() throws Exception {
    NameIdentifier ident = NameIdentifier.of(metalake, catalog, "schema_preflight_failure");
    IOException failure = new IOException("registration lookup failed");
    doThrow(failure).when(entityStore).exists(ident, SCHEMA);
    GravitinoRuntimeException error =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> dispatcher.createSchema(ident, "", ImmutableMap.of("k1", "v1", "k2", "v2")));
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertTrue(error.getMessage().contains(ident.toString()));
    Assertions.assertTrue(error.getMessage().contains("source creation has not been attempted"));
    catalogManager.doWithCatalogWrapper(
        NameIdentifier.of(metalake, catalog),
        c -> {
          boolean sourceExists = c.doWithSchemaOps(ops -> ops.schemaExists(ident));
          Assertions.assertFalse(sourceExists);
          return null;
        });
    verify(entityStore, never()).put(any(SchemaEntity.class), anyBoolean());
  }

  @Test
  void testCopiedSchemaIdentifierCannotMoveExistingEntity() throws Exception {
    NameIdentifier original = NameIdentifier.of(metalake, catalog, "marker_original");
    NameIdentifier copy = NameIdentifier.of(metalake, catalog, "marker_copy");
    dispatcher.createSchema(original, "original", ImmutableMap.of("k1", "v1", "k2", "v2"));
    SchemaEntity originalEntity = entityStore.get(original, SCHEMA, SchemaEntity.class);
    Map<String, String> copiedProperties = new HashMap<>();
    copiedProperties.put("k1", "v1");
    copiedProperties.put("k2", "v2");
    copiedProperties.put(ID_KEY, StringIdentifier.fromId(originalEntity.id()).toString());
    catalogManager.doWithCatalogWrapper(
        NameIdentifier.of(metalake, catalog),
        c -> {
          c.doWithSchemaOps(ops -> ops.createSchema(copy, "copy", copiedProperties));
          return null;
        });
    Assertions.assertThrows(SchemaAlreadyExistsException.class, () -> dispatcher.loadSchema(copy));
    Assertions.assertEquals(
        originalEntity.id(), entityStore.get(original, SCHEMA, SchemaEntity.class).id());
    Assertions.assertFalse(entityStore.exists(copy, SCHEMA));
    Assertions.assertTrue(dispatcher.dropSchema(copy, false));
    Assertions.assertEquals(
        originalEntity.id(), entityStore.get(original, SCHEMA, SchemaEntity.class).id());
    Assertions.assertEquals(
        originalEntity.properties(),
        entityStore.get(original, SCHEMA, SchemaEntity.class).properties());
  }

  /** Explicit imports require audit information. */
  @Test
  public void testImportRejectsNullAudit() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.importSchema(NameIdentifier.of(metalake, catalog, "missing"), null));
  }

  @Test
  public void testCreateAndListSchemas() throws Exception {
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

    IOException registrationFailure = new IOException("store unavailable");
    doThrow(registrationFailure).when(entityStore).put(any(), anyBoolean());
    NameIdentifier schemaIdent2 = NameIdentifier.of(ns, "schema2");
    GravitinoRuntimeException error =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> dispatcher.createSchema(schemaIdent2, "comment", props));
    Assertions.assertSame(registrationFailure, error.getCause());
    Assertions.assertTrue(error.getMessage().contains(schemaIdent2.toString()));
    Assertions.assertTrue(error.getMessage().contains("created in the external catalog"));
    Assertions.assertTrue(error.getMessage().contains("use loadSchema to import it"));
    Assertions.assertTrue(error.getMessage().contains("Do not repeat createSchema"));
    Assertions.assertFalse(entityStore.exists(schemaIdent2, SCHEMA));
    Schema created =
        catalogManager
            .loadCatalogAndWrap(NameIdentifier.of(metalake, catalog))
            .doWithSchemaOps(ops -> ops.loadSchema(schemaIdent2));
    Assertions.assertEquals("schema2", created.name());
    Assertions.assertEquals("comment", created.comment());
    reset(entityStore);
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> dispatcher.createSchema(schemaIdent2, "comment", props));
    Assertions.assertFalse(entityStore.exists(schemaIdent2, SCHEMA));
    dispatcher.loadSchema(schemaIdent2);
    Assertions.assertTrue(entityStore.exists(schemaIdent2, SCHEMA));
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
    doReturn(Collections.emptyList())
        .when(entityStore)
        .list(schemaIdent.namespace(), SchemaEntity.class, SCHEMA);
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
    verify(entityStore).put(any(SchemaEntity.class), eq(false));
  }

  @Test
  void testImportSchemaAfterRegistrationDeletionReusesIdentifier() throws IOException {
    NameIdentifier schemaIdent =
        NameIdentifier.of(metalake, catalog, "schema_soft_deleted_identifier");
    dispatcher.createSchema(schemaIdent, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));
    SchemaEntity schemaEntity = entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class);
    entityStore.delete(schemaIdent, SCHEMA);

    reset(entityStore);
    Assertions.assertDoesNotThrow(() -> dispatcher.loadSchema(schemaIdent));
    Assertions.assertEquals(
        schemaEntity.id(), entityStore.get(schemaIdent, SCHEMA, SchemaEntity.class).id());
    verify(entityStore).put(any(SchemaEntity.class), eq(false));
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
    doReturn(Collections.emptyList())
        .when(entityStore)
        .list(schemaIdent.namespace(), SchemaEntity.class, SCHEMA);
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
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "does not automatically reconcile conflicting external schema registrations"));
    Assertions.assertTrue(exception.getMessage().contains("catalog-specific recovery guidance"));
    verify(entityStore).put(any(SchemaEntity.class), eq(false));
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
  public void testImportSchemaPropagatesOptimisticLockConflict() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema_import_occ");
    dispatcher.createSchema(schemaIdent, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));
    entityStore.delete(schemaIdent, SCHEMA);

    // The schema is only in the catalog, so loading it imports it into the store.
    doThrow(new NoSuchEntityException("mock error"))
        .when(entityStore)
        .get(any(), eq(Entity.EntityType.SCHEMA), any());
    doThrow(new OptimisticLockException("mock conflict"))
        .when(entityStore)
        .put(any(SchemaEntity.class), anyBoolean());
    try {
      Assertions.assertThrows(
          OptimisticLockException.class, () -> dispatcher.loadSchema(schemaIdent));
    } finally {
      reset(entityStore);
    }
  }

  @Test
  public void testDropSchemaPropagatesOptimisticLockConflict() throws IOException {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema_drop_occ");
    dispatcher.createSchema(schemaIdent, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    // A conflict of the store, such as a lock wait timeout, reaches the caller as a conflict
    // rather than an internal error.
    doThrow(new OptimisticLockException("mock conflict"))
        .when(entityStore)
        .delete(any(), any(), anyBoolean());
    try {
      Assertions.assertThrows(
          OptimisticLockException.class, () -> dispatcher.dropSchema(schemaIdent, false));
    } finally {
      reset(entityStore);
    }
  }

  @Test
  public void testDropMissingSchemaPreservesStoredEntity() throws IOException {
    reset(entityStore);
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema_renamed_out_of_band");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(schemaIdent, "comment", props);

    catalogManager.doWithCatalog(
        NameIdentifier.of(metalake, catalog),
        liveCatalog -> {
          TestCatalogOperations testCatalogOperations = (TestCatalogOperations) liveCatalog.ops();
          Assertions.assertTrue(testCatalogOperations.dropSchema(schemaIdent, false));
          return null;
        });

    Assertions.assertFalse(dispatcher.dropSchema(schemaIdent, false));
    Assertions.assertTrue(entityStore.exists(schemaIdent, SCHEMA));
  }

  @Test
  void testDropSchemaRemovedFromSourceReportsMetadataCleanup() throws Exception {
    reset(entityStore);
    NameIdentifier ident = NameIdentifier.of(metalake, catalog, "externally_dropped_schema");
    dispatcher.createSchema(ident, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));
    boolean droppedFromSource =
        catalogManager.doWithCatalogWrapper(
            NameIdentifier.of(metalake, catalog),
            wrapper -> wrapper.doWithSchemaOps(ops -> ops.dropSchema(ident, true)));
    Assertions.assertTrue(droppedFromSource);
    Assertions.assertTrue(entityStore.exists(ident, SCHEMA));

    Assertions.assertTrue(dispatcher.dropSchema(ident, true));
    Assertions.assertFalse(entityStore.exists(ident, SCHEMA));
    Assertions.assertFalse(dispatcher.dropSchema(ident, true));
  }

  @Test
  void testCascadingDropOfMissingSchemaDeletesStoredSecrets() throws Exception {
    reset(entityStore);
    try (SecretManager secrets = memorySecretManager()) {
      SchemaOperationDispatcher d =
          new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secrets);
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, "missing_schema_secret");
      d.createSchema(
          ident,
          "comment",
          ImmutableMap.of("k1", "v1"),
          Map.of("k2", new SecretBinding("memory", "s3cr3t")),
          Map.of());
      SchemaEntity entity = entityStore.get(ident, SCHEMA, SchemaEntity.class);
      SecretUrn urn =
          SecretUrn.buildWriteThrough(
              "memory",
              Map.of(
                  SecretConstants.ATTR_ENTITY_TYPE, "schema",
                  SecretConstants.ATTR_ENTITY_ID, String.valueOf(entity.id()),
                  SecretConstants.ATTR_PROPERTY_KEY, "k2"));
      boolean sourceDropped =
          catalogManager.doWithCatalogWrapper(
              NameIdentifier.of(metalake, catalog),
              wrapper -> wrapper.doWithSchemaOps(ops -> ops.dropSchema(ident, true)));
      Assertions.assertTrue(sourceDropped);
      Assertions.assertFalse(d.dropSchema(ident, false));
      Assertions.assertEquals("s3cr3t", secrets.readSecret(urn));
      Assertions.assertTrue(d.dropSchema(ident, true));
      Assertions.assertFalse(entityStore.exists(ident, SCHEMA));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
    }
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

  @Test
  void testNativeSchemaImportDoesNotOverwriteAnotherRegistration() throws Exception {
    NameIdentifier ident = NameIdentifier.of(metalake, catalog, "schema_native_no_marker");
    TestCatalog source =
        (TestCatalog)
            catalogManager.loadCatalogAndWrap(NameIdentifier.of(metalake, catalog)).catalog();
    ((TestCatalogOperations) source.ops())
        .createSchema(ident, "native", ImmutableMap.of("k1", "v1", "k2", "v2"));
    reset(entityStore);
    try {
      Assertions.assertEquals(ident.name(), dispatcher.loadSchema(ident).name());
      verify(entityStore).put(any(SchemaEntity.class), eq(false));
      verify(entityStore, never()).put(any(SchemaEntity.class), eq(true));
    } finally {
      reset(entityStore);
    }
  }

  @Test
  void testRegistrationFailureRetainsSecretsUsedByCreatedSchema() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      SchemaOperationDispatcher d =
          new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secrets);
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, "schema_secret_partial_create");
      doThrow(new IOException("store unavailable"))
          .when(entityStore)
          .put(any(SchemaEntity.class), eq(true));
      try {
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () ->
                d.createSchema(
                    ident,
                    "comment",
                    ImmutableMap.of("k1", "v1"),
                    Map.of("k2", new SecretBinding("memory", "retained-secret")),
                    Map.of()));
        Schema created =
            catalogManager
                .loadCatalogAndWrap(NameIdentifier.of(metalake, catalog))
                .doWithSchemaOps(ops -> ops.loadSchema(ident));
        SecretUrn urn = SecretUrn.parse(created.properties().get("k2"));
        Assertions.assertEquals("retained-secret", secrets.readSecret(urn));
        reset(entityStore);
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> d.createSchema(ident, "comment", ImmutableMap.of("k1", "v1", "k2", "v2")));
        Assertions.assertFalse(entityStore.exists(ident, SCHEMA));
        Schema imported = d.loadSchema(ident);
        Assertions.assertEquals(
            HiddenPropertyMaskUtils.MASKED_VALUE, imported.properties().get("k2"));
        Assertions.assertFalse(imported.properties().containsKey(ID_KEY));
        Assertions.assertEquals(
            urn.toString(),
            entityStore.get(ident, SCHEMA, SchemaEntity.class).properties().get("k2"));
        Assertions.assertEquals("retained-secret", secrets.readSecret(urn));
        Assertions.assertTrue(d.dropSchema(ident, false));
        Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
      } finally {
        reset(entityStore);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"entity-id", "entity-type", "property-key"})
  void testImportRejectsForeignWriteThroughSecrets(String mismatch) throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      SchemaOperationDispatcher d =
          new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secrets);
      SecretPropertyOperationDispatcher secretDispatcher =
          new SecretPropertyOperationDispatcher(catalogManager, entityStore, idGenerator, secrets);
      NameIdentifier original = NameIdentifier.of(metalake, catalog, "secret_owner_" + mismatch);
      NameIdentifier copy = NameIdentifier.of(metalake, catalog, "secret_copy_" + mismatch);
      d.createSchema(
          original,
          "owner",
          Map.of("k1", "v1"),
          Map.of("k2", new SecretBinding("memory", "original-secret")),
          Map.of());
      SchemaEntity owner = entityStore.get(original, SCHEMA, SchemaEntity.class);
      SecretUrn originalUrn = SecretUrn.parse(owner.properties().get("k2"));
      Map<String, String> properties = new HashMap<>();
      properties.put("k1", "v1");
      if (mismatch.equals("entity-id")) {
        // A copied URN without its source marker would otherwise be imported under a new id.
        properties.put("k2", originalUrn.toString());
      } else {
        long copyId = idGenerator.nextId();
        properties.put(ID_KEY, StringIdentifier.fromId(copyId).toString());
        properties.put(
            "k2",
            SecretUrn.buildWriteThrough(
                    "memory",
                    Map.of(
                        SecretConstants.ATTR_ENTITY_TYPE,
                        mismatch.equals("entity-type") ? "catalog" : "schema",
                        SecretConstants.ATTR_ENTITY_ID,
                        String.valueOf(copyId),
                        SecretConstants.ATTR_PROPERTY_KEY,
                        mismatch.equals("property-key") ? "otherk2" : "k2"))
                .toString());
      }
      catalogManager.doWithCatalogWrapper(
          NameIdentifier.of(metalake, catalog),
          c -> c.doWithSchemaOps(ops -> ops.createSchema(copy, "copy", properties)));

      IllegalArgumentException failure =
          Assertions.assertThrows(IllegalArgumentException.class, () -> d.loadSchema(copy));
      Assertions.assertTrue(failure.getMessage().contains(copy.toString()));
      Assertions.assertTrue(failure.getMessage().contains("property k2"));
      Assertions.assertFalse(entityStore.exists(copy, SCHEMA));
      Assertions.assertThrows(
          NoSuchSchemaException.class, () -> secretDispatcher.getSecrets(copy, SCHEMA));
      Assertions.assertTrue(d.dropSchema(copy, false));
      Assertions.assertEquals("original-secret", secrets.readSecret(originalUrn));
      Assertions.assertEquals(
          owner.id(), entityStore.get(original, SCHEMA, SchemaEntity.class).id());
      Assertions.assertTrue(d.dropSchema(original, false));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testImportRetainsExternalSecretReference(boolean explicitImport) throws Exception {
    NameIdentifier ident = NameIdentifier.of(metalake, catalog, "external_secret_reference");
    String reference = "urn:gravitino-secret:external:credential:k2";
    Schema source =
        catalogManager.doWithCatalogWrapper(
            NameIdentifier.of(metalake, catalog),
            c ->
                c.doWithSchemaOps(
                    ops ->
                        ops.createSchema(ident, "external", Map.of("k1", "v1", "k2", reference))));
    FieldUtils.writeField(source, "auditInfo", AuditInfo.EMPTY, true);

    AuditInfo importAudit =
        AuditInfo.builder().withCreator("alice").withCreateTime(Instant.EPOCH).build();
    Schema imported =
        explicitImport ? dispatcher.importSchema(ident, importAudit) : dispatcher.loadSchema(ident);
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, imported.properties().get("k2"));
    Assertions.assertEquals(
        reference, entityStore.get(ident, SCHEMA, SchemaEntity.class).properties().get("k2"));
    Assertions.assertEquals(explicitImport ? "alice" : null, imported.auditInfo().creator());
    Assertions.assertEquals(
        explicitImport ? "alice" : null,
        entityStore.get(ident, SCHEMA, SchemaEntity.class).auditInfo().creator());
    Assertions.assertTrue(dispatcher.dropSchema(ident, false));
  }

  @Test
  void testCreateSchemaReportsRegistrationConflictAndRetainsSecrets() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      SchemaOperationDispatcher d =
          new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secrets);
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, "schema_registration_conflict");
      EntityAlreadyExistsException conflict =
          new EntityAlreadyExistsException("concurrent registration");
      doThrow(conflict).when(entityStore).put(any(SchemaEntity.class), eq(true));
      try {
        SchemaAlreadyExistsException failure =
            Assertions.assertThrows(
                SchemaAlreadyExistsException.class,
                () ->
                    d.createSchema(
                        ident,
                        "",
                        Map.of("k1", "v1"),
                        Map.of("k2", new SecretBinding("memory", "retained-secret")),
                        Map.of()));
        Assertions.assertSame(conflict, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains(ident.toString()));
        Assertions.assertTrue(failure.getMessage().contains("reconcile"));
        Assertions.assertFalse(failure.getMessage().contains("use loadSchema to import"));
        Schema source =
            catalogManager
                .loadCatalogAndWrap(NameIdentifier.of(metalake, catalog))
                .doWithSchemaOps(ops -> ops.loadSchema(ident));
        Assertions.assertEquals(
            "retained-secret", secrets.readSecret(SecretUrn.parse(source.properties().get("k2"))));
      } finally {
        reset(entityStore);
      }
    }
  }

  @Test
  public void testSecrets() throws Exception {
    try (SecretManager secrets = memorySecretManager()) {
      SchemaOperationDispatcher d =
          new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secrets);
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, "schema_secret");
      Map<String, SecretBinding> bindings = Map.of("k2", new SecretBinding("memory", "s3cr3t"));
      Schema schema =
          d.createSchema(ident, "comment", ImmutableMap.of("k1", "v1"), bindings, Map.of());
      Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, schema.properties().get("k2"));

      Schema stored =
          catalogManager
              .loadCatalogAndWrap(NameIdentifier.of(metalake, catalog))
              .doWithSchemaOps(ops -> ops.loadSchema(ident));
      Assertions.assertTrue(
          SecretPropertyUtils.isSecretProperty("k2", stored.properties().get("k2")));
      SchemaEntity entity = entityStore.get(ident, SCHEMA, SchemaEntity.class);
      SecretUrn urn =
          SecretUrn.buildWriteThrough(
              "memory",
              Map.of(
                  SecretConstants.ATTR_ENTITY_TYPE, "schema",
                  SecretConstants.ATTR_ENTITY_ID, String.valueOf(entity.id()),
                  SecretConstants.ATTR_PROPERTY_KEY, "k2"));
      Assertions.assertEquals("s3cr3t", secrets.readSecret(urn));
      d.alterSchema(ident, SchemaChange.removeProperty("k2"));
      Assertions.assertFalse(
          entityStore.get(ident, SCHEMA, SchemaEntity.class).properties().containsKey("k2"));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
      Assertions.assertThrows(
          SchemaAlreadyExistsException.class,
          () -> d.createSchema(ident, "comment", ImmutableMap.of("k1", "v1"), bindings, Map.of()));
      Assertions.assertTrue(d.dropSchema(ident, false));
      Assertions.assertThrows(IllegalArgumentException.class, () -> secrets.readSecret(urn));
    }
  }

  @Test
  public void testCreateAndAlterSchemaRejectMaskedPlaceholder() {
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schema_masked");
    Map<String, String> createProps =
        ImmutableMap.of("k1", HiddenPropertyMaskUtils.MASKED_VALUE, "k2", "v2");
    testMaskedPlaceholderRejected(
        () -> dispatcher.createSchema(schemaIdent, "comment", createProps), "k1");

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    dispatcher.createSchema(schemaIdent, "comment", props);
    testMaskedPlaceholderRejected(
        () ->
            dispatcher.alterSchema(
                schemaIdent, SchemaChange.setProperty("k3", HiddenPropertyMaskUtils.MASKED_VALUE)),
        "k3");
  }

  private static SecretManager memorySecretManager() {
    Config c = new Config(false) {};
    Properties p = new Properties();
    p.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    p.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    c.loadFromProperties(p);
    return new SecretManager(c);
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
