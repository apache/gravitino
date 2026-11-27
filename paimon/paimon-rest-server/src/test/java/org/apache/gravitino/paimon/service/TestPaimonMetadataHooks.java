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
package org.apache.gravitino.paimon.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLock;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.paimon.service.PaimonMetadataHooks.Change;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

/** Verifies metadata import, creator ownership, failure propagation and deployment isolation. */
public class TestPaimonMetadataHooks {
  @Test
  void testSchemaCreationValidatesLengthAndRegistrationBeforeNativeWrite() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.lockManager()).thenReturn(new LockManager(new Config(false) {}));
    EntityStore store = mock(EntityStore.class);
    when(env.entityStore()).thenReturn(store);
    NameIdentifier existing = NameIdentifier.of("ml", "sales", "old");
    when(store.exists(existing, Entity.EntityType.SCHEMA)).thenReturn(true);
    Supplier<Object> mutation = mock(Supplier.class);
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
      assertThrows(
          IllegalArgumentException.class,
          () ->
              hooks.mutate(
                  Change.CREATE_SCHEMA,
                  NameIdentifier.of("ml", "sales", "s".repeat(129)),
                  mutation));
      verifyNoInteractions(store);
      AlreadyExistsException error =
          assertThrows(
              AlreadyExistsException.class,
              () -> hooks.mutate(Change.CREATE_SCHEMA, existing, mutation));
      assertEquals("DATABASE", error.resourceType());
      verifyNoInteractions(mutation);
    }
  }

  @Test
  void testSchemaAndIdentityValidationPrecedesNativeWrites() {
    PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
    Schema invalid =
        new Schema(
            List.of(new DataField(0, "c".repeat(301), DataTypes.INT())),
            List.of(),
            List.of(),
            Map.of(),
            "");
    assertThrows(IllegalArgumentException.class, () -> hooks.validateCreateTable(invalid));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            hooks.validateTableChanges(
                List.of(SchemaChange.addColumn("c".repeat(301), DataTypes.INT()))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            hooks.validateTableChanges(List.of(SchemaChange.renameColumn("id", "c".repeat(301)))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            hooks.validateTableChanges(
                List.of(SchemaChange.updateColumnComment(new String[] {"id"}, "bad\0comment"))));
    for (boolean enabled : List.of(false, true)) {
      PaimonMetadataHooks mode = new PaimonMetadataHooks(enabled, "ml");
      for (String key : List.of("gravitino.identifier")) {
        Schema identity =
            new Schema(
                List.of(new DataField(0, "id", DataTypes.INT())),
                List.of(),
                List.of(),
                Map.of(key, "untrusted"),
                "");
        assertThrows(IllegalArgumentException.class, () -> mode.validateCreateTable(identity));
        assertThrows(
            IllegalArgumentException.class,
            () -> mode.validateTableChanges(List.of(SchemaChange.setOption(key, "untrusted"))));
        assertThrows(
            IllegalArgumentException.class,
            () -> mode.validateTableChanges(List.of(SchemaChange.removeOption(key))));
      }
    }
    new PaimonMetadataHooks(false, "ml").validateCreateTable(invalid);
  }

  @Test
  void testCreationCannotReuseRegisteredIdentityOrContinueAfterLookupFailure() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.lockManager()).thenReturn(new LockManager(new Config(false) {}));
    EntityStore store = mock(EntityStore.class);
    when(env.entityStore()).thenReturn(store);
    NameIdentifier target = NameIdentifier.of("ml", "sales", "db", "target");
    when(store.exists(target, Entity.EntityType.TABLE)).thenReturn(true);
    Supplier<Object> create = mock(Supplier.class);
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
      assertThrows(
          AlreadyExistsException.class, () -> hooks.mutate(Change.CREATE_TABLE, target, create));
      when(store.exists(target, Entity.EntityType.TABLE))
          .thenThrow(new IOException("lookup failed"));
      assertThrows(
          IllegalStateException.class, () -> hooks.mutate(Change.CREATE_TABLE, target, create));
      verifyNoInteractions(create);
    }
  }

  @Test
  void testColdMetadataImportsDoNotUpgradeAncestorReadLocks() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          GravitinoEnv env = mock(GravitinoEnv.class);
          LockManager locks = new LockManager(new Config(false) {});
          when(env.lockManager()).thenReturn(locks);
          SchemaDispatcher schemas = mock(SchemaDispatcher.class);
          TableDispatcher tables = mock(TableDispatcher.class);
          when(env.internalSchemaDispatcher()).thenReturn(schemas);
          when(env.internalTableDispatcher()).thenReturn(tables);
          when(env.entityStore()).thenReturn(mock(EntityStore.class));
          NameIdentifier catalog = NameIdentifier.of("ml", "sales");
          // Cold schema imports in the real dispatcher need the catalog WRITE lock.
          doAnswer(invocation -> TreeLockUtils.doWithTreeLock(catalog, LockType.WRITE, () -> null))
              .when(schemas)
              .importSchema(any(), any());
          // A cold table may first need to import its parent schema.
          doAnswer(invocation -> TreeLockUtils.doWithTreeLock(catalog, LockType.WRITE, () -> null))
              .when(tables)
              .importTable(any(), any());
          try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
            environments.when(GravitinoEnv::getInstance).thenReturn(env);
            PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
            hooks.mutate(Change.CREATE_SCHEMA, NameIdentifier.of("ml", "sales", "db"), () -> null);
            hooks.mutate(
                Change.CREATE_TABLE, NameIdentifier.of("ml", "sales", "db", "t"), () -> null);
            verify(schemas).importSchema(any(), any());
            verify(tables).importTable(any(), any());
          }
        });
  }

  @Test
  void testRenamePreservesRegisteredIdentityPropertiesAndCreationAudit() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    LockManager locks = mock(LockManager.class);
    TreeLock lock = mock(TreeLock.class);
    EntityStore store = mock(EntityStore.class);
    TableDispatcher tables = mock(TableDispatcher.class);
    when(env.lockManager()).thenReturn(locks);
    when(env.entityStore()).thenReturn(store);
    when(env.internalTableDispatcher()).thenReturn(tables);
    SchemaDispatcher schemas = mock(SchemaDispatcher.class);
    when(env.internalSchemaDispatcher()).thenReturn(schemas);
    when(locks.createTreeLock(any())).thenReturn(lock);
    NameIdentifier source = NameIdentifier.of("ml", "sales", "db", "t");
    NameIdentifier destination = NameIdentifier.of("ml", "sales", "other", "renamed");
    TableEntity original =
        TableEntity.builder()
            .withId(1L)
            .withName("t")
            .withNamespace(Namespace.of("ml", "sales", "db"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build())
            .withProperties(Map.of("registered", "retained"))
            .withComment("comment")
            .build();
    doAnswer(
            invocation -> {
              Function<TableEntity, TableEntity> rename = invocation.getArgument(3);
              TableEntity result = rename.apply(original);
              assertEquals(original.id(), result.id());
              assertEquals(destination.namespace(), result.namespace());
              assertEquals(destination.name(), result.name());
              assertEquals(original.properties(), result.properties());
              assertEquals(original.comment(), result.comment());
              assertEquals(original.auditInfo().creator(), result.auditInfo().creator());
              assertEquals("alice", result.auditInfo().lastModifier());
              return result;
            })
        .when(store)
        .update(eq(source), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PrincipalUtils.doAs(
          new UserPrincipal("alice"),
          () -> {
            new PaimonMetadataHooks(true, "ml").rename(source, destination, () -> {});
            return null;
          });
      verify(tables).importTable(source, AuditInfo.EMPTY);
      verify(store).update(eq(source), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
    }
  }

  @Test
  void testInvalidNamesFailBeforeCreatingOrRenamingBackendObjects() {
    PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
    Supplier<Object> mutation = mock(Supplier.class);
    for (Change change : List.of(Change.CREATE_SCHEMA, Change.CREATE_TABLE)) {
      NameIdentifier identifier =
          change == Change.CREATE_SCHEMA
              ? NameIdentifier.of("ml", "sales", "bad.name")
              : NameIdentifier.of("ml", "sales", "db", "bad.name");
      assertThrows(
          IllegalArgumentException.class, () -> hooks.mutate(change, identifier, mutation));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            hooks.mutate(
                Change.CREATE_TABLE, NameIdentifier.of("ml", "sales", "bad.db", "t"), mutation));
    Runnable rename = mock(Runnable.class);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            hooks.rename(
                NameIdentifier.of("ml", "sales", "db", "t"),
                NameIdentifier.of("ml", "sales", "db", "bad.name"),
                rename));
    verifyNoInteractions(mutation, rename);
  }

  @Test
  void testOversizedNamesAreRejectedBeforeNativeMutation() {
    PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
    NameIdentifier source = NameIdentifier.of("ml", "sales", "db", "source");
    NameIdentifier target = NameIdentifier.of("ml", "sales", "db", "t".repeat(129));
    Supplier<Object> create = mock(Supplier.class);
    Runnable rename = mock(Runnable.class);
    assertThrows(
        IllegalArgumentException.class, () -> hooks.mutate(Change.CREATE_TABLE, target, create));
    assertThrows(IllegalArgumentException.class, () -> hooks.rename(source, target, rename));
    verifyNoInteractions(create, rename);
  }

  @Test
  void testStaleDestinationAndMissingSourceDatabaseKeepNativeSource() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.lockManager()).thenReturn(new LockManager(new Config(false) {}));
    TableDispatcher tables = mock(TableDispatcher.class);
    when(env.internalTableDispatcher()).thenReturn(tables);
    when(env.internalSchemaDispatcher()).thenReturn(mock(SchemaDispatcher.class));
    EntityStore store = mock(EntityStore.class);
    when(env.entityStore()).thenReturn(store);
    NameIdentifier source = NameIdentifier.of("ml", "sales", "db", "source");
    NameIdentifier target = NameIdentifier.of("ml", "sales", "db", "target");
    when(store.exists(target, Entity.EntityType.TABLE)).thenReturn(true);
    Runnable rename = mock(Runnable.class);
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
      assertThrows(AlreadyExistsException.class, () -> hooks.rename(source, target, rename));
      doThrow(new NoSuchSchemaException("source database missing"))
          .when(tables)
          .importTable(source, AuditInfo.EMPTY);
      NoSuchResourceException error =
          assertThrows(NoSuchResourceException.class, () -> hooks.rename(source, target, rename));
      assertEquals("DATABASE", error.resourceType());
      verifyNoInteractions(rename);
    }
  }

  @Test
  void testColdDestinationIsImportedBeforeRenameAndImportFailureKeepsSource() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    LockManager locks = mock(LockManager.class);
    when(env.lockManager()).thenReturn(locks);
    when(locks.createTreeLock(any())).thenReturn(mock(TreeLock.class));
    SchemaDispatcher schemas = mock(SchemaDispatcher.class);
    TableDispatcher tables = mock(TableDispatcher.class);
    when(env.internalSchemaDispatcher()).thenReturn(schemas);
    when(env.internalTableDispatcher()).thenReturn(tables);
    when(env.entityStore()).thenReturn(mock(EntityStore.class));
    NameIdentifier source = NameIdentifier.of("ml", "sales", "db", "t");
    NameIdentifier target = NameIdentifier.of("ml", "sales", "cold", "renamed");
    NameIdentifier targetSchema = NameIdentifier.of("ml", "sales", "cold");
    Runnable rename = mock(Runnable.class);
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
      hooks.rename(source, target, rename);
      InOrder order = inOrder(schemas, rename);
      order.verify(schemas).importSchema(targetSchema, AuditInfo.EMPTY);
      order.verify(rename).run();
      doThrow(new NoSuchSchemaException("missing target"))
          .when(schemas)
          .importSchema(targetSchema, AuditInfo.EMPTY);
      Runnable failedRename = mock(Runnable.class);
      NoSuchResourceException missingTarget =
          assertThrows(
              NoSuchResourceException.class, () -> hooks.rename(source, target, failedRename));
      assertEquals(404, new PaimonExceptionMapper().toResponse(missingTarget).getStatus());
      assertEquals("cold", missingTarget.resourceName());
      doThrow(new NoSuchTableException("missing source"))
          .when(tables)
          .importTable(source, AuditInfo.EMPTY);
      NoSuchResourceException missingSource =
          assertThrows(
              NoSuchResourceException.class, () -> hooks.rename(source, target, failedRename));
      assertEquals(404, new PaimonExceptionMapper().toResponse(missingSource).getStatus());
      assertEquals("t", missingSource.resourceName());
      verifyNoInteractions(failedRename);
    }
  }

  @Test
  void testSuccessfulDropsAndRenamesNotifyAuthorizationHooks() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    LockManager locks = mock(LockManager.class);
    when(env.lockManager()).thenReturn(locks);
    when(locks.createTreeLock(any())).thenReturn(mock(TreeLock.class));
    when(env.internalSchemaDispatcher()).thenReturn(mock(SchemaDispatcher.class));
    when(env.internalTableDispatcher()).thenReturn(mock(TableDispatcher.class));
    when(env.entityStore()).thenReturn(mock(EntityStore.class));
    NameIdentifier table = NameIdentifier.of("ml", "sales", "db", "t");
    NameIdentifier renamed = NameIdentifier.of("ml", "sales", "db", "renamed");
    NameIdentifier schema = NameIdentifier.of("ml", "sales", "db");
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class);
        MockedStatic<AuthorizationUtils> authorization = mockStatic(AuthorizationUtils.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      authorization
          .when(() -> AuthorizationUtils.getMetadataObjectLocation(any(), any()))
          .thenReturn(List.of("location"));
      PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
      hooks.rename(table, renamed, () -> {});
      hooks.mutate(Change.DROP_TABLE, renamed, () -> null);
      hooks.mutate(Change.DROP_SCHEMA, schema, () -> null);
      authorization.verify(
          () ->
              AuthorizationUtils.authorizationPluginRenamePrivileges(
                  table, Entity.EntityType.TABLE, renamed, List.of("location")));
      authorization.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  renamed, Entity.EntityType.TABLE, List.of("location")));
      authorization.verify(
          () ->
              AuthorizationUtils.authorizationPluginRemovePrivileges(
                  schema, Entity.EntityType.SCHEMA, List.of("location")));
    }
  }

  @Test
  void testStaticModeRunsNativeMutationWithoutGravitino() {
    PaimonMetadataHooks hooks = new PaimonMetadataHooks(false, "ml");
    assertEquals(
        "result",
        hooks.mutate(Change.CREATE_TABLE, NameIdentifier.of("ml", "c", "s", "t"), () -> "result"));
  }

  @Test
  void testCreateAndAlterImportsMetadataAndAssignsOnlyNewOwners() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    LockManager locks = mock(LockManager.class);
    TreeLock lock = mock(TreeLock.class);
    when(env.lockManager()).thenReturn(locks);
    when(locks.createTreeLock(any())).thenReturn(lock);
    SchemaDispatcher schemas = mock(SchemaDispatcher.class);
    TableDispatcher tables = mock(TableDispatcher.class);
    OwnerDispatcher owners = mock(OwnerDispatcher.class);
    when(env.internalSchemaDispatcher()).thenReturn(schemas);
    when(env.internalTableDispatcher()).thenReturn(tables);
    when(env.internalOwnerDispatcher()).thenReturn(owners);
    when(env.entityStore()).thenReturn(mock(EntityStore.class));
    NameIdentifier schema = NameIdentifier.of("ml", "sales", "db");
    NameIdentifier table = NameIdentifier.of("ml", "sales", "db", "t");
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PrincipalUtils.doAs(
          new UserPrincipal("alice"),
          () -> {
            PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
            hooks.mutate(Change.CREATE_SCHEMA, schema, () -> null);
            hooks.mutate(Change.CREATE_TABLE, table, () -> null);
            hooks.mutate(Change.ALTER_TABLE, table, () -> null);
            ArgumentCaptor<AuditInfo> audit = ArgumentCaptor.forClass(AuditInfo.class);
            verify(schemas).importSchema(eq(schema), audit.capture());
            assertEquals("alice", audit.getValue().creator());
            verify(owners, times(2)).setOwner(eq("ml"), any(), eq("alice"), eq(Owner.Type.USER));
            verify(tables, times(2)).importTable(eq(table), any());
            return null;
          });
    }
  }

  @Test
  void testFailedMutationDoesNotChangeMetadataAndDropDeletesRegistration() throws Exception {
    GravitinoEnv env = mock(GravitinoEnv.class);
    LockManager locks = mock(LockManager.class);
    TreeLock lock = mock(TreeLock.class);
    EntityStore store = mock(EntityStore.class);
    when(env.lockManager()).thenReturn(locks);
    when(env.entityStore()).thenReturn(store);
    when(locks.createTreeLock(any())).thenReturn(lock);
    NameIdentifier table = NameIdentifier.of("ml", "sales", "db", "t");
    try (MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      PaimonMetadataHooks hooks = new PaimonMetadataHooks(true, "ml");
      assertThrows(
          IllegalArgumentException.class,
          () ->
              hooks.mutate(
                  Change.DROP_TABLE,
                  table,
                  () -> {
                    throw new IllegalArgumentException("native failure");
                  }));
      verifyNoInteractions(store);
      hooks.mutate(Change.DROP_TABLE, table, () -> null);
      verify(store).delete(table, Entity.EntityType.TABLE, false);
      doThrow(new IOException("store failure"))
          .when(store)
          .delete(table, Entity.EntityType.TABLE, false);
      assertThrows(
          IllegalStateException.class, () -> hooks.mutate(Change.DROP_TABLE, table, () -> null));
    }
  }
}
