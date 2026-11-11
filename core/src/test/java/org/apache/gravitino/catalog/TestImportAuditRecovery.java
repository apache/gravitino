/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies the store-side conditions used to recover creation audit after competing imports. */
public class TestImportAuditRecovery {
  private static final AuditInfo ALICE =
      AuditInfo.builder().withCreator("alice").withCreateTime(Instant.EPOCH).build();

  private final EntityStore store = mock(EntityStore.class);
  private final OperationDispatcher dispatcher =
      new OperationDispatcher(null, store, null, null) {};

  @Test
  void testNoUpdateForColdImportOrEmptyFallback() {
    SchemaEntity imported = schema(1L, ALICE);
    Assertions.assertSame(imported, fill(imported, ALICE));
    SchemaEntity empty = schema(1L, AuditInfo.EMPTY);
    Assertions.assertSame(empty, fill(empty, AuditInfo.EMPTY));
    Assertions.assertNull(fill(null, ALICE));
    verifyNoInteractions(store);
  }

  @Test
  void testConcurrentCreatorWins() throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    SchemaEntity current = schema(1L, AuditInfo.builder().withCreator("bob").build());
    currentAtUpdate(observed, current);
    Assertions.assertSame(current, fill(observed, ALICE));
  }

  @Test
  void testRecreatedNameIsNotAttributedToOldCreate() throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    SchemaEntity recreated = schema(2L, AuditInfo.EMPTY);
    currentAtUpdate(observed, recreated);
    when(store.get(observed.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class))
        .thenReturn(recreated);
    Assertions.assertSame(recreated, fill(observed, ALICE));
    Assertions.assertNull(recreated.auditInfo().creator());
    Assertions.assertNull(recreated.auditInfo().createTime());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConflictStopsForKnownCreatorOrAfterOneRetry(boolean knownCreator) throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    SchemaEntity current =
        schema(1L, knownCreator ? AuditInfo.builder().withCreator("bob").build() : AuditInfo.EMPTY);
    when(store.update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any()))
        .thenThrow(new OptimisticLockException("concurrent update"));
    when(store.get(observed.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class))
        .thenReturn(current);
    Assertions.assertSame(current, fill(observed, ALICE));
    verify(store, times(knownCreator ? 1 : 2))
        .update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any());
  }

  @Test
  void testPropertyConflictRetriesOnceWithoutLosingCurrentFields() throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    AuditInfo modified =
        AuditInfo.builder()
            .withLastModifier("bob")
            .withLastModifiedTime(Instant.EPOCH.plusSeconds(1))
            .build();
    SchemaEntity current =
        SchemaEntity.builder()
            .withId(observed.id())
            .withName(observed.name())
            .withNamespace(observed.namespace())
            .withComment("updated")
            .withProperties(Map.of("key", "changed"))
            .withAuditInfo(modified)
            .build();
    when(store.get(observed.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class))
        .thenReturn(current);
    when(store.update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any()))
        .thenThrow(new OptimisticLockException("property update won"))
        .thenAnswer(
            call -> {
              Function<SchemaEntity, SchemaEntity> updater = call.getArgument(3);
              return updater.apply(current);
            });
    SchemaEntity updated = fill(observed, ALICE);
    Assertions.assertEquals(ALICE.creator(), updated.auditInfo().creator());
    Assertions.assertEquals(ALICE.createTime(), updated.auditInfo().createTime());
    Assertions.assertEquals(modified.lastModifier(), updated.auditInfo().lastModifier());
    Assertions.assertEquals(modified.lastModifiedTime(), updated.auditInfo().lastModifiedTime());
    Assertions.assertEquals(current.properties(), updated.properties());
    Assertions.assertEquals(current.comment(), updated.comment());
    verify(store, times(2))
        .update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any());
  }

  @Test
  void testDisappearedRegistrationIsNotRecoveredFromObservedSnapshot() throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    when(store.update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any()))
        .thenThrow(new NoSuchEntityException("Registration was renamed or dropped"));
    Assertions.assertNull(fill(observed, ALICE));
    verify(store)
        .update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any());
    verifyNoMoreInteractions(store);
  }

  @Test
  void testFillKeepsModificationFieldsAndExistingCreateTime() throws Exception {
    AuditInfo existing =
        AuditInfo.builder()
            .withCreateTime(Instant.EPOCH.plusSeconds(1))
            .withLastModifier("modifier")
            .withLastModifiedTime(Instant.EPOCH.plusSeconds(2))
            .build();
    SchemaEntity observed = schema(1L, existing);
    currentAtUpdate(observed, observed);
    SchemaEntity updated = fill(observed, ALICE);
    Assertions.assertEquals("alice", updated.auditInfo().creator());
    Assertions.assertEquals(existing.createTime(), updated.auditInfo().createTime());
    Assertions.assertEquals(existing.lastModifier(), updated.auditInfo().lastModifier());
    Assertions.assertEquals(existing.lastModifiedTime(), updated.auditInfo().lastModifiedTime());
    Assertions.assertEquals(observed.properties(), updated.properties());
    Assertions.assertEquals(observed.comment(), updated.comment());
    Assertions.assertNull(observed.auditInfo().creator());
  }

  @Test
  void testCatalogCreationAuditTakesPrecedence() throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    currentAtUpdate(observed, observed);
    AuditInfo reported =
        AuditInfo.builder()
            .withCreator("source")
            .withCreateTime(Instant.EPOCH.plusSeconds(3))
            .build();
    SchemaEntity updated =
        dispatcher.fillMissingCreationAudit(
            observed, SchemaEntity.class, reported, ALICE, TestImportAuditRecovery::withAudit);
    Assertions.assertEquals("source", updated.auditInfo().creator());
    Assertions.assertEquals(reported.createTime(), updated.auditInfo().createTime());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testStoreFailureReturnsObservedEntity(boolean checkedException) throws Exception {
    SchemaEntity observed = schema(1L, AuditInfo.EMPTY);
    Exception failure =
        checkedException
            ? new IOException("unavailable")
            : new IllegalStateException("unavailable");
    when(store.update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any()))
        .thenThrow(failure);
    Assertions.assertSame(observed, fill(observed, ALICE));
  }

  private void currentAtUpdate(SchemaEntity observed, SchemaEntity current) throws Exception {
    when(store.update(
            eq(observed.nameIdentifier()),
            eq(SchemaEntity.class),
            eq(Entity.EntityType.SCHEMA),
            any()))
        .thenAnswer(
            call -> {
              Function<SchemaEntity, SchemaEntity> updater = call.getArgument(3);
              return updater.apply(current);
            });
  }

  private SchemaEntity fill(SchemaEntity observed, AuditInfo requested) {
    return dispatcher.fillMissingCreationAudit(
        observed,
        SchemaEntity.class,
        AuditInfo.EMPTY,
        requested,
        TestImportAuditRecovery::withAudit);
  }

  private static SchemaEntity schema(long id, AuditInfo audit) {
    return SchemaEntity.builder()
        .withId(id)
        .withName("schema")
        .withNamespace(Namespace.of("metalake", "catalog"))
        .withComment("comment")
        .withProperties(Map.of("key", "value"))
        .withAuditInfo(audit)
        .build();
  }

  private static SchemaEntity withAudit(SchemaEntity current, AuditInfo audit) {
    return SchemaEntity.builder()
        .withId(current.id())
        .withName(current.name())
        .withNamespace(current.namespace())
        .withComment(current.comment())
        .withProperties(current.properties())
        .withAuditInfo(audit)
        .build();
  }
}
