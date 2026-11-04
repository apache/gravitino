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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Verifies policy binding works against the entity store contract without a JDBC backend. */
public class TestPolicyObjectBindingAbstraction {

  /** The manager reads models and submits a relation update to the injected store. */
  @ParameterizedTest
  @MethodSource("supportedObjects")
  public void testUsesEntityStoreRelationOperations(MetadataObject object) throws Exception {
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsRelationOperations.class));
    SupportsRelationOperations relations = mock(SupportsRelationOperations.class);
    when(store.relationOperations()).thenReturn(relations);
    AuditInfo audit =
        AuditInfo.builder().withCreator("original-user").withCreateTime(Instant.now()).build();
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.id()).thenReturn(12L);
    when(policy.namespace()).thenReturn(Namespace.of("m"));
    when(policy.auditInfo()).thenReturn(audit);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy("m", "p");
    when(policy.nameIdentifier()).thenReturn(policyIdent);
    TagEntity tag = HiddenPolicyTag.create(policy, 34L);
    when(store.get(NameIdentifier.of("m"), Entity.EntityType.METALAKE, BaseMetalake.class))
        .thenReturn(
            BaseMetalake.builder()
                .withId(1L)
                .withName("m")
                .withVersion(SchemaVersion.V_0_1)
                .withAuditInfo(audit)
                .build());
    when(store.get(policyIdent, Entity.EntityType.POLICY, PolicyEntity.class)).thenReturn(policy);
    when(store.get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class)).thenReturn(tag);
    NameIdentifier objectIdent = MetadataObjectUtil.toEntityIdent("m", object);
    when(relations.batchListEntitiesByRelation(any(), any(), any()))
        .thenReturn(
            List.of(
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    policy)));
    PolicyManager manager = new PolicyManager(mock(IdGenerator.class), store);
    assertEquals(34L, manager.addPolicyToObject("m", "p", object, audit));
    ArgumentCaptor<RelationUpdate> captured = ArgumentCaptor.forClass(RelationUpdate.class);
    verify(relations).updateEntityRelations(captured.capture());
    RelationUpdate update = captured.getValue();
    assertEquals(SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL, update.relationType());
    assertEquals(objectIdent, update.sourceIdentifier());
    assertEquals(tag.nameIdentifier(), update.targetsToAdd()[0].nameIdentifier());
    assertEquals(audit, update.auditInfo().orElseThrow());
    assertFalse(update.targetsToAdd()[0].relationValue().isPresent());
    assertFalse(
        RelationUpdate.of(
                update.relationType(),
                objectIdent,
                MetadataObjectUtil.toEntityType(object),
                update.targetsToAdd(),
                null)
            .auditInfo()
            .isPresent());
  }

  /** Only the expected selector-free relation may win a concurrent internal binding. */
  @ParameterizedTest
  @ValueSource(strings = {"matching", "selector", "other-policy", "missing"})
  public void testConcurrentPolicyRelationCreation(String winnerType) throws Exception {
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsRelationOperations.class));
    SupportsRelationOperations relations = mock(SupportsRelationOperations.class);
    when(store.relationOperations()).thenReturn(relations);
    AuditInfo audit =
        AuditInfo.builder().withCreator("original-user").withCreateTime(Instant.now()).build();
    PolicyEntity policy = mock(PolicyEntity.class);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy("m", "p");
    when(policy.id()).thenReturn(12L);
    when(policy.namespace()).thenReturn(Namespace.of("m"));
    when(policy.auditInfo()).thenReturn(audit);
    when(policy.nameIdentifier()).thenReturn(policyIdent);
    TagEntity tag = HiddenPolicyTag.create(policy, 34L);
    when(store.get(NameIdentifier.of("m"), Entity.EntityType.METALAKE, BaseMetalake.class))
        .thenReturn(
            BaseMetalake.builder()
                .withId(1L)
                .withName("m")
                .withVersion(SchemaVersion.V_0_1)
                .withAuditInfo(audit)
                .build());
    when(store.get(policyIdent, Entity.EntityType.POLICY, PolicyEntity.class)).thenReturn(policy);
    when(store.get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class)).thenReturn(tag);
    PolicyEntity winner = policy;
    if (winnerType.equals("other-policy")) {
      winner = mock(PolicyEntity.class);
      when(winner.nameIdentifier()).thenReturn(NameIdentifierUtil.ofPolicy("m", "other"));
    }
    List<RelationalEntity<?>> winningRelations =
        winnerType.equals("missing")
            ? List.of()
            : List.of(
                new RelationalEntity<>(
                    SupportsRelationOperations.Type.POLICY_TAG_REL,
                    tag.nameIdentifier(),
                    Entity.EntityType.TAG,
                    winner,
                    winnerType.equals("selector") ? "{}" : null));
    when(relations.batchListEntitiesByRelation(any(), any(), any()))
        .thenReturn(List.of(), winningRelations);
    doAnswer(
            invocation -> {
              RelationUpdate update = invocation.getArgument(0);
              if (update.relationType() == SupportsRelationOperations.Type.POLICY_TAG_REL) {
                throw new EntityAlreadyExistsException("Concurrent relation creation");
              }
              return List.of();
            })
        .when(relations)
        .updateEntityRelations(any(RelationUpdate.class));
    PolicyManager manager = new PolicyManager(mock(IdGenerator.class), store);
    MetadataObject object = MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG);
    if (winnerType.equals("matching")) {
      assertEquals(34L, manager.addPolicyToObject("m", "p", object, audit));
      ArgumentCaptor<RelationUpdate> updates = ArgumentCaptor.forClass(RelationUpdate.class);
      verify(relations, times(2)).updateEntityRelations(updates.capture());
      assertEquals(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          updates.getAllValues().get(1).relationType());
      assertEquals(audit, updates.getAllValues().get(1).auditInfo().orElseThrow());
    } else {
      Class<? extends RuntimeException> expected =
          winnerType.equals("missing")
              ? EntityAlreadyExistsException.class
              : IllegalArgumentException.class;
      assertThrows(expected, () -> manager.addPolicyToObject("m", "p", object, audit));
      verify(relations, never())
          .updateEntityRelations(
              argThat(
                  update ->
                      update.relationType()
                          == SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL));
    }
  }

  private static Stream<MetadataObject> supportedObjects() {
    return Stream.of(
        MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG),
        MetadataObjects.parse("catalog.schema.table.column", MetadataObject.Type.COLUMN),
        MetadataObjects.parse("catalog.schema.model", MetadataObject.Type.SEMANTIC_MODEL));
  }
}
