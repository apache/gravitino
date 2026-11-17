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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.audit.AuditLog;
import org.apache.gravitino.audit.v2.SimpleAuditLogV2;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.hook.SemanticModelHookDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelDatasetsBySourceEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelDatasetsBySourceFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelDatasetsBySourcePreEvent;
import org.apache.gravitino.meta.SemanticModelDatasetListing;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class TestSemanticModelSourceLookup {
  private static final NameIdentifier SOURCE =
      NameIdentifier.of("lake", "sources", "hr", "employees");
  private final EntityStore store = mock(EntityStore.class);
  private final SupportsRelationOperations relations = mock(SupportsRelationOperations.class);
  private final SemanticModelOperationDispatcher operations =
      new SemanticModelOperationDispatcher(
          mock(CatalogManager.class),
          mock(SchemaDispatcher.class),
          store,
          RandomIdGenerator.INSTANCE,
          mock(SecretManager.class));

  @Test
  void testSourceExistenceStructuredSortAndDeduplication() throws Exception {
    when(store.relationOperations()).thenReturn(relations);
    when(relations.<SemanticModelDatasetListing>listEntitiesByRelation(any(RelationQuery.class)))
        .thenReturn(
            List.of(
                new SemanticModelDatasetListing(
                    2L, NameIdentifier.of("lake", "models", "semantic", "a-b"), List.of("a")),
                new SemanticModelDatasetListing(
                    1L,
                    NameIdentifier.of("lake", "models", "semantic", "a"),
                    List.of("z", "A", "a.b", "z", "\uE000", "\uD800\uDC00"))));
    MetadataObject source = NameIdentifierUtil.toMetadataObject(SOURCE, Entity.EntityType.TABLE);
    try (MockedStatic<MetadataObjectUtil> metadata = mockStatic(MetadataObjectUtil.class)) {
      metadata
          .when(() -> MetadataObjectUtil.checkMetadataObject("lake", source))
          .thenAnswer(call -> null);
      assertArrayEquals(
          new NameIdentifier[] {
            dataset("a", "A"), dataset("a", "a.b"), dataset("a", "z"),
            dataset("a", "\uD800\uDC00"), dataset("a", "\uE000"), dataset("a-b", "a")
          },
          operations.listSemanticModelDatasets(SOURCE, Entity.EntityType.TABLE));
      metadata.verify(() -> MetadataObjectUtil.checkMetadataObject("lake", source));
      ArgumentCaptor<RelationQuery> query = ArgumentCaptor.forClass(RelationQuery.class);
      verify(relations).listEntitiesByRelation(query.capture());
      assertEquals(SOURCE, query.getValue().anchorIdentifier());
      assertEquals(Entity.EntityType.TABLE, query.getValue().anchorEntityType());
      assertEquals(
          SupportsRelationOperations.Type.SEMANTIC_MODEL_SOURCE_REL,
          query.getValue().relationType());
    }
  }

  @Test
  void testMissingSourceAndStorageErrorsNeverBecomeEmptyResults() throws Exception {
    MetadataObject source = NameIdentifierUtil.toMetadataObject(SOURCE, Entity.EntityType.VIEW);
    try (MockedStatic<MetadataObjectUtil> metadata = mockStatic(MetadataObjectUtil.class)) {
      metadata
          .when(() -> MetadataObjectUtil.checkMetadataObject("lake", source))
          .thenThrow(new NoSuchMetadataObjectException("missing"));
      assertThrows(
          NoSuchMetadataObjectException.class,
          () -> operations.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW));
      verifyNoInteractions(store);
      metadata
          .when(() -> MetadataObjectUtil.checkMetadataObject("lake", source))
          .thenAnswer(call -> null);
      when(store.relationOperations()).thenReturn(relations);
      IOException failure = new IOException("storage failed");
      when(relations.listEntitiesByRelation(any(RelationQuery.class))).thenThrow(failure);
      assertSame(
          failure,
          assertThrows(
                  RuntimeException.class,
                  () -> operations.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW))
              .getCause());
      when(relations.listEntitiesByRelation(any(RelationQuery.class)))
          .thenThrow(new NoSuchEntityException("source registration missing"));
      assertThrows(
          NoSuchMetadataObjectException.class,
          () -> operations.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW));
    }
  }

  @Test
  void testUnsupportedSourceIsRejectedBeforeStorage() {
    assertThrows(
        IllegalArgumentException.class,
        () -> operations.listSemanticModelDatasets(SOURCE, Entity.EntityType.SCHEMA));
    verifyNoInteractions(store);
  }

  @Test
  void testEmptyLookupAndHookForwarding() throws Exception {
    when(store.relationOperations()).thenReturn(relations);
    when(relations.listEntitiesByRelation(any(RelationQuery.class))).thenReturn(List.of());
    try (MockedStatic<MetadataObjectUtil> metadata = mockStatic(MetadataObjectUtil.class)) {
      assertArrayEquals(
          new NameIdentifier[0],
          operations.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW));
      metadata.verify(
          () ->
              MetadataObjectUtil.checkMetadataObject(
                  "lake", NameIdentifierUtil.toMetadataObject(SOURCE, Entity.EntityType.VIEW)));
    }
    SemanticModelDispatcher delegate = mock(SemanticModelDispatcher.class);
    NameIdentifier[] datasets = {dataset("staff", "employee")};
    when(delegate.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW)).thenReturn(datasets);
    SemanticModelHookDispatcher hook =
        new SemanticModelHookDispatcher(
            delegate,
            () -> {
              throw new AssertionError("Lookup must not modify ownership");
            });
    assertSame(datasets, hook.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW));
  }

  @Test
  void testLookupEventsAndAuditMapping() {
    EventBus bus = mock(EventBus.class);
    SemanticModelDispatcher delegate = mock(SemanticModelDispatcher.class);
    SemanticModelEventDispatcher dispatcher = new SemanticModelEventDispatcher(bus, delegate);
    NameIdentifier[] datasets = {dataset("staff", "employee")};
    when(delegate.listSemanticModelDatasets(SOURCE, Entity.EntityType.TABLE)).thenReturn(datasets);
    assertSame(datasets, dispatcher.listSemanticModelDatasets(SOURCE, Entity.EntityType.TABLE));
    ArgumentCaptor<Event> events = ArgumentCaptor.forClass(Event.class);
    verify(bus, times(2)).dispatchEvent(events.capture());
    ListSemanticModelDatasetsBySourcePreEvent pre =
        assertInstanceOf(
            ListSemanticModelDatasetsBySourcePreEvent.class, events.getAllValues().get(0));
    assertEquals(SOURCE, pre.identifier());
    assertEquals(MetadataObject.Type.TABLE, pre.sourceType());
    assertEquals(OperationType.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE, pre.operationType());
    ListSemanticModelDatasetsBySourceEvent success =
        assertInstanceOf(ListSemanticModelDatasetsBySourceEvent.class, events.getValue());
    assertEquals(1, success.resultCount());
    assertEquals(SOURCE, success.identifier());
    assertEquals(MetadataObject.Type.TABLE, success.sourceType());
    assertEquals(OperationType.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE, success.operationType());
    assertEquals(OperationStatus.SUCCESS, success.operationStatus());
    assertEquals(
        AuditLog.Operation.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE,
        AuditLog.Operation.fromEvent(success));
    SimpleAuditLogV2 successLog = new SimpleAuditLogV2(success);
    assertEquals(SOURCE.toString(), successLog.identifier());
    assertEquals(AuditLog.Operation.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE, successLog.operation());
    assertEquals(AuditLog.Status.SUCCESS, successLog.status());
    reset(bus);
    RuntimeException failure = new RuntimeException("failed");
    doThrow(failure).when(delegate).listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW);
    assertSame(
        failure,
        assertThrows(
            RuntimeException.class,
            () -> dispatcher.listSemanticModelDatasets(SOURCE, Entity.EntityType.VIEW)));
    verify(bus, times(2)).dispatchEvent(events.capture());
    ListSemanticModelDatasetsBySourceFailureEvent failed =
        assertInstanceOf(ListSemanticModelDatasetsBySourceFailureEvent.class, events.getValue());
    assertSame(failure, failed.exception());
    assertEquals(SOURCE, failed.identifier());
    assertEquals(MetadataObject.Type.VIEW, failed.sourceType());
    assertEquals(OperationType.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE, failed.operationType());
    assertEquals(OperationStatus.FAILURE, failed.operationStatus());
    assertEquals(
        AuditLog.Operation.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE,
        AuditLog.Operation.fromEvent(failed));
    SimpleAuditLogV2 failureLog = new SimpleAuditLogV2(failed);
    assertEquals(SOURCE.toString(), failureLog.identifier());
    assertEquals(AuditLog.Operation.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE, failureLog.operation());
    assertEquals(AuditLog.Status.FAILURE, failureLog.status());
  }

  private NameIdentifier dataset(String model, String dataset) {
    return NameIdentifier.of(Namespace.of("lake", "models", "semantic", model), dataset);
  }
}
