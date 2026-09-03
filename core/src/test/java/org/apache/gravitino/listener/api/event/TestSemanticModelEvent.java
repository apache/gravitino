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

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.listener.api.event.semantic.AlterSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.AlterSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.AlterSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.CreateSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.CreateSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.CreateSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.DropSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.DropSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.DropSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.LoadSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.LoadSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.LoadSemanticModelPreEvent;
import org.apache.gravitino.listener.api.info.SemanticModelInfo;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSemanticModelEvent {
  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "schema");

  private SemanticModelEventDispatcher dispatcher;
  private SemanticModelEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private SemanticModel semanticModel;

  @BeforeAll
  void init() {
    this.semanticModel = mockSemanticModel();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    this.dispatcher = new SemanticModelEventDispatcher(eventBus, mockSemanticModelDispatcher());
    this.failureDispatcher =
        new SemanticModelEventDispatcher(eventBus, mockExceptionSemanticModelDispatcher());
  }

  @Test
  void testCreateSemanticModelEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    dispatcher.createSemanticModel(
        identifier,
        semanticModel.comment(),
        semanticModel.definition(),
        semanticModel.properties());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateSemanticModelEvent.class, event.getClass());
    checkSemanticModelInfo(
        ((CreateSemanticModelEvent) event).createdSemanticModelInfo(), semanticModel);
    Assertions.assertEquals(OperationType.CREATE_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(CreateSemanticModelPreEvent.class, preEvent.getClass());
    checkSemanticModelInfo(
        ((CreateSemanticModelPreEvent) preEvent).createSemanticModelRequest(), semanticModel);
    Assertions.assertEquals(OperationType.CREATE_SEMANTIC_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testLoadSemanticModelEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    dispatcher.loadSemanticModel(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadSemanticModelEvent.class, event.getClass());
    checkSemanticModelInfo(
        ((LoadSemanticModelEvent) event).loadedSemanticModelInfo(), semanticModel);
    Assertions.assertEquals(OperationType.LOAD_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(LoadSemanticModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LOAD_SEMANTIC_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testAlterSemanticModelEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    SemanticModelChange change = SemanticModelChange.setProperty("a", "b");
    dispatcher.alterSemanticModel(identifier, change);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterSemanticModelEvent.class, event.getClass());
    checkSemanticModelInfo(
        ((AlterSemanticModelEvent) event).updatedSemanticModelInfo(), semanticModel);
    Assertions.assertArrayEquals(
        new SemanticModelChange[] {change},
        ((AlterSemanticModelEvent) event).semanticModelChanges());
    Assertions.assertEquals(OperationType.ALTER_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(AlterSemanticModelPreEvent.class, preEvent.getClass());
    Assertions.assertArrayEquals(
        new SemanticModelChange[] {change},
        ((AlterSemanticModelPreEvent) preEvent).semanticModelChanges());
    Assertions.assertEquals(OperationType.ALTER_SEMANTIC_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDropSemanticModelEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    dispatcher.dropSemanticModel(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropSemanticModelEvent.class, event.getClass());
    Assertions.assertTrue(((DropSemanticModelEvent) event).isExists());
    Assertions.assertEquals(OperationType.DROP_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(DropSemanticModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_SEMANTIC_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListSemanticModelEvent() {
    dispatcher.listSemanticModels(NAMESPACE);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(NAMESPACE.toString(), event.identifier().toString());
    Assertions.assertEquals(ListSemanticModelEvent.class, event.getClass());
    Assertions.assertEquals(NAMESPACE, ((ListSemanticModelEvent) event).namespace());
    Assertions.assertEquals(2, ((ListSemanticModelEvent) event).resultCount());
    Assertions.assertEquals(OperationType.LIST_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(NAMESPACE.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListSemanticModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(NAMESPACE, ((ListSemanticModelPreEvent) preEvent).namespace());
    Assertions.assertEquals(OperationType.LIST_SEMANTIC_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testCreateSemanticModelFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createSemanticModel(
                identifier,
                semanticModel.comment(),
                semanticModel.definition(),
                semanticModel.properties()));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateSemanticModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((CreateSemanticModelFailureEvent) event).exception().getClass());
    checkSemanticModelInfo(
        ((CreateSemanticModelFailureEvent) event).createSemanticModelRequest(), semanticModel);
    Assertions.assertEquals(OperationType.CREATE_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testLoadSemanticModelFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadSemanticModel(identifier));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadSemanticModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((LoadSemanticModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LOAD_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterSemanticModelFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    SemanticModelChange change = SemanticModelChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterSemanticModel(identifier, change));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterSemanticModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AlterSemanticModelFailureEvent) event).exception().getClass());
    Assertions.assertArrayEquals(
        new SemanticModelChange[] {change},
        ((AlterSemanticModelFailureEvent) event).semanticModelChanges());
    Assertions.assertEquals(OperationType.ALTER_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDropSemanticModelFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropSemanticModel(identifier));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropSemanticModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((DropSemanticModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DROP_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListSemanticModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listSemanticModels(NAMESPACE));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(NAMESPACE.toString(), event.identifier().toString());
    Assertions.assertEquals(ListSemanticModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListSemanticModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(NAMESPACE, ((ListSemanticModelFailureEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_SEMANTIC_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testSemanticModelExistsDoesNotEmitEvent() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    dummyEventListener.clear();

    Assertions.assertTrue(dispatcher.semanticModelExists(identifier));

    Assertions.assertTrue(dummyEventListener.getPostEvents().isEmpty());
    Assertions.assertTrue(dummyEventListener.getPreEvents().isEmpty());
  }

  @Test
  void testSemanticModelInfoPropertiesAreImmutable() {
    Map<String, String> mutableProperties = new HashMap<>();
    mutableProperties.put("a", "b");
    SemanticModelInfo info =
        new SemanticModelInfo("model", "comment", definition(), mutableProperties, null);

    mutableProperties.put("c", "d");
    Assertions.assertEquals(ImmutableMap.of("a", "b"), info.properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> info.properties().put("e", "f"));
  }

  @Test
  void testSemanticModelInfoDefaultsNullPropertiesToEmptyMap() {
    SemanticModelInfo info = new SemanticModelInfo("model", null, definition(), null, null);

    Assertions.assertEquals(ImmutableMap.of(), info.properties());
    Assertions.assertNull(info.comment());
    Assertions.assertNull(info.auditInfo());
  }

  @Test
  void testAlterSemanticModelEventCopiesChanges() {
    NameIdentifier identifier = NameIdentifier.of(NAMESPACE, semanticModel.name());
    SemanticModelChange[] changes = {SemanticModelChange.setProperty("a", "b")};
    AlterSemanticModelEvent event =
        new AlterSemanticModelEvent(
            "user", identifier, changes, new SemanticModelInfo(semanticModel));

    changes[0] = SemanticModelChange.rename("renamed");
    Assertions.assertEquals(
        SemanticModelChange.setProperty("a", "b"), event.semanticModelChanges()[0]);
  }

  private void checkSemanticModelInfo(SemanticModelInfo info, SemanticModel semanticModel) {
    Assertions.assertEquals(semanticModel.name(), info.name());
    Assertions.assertEquals(semanticModel.comment(), info.comment());
    Assertions.assertEquals(semanticModel.definition(), info.definition());
    Assertions.assertEquals(semanticModel.properties(), info.properties());
    Assertions.assertEquals(semanticModel.auditInfo(), info.auditInfo());
  }

  private static SemanticModelDefinition definition() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    return SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
  }

  private SemanticModel mockSemanticModel() {
    SemanticModel model = mock(SemanticModel.class);
    when(model.name()).thenReturn("sales_model");
    when(model.comment()).thenReturn("comment");
    when(model.definition()).thenReturn(definition());
    when(model.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(model.auditInfo()).thenReturn(null);
    return model;
  }

  private SemanticModelDispatcher mockSemanticModelDispatcher() {
    SemanticModelDispatcher d = mock(SemanticModelDispatcher.class);
    when(d.createSemanticModel(
            any(NameIdentifier.class), any(), any(SemanticModelDefinition.class), any(Map.class)))
        .thenReturn(semanticModel);
    when(d.loadSemanticModel(any(NameIdentifier.class))).thenReturn(semanticModel);
    when(d.semanticModelExists(any(NameIdentifier.class))).thenReturn(true);
    when(d.dropSemanticModel(any(NameIdentifier.class))).thenReturn(true);
    when(d.listSemanticModels(any(Namespace.class)))
        .thenReturn(
            new NameIdentifier[] {
              NameIdentifier.of(NAMESPACE, "model1"), NameIdentifier.of(NAMESPACE, "model2")
            });
    when(d.alterSemanticModel(any(NameIdentifier.class), any(SemanticModelChange.class)))
        .thenReturn(semanticModel);
    return d;
  }

  private SemanticModelDispatcher mockExceptionSemanticModelDispatcher() {
    return mock(
        SemanticModelDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }
}
