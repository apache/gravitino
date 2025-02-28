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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.ModelEventDispatcher;
import org.apache.gravitino.listener.api.info.ModelInfo;
import org.apache.gravitino.listener.api.info.ModelVersionInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestModelEvent {
  private ModelEventDispatcher dispatcher;
  private ModelEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Model modelA;
  private Model modelB;
  private NameIdentifier existingIdentA;
  private NameIdentifier existingIdentB;
  private NameIdentifier notExistingIdent;
  private Namespace namespace;
  private ModelVersion firstModelVersion;
  private ModelVersion secondModelVersion;

  @BeforeAll
  void init() {
    this.namespace = Namespace.of("metalake", "catalog", "schema");
    this.modelA = mockModel("modelA", "commentA");
    this.modelB = mockModel("modelB", "commentB");
    this.firstModelVersion =
        mockModelVersion("uriA", new String[] {"aliasProduction"}, "versionInfoA");
    this.secondModelVersion = mockModelVersion("uriB", new String[] {"aliasTest"}, "versionInfoB");
    System.out.println(secondModelVersion.toString());
    this.existingIdentA = NameIdentifierUtil.ofModel("metalake", "catalog", "schema", "modelA");
    this.existingIdentB = NameIdentifierUtil.ofModel("metalake", "catalog", "schema", "modelB");
    this.notExistingIdent =
        NameIdentifierUtil.ofModel("metalake", "catalog", "schema", "not_exist");
    this.dummyEventListener = new DummyEventListener();

    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new ModelEventDispatcher(eventBus, mockTagDispatcher());
    this.failureDispatcher = new ModelEventDispatcher(eventBus, mockExceptionModelDispatcher());
    // TODO: add failure dispatcher tests.
    System.out.println(this.failureDispatcher.toString());
  }

  @Test
  void testRegisterModelEvent() {
    dispatcher.registerModel(existingIdentA, "commentA", ImmutableMap.of("color", "#FFFFFF"));

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RegisterModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.REGISTER_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    // validate model info
    RegisterModelPreEvent registerModelPreEvent = (RegisterModelPreEvent) preEvent;
    ModelInfo modelInfoPreEvent = registerModelPreEvent.registeredModelInfo();
    Assertions.assertEquals("modelA", modelInfoPreEvent.getName());
    Assertions.assertEquals("commentA", modelInfoPreEvent.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfoPreEvent.getProperties());
    Assertions.assertNull(modelInfoPreEvent.modelVersions());
  }

  @Test
  void testGetModelEvent() {
    dispatcher.getModel(existingIdentA);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelPreEvent getModelPreEvent = (GetModelPreEvent) preEvent;
    Assertions.assertEquals(OperationType.GET_MODEL, getModelPreEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, getModelPreEvent.operationStatus());
    Assertions.assertEquals(existingIdentA, getModelPreEvent.identifier());
  }

  @Test
  void testDeleteExistsModelEvent() {
    dispatcher.deleteModel(existingIdentA);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    DeleteModelPreEvent deleteModelPreEvent = (DeleteModelPreEvent) preEvent;
    Assertions.assertEquals(DeleteModelPreEvent.class, deleteModelPreEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, deleteModelPreEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, deleteModelPreEvent.operationStatus());
    Assertions.assertEquals(existingIdentA, deleteModelPreEvent.identifier());
  }

  @Test
  void testDeleteNotExistsModelEvent() {
    dispatcher.deleteModel(notExistingIdent);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    DeleteModelPreEvent deleteModelPreEvent = (DeleteModelPreEvent) preEvent;
    Assertions.assertEquals(DeleteModelPreEvent.class, deleteModelPreEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, deleteModelPreEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, deleteModelPreEvent.operationStatus());
  }

  @Test
  void testListModelEvent() {
    dispatcher.listModels(namespace);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListModelPreEvent listModelPreEvent = (ListModelPreEvent) preEvent;
    Assertions.assertEquals(namespace, listModelPreEvent.namespace());
  }

  @Test
  void testLinkModelVersionEvent() {
    dispatcher.linkModelVersion(
        existingIdentA,
        "uriA",
        new String[] {"aliasProduction"},
        "versionInfoA",
        ImmutableMap.of("color", "#FFFFFF"));

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(LinkModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LINK_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    // validate model info
    LinkModelVersionPreEvent linkModelVersionPreEvent = (LinkModelVersionPreEvent) preEvent;
    ModelInfo modelInfoPreEvent = linkModelVersionPreEvent.linkedModelInfo();
    Assertions.assertEquals("modelA", modelInfoPreEvent.getName());
    Assertions.assertEquals("commentA", modelInfoPreEvent.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfoPreEvent.getProperties());

    // validate model version info
    ModelVersionInfo[] modelVersionInfosPreEvent = modelInfoPreEvent.modelVersions();
    Assertions.assertNotNull(modelVersionInfosPreEvent);
    Assertions.assertEquals(1, modelVersionInfosPreEvent.length);
    Assertions.assertEquals("versionInfoA", modelVersionInfosPreEvent[0].getComment());
    Assertions.assertEquals("uriA", modelVersionInfosPreEvent[0].getUri());
    Assertions.assertEquals("aliasProduction", modelVersionInfosPreEvent[0].getAliases()[0]);
  }

  @Test
  void testGetModelVersionEventViaVersion() {
    dispatcher.getModelVersion(existingIdentA, 1);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testGetModelVersionEventViaAlias() {
    dispatcher.getModelVersion(existingIdentB, "aliasTest");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDeleteModelVersionEventViaVersion() {
    dispatcher.deleteModelVersion(existingIdentA, 1);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDeleteModelVersionEventViaAlias() {
    dispatcher.deleteModelVersion(existingIdentB, "aliasTest");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDeleteModelVersionEventViaVersionNotExists() {
    dispatcher.deleteModelVersion(existingIdentA, 3);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListModelVersionsEvent() {
    dispatcher.listModelVersions(existingIdentA);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListModelVersionsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSIONS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    // validate model info
    ListModelVersionsPreEvent listModelVersionsPreEvent = (ListModelVersionsPreEvent) preEvent;
    ModelInfo modelInfoPreEvent = listModelVersionsPreEvent.listModelVersionInfo();
    Assertions.assertEquals("modelA", modelInfoPreEvent.getName());
    Assertions.assertEquals("commentA", modelInfoPreEvent.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfoPreEvent.getProperties());
    Assertions.assertNull(modelInfoPreEvent.modelVersions());
  }

  private ModelDispatcher mockExceptionModelDispatcher() {
    return mock(
        ModelDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private ModelDispatcher mockTagDispatcher() {
    ModelDispatcher dispatcher = mock(ModelDispatcher.class);

    when(dispatcher.registerModel(existingIdentA, "commentA", ImmutableMap.of("color", "#FFFFFF")))
        .thenReturn(modelA);
    when(dispatcher.registerModel(existingIdentB, "commentB", ImmutableMap.of("color", "#FFFFFF")))
        .thenReturn(modelB);

    when(dispatcher.getModel(existingIdentA)).thenReturn(modelA);
    when(dispatcher.getModel(existingIdentB)).thenReturn(modelB);

    when(dispatcher.deleteModel(existingIdentA)).thenReturn(true);
    when(dispatcher.deleteModel(notExistingIdent)).thenReturn(false);

    when(dispatcher.listModels(namespace))
        .thenReturn(new NameIdentifier[] {existingIdentA, existingIdentB});

    when(dispatcher.getModelVersion(existingIdentA, 1)).thenReturn(firstModelVersion);
    when(dispatcher.getModelVersion(existingIdentB, "aliasTest")).thenReturn(secondModelVersion);

    when(dispatcher.deleteModelVersion(existingIdentA, 1)).thenReturn(true);
    when(dispatcher.deleteModelVersion(existingIdentB, "aliasTest")).thenReturn(true);
    when(dispatcher.deleteModelVersion(existingIdentA, 3)).thenReturn(false);

    when(dispatcher.listModelVersions(existingIdentA)).thenReturn(new int[] {1, 2});

    return dispatcher;
  }

  /**
   * Mock a model with given name and comment.
   *
   * @param name name of the model
   * @param comment comment of the model
   * @return a mock model
   */
  private Model mockModel(String name, String comment) {
    Model model = mock(Model.class);
    when(model.name()).thenReturn(name);
    when(model.comment()).thenReturn(comment);
    when(model.properties()).thenReturn(ImmutableMap.of("color", "#FFFFFF"));
    return model;
  }

  /**
   * Mock a model version with given uri, aliases, and comment.
   *
   * @param uri uri of the model version
   * @param aliases aliases of the model version
   * @param comment comment of the model version, added "model version " prefix.
   * @return a mock model version
   */
  private ModelVersion mockModelVersion(String uri, String[] aliases, String comment) {
    ModelVersion modelVersion = mock(ModelVersion.class);
    when(modelVersion.version()).thenReturn(1);
    when(modelVersion.uri()).thenReturn(uri);
    when(modelVersion.aliases()).thenReturn(aliases);
    when(modelVersion.comment()).thenReturn("model version " + comment);
    when(modelVersion.properties()).thenReturn(ImmutableMap.of("color", "#FFFFFF"));

    return modelVersion;
  }
}
