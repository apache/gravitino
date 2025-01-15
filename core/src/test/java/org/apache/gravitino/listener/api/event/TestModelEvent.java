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
    dispatcher.registerModel(existingIdentA, "comment", ImmutableMap.of("color", "#FFFFFF"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RegisterModelEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.REGISTER_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    RegisterModelEvent registerModelEvent = (RegisterModelEvent) event;
    ModelInfo modelInfo = registerModelEvent.registeredModelInfo();
    Assertions.assertEquals("modelA", modelInfo.getName());
    Assertions.assertEquals("commentA", modelInfo.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfo.getProperties());
    Assertions.assertNull(modelInfo.modelVersions());
  }

  @Test
  void testGetModelEvent() {
    Model model = dispatcher.getModel(existingIdentA);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertNotNull(model);

    // validate model info
    GetModelEvent getModelEvent = (GetModelEvent) event;
    ModelInfo modelInfo = getModelEvent.getModelInfo();
    Assertions.assertEquals("modelA", modelInfo.getName());
    Assertions.assertEquals("commentA", modelInfo.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfo.getProperties());
    Assertions.assertNull(modelInfo.modelVersions());
  }

  @Test
  void testDeleteExistsModelEvent() {
    boolean isExists = dispatcher.deleteModel(existingIdentA);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertTrue(isExists);

    DeleteModelEvent deleteModelEvent = (DeleteModelEvent) event;
    Assertions.assertEquals(existingIdentA, deleteModelEvent.identifier());
    Assertions.assertEquals(DeleteModelEvent.class, deleteModelEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, deleteModelEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, deleteModelEvent.operationStatus());
  }

  @Test
  void testDeleteNotExistsModelEvent() {
    boolean isExists = dispatcher.deleteModel(notExistingIdent);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, event.operationStatus());
    Assertions.assertFalse(isExists);

    DeleteModelEvent deleteModelEvent = (DeleteModelEvent) event;
    Assertions.assertEquals(DeleteModelEvent.class, deleteModelEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, deleteModelEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, deleteModelEvent.operationStatus());
  }

  @Test
  void testListModelEvent() {
    NameIdentifier[] nameIdentifiers = dispatcher.listModels(namespace);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListModelEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    ListModelEvent listModelEvent = (ListModelEvent) event;
    Assertions.assertEquals(namespace, listModelEvent.namespace());

    Assertions.assertEquals(2, nameIdentifiers.length);
    Assertions.assertEquals(existingIdentA, nameIdentifiers[0]);
    Assertions.assertEquals(existingIdentB, nameIdentifiers[1]);
  }

  @Test
  void testLinkModelVersionEvent() {
    dispatcher.linkModelVersion(
        existingIdentA,
        "uriA",
        new String[] {"aliasProduction"},
        "versionInfoA",
        ImmutableMap.of("color", "#FFFFFF"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(LinkModelVersionEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.LINK_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    // validate model info
    LinkModelVersionEvent linkModelVersionEvent = (LinkModelVersionEvent) event;
    ModelInfo modelInfo = linkModelVersionEvent.linkModelVersionInfo();
    Assertions.assertEquals("modelA", modelInfo.getName());
    Assertions.assertEquals("commentA", modelInfo.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfo.getProperties());

    // validate model version info
    ModelVersionInfo[] modelVersionInfos = modelInfo.modelVersions();
    Assertions.assertNotNull(modelVersionInfos);
    Assertions.assertEquals(1, modelVersionInfos.length);
    Assertions.assertEquals("versionInfoA", modelVersionInfos[0].getComment());
    Assertions.assertEquals("uriA", modelVersionInfos[0].getUri());
    Assertions.assertEquals("aliasProduction", modelVersionInfos[0].getAliases()[0]);
  }

  @Test
  void testGetModelVersionEventViaVersion() {
    dispatcher.getModelVersion(existingIdentA, 1);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    // validate model info
    GetModelVersionEvent getModelVersionEvent = (GetModelVersionEvent) event;
    ModelInfo modelInfo = getModelVersionEvent.getModelVersionInfo();
    Assertions.assertEquals("modelA", modelInfo.getName());
    Assertions.assertEquals("commentA", modelInfo.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfo.getProperties());

    // validate model version info
    ModelVersionInfo[] modelVersionInfos = modelInfo.modelVersions();
    Assertions.assertNotNull(modelVersionInfos);
    Assertions.assertEquals(1, modelVersionInfos.length);
    Assertions.assertEquals("model version versionInfoA", modelVersionInfos[0].getComment());
    Assertions.assertEquals("uriA", modelVersionInfos[0].getUri());
    Assertions.assertEquals("aliasProduction", modelVersionInfos[0].getAliases()[0]);
  }

  @Test
  void testGetModelVersionEventViaAlias() {
    dispatcher.getModelVersion(existingIdentB, "aliasTest");
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    // validate model info
    GetModelVersionEvent getModelVersionEvent = (GetModelVersionEvent) event;
    ModelInfo modelInfo = getModelVersionEvent.getModelVersionInfo();
    Assertions.assertEquals("modelB", modelInfo.getName());
    Assertions.assertEquals("commentB", modelInfo.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfo.getProperties());

    // validate model version info
    ModelVersionInfo[] modelVersionInfos = modelInfo.modelVersions();
    Assertions.assertNotNull(modelVersionInfos);
    Assertions.assertEquals(1, modelVersionInfos.length);
    Assertions.assertEquals("model version versionInfoB", modelVersionInfos[0].getComment());
    Assertions.assertEquals("uriB", modelVersionInfos[0].getUri());
    Assertions.assertEquals("aliasTest", modelVersionInfos[0].getAliases()[0]);
  }

  @Test
  void testDeleteModelVersionEventViaVersion() {
    boolean isExists = dispatcher.deleteModelVersion(existingIdentA, 1);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelVersionEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertTrue(isExists);

    // validate model info
    DeleteModelVersionEvent deleteModelVersionEvent = (DeleteModelVersionEvent) event;
    Assertions.assertEquals(existingIdentA, deleteModelVersionEvent.identifier());
    Assertions.assertEquals(DeleteModelVersionEvent.class, deleteModelVersionEvent.getClass());
    Assertions.assertEquals(
        OperationType.DELETE_MODEL_VERSION, deleteModelVersionEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, deleteModelVersionEvent.operationStatus());
  }

  @Test
  void testDeleteModelVersionEventViaAlias() {
    boolean isExists = dispatcher.deleteModelVersion(existingIdentB, "aliasTest");
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelVersionEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertTrue(isExists);

    // validate model info
    DeleteModelVersionEvent deleteModelVersionEvent = (DeleteModelVersionEvent) event;
    Assertions.assertEquals(existingIdentB, deleteModelVersionEvent.identifier());
    Assertions.assertEquals(DeleteModelVersionEvent.class, deleteModelVersionEvent.getClass());
    Assertions.assertEquals(
        OperationType.DELETE_MODEL_VERSION, deleteModelVersionEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, deleteModelVersionEvent.operationStatus());
  }

  @Test
  void testDeleteModelVersionEventViaVersionNotExists() {
    boolean isExists = dispatcher.deleteModelVersion(existingIdentA, 3);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelVersionEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, event.operationStatus());
    Assertions.assertFalse(isExists);

    // validate model info
    DeleteModelVersionEvent deleteModelVersionEvent = (DeleteModelVersionEvent) event;
    Assertions.assertEquals(DeleteModelVersionEvent.class, deleteModelVersionEvent.getClass());
    Assertions.assertEquals(
        OperationType.DELETE_MODEL_VERSION, deleteModelVersionEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, deleteModelVersionEvent.operationStatus());
  }

  @Test
  void testListModelVersionsEvent() {
    int[] versions = dispatcher.listModelVersions(existingIdentA);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListModelVersionsEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSIONS, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(2, versions.length);
    Assertions.assertEquals(1, versions[0]);
    Assertions.assertEquals(2, versions[1]);

    // validate model info
    ListModelVersionsEvent listModelVersionsEvent = (ListModelVersionsEvent) event;
    ModelInfo modelInfo = listModelVersionsEvent.listModelVersionInfo();
    Assertions.assertEquals("modelA", modelInfo.getName());
    Assertions.assertEquals("commentA", modelInfo.getComment());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfo.getProperties());
    Assertions.assertNull(modelInfo.modelVersions());
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

    when(dispatcher.registerModel(existingIdentA, "comment", ImmutableMap.of("color", "#FFFFFF")))
        .thenReturn(modelA);
    when(dispatcher.registerModel(existingIdentB, "comment", ImmutableMap.of("color", "#FFFFFF")))
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
