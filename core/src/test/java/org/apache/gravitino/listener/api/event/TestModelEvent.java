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
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Audit;
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
    this.modelA = getMockModel("modelA", "commentA");
    this.modelB = getMockModel("modelB", "commentB");
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
  void testModelInfo() {
    Model mockModel = getMockModel("model", "comment");
    ModelInfo modelInfo = new ModelInfo(mockModel);

    Assertions.assertEquals("model", modelInfo.name());
    Assertions.assertEquals(1, modelInfo.properties().size());
    Assertions.assertEquals("#FFFFFF", modelInfo.properties().get("color"));

    Assertions.assertTrue(modelInfo.comment().isPresent());
    String comment = modelInfo.comment().get();
    Assertions.assertEquals("comment", comment);

    Assertions.assertFalse(modelInfo.audit().isPresent());

    Assertions.assertTrue(modelInfo.lastVersion().isPresent());
    int lastVersion = modelInfo.lastVersion().get();
    Assertions.assertEquals(1, lastVersion);
  }

  @Test
  void testModelInfoWithoutComment() {
    Model mockModel = getMockModel("model", null);
    ModelInfo modelInfo = new ModelInfo(mockModel);

    Assertions.assertFalse(modelInfo.comment().isPresent());
  }

  @Test
  void testModelInfoWithAudit() {
    Model mockModel = getMockModelWithAudit("model", "comment");
    ModelInfo modelInfo = new ModelInfo(mockModel);

    Assertions.assertEquals("model", modelInfo.name());
    Assertions.assertEquals(1, modelInfo.properties().size());
    Assertions.assertEquals("#FFFFFF", modelInfo.properties().get("color"));

    Assertions.assertTrue(modelInfo.comment().isPresent());
    String comment = modelInfo.comment().get();
    Assertions.assertEquals("comment", comment);

    Assertions.assertTrue(modelInfo.audit().isPresent());
    Audit audit = modelInfo.audit().get();
    Assertions.assertEquals("demo_user", audit.creator());
    Assertions.assertEquals(1611111111111L, audit.createTime().toEpochMilli());
    Assertions.assertEquals("demo_user", audit.lastModifier());
    Assertions.assertEquals(1611111111111L, audit.lastModifiedTime().toEpochMilli());
  }

  @Test
  void testRegisterModelEvent() {
    dispatcher.registerModel(existingIdentA, "commentA", ImmutableMap.of("color", "#FFFFFF"));

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RegisterModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.REGISTER_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    // validate pre-event model info
    RegisterModelPreEvent registerModelPreEvent = (RegisterModelPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, registerModelPreEvent.identifier());
    ModelInfo modelInfoPreEvent = registerModelPreEvent.registerModelRequest();

    Assertions.assertEquals("modelA", modelInfoPreEvent.name());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelInfoPreEvent.properties());
    Assertions.assertTrue(modelInfoPreEvent.comment().isPresent());
    String comment = modelInfoPreEvent.comment().get();
    Assertions.assertEquals("commentA", comment);
    Assertions.assertFalse(modelInfoPreEvent.audit().isPresent());
    Assertions.assertFalse(modelInfoPreEvent.lastVersion().isPresent());
  }

  @Test
  void testRegisterAndLinkModelEvent() {
    dispatcher.registerModel(
        existingIdentA,
        "uriA",
        new String[] {"aliasProduction", "aliasTest"},
        "commentA",
        ImmutableMap.of("color", "#FFFFFF"));
    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RegisterAndLinkModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        OperationType.REGISTER_AND_LINK_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    // validate pre-event model info
    RegisterAndLinkModelPreEvent registerAndLinkModelPreEvent =
        (RegisterAndLinkModelPreEvent) preEvent;
    ModelInfo registerModelRequest = registerAndLinkModelPreEvent.registerModelRequest();

    Assertions.assertEquals("modelA", registerModelRequest.name());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), registerModelRequest.properties());
    Assertions.assertTrue(registerModelRequest.comment().isPresent());
    String comment = registerModelRequest.comment().get();
    Assertions.assertEquals("commentA", comment);
    Assertions.assertFalse(registerModelRequest.audit().isPresent());
    Assertions.assertFalse(registerModelRequest.lastVersion().isPresent());

    // validate pre-event model version info
    ModelVersionInfo modelVersionInfoPreEvent =
        registerAndLinkModelPreEvent.linkModelVersionRequest();
    Assertions.assertEquals("uriA", modelVersionInfoPreEvent.uri());
    Assertions.assertTrue(modelVersionInfoPreEvent.aliases().isPresent());
    String[] aliases = modelVersionInfoPreEvent.aliases().get();
    Assertions.assertEquals(2, aliases.length);
    Assertions.assertEquals("aliasProduction", aliases[0]);
    Assertions.assertEquals("aliasTest", aliases[1]);
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
    Assertions.assertEquals(existingIdentA, deleteModelPreEvent.identifier());
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
        new String[] {"aliasProduction", "aliasTest"},
        "versionInfoA",
        ImmutableMap.of("color", "#FFFFFF"));

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(LinkModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LINK_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    LinkModelVersionPreEvent linkModelVersionPreEvent = (LinkModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, linkModelVersionPreEvent.identifier());
    ModelVersionInfo modelVersionInfo = linkModelVersionPreEvent.linkModelVersionRequest();

    Assertions.assertEquals(1, modelVersionInfo.properties().size());
    Assertions.assertEquals("#FFFFFF", modelVersionInfo.properties().get("color"));

    Assertions.assertEquals("uriA", modelVersionInfo.uri());
    Assertions.assertTrue(modelVersionInfo.aliases().isPresent());
    String[] aliases = modelVersionInfo.aliases().get();
    Assertions.assertEquals(2, aliases.length);
    Assertions.assertEquals("aliasProduction", aliases[0]);
    Assertions.assertEquals("aliasTest", aliases[1]);

    Assertions.assertTrue(modelVersionInfo.comment().isPresent());
    String comment = modelVersionInfo.comment().get();
    Assertions.assertEquals("versionInfoA", comment);

    Assertions.assertFalse(modelVersionInfo.audit().isPresent());
  }

  @Test
  void testGetModelVersionEventViaVersion() {
    dispatcher.getModelVersion(existingIdentA, 1);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelVersionPreEvent getModelVersionPreEvent = (GetModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionPreEvent.identifier());
  }

  @Test
  void testGetModelVersionEventViaAlias() {
    dispatcher.getModelVersion(existingIdentB, "aliasTest");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelVersionPreEvent getModelVersionPreEvent = (GetModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentB, getModelVersionPreEvent.identifier());
    Assertions.assertTrue(getModelVersionPreEvent.alias().isPresent());
    Assertions.assertEquals("aliasTest", getModelVersionPreEvent.alias().get());
    Assertions.assertFalse(getModelVersionPreEvent.version().isPresent());
  }

  @Test
  void testDeleteModelVersionEventViaVersion() {
    dispatcher.deleteModelVersion(existingIdentA, 1);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    DeleteModelVersionPreEvent deleteModelVersionPreEvent = (DeleteModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, deleteModelVersionPreEvent.identifier());
    Assertions.assertTrue(deleteModelVersionPreEvent.version().isPresent());
    Assertions.assertEquals(1, deleteModelVersionPreEvent.version().get());
    Assertions.assertFalse(deleteModelVersionPreEvent.alias().isPresent());
  }

  @Test
  void testDeleteModelVersionEventViaAlias() {
    dispatcher.deleteModelVersion(existingIdentB, "aliasTest");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    DeleteModelVersionPreEvent deleteModelVersionPreEvent = (DeleteModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentB, deleteModelVersionPreEvent.identifier());
    Assertions.assertTrue(deleteModelVersionPreEvent.alias().isPresent());
    Assertions.assertEquals("aliasTest", deleteModelVersionPreEvent.alias().get());
    Assertions.assertFalse(deleteModelVersionPreEvent.version().isPresent());
  }

  @Test
  void testListModelVersionsEvent() {
    dispatcher.listModelVersions(existingIdentA);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSIONS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListModelVersionPreEvent listModelVersionsPreEvent = (ListModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, listModelVersionsPreEvent.identifier());
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

  private Model getMockModel(String name, String comment) {
    return getMockModel(name, comment, ImmutableMap.of("color", "#FFFFFF"));
  }

  private Model getMockModel(String name, String comment, Map<String, String> properties) {
    Model mockModel = mock(Model.class);
    when(mockModel.name()).thenReturn(name);
    when(mockModel.comment()).thenReturn(comment);
    when(mockModel.properties()).thenReturn(properties);
    when(mockModel.latestVersion()).thenReturn(1);
    when(mockModel.auditInfo()).thenReturn(null);

    return mockModel;
  }

  private Model getMockModelWithAudit(String name, String comment) {
    Model model = getMockModel(name, comment);
    Audit mockAudit = mock(Audit.class);

    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(mockAudit.lastModifier()).thenReturn("demo_user");
    when(mockAudit.lastModifiedTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(model.auditInfo()).thenReturn(mockAudit);

    return model;
  }

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
