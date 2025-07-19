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
import java.util.Optional;
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
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
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
  private Model alterNameModel;
  private ModelVersion alterCommentModelVersion;
  private ModelVersion otherAlterCommentModelVersion;
  private ModelChange modelRenameChange;
  private String newModelVersionComment;
  private ModelVersionChange modelUpdateCommentChange;
  private NameIdentifier existingIdentA;
  private NameIdentifier existingIdentB;
  private NameIdentifier notExistingIdent;
  private Namespace namespace;
  private ModelVersion firstModelVersion;
  private ModelVersion secondModelVersion;

  @BeforeAll
  void init() {
    this.newModelVersionComment = "new comment";
    this.namespace = Namespace.of("metalake", "catalog", "schema");
    this.existingIdentA = NameIdentifierUtil.ofModel("metalake", "catalog", "schema", "modelA");
    this.existingIdentB = NameIdentifierUtil.ofModel("metalake", "catalog", "schema", "modelB");

    this.modelA = getMockModel("modelA", "commentA");
    this.modelB = getMockModel("modelB", "commentB");
    this.alterNameModel = getMockModel("modelA_rename", "commentA");
    this.alterCommentModelVersion =
        getMockModelVersion(
            ImmutableMap.of("name1", "uri1", "name2", "uri2"),
            1,
            new String[] {"aliasProduction"},
            newModelVersionComment,
            ImmutableMap.of("color", "#FFFFFF"));
    this.otherAlterCommentModelVersion =
        getMockModelVersion(
            ImmutableMap.of("name3", "uri3"),
            2,
            new String[] {"aliasTest"},
            newModelVersionComment,
            ImmutableMap.of("color", "#FFFFFF"));
    this.modelRenameChange = getMockModelChange("modelA_rename");
    this.modelUpdateCommentChange = getMockModelVersionChange("new comment");

    this.firstModelVersion =
        mockModelVersion(
            ImmutableMap.of("name1", "uri1", "name2", "uri2"),
            new String[] {"aliasProduction"},
            "versionInfoA");
    this.secondModelVersion =
        mockModelVersion(
            ImmutableMap.of("name3", "uri3"), new String[] {"aliasTest"}, "versionInfoB");

    this.notExistingIdent =
        NameIdentifierUtil.ofModel("metalake", "catalog", "schema", "not_exist");

    this.dummyEventListener = new DummyEventListener();

    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new ModelEventDispatcher(eventBus, mockModelDispatcher());
    this.failureDispatcher = new ModelEventDispatcher(eventBus, mockExceptionModelDispatcher());
  }

  @Test
  void testModelInfo() {
    Model mockModel = getMockModel("model", "comment");
    ModelInfo modelInfo = new ModelInfo(mockModel);

    checkModelInfo(modelInfo, mockModel);
  }

  @Test
  void testModelInfoWithoutComment() {
    Model mockModel = getMockModel("model", null);
    ModelInfo modelInfo = new ModelInfo(mockModel);

    checkModelInfo(modelInfo, mockModel);
  }

  @Test
  void testModelInfoWithAudit() {
    Model mockModel = getMockModelWithAudit("model", "comment");
    ModelInfo modelInfo = new ModelInfo(mockModel);

    checkModelInfo(modelInfo, mockModel);
  }

  @Test
  void testModelVersionInfo() {
    ModelVersion modelVersion =
        mockModelVersion(
            ImmutableMap.of("nameA", "uriA"), new String[] {"aliasProduction"}, "versionInfoA");
    ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);

    checkModelVersionInfo(modelVersionInfo, modelVersion);
  }

  @Test
  void testModelVersionInfoWithoutComment() {
    ModelVersion modelVersion =
        mockModelVersion(ImmutableMap.of("nameA", "uriA"), new String[] {"aliasProduction"}, null);
    ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);

    checkModelVersionInfo(modelVersionInfo, modelVersion);
  }

  @Test
  void testModelVersionInfoWithAudit() {
    ModelVersion modelVersion =
        getMockModelWithAudit(
            ImmutableMap.of("nameA", "uriA"), new String[] {"aliasProduction"}, "versionInfoA");
    ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);

    checkModelVersionInfo(modelVersionInfo, modelVersion);
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

    checkModelInfo(modelInfoPreEvent, modelA);

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RegisterModelEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.REGISTER_MODEL, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    // validate post-event model info
    RegisterModelEvent registerModelEvent = (RegisterModelEvent) postEvent;
    Assertions.assertEquals(existingIdentA, registerModelEvent.identifier());
    ModelInfo modelInfo = registerModelEvent.registeredModelInfo();

    checkModelInfo(modelInfo, modelA);
  }

  @Test
  void testRegisterModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.registerModel(
                existingIdentA, "commentA", ImmutableMap.of("color", "#FFFFFF")));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(existingIdentA, event.identifier());
    Assertions.assertEquals(RegisterModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((RegisterModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.REGISTER_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    ModelInfo registerModelRequest = ((RegisterModelFailureEvent) event).registerModelRequest();

    // check model-info
    checkModelInfo(registerModelRequest, modelA);
  }

  @Test
  void testRegisterAndLinkModelEvent() {
    dispatcher.registerModel(
        existingIdentA,
        ImmutableMap.of("name1", "uri1", "name2", "uri2"),
        new String[] {"aliasProduction"},
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

    checkModelInfo(registerModelRequest, modelA);

    // validate pre-event model version info
    ModelVersionInfo linkModelVersionRequest =
        registerAndLinkModelPreEvent.linkModelVersionRequest();

    Assertions.assertEquals(firstModelVersion.uris(), linkModelVersionRequest.uris());
    Assertions.assertEquals("commentA", linkModelVersionRequest.comment().orElse(null));
    checkArray(firstModelVersion.aliases(), linkModelVersionRequest.aliases().orElse(null));
    checkProperties(firstModelVersion.properties(), linkModelVersionRequest.properties());
    checkAudit(firstModelVersion.auditInfo(), linkModelVersionRequest.audit());

    // valiate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RegisterAndLinkModelEvent.class, postEvent.getClass());
    Assertions.assertEquals(
        OperationType.REGISTER_AND_LINK_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    // validate post-event model info
    RegisterAndLinkModelEvent registerAndLinkModelEvent = (RegisterAndLinkModelEvent) postEvent;
    ModelInfo modelInfo = registerAndLinkModelEvent.registeredModelInfo();

    checkModelInfo(modelInfo, modelA);

    // validate post-event model uri info
    Map<String, String> versionUris = registerAndLinkModelEvent.uris();
    Assertions.assertEquals(firstModelVersion.uris(), versionUris);
  }

  @Test
  void testRegisterAndLinkModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.registerModel(
                existingIdentA,
                ImmutableMap.of("name1", "uri1", "name2", "uri2"),
                new String[] {"aliasProduction"},
                "commentA",
                ImmutableMap.of("color", "#FFFFFF")));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(RegisterAndLinkModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(existingIdentA, event.identifier());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((RegisterAndLinkModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.REGISTER_AND_LINK_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    // validate model info
    RegisterAndLinkModelFailureEvent registerAndLinkModelFailureEvent =
        (RegisterAndLinkModelFailureEvent) event;
    ModelInfo registerModelRequest = registerAndLinkModelFailureEvent.registerModelRequest();
    ModelVersionInfo linkModelVersionRequest =
        registerAndLinkModelFailureEvent.linkModelVersionRequest();

    checkModelInfo(registerModelRequest, modelA);
    Assertions.assertEquals(firstModelVersion.uris(), linkModelVersionRequest.uris());
    Assertions.assertEquals("commentA", linkModelVersionRequest.comment().orElse(null));
    checkArray(firstModelVersion.aliases(), linkModelVersionRequest.aliases().orElse(null));
    checkProperties(firstModelVersion.properties(), linkModelVersionRequest.properties());
    checkAudit(firstModelVersion.auditInfo(), linkModelVersionRequest.audit());
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

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    GetModelEvent getModelEvent = (GetModelEvent) postEvent;
    Assertions.assertEquals(existingIdentA, getModelEvent.identifier());
    ModelInfo modelInfo = getModelEvent.modelInfo();

    checkModelInfo(modelInfo, modelA);
  }

  @Test
  void testGetModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.getModel(notExistingIdent));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    GetModelFailureEvent getModelFailureEvent = (GetModelFailureEvent) event;
    Assertions.assertEquals(notExistingIdent, getModelFailureEvent.identifier());
  }

  @Test
  void testDeleteModelEvent() {
    dispatcher.deleteModel(existingIdentA);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    DeleteModelPreEvent deleteModelPreEvent = (DeleteModelPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, deleteModelPreEvent.identifier());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    DeleteModelEvent deleteModelEvent = (DeleteModelEvent) postEvent;
    Assertions.assertEquals(existingIdentA, deleteModelEvent.identifier());
    Assertions.assertTrue(deleteModelEvent.isExists());
  }

  @Test
  void testDeleteModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.deleteModel(notExistingIdent));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(DeleteModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DeleteModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    DeleteModelFailureEvent deleteModelFailureEvent = (DeleteModelFailureEvent) event;
    Assertions.assertEquals(notExistingIdent, deleteModelFailureEvent.identifier());
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

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListModelEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    ListModelEvent listModelEvent = (ListModelEvent) postEvent;
    checkArray(namespace.levels(), listModelEvent.namespace().levels());
  }

  @Test
  void testListModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listModels(namespace));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(ListModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    ListModelFailureEvent listModelFailureEvent = (ListModelFailureEvent) event;
    checkArray(namespace.levels(), listModelFailureEvent.namespace().levels());
  }

  @Test
  void testLinkModelVersionEvent() {
    dispatcher.linkModelVersion(
        existingIdentA,
        ImmutableMap.of("name1", "uri1", "name2", "uri2"),
        new String[] {"aliasProduction"},
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

    checkModelVersionInfo(modelVersionInfo, firstModelVersion);

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(LinkModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.LINK_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    LinkModelVersionEvent linkModelVersionEvent = (LinkModelVersionEvent) postEvent;
    Assertions.assertEquals(existingIdentA, linkModelVersionEvent.identifier());
    ModelVersionInfo postEventModelVersionInfo = linkModelVersionEvent.linkedModelVersionInfo();

    checkModelVersionInfo(postEventModelVersionInfo, firstModelVersion);
  }

  @Test
  void testLinkModelVersionFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.linkModelVersion(
                existingIdentA,
                ImmutableMap.of("name1", "uri1", "name2", "uri2"),
                new String[] {"aliasProduction"},
                "versionInfoA",
                ImmutableMap.of("color", "#FFFFFF")));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(LinkModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((LinkModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LINK_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    LinkModelVersionFailureEvent linkModelVersionFailureEvent =
        (LinkModelVersionFailureEvent) event;
    ModelVersionInfo modelVersionInfo = linkModelVersionFailureEvent.linkModelVersionRequest();

    checkModelVersionInfo(modelVersionInfo, firstModelVersion);
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

    // validate pre-event fields
    Assertions.assertTrue(getModelVersionPreEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionPreEvent.version().get());
    Assertions.assertFalse(getModelVersionPreEvent.alias().isPresent());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    GetModelVersionEvent getModelVersionEvent = (GetModelVersionEvent) postEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionEvent.identifier());
    ModelVersionInfo modelVersionInfo = getModelVersionEvent.modelVersionInfo();

    // validate post-event fields
    Assertions.assertTrue(getModelVersionEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionEvent.version().get());
    Assertions.assertFalse(getModelVersionEvent.alias().isPresent());

    checkModelVersionInfo(modelVersionInfo, firstModelVersion);
  }

  @Test
  void testGetModelVersionFailureEventViaVersion() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getModelVersion(existingIdentA, 3));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    GetModelVersionFailureEvent getModelVersionFailureEvent = (GetModelVersionFailureEvent) event;
    Assertions.assertTrue(getModelVersionFailureEvent.version().isPresent());
    Assertions.assertEquals(3, getModelVersionFailureEvent.version().get());
    Assertions.assertFalse(getModelVersionFailureEvent.alias().isPresent());
  }

  @Test
  void testGetModelVersionEventViaAlias() {
    dispatcher.getModelVersion(existingIdentB, "aliasTest");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    // validate pre-event fields
    GetModelVersionPreEvent getModelVersionPreEvent = (GetModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentB, getModelVersionPreEvent.identifier());
    Assertions.assertTrue(getModelVersionPreEvent.alias().isPresent());
    Assertions.assertEquals("aliasTest", getModelVersionPreEvent.alias().get());
    Assertions.assertFalse(getModelVersionPreEvent.version().isPresent());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    // validate post-event fields
    GetModelVersionEvent getModelVersionEvent = (GetModelVersionEvent) postEvent;
    ModelVersionInfo modelVersionInfo = getModelVersionEvent.modelVersionInfo();
    Assertions.assertTrue(getModelVersionEvent.alias().isPresent());
    Assertions.assertEquals("aliasTest", getModelVersionEvent.alias().get());
    Assertions.assertFalse(getModelVersionEvent.version().isPresent());

    checkModelVersionInfo(modelVersionInfo, secondModelVersion);
  }

  @Test
  void testGetModelVersionFailureEventViaAlias() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getModelVersion(existingIdentB, "aliasNotExist"));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    GetModelVersionFailureEvent getModelVersionFailureEvent = (GetModelVersionFailureEvent) event;
    Assertions.assertTrue(getModelVersionFailureEvent.alias().isPresent());
    Assertions.assertEquals("aliasNotExist", getModelVersionFailureEvent.alias().get());
    Assertions.assertFalse(getModelVersionFailureEvent.version().isPresent());
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

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    DeleteModelVersionEvent deleteModelVersionEvent = (DeleteModelVersionEvent) postEvent;
    Assertions.assertTrue(deleteModelVersionEvent.version().isPresent());
    Assertions.assertEquals(1, deleteModelVersionEvent.version().get());
    Assertions.assertFalse(deleteModelVersionEvent.alias().isPresent());
    Assertions.assertTrue(deleteModelVersionEvent.isExists());
  }

  @Test
  void testGetModelVersionUriFailureEventViaVersion() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getModelVersionUri(existingIdentA, 1, "n1"));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetModelVersionUriFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetModelVersionUriFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    GetModelVersionUriFailureEvent getModelVersionUriFailureEvent =
        (GetModelVersionUriFailureEvent) event;
    Assertions.assertEquals(existingIdentA, getModelVersionUriFailureEvent.identifier());
    Assertions.assertTrue(getModelVersionUriFailureEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionUriFailureEvent.version().get());
    Assertions.assertFalse(getModelVersionUriFailureEvent.alias().isPresent());
    Assertions.assertEquals("n1", getModelVersionUriFailureEvent.uriName());
  }

  @Test
  void testGetModelVersionUriFailureEventViaAlias() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getModelVersionUri(existingIdentA, "alias", "n1"));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetModelVersionUriFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetModelVersionUriFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    GetModelVersionUriFailureEvent getModelVersionUriFailureEvent =
        (GetModelVersionUriFailureEvent) event;
    Assertions.assertEquals(existingIdentA, getModelVersionUriFailureEvent.identifier());
    Assertions.assertTrue(getModelVersionUriFailureEvent.alias().isPresent());
    Assertions.assertEquals("alias", getModelVersionUriFailureEvent.alias().get());
    Assertions.assertFalse(getModelVersionUriFailureEvent.version().isPresent());
    Assertions.assertEquals("n1", getModelVersionUriFailureEvent.uriName());
  }

  @Test
  void testGetModelVersionUriEventViaVersion() {
    dispatcher.getModelVersionUri(existingIdentA, 1, "n1");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionUriPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelVersionUriPreEvent getModelVersionUriPreEvent = (GetModelVersionUriPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriPreEvent.identifier());
    Assertions.assertTrue(getModelVersionUriPreEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionUriPreEvent.version().get());
    Assertions.assertFalse(getModelVersionUriPreEvent.alias().isPresent());
    Assertions.assertEquals("n1", getModelVersionUriPreEvent.uriName());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionUriEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    GetModelVersionUriEvent getModelVersionUriEvent = (GetModelVersionUriEvent) postEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriEvent.identifier());
    Assertions.assertTrue(getModelVersionUriEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionUriEvent.version().get());
    Assertions.assertFalse(getModelVersionUriEvent.alias().isPresent());
    Assertions.assertEquals("n1", getModelVersionUriEvent.uriName());
    Assertions.assertEquals("u1", getModelVersionUriEvent.uri());
  }

  @Test
  void testGetModelVersionUriEventWithoutUriNameViaVersion() {
    dispatcher.getModelVersionUri(existingIdentA, 1, null);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionUriPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelVersionUriPreEvent getModelVersionUriPreEvent = (GetModelVersionUriPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriPreEvent.identifier());
    Assertions.assertTrue(getModelVersionUriPreEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionUriPreEvent.version().get());
    Assertions.assertFalse(getModelVersionUriPreEvent.alias().isPresent());
    Assertions.assertNull(getModelVersionUriPreEvent.uriName());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionUriEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    GetModelVersionUriEvent getModelVersionUriEvent = (GetModelVersionUriEvent) postEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriEvent.identifier());
    Assertions.assertTrue(getModelVersionUriEvent.version().isPresent());
    Assertions.assertEquals(1, getModelVersionUriEvent.version().get());
    Assertions.assertFalse(getModelVersionUriEvent.alias().isPresent());
    Assertions.assertNull(getModelVersionUriEvent.uriName());
    Assertions.assertEquals("u1", getModelVersionUriEvent.uri());
  }

  @Test
  void testGetModelVersionUriEventViaAlias() {
    dispatcher.getModelVersionUri(existingIdentA, "alias", "n1");

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionUriPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelVersionUriPreEvent getModelVersionUriPreEvent = (GetModelVersionUriPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriPreEvent.identifier());
    Assertions.assertTrue(getModelVersionUriPreEvent.alias().isPresent());
    Assertions.assertEquals("alias", getModelVersionUriPreEvent.alias().get());
    Assertions.assertFalse(getModelVersionUriPreEvent.version().isPresent());
    Assertions.assertEquals("n1", getModelVersionUriPreEvent.uriName());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionUriEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    GetModelVersionUriEvent getModelVersionUriEvent = (GetModelVersionUriEvent) postEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriEvent.identifier());
    Assertions.assertTrue(getModelVersionUriEvent.alias().isPresent());
    Assertions.assertEquals("alias", getModelVersionUriEvent.alias().get());
    Assertions.assertFalse(getModelVersionUriEvent.version().isPresent());
    Assertions.assertEquals("n1", getModelVersionUriEvent.uriName());
    Assertions.assertEquals("u1", getModelVersionUriEvent.uri());
  }

  @Test
  void testGetModelVersionUriEventWithoutUriNameViaAlias() {
    dispatcher.getModelVersionUri(existingIdentA, "alias", null);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetModelVersionUriPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    GetModelVersionUriPreEvent getModelVersionUriPreEvent = (GetModelVersionUriPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriPreEvent.identifier());
    Assertions.assertTrue(getModelVersionUriPreEvent.alias().isPresent());
    Assertions.assertEquals("alias", getModelVersionUriPreEvent.alias().get());
    Assertions.assertFalse(getModelVersionUriPreEvent.version().isPresent());
    Assertions.assertNull(getModelVersionUriPreEvent.uriName());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetModelVersionUriEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.GET_MODEL_VERSION_URI, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    GetModelVersionUriEvent getModelVersionUriEvent = (GetModelVersionUriEvent) postEvent;
    Assertions.assertEquals(existingIdentA, getModelVersionUriEvent.identifier());
    Assertions.assertTrue(getModelVersionUriEvent.alias().isPresent());
    Assertions.assertEquals("alias", getModelVersionUriEvent.alias().get());
    Assertions.assertFalse(getModelVersionUriEvent.version().isPresent());
    Assertions.assertNull(getModelVersionUriEvent.uriName());
    Assertions.assertEquals("u1", getModelVersionUriEvent.uri());
  }

  @Test
  void testDeleteModelVersionFailureEventViaVersion() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.deleteModelVersion(existingIdentA, 3));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(DeleteModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((DeleteModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    DeleteModelVersionFailureEvent deleteModelVersionFailureEvent =
        (DeleteModelVersionFailureEvent) event;
    Assertions.assertTrue(deleteModelVersionFailureEvent.version().isPresent());
    Assertions.assertEquals(3, deleteModelVersionFailureEvent.version().get());
    Assertions.assertFalse(deleteModelVersionFailureEvent.alias().isPresent());
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

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    DeleteModelVersionEvent deleteModelVersionEvent = (DeleteModelVersionEvent) postEvent;
    Assertions.assertTrue(deleteModelVersionEvent.alias().isPresent());
    Assertions.assertEquals("aliasTest", deleteModelVersionEvent.alias().get());
    Assertions.assertFalse(deleteModelVersionEvent.version().isPresent());
    Assertions.assertTrue(deleteModelVersionEvent.isExists());
  }

  @Test
  void testDeleteModelVersionFailureEventViaAlias() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.deleteModelVersion(existingIdentB, "aliasNotExist"));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(DeleteModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((DeleteModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    DeleteModelVersionFailureEvent deleteModelVersionFailureEvent =
        (DeleteModelVersionFailureEvent) event;
    Assertions.assertTrue(deleteModelVersionFailureEvent.alias().isPresent());
    Assertions.assertEquals("aliasNotExist", deleteModelVersionFailureEvent.alias().get());
    Assertions.assertFalse(deleteModelVersionFailureEvent.version().isPresent());
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

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListModelVersionsEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSIONS, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    ListModelVersionsEvent listModelVersionsEvent = (ListModelVersionsEvent) postEvent;
    Assertions.assertEquals(existingIdentA, listModelVersionsEvent.identifier());
    Assertions.assertEquals(2, listModelVersionsEvent.versions().length);
    Assertions.assertArrayEquals(new int[] {1, 2}, listModelVersionsEvent.versions());
  }

  @Test
  void testListModelVersionInfosEvent() {
    dispatcher.listModelVersionInfos(existingIdentA);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSIONS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListModelVersionPreEvent listModelVersionsPreEvent = (ListModelVersionPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, listModelVersionsPreEvent.identifier());

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListModelVersionInfosEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSION_INFOS, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    ListModelVersionInfosEvent listModelVersionInfosEvent = (ListModelVersionInfosEvent) postEvent;
    Assertions.assertEquals(existingIdentA, listModelVersionInfosEvent.identifier());
    Assertions.assertEquals(1, listModelVersionInfosEvent.versions().length);
    checkModelVersionInfo(listModelVersionInfosEvent.versions()[0], firstModelVersion);
  }

  @Test
  void testListModelVersionsFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listModelVersions(existingIdentA));

    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(ListModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_MODEL_VERSIONS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    ListModelVersionFailureEvent listModelVersionsFailureEvent =
        (ListModelVersionFailureEvent) event;
    Assertions.assertEquals(existingIdentA, listModelVersionsFailureEvent.identifier());
  }

  @Test
  void testAlterModelPreEvent() {
    dispatcher.alterModel(existingIdentA, modelRenameChange);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AlterModelPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    AlterModelPreEvent alterModelPreEvent = (AlterModelPreEvent) preEvent;
    Assertions.assertEquals(existingIdentA, alterModelPreEvent.identifier());
    ModelChange[] changes = alterModelPreEvent.modelChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelRenameChange, changes[0]);
  }

  @Test
  void testAlterModelEvent() {
    dispatcher.alterModel(existingIdentA, modelRenameChange);

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterModelEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    AlterModelEvent alterModelEvent = (AlterModelEvent) postEvent;
    ModelChange[] changes = alterModelEvent.modelChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelRenameChange, changes[0]);
    ModelInfo modelInfo = alterModelEvent.updatedModelInfo();

    checkModelInfo(modelInfo, alterNameModel);
  }

  @Test
  void testAlterModelFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterModel(existingIdentA, modelRenameChange));

    // validate failure event
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(AlterModelFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterModelFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    AlterModelFailureEvent alterModelFailureEvent = (AlterModelFailureEvent) event;
    ModelChange[] changes = alterModelFailureEvent.modelChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelRenameChange, changes[0]);
  }

  private ModelDispatcher mockExceptionModelDispatcher() {
    return mock(
        ModelDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  @Test
  void testAlterModelVersionPreEventWithVersion() {
    dispatcher.alterModelVersion(existingIdentA, 1, modelUpdateCommentChange);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AlterModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    AlterModelVersionPreEvent alterModelVersionPreEvent = (AlterModelVersionPreEvent) preEvent;
    ModelVersionChange[] changes = alterModelVersionPreEvent.modelVersionChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelUpdateCommentChange, changes[0]);

    // validate alias and version fields
    Assertions.assertEquals(1, alterModelVersionPreEvent.version());
  }

  @Test
  void testAlterModelVersionPreEventWithAlias() {
    dispatcher.alterModelVersion(existingIdentB, "aliasTest", modelUpdateCommentChange);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AlterModelVersionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL_VERSION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    AlterModelVersionPreEvent alterModelVersionPreEvent = (AlterModelVersionPreEvent) preEvent;
    ModelVersionChange[] changes = alterModelVersionPreEvent.modelVersionChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelUpdateCommentChange, changes[0]);

    // validate alias and version fields
    Assertions.assertEquals("aliasTest", alterModelVersionPreEvent.alias());
  }

  @Test
  void testAlterModelVersionEventWithVersion() {
    dispatcher.alterModelVersion(existingIdentA, 1, modelUpdateCommentChange);

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    AlterModelVersionEvent alterModelVersionEvent = (AlterModelVersionEvent) postEvent;
    ModelVersionChange[] changes = alterModelVersionEvent.modelVersionChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelUpdateCommentChange, changes[0]);
    ModelVersionInfo modelVersionInfo = alterModelVersionEvent.alteredModelVersionInfo();

    // validate ModelVersionInfo
    Assertions.assertEquals(
        ImmutableMap.of("name1", "uri1", "name2", "uri2"), modelVersionInfo.uris());
    Assertions.assertTrue(modelVersionInfo.aliases().isPresent());
    Assertions.assertArrayEquals(
        new String[] {"aliasProduction"}, modelVersionInfo.aliases().get());
    Assertions.assertTrue(modelVersionInfo.comment().isPresent());
    Assertions.assertEquals(newModelVersionComment, modelVersionInfo.comment().get());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelVersionInfo.properties());
  }

  @Test
  void testAlterModelVersionEventWithAlias() {
    dispatcher.alterModelVersion(existingIdentB, "aliasTest", modelUpdateCommentChange);

    // validate post-event
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterModelVersionEvent.class, postEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL_VERSION, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    AlterModelVersionEvent alterModelVersionEvent = (AlterModelVersionEvent) postEvent;
    ModelVersionChange[] changes = alterModelVersionEvent.modelVersionChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelUpdateCommentChange, changes[0]);
    ModelVersionInfo modelVersionInfo = alterModelVersionEvent.alteredModelVersionInfo();

    // validate ModelVersionInfo
    Assertions.assertEquals(ImmutableMap.of("name3", "uri3"), modelVersionInfo.uris());
    Assertions.assertTrue(modelVersionInfo.aliases().isPresent());
    Assertions.assertArrayEquals(new String[] {"aliasTest"}, modelVersionInfo.aliases().get());
    Assertions.assertTrue(modelVersionInfo.comment().isPresent());
    Assertions.assertEquals(newModelVersionComment, modelVersionInfo.comment().get());
    Assertions.assertEquals(ImmutableMap.of("color", "#FFFFFF"), modelVersionInfo.properties());
  }

  @Test
  void testAlterModelVersionFailureEventWithVersion() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterModelVersion(existingIdentA, 1, modelUpdateCommentChange));

    // validate failure event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AlterModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    AlterModelVersionFailureEvent alterModelVersionFailureEvent =
        (AlterModelVersionFailureEvent) event;
    ModelVersionChange[] changes = alterModelVersionFailureEvent.modelVersionChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelUpdateCommentChange, changes[0]);

    // validate alias and version fields
    Assertions.assertEquals(1, alterModelVersionFailureEvent.version());
  }

  @Test
  void testAlterModelVersionFailureEventWithAlias() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.alterModelVersion(
                existingIdentB, "aliasTest", modelUpdateCommentChange));

    // validate failure event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterModelVersionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AlterModelVersionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.ALTER_MODEL_VERSION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    AlterModelVersionFailureEvent alterModelVersionFailureEvent =
        (AlterModelVersionFailureEvent) event;
    ModelVersionChange[] changes = alterModelVersionFailureEvent.modelVersionChanges();
    Assertions.assertEquals(1, changes.length);
    Assertions.assertEquals(modelUpdateCommentChange, changes[0]);

    // validate alias and version fields
    Assertions.assertEquals("aliasTest", alterModelVersionFailureEvent.alias());
  }

  private ModelDispatcher mockModelDispatcher() {
    ModelDispatcher dispatcher = mock(ModelDispatcher.class);

    when(dispatcher.registerModel(existingIdentA, "commentA", ImmutableMap.of("color", "#FFFFFF")))
        .thenReturn(modelA);
    when(dispatcher.registerModel(existingIdentB, "commentB", ImmutableMap.of("color", "#FFFFFF")))
        .thenReturn(modelB);
    when(dispatcher.registerModel(
            existingIdentA,
            ImmutableMap.of("name1", "uri1", "name2", "uri2"),
            new String[] {"aliasProduction"},
            "commentA",
            ImmutableMap.of("color", "#FFFFFF")))
        .thenReturn(modelA);

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
    when(dispatcher.listModelVersionInfos(existingIdentA))
        .thenReturn(new ModelVersion[] {firstModelVersion});

    when(dispatcher.alterModel(existingIdentA, new ModelChange[] {modelRenameChange}))
        .thenReturn(alterNameModel);

    when(dispatcher.alterModelVersion(existingIdentA, 1, modelUpdateCommentChange))
        .thenReturn(alterCommentModelVersion);
    when(dispatcher.alterModelVersion(existingIdentB, "aliasTest", modelUpdateCommentChange))
        .thenReturn(otherAlterCommentModelVersion);

    when(dispatcher.getModelVersionUri(existingIdentA, 1, "n1")).thenReturn("u1");
    when(dispatcher.getModelVersionUri(existingIdentA, 1, null)).thenReturn("u1");
    when(dispatcher.getModelVersionUri(existingIdentA, "alias", "n1")).thenReturn("u1");
    when(dispatcher.getModelVersionUri(existingIdentA, "alias", null)).thenReturn("u1");
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

  private ModelVersion getMockModelVersion(
      Map<String, String> uris,
      int version,
      String[] aliases,
      String comment,
      Map<String, String> properties) {
    ModelVersion mockModelVersion = mock(ModelVersion.class);
    when(mockModelVersion.version()).thenReturn(version);
    when(mockModelVersion.uris()).thenReturn(uris);
    when(mockModelVersion.aliases()).thenReturn(aliases);
    when(mockModelVersion.comment()).thenReturn(comment);
    when(mockModelVersion.properties()).thenReturn(properties);

    return mockModelVersion;
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

  private ModelVersion mockModelVersion(
      Map<String, String> uris, String[] aliases, String comment) {
    ModelVersion modelVersion = mock(ModelVersion.class);
    when(modelVersion.version()).thenReturn(1);
    when(modelVersion.uris()).thenReturn(uris);
    when(modelVersion.aliases()).thenReturn(aliases);
    when(modelVersion.comment()).thenReturn(comment);
    when(modelVersion.properties()).thenReturn(ImmutableMap.of("color", "#FFFFFF"));

    return modelVersion;
  }

  private ModelVersion getMockModelWithAudit(
      Map<String, String> uris, String[] aliases, String comment) {
    ModelVersion modelVersion = mockModelVersion(uris, aliases, comment);
    Audit mockAudit = mock(Audit.class);

    when(mockAudit.creator()).thenReturn("demo_user");
    when(mockAudit.createTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(mockAudit.lastModifier()).thenReturn("demo_user");
    when(mockAudit.lastModifiedTime()).thenReturn(Instant.ofEpochMilli(1611111111111L));
    when(modelVersion.auditInfo()).thenReturn(mockAudit);

    return modelVersion;
  }

  private void checkModelInfo(ModelInfo modelInfo, Model model) {
    // check normal fields
    Assertions.assertEquals(model.name(), modelInfo.name());
    Assertions.assertEquals(model.comment(), modelInfo.comment().orElse(null));

    // check properties
    checkProperties(model.properties(), modelInfo.properties());

    // check audit
    checkAudit(model.auditInfo(), modelInfo.audit());
  }

  private void checkModelVersionInfo(ModelVersionInfo modelVersionInfo, ModelVersion modelVersion) {
    // check normal fields
    Assertions.assertEquals(modelVersion.uris(), modelVersionInfo.uris());
    Assertions.assertEquals(modelVersion.comment(), modelVersionInfo.comment().orElse(null));

    // check aliases
    checkArray(modelVersion.aliases(), modelVersionInfo.aliases().orElse(null));

    // check properties
    checkProperties(modelVersion.properties(), modelVersionInfo.properties());

    // check audit
    checkAudit(modelVersion.auditInfo(), modelVersionInfo.audit());
  }

  private void checkProperties(
      Map<String, String> expectedProperties, Map<String, String> properties) {
    Assertions.assertEquals(expectedProperties.size(), properties.size());
    expectedProperties.forEach(
        (key, value) -> {
          Assertions.assertTrue(properties.containsKey(key));
          Assertions.assertEquals(value, properties.get(key));
        });
  }

  private void checkAudit(Audit expectedAudit, Optional<Audit> audit) {
    if (expectedAudit == null) {
      Assertions.assertFalse(audit.isPresent());
      return;
    }

    Assertions.assertTrue(audit.isPresent());
    Audit auditInfo = audit.get();

    Assertions.assertEquals(auditInfo.creator(), auditInfo.creator());
    Assertions.assertEquals(auditInfo.createTime(), auditInfo.createTime());
    Assertions.assertEquals(auditInfo.lastModifier(), auditInfo.lastModifier());
    Assertions.assertEquals(auditInfo.lastModifiedTime(), auditInfo.lastModifiedTime());
  }

  private void checkArray(String[] expected, String[] actual) {
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(expected.length, actual.length);
    Assertions.assertArrayEquals(expected, actual);
  }

  private ModelChange getMockModelChange(String newName) {
    ModelChange.RenameModel mockObject = mock(ModelChange.RenameModel.class);
    when(mockObject.newName()).thenReturn(newName);

    return mockObject;
  }

  private ModelVersionChange getMockModelVersionChange(String newName) {
    ModelVersionChange.UpdateComment mockObject = mock(ModelVersionChange.UpdateComment.class);
    when(mockObject.newComment()).thenReturn(newName);

    return mockObject;
  }
}
