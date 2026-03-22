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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.FunctionEventDispatcher;
import org.apache.gravitino.listener.api.info.FunctionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestFunctionEvent {
  private FunctionEventDispatcher dispatcher;
  private FunctionEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Function function;

  @BeforeAll
  void init() {
    this.function = mockFunction();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    FunctionDispatcher functionDispatcher = mockFunctionDispatcher();
    this.dispatcher = new FunctionEventDispatcher(eventBus, functionDispatcher);
    FunctionDispatcher functionExceptionDispatcher = mockExceptionFunctionDispatcher();
    this.failureDispatcher = new FunctionEventDispatcher(eventBus, functionExceptionDispatcher);
  }

  @Test
  void testRegisterFunctionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    dispatcher.registerFunction(
        identifier,
        function.comment(),
        function.functionType(),
        function.deterministic(),
        function.definitions());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(RegisterFunctionEvent.class, event.getClass());
    FunctionInfo functionInfo = ((RegisterFunctionEvent) event).registeredFunctionInfo();
    checkFunctionInfo(functionInfo, function);
    Assertions.assertEquals(OperationType.REGISTER_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(RegisterFunctionPreEvent.class, preEvent.getClass());
    FunctionInfo requestInfo = ((RegisterFunctionPreEvent) preEvent).registerFunctionRequest();
    Assertions.assertEquals(function.functionType(), requestInfo.functionType());
    Assertions.assertEquals(function.deterministic(), requestInfo.deterministic());
    Assertions.assertEquals(OperationType.REGISTER_FUNCTION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testGetFunctionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    dispatcher.getFunction(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(GetFunctionEvent.class, event.getClass());
    FunctionInfo functionInfo = ((GetFunctionEvent) event).functionInfo();
    checkFunctionInfo(functionInfo, function);
    Assertions.assertEquals(OperationType.GET_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(GetFunctionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_FUNCTION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testAlterFunctionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    FunctionChange change = FunctionChange.updateComment("new comment");
    dispatcher.alterFunction(identifier, change);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterFunctionEvent.class, event.getClass());
    FunctionInfo functionInfo = ((AlterFunctionEvent) event).updatedFunctionInfo();
    checkFunctionInfo(functionInfo, function);
    Assertions.assertEquals(1, ((AlterFunctionEvent) event).functionChanges().length);
    Assertions.assertEquals(change, ((AlterFunctionEvent) event).functionChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(AlterFunctionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(1, ((AlterFunctionPreEvent) preEvent).functionChanges().length);
    Assertions.assertEquals(change, ((AlterFunctionPreEvent) preEvent).functionChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_FUNCTION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDropFunctionEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    dispatcher.dropFunction(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropFunctionEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropFunctionEvent) event).isExists());
    Assertions.assertEquals(OperationType.DROP_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(DropFunctionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_FUNCTION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListFunctionEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog", "schema");
    dispatcher.listFunctions(namespace);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListFunctionEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListFunctionEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(namespace.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListFunctionPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(namespace, ((ListFunctionPreEvent) preEvent).namespace());
    Assertions.assertEquals(OperationType.LIST_FUNCTION, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testRegisterFunctionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.registerFunction(
                identifier,
                function.comment(),
                function.functionType(),
                function.deterministic(),
                function.definitions()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(RegisterFunctionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((RegisterFunctionFailureEvent) event).exception().getClass());
    Assertions.assertNotNull(((RegisterFunctionFailureEvent) event).registerFunctionRequest());
    Assertions.assertEquals(OperationType.REGISTER_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetFunctionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.getFunction(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(GetFunctionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetFunctionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterFunctionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    FunctionChange change = FunctionChange.updateComment("new comment");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.alterFunction(identifier, change));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterFunctionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AlterFunctionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterFunctionFailureEvent) event).functionChanges().length);
    Assertions.assertEquals(change, ((AlterFunctionFailureEvent) event).functionChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDropFunctionFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", "func");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropFunction(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropFunctionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropFunctionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DROP_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListFunctionFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog", "schema");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listFunctions(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListFunctionFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListFunctionFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListFunctionFailureEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_FUNCTION, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private void checkFunctionInfo(FunctionInfo functionInfo, Function func) {
    Assertions.assertEquals(func.name(), functionInfo.name());
    Assertions.assertEquals(func.functionType(), functionInfo.functionType());
    Assertions.assertEquals(func.deterministic(), functionInfo.deterministic());
    Assertions.assertEquals(func.comment(), functionInfo.comment());
    Assertions.assertArrayEquals(func.definitions(), functionInfo.definitions());
    Assertions.assertEquals(func.auditInfo(), functionInfo.auditInfo());
  }

  private Function mockFunction() {
    Function func = mock(Function.class);
    when(func.name()).thenReturn("func");
    when(func.comment()).thenReturn("test function");
    when(func.functionType()).thenReturn(FunctionType.SCALAR);
    when(func.deterministic()).thenReturn(true);
    when(func.definitions()).thenReturn(new FunctionDefinition[0]);
    when(func.auditInfo()).thenReturn(null);
    return func;
  }

  private FunctionDispatcher mockFunctionDispatcher() {
    FunctionDispatcher dispatcher = mock(FunctionDispatcher.class);
    when(dispatcher.registerFunction(
            any(NameIdentifier.class),
            any(String.class),
            any(FunctionType.class),
            any(Boolean.class),
            any(FunctionDefinition[].class)))
        .thenReturn(function);
    when(dispatcher.getFunction(any(NameIdentifier.class))).thenReturn(function);
    when(dispatcher.dropFunction(any(NameIdentifier.class))).thenReturn(true);
    when(dispatcher.listFunctions(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterFunction(any(NameIdentifier.class), any(FunctionChange.class)))
        .thenReturn(function);
    return dispatcher;
  }

  private FunctionDispatcher mockExceptionFunctionDispatcher() {
    FunctionDispatcher dispatcher =
        mock(
            FunctionDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
