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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SchemaEventDispatcher;
import org.apache.gravitino.listener.api.info.SchemaInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.stubbing.Answer;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSchemaEvent {
  private SchemaEventDispatcher dispatcher;
  private SchemaEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Schema schema;

  @BeforeAll
  void init() {
    this.schema = mockSchema();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    SchemaDispatcher schemaDispatcher = mockSchemaDispatcher();
    this.dispatcher = new SchemaEventDispatcher(eventBus, schemaDispatcher);
    SchemaDispatcher schemaExceptionDispatcher = mockExceptionSchemaDispatcher();
    this.failureDispatcher = new SchemaEventDispatcher(eventBus, schemaExceptionDispatcher);
  }

  @Test
  void testCreateSchemaEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    dispatcher.createSchema(identifier, schema.comment(), schema.properties());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateSchemaEvent.class, event.getClass());
    SchemaInfo schemaInfo = ((CreateSchemaEvent) event).createdSchemaInfo();
    checkSchemaInfo(schemaInfo, schema);
    Assertions.assertEquals(OperationType.CREATE_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(CreateSchemaPreEvent.class, preEvent.getClass());
    SchemaInfo preSchemaInfo = ((CreateSchemaPreEvent) preEvent).createSchemaRequest();
    checkSchemaInfo(preSchemaInfo, schema);
    Assertions.assertEquals(OperationType.CREATE_SCHEMA, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testLoadSchemaEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    dispatcher.loadSchema(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadSchemaEvent.class, event.getClass());
    SchemaInfo schemaInfo = ((LoadSchemaEvent) event).loadedSchemaInfo();
    checkSchemaInfo(schemaInfo, schema);
    Assertions.assertEquals(OperationType.LOAD_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(LoadSchemaPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LOAD_SCHEMA, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListSchemaEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    dispatcher.listSchemas(namespace);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListSchemaEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListSchemaEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(namespace.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListSchemaPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(namespace, ((ListSchemaPreEvent) preEvent).namespace());
    Assertions.assertEquals(OperationType.LIST_SCHEMA, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testAlterSchemaEvent() {
    SchemaChange schemaChange = SchemaChange.setProperty("a", "b");
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    dispatcher.alterSchema(identifier, schemaChange);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterSchemaEvent.class, event.getClass());
    SchemaInfo schemaInfo = ((AlterSchemaEvent) event).updatedSchemaInfo();
    checkSchemaInfo(schemaInfo, schema);
    Assertions.assertEquals(1, ((AlterSchemaEvent) event).schemaChanges().length);
    Assertions.assertEquals(schemaChange, ((AlterSchemaEvent) event).schemaChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(AlterSchemaPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(1, ((AlterSchemaPreEvent) preEvent).schemaChanges().length);
    Assertions.assertEquals(schemaChange, ((AlterSchemaPreEvent) preEvent).schemaChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_SCHEMA, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDropSchemaEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    dispatcher.dropSchema(identifier, true);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropSchemaEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropSchemaEvent) event).cascade());
    Assertions.assertEquals(false, ((DropSchemaEvent) event).isExists());
    Assertions.assertEquals(OperationType.DROP_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(DropSchemaPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_SCHEMA, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testCreateSchemaFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createSchema(identifier, schema.comment(), schema.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateSchemaFailureEvent) event).exception().getClass());
    checkSchemaInfo(((CreateSchemaFailureEvent) event).createSchemaRequest(), schema);
    Assertions.assertEquals(OperationType.CREATE_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testLoadSchemaFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadSchema(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LOAD_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterSchemaFailureEvent() {
    // alter schema
    SchemaChange schemaChange = SchemaChange.setProperty("a", "b");
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterSchema(identifier, schemaChange));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterSchemaFailureEvent) event).schemaChanges().length);
    Assertions.assertEquals(schemaChange, ((AlterSchemaFailureEvent) event).schemaChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDropSchemaFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", schema.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropSchema(identifier, true));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(true, ((DropSchemaFailureEvent) event).cascade());
    Assertions.assertEquals(OperationType.DROP_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListSchemaFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listSchemas(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListSchemaFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListSchemaFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListSchemaFailureEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_SCHEMA, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private void checkSchemaInfo(SchemaInfo schemaInfo, Schema schema) {
    Assertions.assertEquals(schema.name(), schemaInfo.name());
    Assertions.assertEquals(schema.properties(), schemaInfo.properties());
    Assertions.assertEquals(schema.comment(), schemaInfo.comment());
  }

  private Schema mockSchema() {
    Schema schema = mock(Schema.class);
    when(schema.comment()).thenReturn("comment");
    when(schema.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(schema.name()).thenReturn("schema");
    when(schema.auditInfo()).thenReturn(null);
    return schema;
  }

  private SchemaDispatcher mockSchemaDispatcher() {
    SchemaDispatcher dispatcher = mock(SchemaDispatcher.class);
    when(dispatcher.createSchema(any(NameIdentifier.class), any(String.class), any(Map.class)))
        .thenReturn(schema);
    when(dispatcher.loadSchema(any(NameIdentifier.class))).thenReturn(schema);
    when(dispatcher.dropSchema(any(NameIdentifier.class), eq(true))).thenReturn(false);
    when(dispatcher.listSchemas(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterSchema(any(NameIdentifier.class), any(SchemaChange.class)))
        .thenReturn(schema);
    return dispatcher;
  }

  private SchemaDispatcher mockExceptionSchemaDispatcher() {
    SchemaDispatcher dispatcher =
        mock(
            SchemaDispatcher.class,
            (Answer)
                invocation -> {
                  throw new GravitinoRuntimeException("Exception for all methods");
                });
    return dispatcher;
  }
}
