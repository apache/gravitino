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
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.ViewEventDispatcher;
import org.apache.gravitino.listener.api.event.view.AlterViewEvent;
import org.apache.gravitino.listener.api.event.view.AlterViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.AlterViewPreEvent;
import org.apache.gravitino.listener.api.event.view.CreateViewEvent;
import org.apache.gravitino.listener.api.event.view.CreateViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.CreateViewPreEvent;
import org.apache.gravitino.listener.api.event.view.DropViewEvent;
import org.apache.gravitino.listener.api.event.view.DropViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.DropViewPreEvent;
import org.apache.gravitino.listener.api.event.view.ListViewEvent;
import org.apache.gravitino.listener.api.event.view.ListViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.ListViewPreEvent;
import org.apache.gravitino.listener.api.event.view.LoadViewEvent;
import org.apache.gravitino.listener.api.event.view.LoadViewFailureEvent;
import org.apache.gravitino.listener.api.event.view.LoadViewPreEvent;
import org.apache.gravitino.listener.api.info.ViewInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestViewEvent {
  private ViewEventDispatcher dispatcher;
  private ViewEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private View view;

  @BeforeAll
  void init() {
    this.view = mockView();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    ViewDispatcher viewDispatcher = mockViewDispatcher();
    this.dispatcher = new ViewEventDispatcher(eventBus, viewDispatcher);
    ViewDispatcher exceptionDispatcher = mockExceptionViewDispatcher();
    this.failureDispatcher = new ViewEventDispatcher(eventBus, exceptionDispatcher);
  }

  @Test
  void testCreateViewEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    dispatcher.createView(
        identifier,
        view.comment(),
        view.columns(),
        view.representations(),
        view.defaultCatalog(),
        view.defaultSchema(),
        view.properties());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateViewEvent.class, event.getClass());
    checkViewInfo(((CreateViewEvent) event).createdViewInfo(), view);
    Assertions.assertEquals(OperationType.CREATE_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(CreateViewPreEvent.class, preEvent.getClass());
    checkViewInfo(((CreateViewPreEvent) preEvent).createViewRequest(), view);
    Assertions.assertEquals(OperationType.CREATE_VIEW, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testLoadViewEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    dispatcher.loadView(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadViewEvent.class, event.getClass());
    checkViewInfo(((LoadViewEvent) event).loadedViewInfo(), view);
    Assertions.assertEquals(OperationType.LOAD_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(LoadViewPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LOAD_VIEW, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testAlterViewEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    ViewChange change = ViewChange.setProperty("a", "b");
    dispatcher.alterView(identifier, change);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterViewEvent.class, event.getClass());
    checkViewInfo(((AlterViewEvent) event).updatedViewInfo(), view);
    Assertions.assertEquals(1, ((AlterViewEvent) event).viewChanges().length);
    Assertions.assertEquals(change, ((AlterViewEvent) event).viewChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(AlterViewPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(1, ((AlterViewPreEvent) preEvent).viewChanges().length);
    Assertions.assertEquals(change, ((AlterViewPreEvent) preEvent).viewChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_VIEW, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testDropViewEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    dispatcher.dropView(identifier);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropViewEvent.class, event.getClass());
    Assertions.assertTrue(((DropViewEvent) event).isExists());
    Assertions.assertEquals(OperationType.DROP_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(identifier, preEvent.identifier());
    Assertions.assertEquals(DropViewPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DROP_VIEW, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testListViewEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog", "schema");
    dispatcher.listViews(namespace);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListViewEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListViewEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(namespace.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListViewPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(namespace, ((ListViewPreEvent) preEvent).namespace());
    Assertions.assertEquals(OperationType.LIST_VIEW, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
  }

  @Test
  void testCreateViewFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createView(
                identifier,
                view.comment(),
                view.columns(),
                view.representations(),
                view.defaultCatalog(),
                view.defaultSchema(),
                view.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateViewFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateViewFailureEvent) event).exception().getClass());
    checkViewInfo(((CreateViewFailureEvent) event).createViewRequest(), view);
    Assertions.assertEquals(OperationType.CREATE_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testLoadViewFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadView(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadViewFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadViewFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LOAD_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterViewFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    ViewChange change = ViewChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.alterView(identifier, change));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterViewFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterViewFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterViewFailureEvent) event).viewChanges().length);
    Assertions.assertEquals(change, ((AlterViewFailureEvent) event).viewChanges()[0]);
    Assertions.assertEquals(OperationType.ALTER_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDropViewFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "schema", view.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropView(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropViewFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropViewFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DROP_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListViewFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog", "schema");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listViews(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListViewFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListViewFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListViewFailureEvent) event).namespace());
    Assertions.assertEquals(OperationType.LIST_VIEW, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private void checkViewInfo(ViewInfo viewInfo, View view) {
    Assertions.assertEquals(view.name(), viewInfo.name());
    Assertions.assertEquals(view.comment(), viewInfo.comment());
    Assertions.assertEquals(view.defaultCatalog(), viewInfo.defaultCatalog());
    Assertions.assertEquals(view.defaultSchema(), viewInfo.defaultSchema());
    Assertions.assertEquals(view.properties(), viewInfo.properties());
    Assertions.assertArrayEquals(view.columns(), viewInfo.columns());
    Assertions.assertArrayEquals(view.representations(), viewInfo.representations());
    Assertions.assertEquals(view.auditInfo(), viewInfo.auditInfo());
  }

  private View mockView() {
    View v = mock(View.class);
    Representation rep =
        SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build();
    when(v.name()).thenReturn("view");
    when(v.comment()).thenReturn("comment");
    when(v.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(v.columns()).thenReturn(new Column[] {Column.of("a", Types.IntegerType.get())});
    when(v.representations()).thenReturn(new Representation[] {rep});
    when(v.defaultCatalog()).thenReturn("dc");
    when(v.defaultSchema()).thenReturn("ds");
    when(v.auditInfo()).thenReturn(null);
    return v;
  }

  private ViewDispatcher mockViewDispatcher() {
    ViewDispatcher d = mock(ViewDispatcher.class);
    when(d.createView(
            any(NameIdentifier.class),
            any(),
            any(Column[].class),
            any(Representation[].class),
            any(),
            any(),
            any(Map.class)))
        .thenReturn(view);
    when(d.loadView(any(NameIdentifier.class))).thenReturn(view);
    when(d.dropView(any(NameIdentifier.class))).thenReturn(true);
    when(d.listViews(any(Namespace.class))).thenReturn(null);
    when(d.alterView(any(NameIdentifier.class), any(ViewChange.class))).thenReturn(view);
    return d;
  }

  private ViewDispatcher mockExceptionViewDispatcher() {
    return mock(
        ViewDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }
}
