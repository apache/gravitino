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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.CatalogInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCatalogEvent {
  private CatalogEventDispatcher dispatcher;
  private CatalogEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Catalog catalog;

  @BeforeAll
  void init() {
    this.catalog = mockCatalog();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    CatalogDispatcher catalogDispatcher = mockCatalogDispatcher();
    this.dispatcher = new CatalogEventDispatcher(eventBus, catalogDispatcher);
    CatalogDispatcher catalogExceptionDispatcher = mockExceptionCatalogDispatcher();
    this.failureDispatcher = new CatalogEventDispatcher(eventBus, catalogExceptionDispatcher);
  }

  @Test
  void testCreateCatalogEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    dispatcher.createCatalog(
        identifier, catalog.type(), catalog.provider(), catalog.comment(), catalog.properties());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateCatalogEvent.class, event.getClass());
    CatalogInfo catalogInfo = ((CreateCatalogEvent) event).createdCatalogInfo();
    checkCatalogInfo(catalogInfo, catalog);
  }

  @Test
  void testLoadCatalogEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    dispatcher.loadCatalog(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadCatalogEvent.class, event.getClass());
    CatalogInfo catalogInfo = ((LoadCatalogEvent) event).loadedCatalogInfo();
    checkCatalogInfo(catalogInfo, catalog);
  }

  @Test
  void testAlterCatalogEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    CatalogChange catalogChange = CatalogChange.setProperty("a", "b");
    dispatcher.alterCatalog(identifier, catalogChange);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterCatalogEvent.class, event.getClass());
    CatalogInfo catalogInfo = ((AlterCatalogEvent) event).updatedCatalogInfo();
    checkCatalogInfo(catalogInfo, catalog);
    CatalogChange[] catalogChanges = ((AlterCatalogEvent) event).catalogChanges();
    Assertions.assertEquals(1, catalogChanges.length);
    Assertions.assertEquals(catalogChange, catalogChanges[0]);
  }

  @Test
  void testDropCatalogEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    dispatcher.dropCatalog(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropCatalogEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropCatalogEvent) event).isExists());
  }

  @Test
  void testListCatalogEvent() {
    Namespace namespace = Namespace.of("metalake");
    dispatcher.listCatalogs(namespace);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListCatalogEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListCatalogEvent) event).namespace());
  }

  @Test
  void testListCatalogInfoEvent() {
    Namespace namespace = Namespace.of("metalake");
    dispatcher.listCatalogsInfo(namespace);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListCatalogEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListCatalogEvent) event).namespace());
  }

  @Test
  void testCreateCatalogFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", catalog.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createCatalog(
                identifier,
                catalog.type(),
                catalog.provider(),
                catalog.comment(),
                catalog.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((CreateCatalogFailureEvent) event).exception().getClass());
    checkCatalogInfo(((CreateCatalogFailureEvent) event).createCatalogRequest(), catalog);
  }

  @Test
  void testLoadCatalogFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadCatalog(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadCatalogFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterCatalogFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    CatalogChange catalogChange = CatalogChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterCatalog(identifier, catalogChange));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterCatalogFailureEvent) event).exception().getClass());
    CatalogChange[] catalogChanges = ((AlterCatalogFailureEvent) event).catalogChanges();
    Assertions.assertEquals(1, catalogChanges.length);
    Assertions.assertEquals(catalogChange, catalogChanges[0]);
  }

  @Test
  void testDropCatalogFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropCatalog(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropCatalogFailureEvent) event).exception().getClass());
  }

  @Test
  void testListCatalogFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listCatalogs(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListCatalogFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListCatalogFailureEvent) event).namespace());
  }

  @Test
  void testListCatalogInfoFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listCatalogsInfo(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListCatalogFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListCatalogFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListCatalogFailureEvent) event).namespace());
  }

  private void checkCatalogInfo(CatalogInfo catalogInfo, Catalog catalog) {
    Assertions.assertEquals(catalog.name(), catalogInfo.name());
    Assertions.assertEquals(catalog.type(), catalogInfo.type());
    Assertions.assertEquals(catalog.provider(), catalogInfo.provider());
    Assertions.assertEquals(catalog.properties(), catalogInfo.properties());
    Assertions.assertEquals(catalog.comment(), catalogInfo.comment());
  }

  private Catalog mockCatalog() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.comment()).thenReturn("comment");
    when(catalog.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(catalog.name()).thenReturn("catalog");
    when(catalog.provider()).thenReturn("hive");
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalog.auditInfo()).thenReturn(null);
    return catalog;
  }

  private CatalogDispatcher mockCatalogDispatcher() {
    CatalogDispatcher dispatcher = mock(CatalogDispatcher.class);
    when(dispatcher.createCatalog(
            any(NameIdentifier.class),
            any(Catalog.Type.class),
            any(String.class),
            any(String.class),
            any(Map.class)))
        .thenReturn(catalog);
    when(dispatcher.loadCatalog(any(NameIdentifier.class))).thenReturn(catalog);
    when(dispatcher.dropCatalog(any(NameIdentifier.class), anyBoolean())).thenReturn(true);
    when(dispatcher.listCatalogs(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterCatalog(any(NameIdentifier.class), any(CatalogChange.class)))
        .thenReturn(catalog);
    return dispatcher;
  }

  private CatalogDispatcher mockExceptionCatalogDispatcher() {
    CatalogDispatcher dispatcher =
        mock(
            CatalogDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
