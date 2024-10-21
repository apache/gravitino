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
import org.apache.gravitino.Metalake;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.MetalakeEventDispatcher;
import org.apache.gravitino.listener.api.info.MetalakeInfo;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestMetalakeEvent {
  private MetalakeEventDispatcher dispatcher;
  private MetalakeEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Metalake metalake;

  @BeforeAll
  void init() {
    this.metalake = mockMetalake();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    MetalakeDispatcher metalakeDispatcher = mockMetalakeDispatcher();
    this.dispatcher = new MetalakeEventDispatcher(eventBus, metalakeDispatcher);
    MetalakeDispatcher metalakeExceptionDispatcher = mockExceptionMetalakeDispatcher();
    this.failureDispatcher = new MetalakeEventDispatcher(eventBus, metalakeExceptionDispatcher);
  }

  @Test
  void testCreateMetalakeEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    dispatcher.createMetalake(identifier, metalake.comment(), metalake.properties());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateMetalakeEvent.class, event.getClass());
    MetalakeInfo metalakeInfo = ((CreateMetalakeEvent) event).createdMetalakeInfo();
    checkMetalakeInfo(metalakeInfo, metalake);
  }

  @Test
  void testLoadMetalakeEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    dispatcher.loadMetalake(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadMetalakeEvent.class, event.getClass());
    MetalakeInfo metalakeInfo = ((LoadMetalakeEvent) event).loadedMetalakeInfo();
    checkMetalakeInfo(metalakeInfo, metalake);
  }

  @Test
  void testAlterMetalakeEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    MetalakeChange metalakeChange = MetalakeChange.setProperty("a", "b");
    dispatcher.alterMetalake(identifier, metalakeChange);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterMetalakeEvent.class, event.getClass());
    MetalakeInfo metalakeInfo = ((AlterMetalakeEvent) event).updatedMetalakeInfo();
    checkMetalakeInfo(metalakeInfo, metalake);
    MetalakeChange[] metalakeChanges = ((AlterMetalakeEvent) event).metalakeChanges();
    Assertions.assertTrue(metalakeChanges.length == 1);
    Assertions.assertEquals(metalakeChange, metalakeChanges[0]);
  }

  @Test
  void testDropMetalakeEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    dispatcher.dropMetalake(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropMetalakeEvent.class, event.getClass());
    Assertions.assertTrue(((DropMetalakeEvent) event).isExists());
  }

  @Test
  void testListMetalakeEvent() {
    dispatcher.listMetalakes();
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertNull(event.identifier());
    Assertions.assertEquals(ListMetalakeEvent.class, event.getClass());
  }

  @Test
  void testCreateMetalakeFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createMetalake(
                identifier, metalake.comment(), metalake.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((CreateMetalakeFailureEvent) event).exception().getClass());
    checkMetalakeInfo(((CreateMetalakeFailureEvent) event).createMetalakeRequest(), metalake);
  }

  @Test
  void testLoadMetalakeFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadMetalake(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadMetalakeFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterMetalakeFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    MetalakeChange metalakeChange = MetalakeChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterMetalake(identifier, metalakeChange));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AlterMetalakeFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterMetalakeFailureEvent) event).metalakeChanges().length);
    Assertions.assertEquals(
        metalakeChange, ((AlterMetalakeFailureEvent) event).metalakeChanges()[0]);
  }

  @Test
  void testDropMetalakeFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropMetalake(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropMetalakeFailureEvent) event).exception().getClass());
  }

  @Test
  void testListMetalakeFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listMetalakes());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertNull(event.identifier());
    Assertions.assertEquals(ListMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListMetalakeFailureEvent) event).exception().getClass());
  }

  private void checkMetalakeInfo(MetalakeInfo metalakeInfo, Metalake metalake) {
    Assertions.assertEquals(metalake.name(), metalakeInfo.name());
    Assertions.assertEquals(metalake.properties(), metalakeInfo.properties());
    Assertions.assertEquals(metalake.comment(), metalakeInfo.comment());
    Assertions.assertEquals(metalake.auditInfo(), metalakeInfo.auditInfo());
  }

  private Metalake mockMetalake() {
    Metalake metalake = mock(Metalake.class);
    when(metalake.comment()).thenReturn("comment");
    when(metalake.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(metalake.name()).thenReturn("metalake");
    when(metalake.auditInfo()).thenReturn(null);
    return metalake;
  }

  private MetalakeDispatcher mockMetalakeDispatcher() {
    MetalakeDispatcher dispatcher = mock(MetalakeDispatcher.class);
    when(dispatcher.createMetalake(any(NameIdentifier.class), any(String.class), any(Map.class)))
        .thenReturn(metalake);
    when(dispatcher.loadMetalake(any(NameIdentifier.class))).thenReturn(metalake);
    when(dispatcher.dropMetalake(any(NameIdentifier.class), anyBoolean())).thenReturn(true);
    when(dispatcher.listMetalakes()).thenReturn(null);
    when(dispatcher.alterMetalake(any(NameIdentifier.class), any(MetalakeChange.class)))
        .thenReturn(metalake);
    return dispatcher;
  }

  private MetalakeDispatcher mockExceptionMetalakeDispatcher() {
    MetalakeDispatcher dispatcher =
        mock(
            MetalakeDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
