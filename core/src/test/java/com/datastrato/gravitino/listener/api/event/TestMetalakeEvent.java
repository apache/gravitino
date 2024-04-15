/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.listener.DummyEventListener;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;
import com.datastrato.gravitino.metalake.MetalakeDispatcher;
import com.datastrato.gravitino.metalake.MetalakeEventDispatcher;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
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
  void testCreateMetalake() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    dispatcher.createMetalake(identifier, metalake.comment(), metalake.properties());
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateMetalakeEvent.class, event.getClass());
    MetalakeInfo metalakeInfo = ((CreateMetalakeEvent) event).createdMetalakeInfo();
    checkMetalakeInfo(metalakeInfo, metalake);
  }

  @Test
  void testLoadMetalake() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    dispatcher.loadMetalake(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadMetalakeEvent.class, event.getClass());
    MetalakeInfo metalakeInfo = ((LoadMetalakeEvent) event).loadedMetalakeInfo();
    checkMetalakeInfo(metalakeInfo, metalake);
  }

  @Test
  void testAlterMetalake() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    MetalakeChange metalakeChange = MetalakeChange.setProperty("a", "b");
    dispatcher.alterMetalake(identifier, metalakeChange);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterMetalakeEvent.class, event.getClass());
    MetalakeInfo metalakeInfo = ((AlterMetalakeEvent) event).updatedMetalakeInfo();
    checkMetalakeInfo(metalakeInfo, metalake);
    MetalakeChange[] metalakeChanges = ((AlterMetalakeEvent) event).metalakeChanges();
    Assertions.assertTrue(metalakeChanges.length == 1);
    Assertions.assertEquals(metalakeChange, metalakeChanges[0]);
  }

  @Test
  void testDropMetalake() {
    NameIdentifier identifier = NameIdentifier.of("metalake");
    dispatcher.dropMetalake(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropMetalakeEvent.class, event.getClass());
    Assertions.assertTrue(((DropMetalakeEvent) event).isExists());
  }

  @Test
  void testListMetalake() {
    dispatcher.listMetalakes();
    Event event = dummyEventListener.popEvent();
    Assertions.assertNull(event.identifier());
    Assertions.assertEquals(ListMetalakeEvent.class, event.getClass());
  }

  @Test
  void testCreateMetalakeFailure() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createMetalake(
                identifier, metalake.comment(), metalake.properties()));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((CreateMetalakeFailureEvent) event).exception().getClass());
    checkMetalakeInfo(((CreateMetalakeFailureEvent) event).createMetalakeRequest(), metalake);
  }

  @Test
  void testLoadMetalakeFailure() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadMetalake(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadMetalakeFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterMetalakeFailure() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    MetalakeChange metalakeChange = MetalakeChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterMetalake(identifier, metalakeChange));
    Event event = dummyEventListener.popEvent();
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
  void testDropMetalakeFailure() {
    NameIdentifier identifier = NameIdentifier.of(metalake.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropMetalake(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropMetalakeFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropMetalakeFailureEvent) event).exception().getClass());
  }

  @Test
  void testListMetalakeFailure() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listMetalakes());
    Event event = dummyEventListener.popEvent();
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
    when(dispatcher.dropMetalake(any(NameIdentifier.class))).thenReturn(true);
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
