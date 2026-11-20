/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.listener.api.event;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.OwnerEventManager;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.OwnerInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public class TestOwnerEvent {
  private OwnerEventManager ownerManager;
  private OwnerManager innerOwnerManager;
  private DummyEventListener dummyEventListener;

  interface TestEntityStore extends EntityStore, SupportsRelationOperations {
    // This interface is used to mock the EntityStore for testing purposes.
  }

  @BeforeAll
  void init() {
    TestEntityStore entityStore = Mockito.mock(TestEntityStore.class);
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    this.innerOwnerManager = new OwnerManager(entityStore);
    this.ownerManager = new OwnerEventManager(eventBus, innerOwnerManager);
  }

  static class OwnerImpl implements Owner {
    private final String name;
    private final Type type;

    OwnerImpl(String name, Type type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type type() {
      return type;
    }
  }

  @Test
  void testGlossaryTransferEventsReflectPartialCompletion() {
    String lake = "lake";
    MetadataObject first = MetadataObjects.parse("first", MetadataObject.Type.GLOSSARY_TERM);
    MetadataObject second = MetadataObjects.parse("second", MetadataObject.Type.GLOSSARY_TERM);
    MetadataObject last = MetadataObjects.parse("last", MetadataObject.Type.GLOSSARY_TERM);
    OwnerDispatcher transfers = Mockito.mock(OwnerDispatcher.class);
    DummyEventListener listener = new DummyEventListener();
    OwnerEventManager dispatcher =
        new OwnerEventManager(new EventBus(List.of(listener)), transfers);
    RuntimeException failure = new RuntimeException("transfer rejected");
    doThrow(failure).when(transfers).setOwner(lake, second, "user", Owner.Type.USER);

    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                dispatcher.setOwners(
                    lake, List.of(first, first, second, last), "user", Owner.Type.USER)));
    Mockito.verify(transfers).setOwner(lake, first, "user", Owner.Type.USER);
    Mockito.verify(transfers).setOwner(lake, second, "user", Owner.Type.USER);
    Mockito.verifyNoMoreInteractions(transfers);
    Assertions.assertEquals(2, listener.getPreEvents().size());
    List<Event> events = listener.getPostEvents();
    Assertions.assertEquals(2, events.size());
    Assertions.assertEquals("first", events.get(0).identifier().name());
    Assertions.assertEquals(OperationStatus.SUCCESS, events.get(0).operationStatus());
    Assertions.assertEquals("second", events.get(1).identifier().name());
    Assertions.assertEquals(OperationStatus.FAILURE, events.get(1).operationStatus());
  }

  @Test
  void testSetOwner() {
    String metalake = "test_metalake";
    MetadataObject metadataObject =
        MetadataObjects.of(
            ImmutableList.of("test_catalog", "test_schema", "test_table"),
            MetadataObject.Type.TABLE);
    NameIdentifier identifier =
        NameIdentifier.of(metalake, "test_catalog", "test_schema", "test_table");
    String ownerName = "test_owner";
    Owner.Type ownerType = Owner.Type.USER;

    OwnerManager spyOwnerManger = Mockito.spy(innerOwnerManager);
    doNothing().when(spyOwnerManger).setOwner(metalake, metadataObject, ownerName, ownerType);
    ownerManager.setOwnerManager(spyOwnerManger);

    try {
      ownerManager.setOwner(metalake, metadataObject, ownerName, ownerType);
      List<PreEvent> preEvents = dummyEventListener.getPreEvents();
      List<Event> postEvents = dummyEventListener.getPostEvents();

      Assertions.assertEquals(1, preEvents.size());
      SetOwnerPreEvent setOwnerPreEvent = (SetOwnerPreEvent) preEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, setOwnerPreEvent.metadataObjectType());
      Assertions.assertEquals(new OwnerInfo(ownerName, ownerType), setOwnerPreEvent.ownerInfo());
      Assertions.assertEquals(identifier, setOwnerPreEvent.identifier());
      Assertions.assertEquals(OperationStatus.UNPROCESSED, setOwnerPreEvent.operationStatus());
      Assertions.assertEquals(OperationType.SET_OWNER, setOwnerPreEvent.operationType());

      Assertions.assertEquals(1, postEvents.size());
      SetOwnerEvent setOwnerEvent = (SetOwnerEvent) postEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, setOwnerEvent.metadataObjectType());
      Assertions.assertEquals(new OwnerInfo(ownerName, ownerType), setOwnerEvent.ownerInfo());
      Assertions.assertEquals(identifier, setOwnerEvent.identifier());
      Assertions.assertEquals(OperationStatus.SUCCESS, setOwnerEvent.operationStatus());
      Assertions.assertEquals(OperationType.SET_OWNER, setOwnerEvent.operationType());

    } finally {
      ownerManager.setOwnerManager(innerOwnerManager);
      dummyEventListener.clear();
    }

    spyOwnerManger = Mockito.spy(innerOwnerManager);
    doThrow(new RuntimeException("Test exception"))
        .when(spyOwnerManger)
        .setOwner(metalake, metadataObject, ownerName, ownerType);
    ownerManager.setOwnerManager(spyOwnerManger);

    try {
      ownerManager.setOwner(metalake, metadataObject, ownerName, ownerType);
      Assertions.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      List<PreEvent> preEvents = dummyEventListener.getPreEvents();
      List<Event> postEvents = dummyEventListener.getPostEvents();

      Assertions.assertEquals(1, preEvents.size());
      SetOwnerPreEvent setOwnerPreEvent = (SetOwnerPreEvent) preEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, setOwnerPreEvent.metadataObjectType());
      Assertions.assertEquals(new OwnerInfo(ownerName, ownerType), setOwnerPreEvent.ownerInfo());
      Assertions.assertEquals(identifier, setOwnerPreEvent.identifier());
      Assertions.assertEquals(OperationStatus.UNPROCESSED, setOwnerPreEvent.operationStatus());
      Assertions.assertEquals(OperationType.SET_OWNER, setOwnerPreEvent.operationType());

      Assertions.assertEquals(1, postEvents.size());
      Assertions.assertTrue(postEvents.get(0) instanceof SetOwnerFailureEvent);
      SetOwnerFailureEvent setOwnerFailureEvent = (SetOwnerFailureEvent) postEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, setOwnerFailureEvent.metadataObjectType());
      Assertions.assertEquals(
          new OwnerInfo(ownerName, ownerType), setOwnerFailureEvent.ownerInfo());
      Assertions.assertEquals(identifier, setOwnerFailureEvent.identifier());
      Assertions.assertEquals(OperationStatus.FAILURE, setOwnerFailureEvent.operationStatus());
      Assertions.assertEquals(OperationType.SET_OWNER, setOwnerFailureEvent.operationType());
    } finally {
      ownerManager.setOwnerManager(innerOwnerManager);
      dummyEventListener.clear();
    }
  }

  @Test
  void testGetOwner() {
    String metalake = "test_metalake";
    MetadataObject metadataObject =
        MetadataObjects.of(
            ImmutableList.of("test_catalog", "test_schema", "test_table"),
            MetadataObject.Type.TABLE);
    NameIdentifier identifier =
        NameIdentifier.of(metalake, "test_catalog", "test_schema", "test_table");
    Owner expectedOwner = new OwnerImpl("test_owner", Owner.Type.USER);

    OwnerManager spyOwnerManger = Mockito.spy(innerOwnerManager);
    doReturn(java.util.Optional.of(expectedOwner))
        .when(spyOwnerManger)
        .getOwner(metalake, metadataObject);
    ownerManager.setOwnerManager(spyOwnerManger);

    try {
      java.util.Optional<Owner> owner = ownerManager.getOwner(metalake, metadataObject);
      Assertions.assertTrue(owner.isPresent());
      Assertions.assertEquals(expectedOwner.name(), owner.get().name());
      Assertions.assertEquals(expectedOwner.type(), owner.get().type());

      List<PreEvent> preEvents = dummyEventListener.getPreEvents();
      List<Event> postEvents = dummyEventListener.getPostEvents();

      Assertions.assertEquals(1, preEvents.size());
      GetOwnerPreEvent getOwnerPreEvent = (GetOwnerPreEvent) preEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, getOwnerPreEvent.metadataObjectType());
      Assertions.assertNull(getOwnerPreEvent.ownerInfo());
      Assertions.assertEquals(identifier, getOwnerPreEvent.identifier());
      Assertions.assertEquals(OperationStatus.UNPROCESSED, getOwnerPreEvent.operationStatus());
      Assertions.assertEquals(OperationType.GET_OWNER, getOwnerPreEvent.operationType());

      Assertions.assertEquals(1, postEvents.size());
      GetOwnerEvent getOwnerEvent = (GetOwnerEvent) postEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, getOwnerEvent.metadataObjectType());
      Assertions.assertEquals(
          new OwnerInfo(expectedOwner.name(), expectedOwner.type()), getOwnerEvent.ownerInfo());
      Assertions.assertEquals(identifier, getOwnerEvent.identifier());
      Assertions.assertEquals(OperationStatus.SUCCESS, getOwnerEvent.operationStatus());
      Assertions.assertEquals(OperationType.GET_OWNER, getOwnerEvent.operationType());
    } finally {
      ownerManager.setOwnerManager(innerOwnerManager);
      dummyEventListener.clear();
    }

    spyOwnerManger = Mockito.spy(innerOwnerManager);
    doThrow(new RuntimeException("Test exception"))
        .when(spyOwnerManger)
        .getOwner(metalake, metadataObject);
    ownerManager.setOwnerManager(spyOwnerManger);

    try {
      ownerManager.getOwner(metalake, metadataObject);
      Assertions.fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      List<PreEvent> preEvents = dummyEventListener.getPreEvents();
      List<Event> postEvents = dummyEventListener.getPostEvents();

      Assertions.assertEquals(1, preEvents.size());
      GetOwnerPreEvent getOwnerPreEvent = (GetOwnerPreEvent) preEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, getOwnerPreEvent.metadataObjectType());
      Assertions.assertNull(getOwnerPreEvent.ownerInfo());
      Assertions.assertEquals(identifier, getOwnerPreEvent.identifier());
      Assertions.assertEquals(OperationStatus.UNPROCESSED, getOwnerPreEvent.operationStatus());
      Assertions.assertEquals(OperationType.GET_OWNER, getOwnerPreEvent.operationType());

      Assertions.assertEquals(1, postEvents.size());
      Assertions.assertTrue(postEvents.get(0) instanceof GetOwnerFailureEvent);
      GetOwnerFailureEvent getOwnerFailureEvent = (GetOwnerFailureEvent) postEvents.get(0);
      Assertions.assertEquals(MetadataObject.Type.TABLE, getOwnerFailureEvent.metadataObjectType());
      Assertions.assertNull(getOwnerFailureEvent.ownerInfo());
      Assertions.assertEquals(identifier, getOwnerFailureEvent.identifier());
      Assertions.assertEquals(OperationStatus.FAILURE, getOwnerFailureEvent.operationStatus());
      Assertions.assertEquals(OperationType.GET_OWNER, getOwnerFailureEvent.operationType());

    } finally {
      ownerManager.setOwnerManager(innerOwnerManager);
      dummyEventListener.clear();
    }
  }
}
