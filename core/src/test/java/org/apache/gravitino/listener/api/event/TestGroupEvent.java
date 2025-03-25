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

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGroupEvent {

  private static final String METALAKE = "demo_metalake";
  private static final String INEXIST_METALAKE = "inexist_metalake";
  private AccessControlEventDispatcher dispatcher;
  private AccessControlEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private String groupName;
  private String otherGroupName;
  private String inExistGroupName;
  private Group group;
  private Group otherGroup;
  private NameIdentifier identifier;

  @BeforeAll
  void init() {
    this.groupName = "group_test";
    this.otherGroupName = "group_admin";
    this.inExistGroupName = "group_not_exist";
    this.group = getMockGroup(groupName, ImmutableList.of("test", "engineer"));
    this.otherGroup = getMockGroup(otherGroupName, null);
    this.identifier = NameIdentifier.of(METALAKE);

    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new AccessControlEventDispatcher(eventBus, mockGroupDispatcher());
    this.failureDispatcher =
        new AccessControlEventDispatcher(eventBus, mockExceptionGroupDispatcher());

    System.out.println(failureDispatcher);
  }

  @Test
  void testAddGroupPreEvent() {
    dispatcher.addGroup(METALAKE, groupName);

    // validate event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AddGroupPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.ADD_GROUP, preEvent.operationType());

    AddGroupPreEvent addGroupPreEvent = (AddGroupPreEvent) preEvent;
    Assertions.assertEquals(identifier, addGroupPreEvent.identifier());
    Assertions.assertEquals(groupName, addGroupPreEvent.groupName());
  }

  @Test
  void testGetGroupPreEventWithExistingGroup() {
    dispatcher.getGroup(METALAKE, groupName);

    // validate event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetGroupPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GET_GROUP, preEvent.operationType());

    GetGroupPreEvent getGroupPreEvent = (GetGroupPreEvent) preEvent;
    Assertions.assertEquals(identifier, getGroupPreEvent.identifier());
    Assertions.assertEquals(groupName, getGroupPreEvent.groupName());
  }

  @Test
  void testGroupNotFoundGetGroupPreEvent() {
    Assertions.assertThrows(
        NoSuchGroupException.class, () -> dispatcher.getGroup(METALAKE, inExistGroupName));
  }

  @Test
  void testGetGroupPreEventWithInexistMetalake() {
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> dispatcher.getGroup(INEXIST_METALAKE, groupName));
  }

  @Test
  void testListGroupsPreEvent() {
    dispatcher.listGroups(METALAKE);

    // validate event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListGroupsPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.LIST_GROUPS, preEvent.operationType());

    ListGroupsPreEvent listGroupsPreEvent = (ListGroupsPreEvent) preEvent;
    Assertions.assertEquals(identifier, listGroupsPreEvent.identifier());
  }

  @Test
  void testListGroupNamesPreEvent() {
    dispatcher.listGroupNames(METALAKE);

    // validate event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListGroupNamesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.LIST_GROUP_NAMES, preEvent.operationType());

    ListGroupNamesPreEvent listGroupNamesPreEvent = (ListGroupNamesPreEvent) preEvent;
    Assertions.assertEquals(identifier, listGroupNamesPreEvent.identifier());
  }

  @Test
  void testRemoveGroupPreEvent() {
    dispatcher.removeGroup(METALAKE, groupName);

    // validate event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RemoveGroupPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_GROUP, preEvent.operationType());

    RemoveGroupPreEvent removeGroupPreEvent = (RemoveGroupPreEvent) preEvent;
    Assertions.assertEquals(identifier, removeGroupPreEvent.identifier());
    Assertions.assertEquals(groupName, removeGroupPreEvent.groupName());
  }

  private Group getMockGroup(String name, List<String> roles) {
    Group mockGroup = mock(Group.class);
    when(mockGroup.name()).thenReturn(name);
    when(mockGroup.roles()).thenReturn(roles);

    return mockGroup;
  }

  private AccessControlEventDispatcher mockGroupDispatcher() {
    AccessControlEventDispatcher dispatcher = mock(AccessControlEventDispatcher.class);
    when(dispatcher.addGroup(METALAKE, groupName)).thenReturn(group);
    when(dispatcher.addGroup(METALAKE, otherGroupName)).thenReturn(otherGroup);

    when(dispatcher.removeGroup(METALAKE, groupName)).thenReturn(true);
    when(dispatcher.removeGroup(METALAKE, inExistGroupName)).thenReturn(false);

    when(dispatcher.listGroups(METALAKE)).thenReturn(new Group[] {group, otherGroup});
    when(dispatcher.listGroupNames(METALAKE)).thenReturn(new String[] {groupName, otherGroupName});

    when(dispatcher.getGroup(METALAKE, groupName)).thenReturn(group);
    when(dispatcher.getGroup(METALAKE, inExistGroupName))
        .thenThrow(new NoSuchGroupException("group not found"));
    when(dispatcher.getGroup(INEXIST_METALAKE, groupName))
        .thenThrow(new NoSuchMetalakeException("metalake not found"));

    return dispatcher;
  }

  private AccessControlEventDispatcher mockExceptionGroupDispatcher() {
    return mock(
        AccessControlEventDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }
}
