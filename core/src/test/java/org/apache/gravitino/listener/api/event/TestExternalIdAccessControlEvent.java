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
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.AccessControlEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestExternalIdAccessControlEvent {

  private static final String METALAKE = "demo_metalake";
  private static final String USER = "user_test";
  private static final String GROUP = "group_test";
  private static final String USER_EXT_ID = "ext-user-1";
  private static final String GROUP_EXT_ID = "ext-group-1";

  private AccessControlEventDispatcher dispatcher;
  private AccessControlEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private User user;
  private Group group;

  @BeforeAll
  void init() {
    user = mockUser(USER, USER_EXT_ID, true);
    group = mockGroup(GROUP, GROUP_EXT_ID);

    dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    dispatcher = new AccessControlEventDispatcher(eventBus, mockDispatcher());
    failureDispatcher = new AccessControlEventDispatcher(eventBus, mockFailureDispatcher());
  }

  @Test
  void testAddUserWithExternalIdEvents() {
    dispatcher.addUser(METALAKE, USER, USER_EXT_ID, true);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AddUserPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ADD_USER, preEvent.operationType());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddUserEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.ADD_USER, event.operationType());
  }

  @Test
  void testGetUserByExternalIdEvents() {
    dispatcher.getUserByExternalId(METALAKE, USER_EXT_ID);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetUserByExternalIdPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        AuthorizationUtils.ofUserExternalId(METALAKE, USER_EXT_ID), preEvent.identifier());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetUserByExternalIdEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.GET_USER_BY_EXTERNAL_ID, event.operationType());
  }

  @Test
  void testEnableUserEvents() {
    dispatcher.enableUser(METALAKE, USER_EXT_ID);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(EnableUserPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ENABLE_USER, preEvent.operationType());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(EnableUserEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.ENABLE_USER, event.operationType());
  }

  @Test
  void testRemoveUserByExternalIdEvents() {
    dispatcher.removeUserByExternalId(METALAKE, USER_EXT_ID);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RemoveUserByExternalIdPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.REMOVE_USER_BY_EXTERNAL_ID, preEvent.operationType());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveUserByExternalIdEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.REMOVE_USER_BY_EXTERNAL_ID, event.operationType());
  }

  @Test
  void testAddGroupWithExternalIdEvents() {
    dispatcher.addGroup(METALAKE, GROUP, GROUP_EXT_ID);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AddGroupPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ADD_GROUP, preEvent.operationType());

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddGroupEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.ADD_GROUP, event.operationType());
  }

  @Test
  void testGetGroupByExternalIdFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getGroupByExternalId(METALAKE, GROUP_EXT_ID));

    dummyEventListener.popPreEvent();
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetGroupByExternalIdFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.GET_GROUP_BY_EXTERNAL_ID, event.operationType());
  }

  private AccessControlEventDispatcher mockDispatcher() {
    AccessControlEventDispatcher mockDispatcher = mock(AccessControlEventDispatcher.class);
    when(mockDispatcher.addUser(METALAKE, USER, USER_EXT_ID, true)).thenReturn(user);
    when(mockDispatcher.getUserByExternalId(METALAKE, USER_EXT_ID)).thenReturn(user);
    when(mockDispatcher.enableUser(METALAKE, USER_EXT_ID)).thenReturn(user);
    when(mockDispatcher.removeUserByExternalId(METALAKE, USER_EXT_ID)).thenReturn(true);
    when(mockDispatcher.addGroup(METALAKE, GROUP, GROUP_EXT_ID)).thenReturn(group);
    return mockDispatcher;
  }

  private AccessControlEventDispatcher mockFailureDispatcher() {
    return mock(
        AccessControlEventDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private User mockUser(String name, String externalId, boolean enabled) {
    User mockUser = mock(User.class);
    when(mockUser.name()).thenReturn(name);
    when(mockUser.externalId()).thenReturn(externalId);
    when(mockUser.enabled()).thenReturn(enabled);
    when(mockUser.roles()).thenReturn(ImmutableList.of());
    return mockUser;
  }

  private Group mockGroup(String name, String externalId) {
    Group mockGroup = mock(Group.class);
    when(mockGroup.name()).thenReturn(name);
    when(mockGroup.externalId()).thenReturn(externalId);
    when(mockGroup.roles()).thenReturn(ImmutableList.of());
    return mockGroup;
  }
}
