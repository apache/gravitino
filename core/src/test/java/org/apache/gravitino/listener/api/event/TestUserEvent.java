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
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUserEvent {
  private static final String METALAKE = "demo_metalake";
  private static final String INEXIST_METALAKE = "inexist_metalake";
  private AccessControlEventDispatcher dispatcher;
  private AccessControlEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private String userName;
  private String otherUserName;
  private String inExistUserName;
  private User user;
  private User otherUser;
  private NameIdentifier identifier;

  @BeforeAll
  void init() {
    this.userName = "user_test";
    this.otherUserName = "user_admin";
    this.inExistUserName = "user_not_exist";
    this.user = getMockUser(userName, ImmutableList.of("test", "engineer"));
    this.otherUser = getMockUser(otherUserName, null);
    this.identifier = NameIdentifier.of(METALAKE);

    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new AccessControlEventDispatcher(eventBus, mockUserDispatcher());
    this.failureDispatcher =
        new AccessControlEventDispatcher(eventBus, mockExceptionUserDispatcher());

    System.out.println(failureDispatcher);
  }

  @Test
  void testAddUserPreEvent() {
    dispatcher.addUser(METALAKE, otherUserName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(AddUserPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.ADD_USER, preEvent.operationType());

    AddUserPreEvent addUserPreEvent = (AddUserPreEvent) preEvent;
    Assertions.assertEquals(identifier, addUserPreEvent.identifier());
    String userName = addUserPreEvent.userName();
    Assertions.assertEquals(otherUserName, userName);
  }

  @Test
  void testGetUserPreEventWithExistingUser() {
    dispatcher.getUser(METALAKE, userName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetUserPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GET_USER, preEvent.operationType());

    GetUserPreEvent getUserPreEvent = (GetUserPreEvent) preEvent;
    Assertions.assertEquals(identifier, getUserPreEvent.identifier());
    String requestedUserName = getUserPreEvent.userName();
    Assertions.assertEquals(userName, requestedUserName);
  }

  @Test
  void testGetUserPreEventWithNonExistingUser() {
    Assertions.assertThrows(
        NoSuchUserException.class, () -> dispatcher.getUser(METALAKE, inExistUserName));
  }

  @Test
  void testGetUserPreEventWithNonExistingMetalake() {
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> dispatcher.getUser(INEXIST_METALAKE, userName));
  }

  @Test
  void testListUserPreEvent() {
    dispatcher.listUsers(METALAKE);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListUsersPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListUsersPreEvent listUsersPreEvent = (ListUsersPreEvent) preEvent;
    Assertions.assertEquals(identifier, listUsersPreEvent.identifier());
  }

  @Test
  void testListUserNamesPreEvent() {
    dispatcher.listUserNames(METALAKE);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListUserNamesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListUserNamesPreEvent listUserNamesPreEvent = (ListUserNamesPreEvent) preEvent;
    Assertions.assertEquals(identifier, listUserNamesPreEvent.identifier());
  }

  @Test
  void testRemoveUserPreEventWithExistingUser() {
    dispatcher.removeUser(METALAKE, userName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RemoveUserPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_USER, preEvent.operationType());

    RemoveUserPreEvent removeUserPreEvent = (RemoveUserPreEvent) preEvent;
    Assertions.assertEquals(identifier, removeUserPreEvent.identifier());
    String removedUserName = removeUserPreEvent.userName();
    Assertions.assertEquals(userName, removedUserName);
  }

  private AccessControlEventDispatcher mockUserDispatcher() {
    AccessControlEventDispatcher dispatcher = mock(AccessControlEventDispatcher.class);
    when(dispatcher.addUser(METALAKE, userName)).thenReturn(user);
    when(dispatcher.addUser(METALAKE, otherUserName)).thenReturn(otherUser);

    when(dispatcher.removeUser(METALAKE, userName)).thenReturn(true);
    when(dispatcher.removeUser(METALAKE, inExistUserName)).thenReturn(false);

    when(dispatcher.listUsers(METALAKE)).thenReturn(new User[] {user, otherUser});
    when(dispatcher.listUserNames(METALAKE)).thenReturn(new String[] {userName, otherUserName});

    when(dispatcher.getUser(METALAKE, userName)).thenReturn(user);
    when(dispatcher.getUser(METALAKE, inExistUserName))
        .thenThrow(new NoSuchUserException("user not found"));
    when(dispatcher.getUser(INEXIST_METALAKE, userName))
        .thenThrow(new NoSuchMetalakeException("user not found"));

    return dispatcher;
  }

  private AccessControlEventDispatcher mockExceptionUserDispatcher() {
    return mock(
        AccessControlEventDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private User getMockUser(String name, List<String> roles) {
    User user = mock(User.class);
    when(user.name()).thenReturn(name);
    when(user.roles()).thenReturn(roles);

    return user;
  }
}
