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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.PagedResult;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.bulk.UserAdd;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.listener.AccessControlEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.UserInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
  private NameIdentifier otherIdentifier;
  private NameIdentifier inExistIdentifier;
  private List<String> grantedRoles;
  private List<String> revokedRoles;

  @BeforeAll
  void init() {
    this.userName = "user_test";
    this.otherUserName = "user_admin";
    this.inExistUserName = "user_not_exist";
    this.user = getMockUser(userName, ImmutableList.of("test", "engineer"));
    this.otherUser = getMockUser(otherUserName, null);
    this.identifier = NameIdentifierUtil.ofUser(METALAKE, userName);
    this.otherIdentifier = NameIdentifierUtil.ofUser(METALAKE, otherUserName);
    this.inExistIdentifier = NameIdentifierUtil.ofUser(METALAKE, inExistUserName);
    this.grantedRoles = ImmutableList.of("test", "engineer");
    this.revokedRoles = ImmutableList.of("admin", "scientist");

    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new AccessControlEventDispatcher(eventBus, mockUserDispatcher());
    this.failureDispatcher =
        new AccessControlEventDispatcher(eventBus, mockExceptionUserDispatcher());
  }

  @Test
  void testUserInfo() {
    User mockUser = getMockUser("mock_user", ImmutableList.of("admin"));
    UserInfo info = new UserInfo(mockUser);

    Assertions.assertEquals(1L, info.id());
    Assertions.assertEquals("mock_user", info.name());
    Assertions.assertEquals(ImmutableList.of("admin"), info.roles());
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
    Assertions.assertEquals(otherIdentifier, addUserPreEvent.identifier());
    String userName = addUserPreEvent.userName();
    Assertions.assertEquals(otherUserName, userName);
  }

  @Test
  void testAddUserEvent() {
    dispatcher.addUser(METALAKE, userName);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddUserEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.ADD_USER, event.operationType());

    AddUserEvent addUserEvent = (AddUserEvent) event;
    Assertions.assertEquals(identifier, addUserEvent.identifier());
    UserInfo userInfo = addUserEvent.addedUserInfo();

    validateUserInfo(userInfo, user);
  }

  @Test
  void testAddUserFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.addUser(METALAKE, userName));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddUserFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.ADD_USER, event.operationType());

    AddUserFailureEvent addUserFailureEvent = (AddUserFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofUser(METALAKE, userName), addUserFailureEvent.identifier());
    Assertions.assertEquals(userName, addUserFailureEvent.userName());
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
  void testGetUserEventWithExistingUser() {
    dispatcher.getUser(METALAKE, userName);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetUserEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.GET_USER, event.operationType());

    GetUserEvent getUserEvent = (GetUserEvent) event;
    Assertions.assertEquals(identifier, getUserEvent.identifier());
    UserInfo userInfo = getUserEvent.loadedUserInfo();

    validateUserInfo(userInfo, user);
  }

  @Test
  void testGetUserEventWithNonExistingUser() {
    Assertions.assertThrows(
        NoSuchUserException.class, () -> dispatcher.getUser(METALAKE, inExistUserName));
  }

  @Test
  void testGetUserEventWithNonExistingMetalake() {
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> dispatcher.getUser(INEXIST_METALAKE, userName));
  }

  @Test
  void testGetUserFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getUser(METALAKE, inExistUserName));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetUserFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GET_USER, event.operationType());

    GetUserFailureEvent getUserFailureEvent = (GetUserFailureEvent) event;
    Assertions.assertEquals(inExistIdentifier, getUserFailureEvent.identifier());
    Assertions.assertEquals(inExistUserName, getUserFailureEvent.userName());
  }

  @Test
  void testListUserPreEvent() {
    dispatcher.listUsers(METALAKE);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListUsersPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListUsersPreEvent listUsersPreEvent = (ListUsersPreEvent) preEvent;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), listUsersPreEvent.identifier());
  }

  @Test
  void testListUserEvent() {
    dispatcher.listUsers(METALAKE);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListUsersEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USERS, event.operationType());

    ListUsersEvent listUsersEvent = (ListUsersEvent) event;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), listUsersEvent.identifier());
  }

  @Test
  void testListUserFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listUsers(INEXIST_METALAKE));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListUsersFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USERS, event.operationType());

    ListUsersFailureEvent listUsersFailureEvent = (ListUsersFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifier.of(INEXIST_METALAKE), listUsersFailureEvent.identifier());
  }

  @Test
  void testListUsersPagedPreEvent() {
    dispatcher.listUsers(METALAKE, 0, 10);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListUsersPagedPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USERS_PAGED, preEvent.operationType());

    ListUsersPagedPreEvent pagedPreEvent = (ListUsersPagedPreEvent) preEvent;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), pagedPreEvent.identifier());
    Assertions.assertEquals(0, pagedPreEvent.offset());
    Assertions.assertEquals(10, pagedPreEvent.limit());
  }

  @Test
  void testListUsersPagedEvent() {
    dispatcher.listUsers(METALAKE, 0, 10);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListUsersPagedEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USERS_PAGED, event.operationType());

    ListUsersPagedEvent pagedEvent = (ListUsersPagedEvent) event;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), pagedEvent.identifier());
    Assertions.assertEquals(0, pagedEvent.offset());
    Assertions.assertEquals(10, pagedEvent.limit());
    Assertions.assertEquals(2, pagedEvent.resultCount());
    Assertions.assertEquals(2L, pagedEvent.totalCount());
  }

  @Test
  void testListUsersPagedFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listUsers(METALAKE, 1, 5));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListUsersPagedFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USERS_PAGED, event.operationType());

    ListUsersPagedFailureEvent failureEvent = (ListUsersPagedFailureEvent) event;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), failureEvent.identifier());
    Assertions.assertEquals(1, failureEvent.offset());
    Assertions.assertEquals(5, failureEvent.limit());
  }

  @Test
  void testCountUsersPreEvent() {
    dispatcher.countUsers(METALAKE);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(CountUsersPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.COUNT_USERS, preEvent.operationType());
    Assertions.assertEquals(NameIdentifier.of(METALAKE), preEvent.identifier());
  }

  @Test
  void testCountUsersEvent() {
    dispatcher.countUsers(METALAKE);

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CountUsersEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.COUNT_USERS, event.operationType());

    CountUsersEvent countEvent = (CountUsersEvent) event;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), countEvent.identifier());
    Assertions.assertEquals(2L, countEvent.count());
  }

  @Test
  void testCountUsersFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.countUsers(INEXIST_METALAKE));

    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CountUsersFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.COUNT_USERS, event.operationType());
    Assertions.assertEquals(NameIdentifier.of(INEXIST_METALAKE), event.identifier());
  }

  @Test
  void testListUserNamesPreEvent() {
    dispatcher.listUserNames(METALAKE);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListUserNamesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    ListUserNamesPreEvent listUserNamesPreEvent = (ListUserNamesPreEvent) preEvent;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), listUserNamesPreEvent.identifier());
  }

  @Test
  void testListUserNamesEvent() {
    dispatcher.listUserNames(METALAKE);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListUserNamesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USER_NAMES, event.operationType());

    ListUserNamesEvent listUserNamesEvent = (ListUserNamesEvent) event;
    Assertions.assertEquals(NameIdentifier.of(METALAKE), listUserNamesEvent.identifier());
  }

  @Test
  void testListUserNamesFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listUserNames(INEXIST_METALAKE));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListUserNamesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_USER_NAMES, event.operationType());

    ListUserNamesFailureEvent listUserNamesFailureEvent = (ListUserNamesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifier.of(INEXIST_METALAKE), listUserNamesFailureEvent.identifier());
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

  @Test
  void testRemoveUserEventWithExistingUser() {
    dispatcher.removeUser(METALAKE, userName);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveUserEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_USER, event.operationType());

    RemoveUserEvent removeUserEvent = (RemoveUserEvent) event;
    Assertions.assertEquals(identifier, removeUserEvent.identifier());
    Assertions.assertTrue(removeUserEvent.isExists());
    Assertions.assertEquals(userName, removeUserEvent.removedUserName());
  }

  @Test
  void testRemoveUserPreEventWithNonExistingUser() {
    dispatcher.removeUser(METALAKE, inExistUserName);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveUserEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_USER, event.operationType());

    RemoveUserEvent removeUserEvent = (RemoveUserEvent) event;
    Assertions.assertEquals(inExistIdentifier, removeUserEvent.identifier());
    Assertions.assertFalse(removeUserEvent.isExists());
    Assertions.assertEquals(inExistUserName, removeUserEvent.removedUserName());
  }

  @Test
  void testRemoveUserFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.removeUser(METALAKE, inExistUserName));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveUserFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_USER, event.operationType());

    RemoveUserFailureEvent removeUserFailureEvent = (RemoveUserFailureEvent) event;
    Assertions.assertEquals(inExistIdentifier, removeUserFailureEvent.identifier());
    Assertions.assertEquals(inExistUserName, removeUserFailureEvent.userName());
  }

  @Test
  void testGrantRolesToUserPreEvent() {
    dispatcher.grantRolesToUser(METALAKE, grantedRoles, userName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GrantUserRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_USER_ROLES, preEvent.operationType());

    GrantUserRolesPreEvent grantUserRolesEvent = (GrantUserRolesPreEvent) preEvent;
    Assertions.assertEquals(identifier, grantUserRolesEvent.identifier());
    String grantedUserName = grantUserRolesEvent.userName();
    Assertions.assertEquals(userName, grantedUserName);
    Assertions.assertEquals(grantedRoles, grantUserRolesEvent.roles());
  }

  @Test
  void testGrantRolesToUserEvent() {
    dispatcher.grantRolesToUser(METALAKE, grantedRoles, userName);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantUserRolesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_USER_ROLES, event.operationType());

    GrantUserRolesEvent grantUserRolesEvent = (GrantUserRolesEvent) event;
    Assertions.assertEquals(identifier, grantUserRolesEvent.identifier());
    Assertions.assertEquals(
        String.join(",", grantedRoles), grantUserRolesEvent.customInfo().get("roleNames"));
    UserInfo userInfo = grantUserRolesEvent.grantUserInfo();

    validateUserInfo(userInfo, user);
  }

  @Test
  void testGrantRolesToUserFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.grantRolesToUser(METALAKE, grantedRoles, inExistUserName));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantUserRolesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_USER_ROLES, event.operationType());

    GrantUserRolesFailureEvent grantUserRolesFailureEvent = (GrantUserRolesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofUser(METALAKE, inExistUserName),
        grantUserRolesFailureEvent.identifier());
    Assertions.assertEquals(inExistUserName, grantUserRolesFailureEvent.userName());
    Assertions.assertEquals(grantedRoles, grantUserRolesFailureEvent.roles());
    Assertions.assertEquals(
        String.join(",", grantedRoles), grantUserRolesFailureEvent.customInfo().get("roleNames"));
  }

  @Test
  void testRevokeRolesUserPreEvent() {
    dispatcher.revokeRolesFromUser(METALAKE, revokedRoles, otherUserName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RevokeUserRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_USER_ROLES, preEvent.operationType());

    RevokeUserRolesPreEvent revokeUserRolesEvent = (RevokeUserRolesPreEvent) preEvent;
    Assertions.assertEquals(otherIdentifier, revokeUserRolesEvent.identifier());
    String revokedUserName = revokeUserRolesEvent.userName();
    Assertions.assertEquals(otherUserName, revokedUserName);
    Assertions.assertEquals(revokedRoles, revokeUserRolesEvent.roles());
  }

  @Test
  void testRevokeRolesUserEvent() {
    dispatcher.revokeRolesFromUser(METALAKE, revokedRoles, otherUserName);

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RevokeUserRolesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_USER_ROLES, event.operationType());

    RevokeUserRolesEvent revokeUserRolesEvent = (RevokeUserRolesEvent) event;
    Assertions.assertEquals(otherIdentifier, revokeUserRolesEvent.identifier());
    Assertions.assertEquals(
        String.join(",", revokedRoles), revokeUserRolesEvent.customInfo().get("roleNames"));
    UserInfo userInfo = revokeUserRolesEvent.revokedUserInfo();

    validateUserInfo(userInfo, otherUser);
  }

  @Test
  void testRevokeRolesUserFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.revokeRolesFromUser(METALAKE, revokedRoles, inExistUserName));

    // validate post-event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RevokeUserRolesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_USER_ROLES, event.operationType());

    RevokeUserRolesFailureEvent revokeUserRolesFailureEvent = (RevokeUserRolesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofUser(METALAKE, inExistUserName),
        revokeUserRolesFailureEvent.identifier());
    Assertions.assertEquals(inExistUserName, revokeUserRolesFailureEvent.userName());
    Assertions.assertEquals(revokedRoles, revokeUserRolesFailureEvent.roles());
    Assertions.assertEquals(
        String.join(",", revokedRoles), revokeUserRolesFailureEvent.customInfo().get("roleNames"));
  }

  @Test
  void testBulkAddUsersDispatchesPerUserEvents() {
    dummyEventListener.clear();

    dispatcher.addUsers(METALAKE, Arrays.asList(new UserAdd(userName), new UserAdd(otherUserName)));

    Assertions.assertEquals(2, dummyEventListener.getPreEvents().size());
    PreEvent firstPreEvent = dummyEventListener.getPreEvents().get(0);
    Assertions.assertEquals(AddUserPreEvent.class, firstPreEvent.getClass());
    Assertions.assertEquals(identifier, firstPreEvent.identifier());
    Assertions.assertEquals(userName, ((AddUserPreEvent) firstPreEvent).userName());
    PreEvent secondPreEvent = dummyEventListener.getPreEvents().get(1);
    Assertions.assertEquals(AddUserPreEvent.class, secondPreEvent.getClass());
    Assertions.assertEquals(otherIdentifier, secondPreEvent.identifier());
    Assertions.assertEquals(otherUserName, ((AddUserPreEvent) secondPreEvent).userName());

    Assertions.assertEquals(2, dummyEventListener.getPostEvents().size());
    Event firstEvent = dummyEventListener.getPostEvents().get(0);
    Assertions.assertEquals(AddUserEvent.class, firstEvent.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, firstEvent.operationStatus());
    validateUserInfo(((AddUserEvent) firstEvent).addedUserInfo(), user);
    Event secondEvent = dummyEventListener.getPostEvents().get(1);
    Assertions.assertEquals(AddUserFailureEvent.class, secondEvent.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, secondEvent.operationStatus());
    Assertions.assertEquals(otherIdentifier, secondEvent.identifier());
    Assertions.assertEquals(otherUserName, ((AddUserFailureEvent) secondEvent).userName());
  }

  @Test
  void testBulkRemoveUsersDispatchesPerUserEvents() {
    dummyEventListener.clear();

    dispatcher.removeUsers(METALAKE, Arrays.asList(userName, inExistUserName), Optional.empty());

    Assertions.assertEquals(2, dummyEventListener.getPreEvents().size());
    PreEvent firstPreEvent = dummyEventListener.getPreEvents().get(0);
    Assertions.assertEquals(RemoveUserPreEvent.class, firstPreEvent.getClass());
    Assertions.assertEquals(identifier, firstPreEvent.identifier());
    Assertions.assertEquals(userName, ((RemoveUserPreEvent) firstPreEvent).userName());
    PreEvent secondPreEvent = dummyEventListener.getPreEvents().get(1);
    Assertions.assertEquals(RemoveUserPreEvent.class, secondPreEvent.getClass());
    Assertions.assertEquals(inExistIdentifier, secondPreEvent.identifier());
    Assertions.assertEquals(inExistUserName, ((RemoveUserPreEvent) secondPreEvent).userName());

    Assertions.assertEquals(2, dummyEventListener.getPostEvents().size());
    Event firstEvent = dummyEventListener.getPostEvents().get(0);
    Assertions.assertEquals(RemoveUserEvent.class, firstEvent.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, firstEvent.operationStatus());
    Assertions.assertEquals(userName, ((RemoveUserEvent) firstEvent).removedUserName());
    Assertions.assertTrue(((RemoveUserEvent) firstEvent).isExists());
    Event secondEvent = dummyEventListener.getPostEvents().get(1);
    Assertions.assertEquals(RemoveUserFailureEvent.class, secondEvent.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, secondEvent.operationStatus());
    Assertions.assertEquals(inExistIdentifier, secondEvent.identifier());
    Assertions.assertEquals(inExistUserName, ((RemoveUserFailureEvent) secondEvent).userName());
  }

  private AccessControlEventDispatcher mockUserDispatcher() {
    AccessControlEventDispatcher dispatcher = mock(AccessControlEventDispatcher.class);
    when(dispatcher.addUser(METALAKE, userName)).thenReturn(user);
    when(dispatcher.addUser(METALAKE, otherUserName)).thenReturn(otherUser);
    when(dispatcher.addUsers(eq(METALAKE), any()))
        .thenReturn(
            Arrays.asList(
                BulkItemResult.success(0, userName, user),
                BulkItemResult.failure(
                    1, otherUserName, new GravitinoRuntimeException("Failed to add user"))));

    when(dispatcher.removeUser(METALAKE, userName)).thenReturn(true);
    when(dispatcher.removeUser(METALAKE, inExistUserName)).thenReturn(false);
    when(dispatcher.removeUsers(eq(METALAKE), any(), any()))
        .thenReturn(
            Arrays.asList(
                BulkItemResult.success(0, userName),
                BulkItemResult.failure(
                    1, inExistUserName, new NoSuchUserException("user not found"))));

    when(dispatcher.listUsers(METALAKE)).thenReturn(new User[] {user, otherUser});
    when(dispatcher.listUsers(eq(METALAKE), eq(0), eq(10)))
        .thenReturn(new PagedResult<>(2, Arrays.asList(user, otherUser)));
    when(dispatcher.countUsers(METALAKE)).thenReturn(2L);
    when(dispatcher.listUserNames(METALAKE)).thenReturn(new String[] {userName, otherUserName});

    when(dispatcher.getUser(METALAKE, userName)).thenReturn(user);
    when(dispatcher.getUser(METALAKE, inExistUserName))
        .thenThrow(new NoSuchUserException("user not found"));
    when(dispatcher.getUser(INEXIST_METALAKE, userName))
        .thenThrow(new NoSuchMetalakeException("user not found"));
    when(dispatcher.grantRolesToUser(METALAKE, grantedRoles, userName)).thenReturn(user);
    when(dispatcher.revokeRolesFromUser(METALAKE, revokedRoles, otherUserName))
        .thenReturn(otherUser);

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
    when(user.id()).thenReturn(1L);
    when(user.name()).thenReturn(name);
    when(user.roles()).thenReturn(roles);

    return user;
  }

  private void validateUserInfo(UserInfo userInfo, User expectedUser) {
    Assertions.assertEquals(expectedUser.id(), userInfo.id());
    Assertions.assertEquals(userInfo.name(), expectedUser.name());
    Assertions.assertEquals(userInfo.roles(), expectedUser.roles());
  }
}
