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
import org.apache.gravitino.listener.AccessControlEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.GroupInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
  private List<String> grantedRoles;
  private List<String> revokedRoles;

  @BeforeAll
  void init() {
    this.groupName = "group_test";
    this.otherGroupName = "group_admin";
    this.inExistGroupName = "group_not_exist";
    this.group = getMockGroup(groupName, ImmutableList.of("test", "engineer"));
    this.otherGroup = getMockGroup(otherGroupName, null);
    this.identifier = NameIdentifier.of(METALAKE);
    this.grantedRoles = ImmutableList.of("test", "engineer");
    this.revokedRoles = ImmutableList.of("admin", "scientist");

    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new AccessControlEventDispatcher(eventBus, mockGroupDispatcher());
    this.failureDispatcher =
        new AccessControlEventDispatcher(eventBus, mockExceptionGroupDispatcher());

    System.out.println(failureDispatcher);
  }

  @Test
  void testGroupInfo() {
    Group mockGroup = getMockGroup("group_test", ImmutableList.of("test", "engineer"));
    GroupInfo groupInfo = new GroupInfo(mockGroup);

    validateGroup(groupInfo, mockGroup);
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
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), addGroupPreEvent.identifier());
    Assertions.assertEquals(groupName, addGroupPreEvent.groupName());
  }

  @Test
  void testAddGroupEvent() {
    dispatcher.addGroup(METALAKE, groupName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddGroupEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.ADD_GROUP, event.operationType());

    AddGroupEvent addGroupEvent = (AddGroupEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), addGroupEvent.identifier());

    validateGroup(addGroupEvent.addedGroupInfo(), group);
  }

  @Test
  void testAddGroupFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.addGroup(METALAKE, groupName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AddGroupFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.ADD_GROUP, event.operationType());

    AddGroupFailureEvent addGroupFailureEvent = (AddGroupFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), addGroupFailureEvent.identifier());
    Assertions.assertEquals(groupName, addGroupFailureEvent.groupName());
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
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), getGroupPreEvent.identifier());
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
  void testGetGroupEventWithExistingGroup() {
    dispatcher.getGroup(METALAKE, groupName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetGroupEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.GET_GROUP, event.operationType());

    GetGroupEvent getGroupEvent = (GetGroupEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), getGroupEvent.identifier());

    validateGroup(getGroupEvent.loadedGroupInfo(), group);
  }

  @Test
  void testGetGroupEventWithInexistMetalake() {
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> dispatcher.getGroup(INEXIST_METALAKE, groupName));
  }

  @Test
  void testGetGroupEventWithGroupNotFound() {
    Assertions.assertThrows(
        NoSuchGroupException.class, () -> dispatcher.getGroup(METALAKE, inExistGroupName));
  }

  @Test
  void testGetGroupFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> failureDispatcher.getGroup(METALAKE, groupName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetGroupFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GET_GROUP, event.operationType());

    GetGroupFailureEvent getGroupFailureEvent = (GetGroupFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), getGroupFailureEvent.identifier());
    Assertions.assertEquals(groupName, getGroupFailureEvent.groupName());
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
  void testListGroupsEvent() {
    dispatcher.listGroups(METALAKE);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListGroupsEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_GROUPS, event.operationType());

    ListGroupsEvent listGroupsEvent = (ListGroupsEvent) event;
    Assertions.assertEquals(identifier, listGroupsEvent.identifier());
  }

  @Test
  void testListGroupsFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> failureDispatcher.listGroups(METALAKE));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListGroupsFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_GROUPS, event.operationType());

    ListGroupsFailureEvent listGroupsFailureEvent = (ListGroupsFailureEvent) event;
    Assertions.assertEquals(identifier, listGroupsFailureEvent.identifier());
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
  void testListGroupNamesEvent() {
    dispatcher.listGroupNames(METALAKE);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListGroupNamesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_GROUP_NAMES, event.operationType());

    ListGroupNamesEvent listGroupNamesEvent = (ListGroupNamesEvent) event;
    Assertions.assertEquals(identifier, listGroupNamesEvent.identifier());
  }

  @Test
  void testListGroupNamesFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> failureDispatcher.listGroupNames(METALAKE));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListGroupNamesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_GROUP_NAMES, event.operationType());

    ListGroupNamesFailureEvent listGroupNamesFailureEvent = (ListGroupNamesFailureEvent) event;
    Assertions.assertEquals(identifier, listGroupNamesFailureEvent.identifier());
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
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), removeGroupPreEvent.identifier());
    Assertions.assertEquals(groupName, removeGroupPreEvent.groupName());
  }

  @Test
  void testRemoveGroupEventWithExistingGroup() {
    dispatcher.removeGroup(METALAKE, groupName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveGroupEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_GROUP, event.operationType());

    RemoveGroupEvent removeGroupEvent = (RemoveGroupEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), removeGroupEvent.identifier());
    Assertions.assertTrue(removeGroupEvent.isExists());
    Assertions.assertEquals(groupName, removeGroupEvent.removedGroupName());
  }

  @Test
  void testRemoveGroupEventWithNonExistingGroup() {
    dispatcher.removeGroup(METALAKE, inExistGroupName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveGroupEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_GROUP, event.operationType());

    RemoveGroupEvent removeGroupEvent = (RemoveGroupEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, inExistGroupName), removeGroupEvent.identifier());
    Assertions.assertFalse(removeGroupEvent.isExists());
    Assertions.assertEquals(inExistGroupName, removeGroupEvent.removedGroupName());
  }

  @Test
  void testRemoveGroupFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> failureDispatcher.removeGroup(METALAKE, groupName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RemoveGroupFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.REMOVE_GROUP, event.operationType());

    RemoveGroupFailureEvent removeGroupFailureEvent = (RemoveGroupFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), removeGroupFailureEvent.identifier());
    Assertions.assertEquals(groupName, removeGroupFailureEvent.groupName());
  }

  @Test
  void testGrantRolesToGroupPreEvent() {
    dispatcher.grantRolesToGroup(METALAKE, grantedRoles, groupName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GrantGroupRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_GROUP_ROLES, preEvent.operationType());

    GrantGroupRolesPreEvent grantGroupRolesPreEvent = (GrantGroupRolesPreEvent) preEvent;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), grantGroupRolesPreEvent.identifier());
    String grantedUserName = grantGroupRolesPreEvent.groupName();
    Assertions.assertEquals(groupName, grantedUserName);
    Assertions.assertEquals(grantedRoles, grantGroupRolesPreEvent.roles());
  }

  @Test
  void testGrantRolesToGroupEvent() {
    dispatcher.grantRolesToGroup(METALAKE, grantedRoles, groupName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantGroupRolesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_GROUP_ROLES, event.operationType());

    GrantGroupRolesEvent grantGroupRolesEvent = (GrantGroupRolesEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), grantGroupRolesEvent.identifier());
    GroupInfo groupInfo = grantGroupRolesEvent.grantedGroupInfo();

    validateGroup(groupInfo, group);
  }

  @Test
  void testGrantRolesToGroupFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.grantRolesToGroup(METALAKE, grantedRoles, groupName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantGroupRolesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_GROUP_ROLES, event.operationType());

    GrantGroupRolesFailureEvent grantGroupRolesFailureEvent = (GrantGroupRolesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, groupName), grantGroupRolesFailureEvent.identifier());
    Assertions.assertEquals(groupName, grantGroupRolesFailureEvent.groupName());
    Assertions.assertEquals(grantedRoles, grantGroupRolesFailureEvent.roles());
  }

  @Test
  void testRevokeRolesFromGroupPreEvent() {
    dispatcher.revokeRolesFromGroup(METALAKE, revokedRoles, otherGroupName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RevokeGroupRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_GROUP_ROLES, preEvent.operationType());

    RevokeGroupRolesPreEvent revokeGroupRolesPreEvent = (RevokeGroupRolesPreEvent) preEvent;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, otherGroupName),
        revokeGroupRolesPreEvent.identifier());
    String revokedUserName = revokeGroupRolesPreEvent.groupName();
    Assertions.assertEquals(otherGroupName, revokedUserName);
    Assertions.assertEquals(revokedRoles, revokeGroupRolesPreEvent.roles());
  }

  @Test
  void testRevokeRolesFromGroupEvent() {
    dispatcher.revokeRolesFromGroup(METALAKE, revokedRoles, otherGroupName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RevokeGroupRolesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_GROUP_ROLES, event.operationType());

    RevokeGroupRolesEvent revokeGroupRolesEvent = (RevokeGroupRolesEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, otherGroupName), revokeGroupRolesEvent.identifier());
    GroupInfo groupInfo = revokeGroupRolesEvent.revokedGroupInfo();

    validateGroup(groupInfo, otherGroup);
  }

  @Test
  void testRevokeRolesFromGroupFailureEvent() {
    Assertions.assertThrows(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.revokeRolesFromGroup(METALAKE, revokedRoles, otherGroupName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RevokeGroupRolesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_GROUP_ROLES, event.operationType());

    RevokeGroupRolesFailureEvent revokeGroupRolesFailureEvent =
        (RevokeGroupRolesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofGroup(METALAKE, otherGroupName),
        revokeGroupRolesFailureEvent.identifier());
    Assertions.assertEquals(otherGroupName, revokeGroupRolesFailureEvent.groupName());
    Assertions.assertEquals(revokedRoles, revokeGroupRolesFailureEvent.roles());
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
    when(dispatcher.grantRolesToGroup(METALAKE, grantedRoles, groupName)).thenReturn(group);
    when(dispatcher.revokeRolesFromGroup(METALAKE, revokedRoles, otherGroupName))
        .thenReturn(otherGroup);

    return dispatcher;
  }

  private AccessControlEventDispatcher mockExceptionGroupDispatcher() {
    return mock(
        AccessControlEventDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private void validateGroup(GroupInfo groupInfo, Group group) {
    Assertions.assertEquals(group.name(), groupInfo.name());
    Assertions.assertEquals(group.roles(), groupInfo.roles());
  }
}
