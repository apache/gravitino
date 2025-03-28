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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRoleEvent {
  private static final String METALAKE = "demo_metalake";
  private String groupName;
  private String userName;
  private AccessControlEventDispatcher dispatcher;
  private AccessControlEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private String roleName;
  private String otherRoleName;
  private NameIdentifier identifier;
  private Map<String, String> properties;
  private List<SecurableObject> securableObjects;
  private Set<Privilege> privileges;
  private MetadataObject metadataObject;
  private Role role;

  @BeforeAll
  void init() {
    this.groupName = "demo_group";
    this.userName = "demo_user";
    this.roleName = "admin";
    this.otherRoleName = "user";
    this.identifier = NameIdentifier.of(METALAKE);
    this.properties = ImmutableMap.of("comment", "test comment");
    this.securableObjects =
        ImmutableList.of(
            getMockSecurableObjects(Privilege.Name.CREATE_TABLE, Privilege.Name.MODIFY_TABLE));
    this.role = getMockRole(roleName, properties, securableObjects);
    this.privileges =
        Sets.newHashSet(
            Privileges.allow(Privilege.Name.CREATE_TABLE),
            Privileges.allow(Privilege.Name.MODIFY_TABLE));
    this.metadataObject = getMockMetadataObject("test_metalake", MetadataObject.Type.METALAKE);

    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    this.dispatcher = new AccessControlEventDispatcher(eventBus, mockRoleDispatcher());
    this.failureDispatcher =
        new AccessControlEventDispatcher(eventBus, mockExceptionRoleDispatcher());

    System.out.println(failureDispatcher);
  }

  @Test
  void testCreateRole() {
    dispatcher.createRole(
        METALAKE, roleName, ImmutableMap.of("comment", "test comment"), securableObjects);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(CreateRolePreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.CREATE_ROLE, preEvent.operationType());

    CreateRolePreEvent createRolePreEvent = (CreateRolePreEvent) preEvent;
    Assertions.assertEquals(identifier, createRolePreEvent.identifier());
    Assertions.assertEquals(roleName, createRolePreEvent.roleName());
    Assertions.assertEquals(properties, createRolePreEvent.properties());
    Assertions.assertEquals(securableObjects, createRolePreEvent.securableObjects());
  }

  @Test
  void testDeleteRolePreEvent() {
    dispatcher.deleteRole(METALAKE, roleName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(DeleteRolePreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.DELETE_ROLE, preEvent.operationType());

    DeleteRolePreEvent deleteRolePreEvent = (DeleteRolePreEvent) preEvent;
    Assertions.assertEquals(identifier, deleteRolePreEvent.identifier());
    Assertions.assertEquals(roleName, deleteRolePreEvent.roleName());
  }

  @Test
  void testGetRolePreEvent() {
    dispatcher.getRole(METALAKE, roleName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GetRolePreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GET_ROLE, preEvent.operationType());

    GetRolePreEvent getRolePreEvent = (GetRolePreEvent) preEvent;
    Assertions.assertEquals(identifier, getRolePreEvent.identifier());
    Assertions.assertEquals(roleName, getRolePreEvent.roleName());
  }

  @Test
  void testGrantRolesToGroup() {
    dispatcher.grantRolesToGroup(METALAKE, ImmutableList.of(roleName, otherRoleName), groupName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GrantRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_ROLES, preEvent.operationType());

    GrantRolesPreEvent grantRolesPreEvent = (GrantRolesPreEvent) preEvent;
    Assertions.assertEquals(identifier, grantRolesPreEvent.identifier());
    Assertions.assertEquals(ImmutableList.of(roleName, otherRoleName), grantRolesPreEvent.roles());
    Assertions.assertFalse(grantRolesPreEvent.userName().isPresent());
    Assertions.assertTrue(grantRolesPreEvent.groupName().isPresent());
    Assertions.assertEquals(groupName, grantRolesPreEvent.groupName().get());
  }

  @Test
  void testGrantRolesToUser() {
    dispatcher.grantRolesToUser(METALAKE, ImmutableList.of(roleName, otherRoleName), userName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GrantRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_ROLES, preEvent.operationType());

    GrantRolesPreEvent grantRolesPreEvent = (GrantRolesPreEvent) preEvent;
    Assertions.assertEquals(identifier, grantRolesPreEvent.identifier());
    Assertions.assertEquals(ImmutableList.of(roleName, otherRoleName), grantRolesPreEvent.roles());
    Assertions.assertTrue(grantRolesPreEvent.userName().isPresent());
    Assertions.assertFalse(grantRolesPreEvent.groupName().isPresent());
    Assertions.assertEquals(userName, grantRolesPreEvent.userName().get());
  }

  @Test
  void testRevokeRolesFromGroup() {
    dispatcher.revokeRolesFromGroup(METALAKE, ImmutableList.of(roleName, otherRoleName), groupName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RevokeRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_ROLES, preEvent.operationType());

    RevokeRolesPreEvent revokeRolesPreEvent = (RevokeRolesPreEvent) preEvent;
    Assertions.assertEquals(identifier, revokeRolesPreEvent.identifier());
    Assertions.assertEquals(ImmutableList.of(roleName, otherRoleName), revokeRolesPreEvent.roles());
    Assertions.assertFalse(revokeRolesPreEvent.userName().isPresent());
    Assertions.assertTrue(revokeRolesPreEvent.groupName().isPresent());
    Assertions.assertEquals(groupName, revokeRolesPreEvent.groupName().get());
  }

  @Test
  void testRevokeRolesFromUser() {
    dispatcher.revokeRolesFromUser(METALAKE, ImmutableList.of(roleName, otherRoleName), userName);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RevokeRolesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_ROLES, preEvent.operationType());

    RevokeRolesPreEvent revokeRolesPreEvent = (RevokeRolesPreEvent) preEvent;
    Assertions.assertEquals(identifier, revokeRolesPreEvent.identifier());
    Assertions.assertEquals(ImmutableList.of(roleName, otherRoleName), revokeRolesPreEvent.roles());
    Assertions.assertTrue(revokeRolesPreEvent.userName().isPresent());
    Assertions.assertFalse(revokeRolesPreEvent.groupName().isPresent());
    Assertions.assertEquals(userName, revokeRolesPreEvent.userName().get());
  }

  @Test
  void testListRolesEvent() {
    dispatcher.listRoleNames(METALAKE);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListRoleNamesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.LIST_ROLE_NAMES, preEvent.operationType());

    ListRoleNamesPreEvent listRoleNamesPreEvent = (ListRoleNamesPreEvent) preEvent;
    Assertions.assertEquals(identifier, listRoleNamesPreEvent.identifier());
    Assertions.assertFalse(listRoleNamesPreEvent.object().isPresent());
  }

  @Test
  void testListRolesFromObject() {
    dispatcher.listRoleNamesByObject(METALAKE, securableObjects.get(0));

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(ListRoleNamesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.LIST_ROLE_NAMES, preEvent.operationType());

    ListRoleNamesPreEvent listRoleNamesPreEvent = (ListRoleNamesPreEvent) preEvent;
    Assertions.assertEquals(identifier, listRoleNamesPreEvent.identifier());
    Assertions.assertTrue(listRoleNamesPreEvent.object().isPresent());
    Assertions.assertEquals(securableObjects.get(0), listRoleNamesPreEvent.object().get());
  }

  @Test
  void testGrantPrivilegesToRole() {
    dispatcher.grantPrivilegeToRole(METALAKE, roleName, metadataObject, privileges);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GrantPrivilegesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_PRIVILEGES, preEvent.operationType());

    GrantPrivilegesPreEvent grantPrivilegesPreEvent = (GrantPrivilegesPreEvent) preEvent;
    Assertions.assertEquals(identifier, grantPrivilegesPreEvent.identifier());
    Assertions.assertEquals(roleName, grantPrivilegesPreEvent.roleName());
    Assertions.assertEquals(metadataObject, grantPrivilegesPreEvent.object());
    Assertions.assertEquals(privileges, grantPrivilegesPreEvent.privileges());
  }

  @Test
  void testRevokePrivilegesFromRole() {
    dispatcher.revokePrivilegesFromRole(METALAKE, roleName, metadataObject, privileges);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RevokePrivilegesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_PRIVILEGES, preEvent.operationType());

    RevokePrivilegesPreEvent revokePrivilegesPreEvent = (RevokePrivilegesPreEvent) preEvent;
    Assertions.assertEquals(identifier, revokePrivilegesPreEvent.identifier());
    Assertions.assertEquals(roleName, revokePrivilegesPreEvent.roleName());
    Assertions.assertEquals(metadataObject, revokePrivilegesPreEvent.object());
    Assertions.assertEquals(privileges, revokePrivilegesPreEvent.privileges());
  }

  private AccessControlEventDispatcher mockRoleDispatcher() {
    AccessControlEventDispatcher dispatcher = mock(AccessControlEventDispatcher.class);
    Group mockGroup = getMockGroup(groupName, ImmutableList.of(roleName, otherRoleName));
    User mockUser = getMockUser(userName, ImmutableList.of(roleName, otherRoleName));

    when(dispatcher.createRole(METALAKE, roleName, properties, securableObjects)).thenReturn(role);
    when(dispatcher.deleteRole(METALAKE, roleName)).thenReturn(true);
    when(dispatcher.deleteRole(METALAKE, otherRoleName)).thenReturn(false);
    when(dispatcher.getRole(METALAKE, roleName)).thenReturn(role);
    when(dispatcher.grantRolesToGroup(
            METALAKE, ImmutableList.of(roleName, otherRoleName), groupName))
        .thenReturn(mockGroup);
    when(dispatcher.grantRolesToUser(METALAKE, ImmutableList.of(roleName, otherRoleName), userName))
        .thenReturn(mockUser);
    when(dispatcher.revokeRolesFromGroup(
            METALAKE, ImmutableList.of(roleName, otherRoleName), groupName))
        .thenReturn(mockGroup);
    when(dispatcher.listRoleNames(METALAKE)).thenReturn(new String[] {roleName, otherRoleName});
    when(dispatcher.listRoleNamesByObject(METALAKE, securableObjects.get(0)))
        .thenReturn(new String[] {roleName, otherRoleName});
    when(dispatcher.grantPrivilegeToRole(METALAKE, roleName, metadataObject, privileges))
        .thenReturn(role);
    when(dispatcher.revokePrivilegesFromRole(METALAKE, roleName, metadataObject, privileges))
        .thenReturn(role);

    return dispatcher;
  }

  private AccessControlEventDispatcher mockExceptionRoleDispatcher() {
    return mock(
        AccessControlEventDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  public SecurableObject getMockSecurableObjects(Privilege.Name... names) {
    SecurableObject mockSecurableObject = mock(SecurableObject.class);
    List<Privilege> privileges =
        Arrays.stream(names).map(Privileges::allow).collect(Collectors.toList());
    when(mockSecurableObject.privileges()).thenReturn(privileges);

    return mockSecurableObject;
  }

  public Role getMockRole(
      String roleName, Map<String, String> properties, List<SecurableObject> securableObjects) {
    Role mockRole = mock(Role.class);
    when(mockRole.name()).thenReturn(roleName);
    when(mockRole.properties()).thenReturn(properties);
    when(mockRole.securableObjects()).thenReturn(securableObjects);

    return mockRole;
  }

  private Group getMockGroup(String name, List<String> roles) {
    Group mockGroup = mock(Group.class);
    when(mockGroup.name()).thenReturn(name);
    when(mockGroup.roles()).thenReturn(roles);

    return mockGroup;
  }

  private User getMockUser(String name, List<String> roles) {
    User user = mock(User.class);
    when(user.name()).thenReturn(name);
    when(user.roles()).thenReturn(roles);

    return user;
  }

  private MetadataObject getMockMetadataObject(String name, MetadataObject.Type type) {
    MetadataObject mockMetadataObject = mock(MetadataObject.class);
    when(mockMetadataObject.name()).thenReturn(name);
    when(mockMetadataObject.type()).thenReturn(type);

    return mockMetadataObject;
  }
}
