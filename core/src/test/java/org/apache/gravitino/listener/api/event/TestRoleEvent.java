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
import org.apache.gravitino.listener.AccessControlEventDispatcher;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.RoleInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
  void testRoleInfo() {
    role = getMockRole(roleName, properties, securableObjects);
    RoleInfo roleInfo = new RoleInfo(role);

    Assertions.assertEquals(roleName, roleInfo.roleName());
    Assertions.assertEquals(properties, roleInfo.properties());
    Assertions.assertEquals(securableObjects, roleInfo.securableObjects());

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> roleInfo.securableObjects().add(null));

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> roleInfo.properties().put("test", "test"));
  }

  @Test
  void testRoleInfoWithNullSecurableObjects() {
    RoleInfo roleInfo = new RoleInfo("test_role", ImmutableMap.of("comment", "test comment"), null);
    Assertions.assertEquals("test_role", roleInfo.roleName());
    Assertions.assertEquals(ImmutableMap.of("comment", "test comment"), roleInfo.properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> roleInfo.properties().put("testKey", "testVal"));
    Assertions.assertEquals(Collections.emptyList(), roleInfo.securableObjects());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> roleInfo.securableObjects().add(null));
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
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), createRolePreEvent.identifier());
    Assertions.assertEquals(roleName, createRolePreEvent.roleName());
    Assertions.assertEquals(properties, createRolePreEvent.properties());
    Assertions.assertEquals(securableObjects, createRolePreEvent.securableObjects());
  }

  @Test
  void testCreateRoleEvent() {
    dispatcher.createRole(
        METALAKE, roleName, ImmutableMap.of("comment", "test comment"), securableObjects);
    Role roleObject = getMockRole(roleName, properties, securableObjects);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateRoleEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.CREATE_ROLE, event.operationType());

    CreateRoleEvent createRoleEvent = (CreateRoleEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), createRoleEvent.identifier());
    RoleInfo roleInfo = createRoleEvent.createdRoleInfo();

    validateRoleInfo(roleInfo, roleObject);
  }

  @Test
  void testCreateRoleFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createRole(METALAKE, roleName, properties, securableObjects));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateRoleFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.CREATE_ROLE, event.operationType());

    CreateRoleFailureEvent createRoleFailureEvent = (CreateRoleFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), createRoleFailureEvent.identifier());
    RoleInfo roleInfo = createRoleFailureEvent.createRoleRequest();
    Assertions.assertEquals(roleName, roleInfo.roleName());
    Assertions.assertEquals(properties, roleInfo.properties());
    Assertions.assertEquals(securableObjects, roleInfo.securableObjects());
  }

  @Test
  void testCreateRoleFailureEventWithNullSecurableObjects() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createRole(METALAKE, roleName, properties, null));

    // Validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateRoleFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    CreateRoleFailureEvent createRoleFailureEvent = (CreateRoleFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), createRoleFailureEvent.identifier());

    RoleInfo roleInfo = createRoleFailureEvent.createRoleRequest();
    Assertions.assertEquals(roleName, roleInfo.roleName());
    Assertions.assertEquals(properties, roleInfo.properties());
    Assertions.assertNotNull(roleInfo.securableObjects());
    Assertions.assertTrue(roleInfo.securableObjects().isEmpty());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> roleInfo.securableObjects().add(null));
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
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), deleteRolePreEvent.identifier());
    Assertions.assertEquals(roleName, deleteRolePreEvent.roleName());
  }

  @Test
  void testDeleteRoleEventWithExistIdentifier() {
    dispatcher.deleteRole(METALAKE, roleName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteRoleEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.DELETE_ROLE, event.operationType());

    DeleteRoleEvent deleteRoleEvent = (DeleteRoleEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), deleteRoleEvent.identifier());
    Assertions.assertEquals(roleName, deleteRoleEvent.roleName());
    Assertions.assertTrue(deleteRoleEvent.isExists());
  }

  @Test
  void testDeleteRoleEventWithNotExistIdentifier() {
    dispatcher.deleteRole(METALAKE, otherRoleName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteRoleEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.DELETE_ROLE, event.operationType());

    DeleteRoleEvent deleteRoleEvent = (DeleteRoleEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, otherRoleName), deleteRoleEvent.identifier());
    Assertions.assertEquals(otherRoleName, deleteRoleEvent.roleName());
  }

  @Test
  void testDeleteRoleFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.deleteRole(METALAKE, roleName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteRoleFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.DELETE_ROLE, event.operationType());

    DeleteRoleFailureEvent deleteRoleFailureEvent = (DeleteRoleFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), deleteRoleFailureEvent.identifier());
    Assertions.assertEquals(roleName, deleteRoleFailureEvent.roleName());
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
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), getRolePreEvent.identifier());
    Assertions.assertEquals(roleName, getRolePreEvent.roleName());
  }

  @Test
  void testGetRoleEvent() {
    dispatcher.getRole(METALAKE, roleName);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetRoleEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.GET_ROLE, event.operationType());

    GetRoleEvent getRoleEvent = (GetRoleEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), getRoleEvent.identifier());
    RoleInfo roleInfo = getRoleEvent.roleInfo();

    validateRoleInfo(roleInfo, roleName, properties, securableObjects);
  }

  @Test
  void testGetRoleFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.getRole(METALAKE, roleName));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetRoleFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GET_ROLE, event.operationType());

    GetRoleFailureEvent getRoleFailureEvent = (GetRoleFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), getRoleFailureEvent.identifier());
    Assertions.assertEquals(roleName, getRoleFailureEvent.roleName());
  }

  @Test
  void testListRolesPreEvent() {
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
  void testListRolesEvent() {
    dispatcher.listRoleNames(METALAKE);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListRoleNamesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_ROLE_NAMES, event.operationType());

    ListRoleNamesEvent listRoleNamesEvent = (ListRoleNamesEvent) event;
    Assertions.assertEquals(identifier, listRoleNamesEvent.identifier());
    Assertions.assertFalse(listRoleNamesEvent.object().isPresent());
  }

  @Test
  void testListRolesFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listRoleNames(METALAKE));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListRoleNamesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_ROLE_NAMES, event.operationType());

    ListRoleNamesFailureEvent listRoleNamesFailureEvent = (ListRoleNamesFailureEvent) event;
    Assertions.assertEquals(identifier, listRoleNamesFailureEvent.identifier());
    Assertions.assertFalse(listRoleNamesFailureEvent.metadataObject().isPresent());
  }

  @Test
  void testListRolesFromObjectPreEvent() {
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
  void testListRoleFromObjectEvent() {
    dispatcher.listRoleNamesByObject(METALAKE, securableObjects.get(0));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListRoleNamesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_ROLE_NAMES, event.operationType());

    ListRoleNamesEvent listRoleNamesEvent = (ListRoleNamesEvent) event;
    Assertions.assertEquals(identifier, listRoleNamesEvent.identifier());
    Assertions.assertTrue(listRoleNamesEvent.object().isPresent());
    Assertions.assertEquals(securableObjects.get(0), listRoleNamesEvent.object().get());
  }

  @Test
  void testListRoleFromObjectFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listRoleNamesByObject(METALAKE, securableObjects.get(0)));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListRoleNamesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.LIST_ROLE_NAMES, event.operationType());

    ListRoleNamesFailureEvent listRoleNamesFailureEvent = (ListRoleNamesFailureEvent) event;
    Assertions.assertEquals(identifier, listRoleNamesFailureEvent.identifier());
    Assertions.assertTrue(listRoleNamesFailureEvent.metadataObject().isPresent());
    Assertions.assertEquals(
        securableObjects.get(0), listRoleNamesFailureEvent.metadataObject().get());
  }

  @Test
  void testGrantPrivilegesToRolePreEvent() {
    dispatcher.grantPrivilegeToRole(METALAKE, roleName, metadataObject, privileges);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(GrantPrivilegesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_PRIVILEGES, preEvent.operationType());

    GrantPrivilegesPreEvent grantPrivilegesPreEvent = (GrantPrivilegesPreEvent) preEvent;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), grantPrivilegesPreEvent.identifier());
    Assertions.assertEquals(roleName, grantPrivilegesPreEvent.roleName());
    Assertions.assertEquals(metadataObject, grantPrivilegesPreEvent.object());
    Assertions.assertEquals(privileges, grantPrivilegesPreEvent.privileges());
  }

  @Test
  void testGrantPrivilegesToRoleEvent() {
    dispatcher.grantPrivilegeToRole(METALAKE, roleName, metadataObject, privileges);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantPrivilegesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_PRIVILEGES, event.operationType());

    GrantPrivilegesEvent grantPrivilegesEvent = (GrantPrivilegesEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), grantPrivilegesEvent.identifier());
    RoleInfo roleInfo = grantPrivilegesEvent.grantedRoleInfo();
    validateRoleInfo(roleInfo, roleName, properties, securableObjects);
  }

  @Test
  void testGrantPrivilegesToRoleFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.grantPrivilegeToRole(METALAKE, roleName, metadataObject, privileges));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantPrivilegesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_PRIVILEGES, event.operationType());

    GrantPrivilegesFailureEvent grantPrivilegesFailureEvent = (GrantPrivilegesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), grantPrivilegesFailureEvent.identifier());
    Assertions.assertEquals(roleName, grantPrivilegesFailureEvent.roleName());
    Assertions.assertEquals(metadataObject, grantPrivilegesFailureEvent.object());
    Assertions.assertEquals(privileges, grantPrivilegesFailureEvent.privileges());
  }

  @Test
  void testGrantPrivilegesFailureEventWithNullPrivileges() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.grantPrivilegeToRole(METALAKE, roleName, metadataObject, null));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GrantPrivilegesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.GRANT_PRIVILEGES, event.operationType());

    GrantPrivilegesFailureEvent grantPrivilegesFailureEvent = (GrantPrivilegesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), grantPrivilegesFailureEvent.identifier());
    Assertions.assertEquals(roleName, grantPrivilegesFailureEvent.roleName());
    Assertions.assertEquals(metadataObject, grantPrivilegesFailureEvent.object());
    Assertions.assertNotNull(grantPrivilegesFailureEvent.privileges());
    Assertions.assertTrue(grantPrivilegesFailureEvent.privileges().isEmpty());

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> grantPrivilegesFailureEvent.privileges().add(null));
  }

  @Test
  void testRevokePrivilegesFromRolePreEvent() {
    dispatcher.revokePrivilegesFromRole(METALAKE, roleName, metadataObject, privileges);

    // validate pre-event
    PreEvent preEvent = dummyEventListener.popPreEvent();
    Assertions.assertEquals(RevokePrivilegesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_PRIVILEGES, preEvent.operationType());

    RevokePrivilegesPreEvent revokePrivilegesPreEvent = (RevokePrivilegesPreEvent) preEvent;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), revokePrivilegesPreEvent.identifier());
    Assertions.assertEquals(roleName, revokePrivilegesPreEvent.roleName());
    Assertions.assertEquals(metadataObject, revokePrivilegesPreEvent.object());
    Assertions.assertEquals(privileges, revokePrivilegesPreEvent.privileges());
  }

  @Test
  void testRevokePrivilegesFromRoleEvent() {
    dispatcher.revokePrivilegesFromRole(METALAKE, roleName, metadataObject, privileges);

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RevokePrivilegesEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_PRIVILEGES, event.operationType());

    RevokePrivilegesEvent revokePrivilegesEvent = (RevokePrivilegesEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), revokePrivilegesEvent.identifier());
    RoleInfo roleInfo = revokePrivilegesEvent.revokedRoleInfo();
    validateRoleInfo(roleInfo, roleName, properties, securableObjects);
  }

  @Test
  void testRevokePrivilegesFromRoleFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.revokePrivilegesFromRole(
                METALAKE, roleName, metadataObject, privileges));

    // validate event
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(RevokePrivilegesFailureEvent.class, event.getClass());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(OperationType.REVOKE_PRIVILEGES, event.operationType());

    RevokePrivilegesFailureEvent revokePrivilegesFailureEvent =
        (RevokePrivilegesFailureEvent) event;
    Assertions.assertEquals(
        NameIdentifierUtil.ofRole(METALAKE, roleName), revokePrivilegesFailureEvent.identifier());
    Assertions.assertEquals(roleName, revokePrivilegesFailureEvent.roleName());
    Assertions.assertEquals(metadataObject, revokePrivilegesFailureEvent.metadataObject());
    Assertions.assertEquals(privileges, revokePrivilegesFailureEvent.privileges());
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

  private void validateRoleInfo(RoleInfo roleinfo, Role roleObject) {
    Assertions.assertEquals(roleObject.name(), roleinfo.roleName());
    Assertions.assertEquals(roleObject.properties(), roleinfo.properties());
    Assertions.assertEquals(roleObject.securableObjects(), roleinfo.securableObjects());
  }

  private void validateRoleInfo(
      RoleInfo roleinfo,
      String roleName,
      Map<String, String> properties,
      List<SecurableObject> securableObjects) {
    Assertions.assertEquals(roleName, roleinfo.roleName());
    Assertions.assertEquals(properties, roleinfo.properties());
    Assertions.assertEquals(securableObjects, roleinfo.securableObjects());
  }
}
