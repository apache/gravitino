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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.info.GroupInfo;
import org.apache.gravitino.listener.api.info.RoleInfo;
import org.apache.gravitino.listener.api.info.UserInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * An implementation of the {@link AccessControlDispatcher} interface that dispatches events to the
 * specified event bus.
 */
public class AccessControlEventDispatcher implements AccessControlDispatcher {
  private final EventBus eventBus;
  private final AccessControlDispatcher dispatcher;

  /**
   * Construct a new {@link AccessControlEventDispatcher} instance with the specified event bus and
   * access control dispatcher.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link AccessControlDispatcher} that will perform the actual
   *     access control operations.
   */
  public AccessControlEventDispatcher(EventBus eventBus, AccessControlDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  /** {@inheritDoc} */
  @Override
  public User addUser(String metalake, String user)
      throws UserAlreadyExistsException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new AddUserPreEvent(initiator, metalake, user));
    try {
      User userObject = dispatcher.addUser(metalake, user);
      eventBus.dispatchEvent(new AddUserEvent(initiator, metalake, new UserInfo(userObject)));

      return userObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AddUserFailureEvent(initiator, metalake, e, user));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new RemoveUserPreEvent(initiator, metalake, user));
    try {
      boolean isExists = dispatcher.removeUser(metalake, user);
      eventBus.dispatchEvent(new RemoveUserEvent(initiator, metalake, user, isExists));

      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new RemoveUserFailureEvent(initiator, metalake, e, user));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetUserPreEvent(initiator, metalake, user));
    try {
      User userObject = dispatcher.getUser(metalake, user);
      eventBus.dispatchEvent(new GetUserEvent(initiator, metalake, new UserInfo(userObject)));

      return userObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetUserFailureEvent(initiator, metalake, e, user));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User[] listUsers(String metalake) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListUsersPreEvent(initiator, metalake));
    try {
      User[] users = dispatcher.listUsers(metalake);
      eventBus.dispatchEvent(new ListUsersEvent(initiator, metalake));

      return users;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListUsersFailureEvent(initiator, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listUserNames(String metalake) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListUserNamesPreEvent(initiator, metalake));
    try {
      String[] userNames = dispatcher.listUserNames(metalake);
      eventBus.dispatchEvent(new ListUserNamesEvent(initiator, metalake));

      return userNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListUserNamesFailureEvent(initiator, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new AddGroupPreEvent(initiator, metalake, group));
    try {
      Group groupObject = dispatcher.addGroup(metalake, group);
      eventBus.dispatchEvent(new AddGroupEvent(initiator, metalake, new GroupInfo(groupObject)));

      return groupObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AddGroupFailureEvent(initiator, metalake, e, group));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new RemoveGroupPreEvent(initiator, metalake, group));
    try {
      boolean isExists = dispatcher.removeGroup(metalake, group);
      eventBus.dispatchEvent(new RemoveGroupEvent(initiator, metalake, group, isExists));

      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new RemoveGroupFailureEvent(initiator, metalake, e, group));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetGroupPreEvent(initiator, metalake, group));
    try {
      Group groupObject = dispatcher.getGroup(metalake, group);
      eventBus.dispatchEvent(new GetGroupEvent(initiator, metalake, new GroupInfo(groupObject)));

      return groupObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetGroupFailureEvent(initiator, metalake, e, group));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group[] listGroups(String metalake) {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListGroupsPreEvent(initiator, metalake));
    try {
      Group[] groups = dispatcher.listGroups(metalake);
      eventBus.dispatchEvent(new ListGroupsEvent(initiator, metalake));

      return groups;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListGroupsFailureEvent(initiator, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listGroupNames(String metalake) {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListGroupNamesPreEvent(initiator, metalake));
    try {
      String[] groupNames = dispatcher.listGroupNames(metalake);
      eventBus.dispatchEvent(new ListGroupNamesEvent(initiator, metalake));

      return groupNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListGroupNamesFailureEvent(initiator, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GrantUserRolesPreEvent(initiator, metalake, user, roles));
    try {
      User userObject = dispatcher.grantRolesToUser(metalake, roles, user);
      eventBus.dispatchEvent(
          new GrantUserRolesEvent(initiator, metalake, new UserInfo(userObject), roles));

      return userObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GrantUserRolesFailureEvent(initiator, metalake, e, user, roles));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GrantGroupRolesPreEvent(initiator, metalake, group, roles));
    try {
      Group groupObject = dispatcher.grantRolesToGroup(metalake, roles, group);
      eventBus.dispatchEvent(
          new GrantGroupRolesEvent(initiator, metalake, new GroupInfo(groupObject), roles));

      return groupObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GrantGroupRolesFailureEvent(initiator, metalake, e, group, roles));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new RevokeGroupRolesPreEvent(initiator, metalake, group, roles));
    try {
      Group groupObject = dispatcher.revokeRolesFromGroup(metalake, roles, group);
      eventBus.dispatchEvent(
          new RevokeGroupRolesEvent(initiator, metalake, new GroupInfo(groupObject), roles));

      return groupObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new RevokeGroupRolesFailureEvent(initiator, metalake, e, group, roles));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new RevokeUserRolesPreEvent(initiator, metalake, user, roles));
    try {
      User userObject = dispatcher.revokeRolesFromUser(metalake, roles, user);
      eventBus.dispatchEvent(
          new RevokeUserRolesEvent(initiator, metalake, new UserInfo(userObject), roles));

      return userObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new RevokeUserRolesFailureEvent(initiator, metalake, e, user, roles));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean isServiceAdmin(String user) {
    return dispatcher.isServiceAdmin(user);
  }

  /** {@inheritDoc} */
  @Override
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new CreateRolePreEvent(initiator, metalake, role, properties, securableObjects));
    try {
      Role roleObject = dispatcher.createRole(metalake, role, properties, securableObjects);
      eventBus.dispatchEvent(new CreateRoleEvent(initiator, metalake, new RoleInfo(roleObject)));

      return roleObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateRoleFailureEvent(
              initiator, metalake, e, new RoleInfo(role, properties, securableObjects)));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetRolePreEvent(initiator, metalake, role));
    try {
      Role roleObject = dispatcher.getRole(metalake, role);
      eventBus.dispatchEvent(new GetRoleEvent(initiator, metalake, new RoleInfo(roleObject)));

      return roleObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetRoleFailureEvent(initiator, metalake, e, role));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new DeleteRolePreEvent(initiator, metalake, role));
    try {
      boolean isExists = dispatcher.deleteRole(metalake, role);
      eventBus.dispatchEvent(new DeleteRoleEvent(initiator, metalake, role, isExists));

      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DeleteRoleFailureEvent(initiator, metalake, e, role));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listRoleNames(String metalake) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListRoleNamesPreEvent(initiator, metalake));
    try {
      String[] roleNames = dispatcher.listRoleNames(metalake);
      eventBus.dispatchEvent(new ListRoleNamesEvent(initiator, metalake));

      return roleNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListRoleNamesFailureEvent(initiator, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listRoleNamesByObject(String metalake, MetadataObject object)
      throws NoSuchMetalakeException, NoSuchMetadataObjectException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListRoleNamesPreEvent(initiator, metalake, object));
    try {
      String[] roleNames = dispatcher.listRoleNamesByObject(metalake, object);
      eventBus.dispatchEvent(new ListRoleNamesEvent(initiator, metalake, object));

      return roleNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListRoleNamesFailureEvent(initiator, metalake, e, object));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role grantPrivilegeToRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchMetalakeException, NoSuchRoleException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new GrantPrivilegesPreEvent(initiator, metalake, role, object, privileges));
    try {
      Role roleObject = dispatcher.grantPrivilegeToRole(metalake, role, object, privileges);
      eventBus.dispatchEvent(
          new GrantPrivilegesEvent(
              initiator, metalake, new RoleInfo(roleObject), privileges, object));

      return roleObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GrantPrivilegesFailureEvent(initiator, metalake, e, role, object, privileges));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchMetalakeException, NoSuchRoleException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(
        new RevokePrivilegesPreEvent(initiator, metalake, role, object, privileges));
    try {
      Role roleObject = dispatcher.revokePrivilegesFromRole(metalake, role, object, privileges);
      eventBus.dispatchEvent(
          new RevokePrivilegesEvent(
              initiator, metalake, new RoleInfo(roleObject), object, privileges));

      return roleObject;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new RevokePrivilegesFailureEvent(initiator, metalake, e, role, object, privileges));
      throw e;
    }
  }
}
