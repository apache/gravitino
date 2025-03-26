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
import org.apache.gravitino.NameIdentifier;
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

    eventBus.dispatchEvent(new AddUserPreEvent(initiator, NameIdentifier.of(metalake), user));
    try {
      // TODO add Event
      return dispatcher.addUser(metalake, user);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new RemoveUserPreEvent(initiator, NameIdentifier.of(metalake), user));
    try {
      // TODO: add Event
      return dispatcher.removeUser(metalake, user);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetUserPreEvent(initiator, NameIdentifier.of(metalake), user));
    try {
      return dispatcher.getUser(metalake, user);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User[] listUsers(String metalake) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListUsersPreEvent(initiator, NameIdentifier.of(metalake)));
    try {
      // TODO: add Event
      return dispatcher.listUsers(metalake);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listUserNames(String metalake) throws NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListUserNamesPreEvent(initiator, NameIdentifier.of(metalake)));
    try {
      // TODO: add Event
      return dispatcher.listUserNames(metalake);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    try {
      String initiator = PrincipalUtils.getCurrentUserName();

      eventBus.dispatchEvent(new AddGroupPreEvent(initiator, NameIdentifier.of(metalake), group));
      return dispatcher.addGroup(metalake, group);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    try {
      String initiator = PrincipalUtils.getCurrentUserName();

      eventBus.dispatchEvent(
          new RemoveGroupPreEvent(initiator, NameIdentifier.of(metalake), group));
      return dispatcher.removeGroup(metalake, group);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new GetGroupPreEvent(initiator, NameIdentifier.of(metalake), group));
    try {
      return dispatcher.getGroup(metalake, group);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group[] listGroups(String metalake) {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListGroupsPreEvent(initiator, NameIdentifier.of(metalake)));
    try {
      // TODO: add Event
      return dispatcher.listGroups(metalake);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listGroupNames(String metalake) {
    String initiator = PrincipalUtils.getCurrentUserName();

    eventBus.dispatchEvent(new ListGroupNamesPreEvent(initiator, NameIdentifier.of(metalake)));
    try {
      // TODO: add Event
      return dispatcher.listGroupNames(metalake);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.grantRolesToUser(metalake, roles, user);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.grantRolesToGroup(metalake, roles, group);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.revokeRolesFromGroup(metalake, roles, group);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.revokeRolesFromUser(metalake, roles, user);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean isServiceAdmin(String user) {
    try {
      // TODO: add Event
      return dispatcher.isServiceAdmin(user);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.createRole(metalake, role, properties, securableObjects);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.getRole(metalake, role);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.deleteRole(metalake, role);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listRoleNames(String metalake) throws NoSuchMetalakeException {
    try {
      // TODO: add Event
      return dispatcher.listRoleNames(metalake);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listRoleNamesByObject(String metalake, MetadataObject object)
      throws NoSuchMetalakeException, NoSuchMetadataObjectException {
    try {
      // TODO: add Event
      return dispatcher.listRoleNamesByObject(metalake, object);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role grantPrivilegeToRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchGroupException, NoSuchRoleException {
    try {
      // TODO: add Event
      return dispatcher.grantPrivilegeToRole(metalake, role, object, privileges);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchMetalakeException, NoSuchRoleException {
    try {
      // TODO: add Event
      return dispatcher.revokePrivilegesFromRole(metalake, role, object, privileges);
    } catch (Exception e) {
      // TODO: add failure event
      throw e;
    }
  }
}
