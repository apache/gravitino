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
package org.apache.gravitino.hook;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code AccessControlHookDispatcher} is a decorator for {@link AccessControlDispatcher} that not
 * only delegates access control operations to the underlying access control dispatcher but also
 * executes some hook operations before or after the underlying operations.
 */
public class AccessControlHookDispatcher implements AccessControlDispatcher {
  private final AccessControlDispatcher dispatcher;

  public AccessControlHookDispatcher(AccessControlDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public User addUser(String metalake, String user)
      throws UserAlreadyExistsException, NoSuchMetalakeException {
    return dispatcher.addUser(metalake, user);
  }

  @Override
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    return dispatcher.removeUser(metalake, user);
  }

  @Override
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
    return dispatcher.getUser(metalake, user);
  }

  @Override
  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    return dispatcher.addGroup(metalake, group);
  }

  @Override
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    return dispatcher.removeGroup(metalake, group);
  }

  @Override
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
    return dispatcher.getGroup(metalake, group);
  }

  @Override
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    return dispatcher.grantRolesToUser(metalake, roles, user);
  }

  @Override
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    return dispatcher.grantRolesToGroup(metalake, roles, group);
  }

  @Override
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    return dispatcher.revokeRolesFromGroup(metalake, roles, group);
  }

  @Override
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    return dispatcher.revokeRolesFromUser(metalake, roles, user);
  }

  @Override
  public boolean isServiceAdmin(String user) {
    return dispatcher.isServiceAdmin(user);
  }

  @Override
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    Role createdRole = dispatcher.createRole(metalake, role, properties, securableObjects);

    // Set the creator as the owner of role.
    OwnerManager ownerManager = GravitinoEnv.getInstance().ownerManager();
    if (ownerManager != null) {
      ownerManager.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              AuthorizationUtils.ofRole(metalake, role), Entity.EntityType.ROLE),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return createdRole;
  }

  @Override
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return dispatcher.getRole(metalake, role);
  }

  @Override
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    return dispatcher.deleteRole(metalake, role);
  }
}
