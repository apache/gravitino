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
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
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
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code AccessControlHookDispatcher} is a decorator for {@link AccessControlDispatcher} that not
 * only delegates access control operations to the underlying access control dispatcher but also
 * executes some hook operations before or after the underlying operations.
 */
@Slf4j
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
  public User[] listUsers(String metalake) throws NoSuchMetalakeException {
    return dispatcher.listUsers(metalake);
  }

  @Override
  public String[] listUserNames(String metalake) throws NoSuchMetalakeException {
    return dispatcher.listUserNames(metalake);
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
  public Group[] listGroups(String metalake) throws NoSuchMetalakeException {
    return dispatcher.listGroups(metalake);
  }

  @Override
  public String[] listGroupNames(String metalake) throws NoSuchMetalakeException {
    return dispatcher.listGroupNames(metalake);
  }

  @Override
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    User grantedUser = dispatcher.grantRolesToUser(metalake, roles, user);
    notifyRoleUserRelChange(metalake, roles);
    return grantedUser;
  }

  @Override
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    Group grantedGroup = dispatcher.grantRolesToGroup(metalake, roles, group);
    notifyRoleUserRelChange(metalake, roles);
    return grantedGroup;
  }

  @Override
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    Group revokedGroup = dispatcher.revokeRolesFromGroup(metalake, roles, group);
    notifyRoleUserRelChange(metalake, roles);
    return revokedGroup;
  }

  @Override
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    User revokedUser = dispatcher.revokeRolesFromUser(metalake, roles, user);
    notifyRoleUserRelChange(metalake, roles);
    return revokedUser;
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
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(metalake, PrincipalUtils.getCurrentUserName());

    Role createdRole = dispatcher.createRole(metalake, role, properties, securableObjects);

    // Set the creator as the owner of role.
    OwnerDispatcher ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerDispatcher != null) {
      ownerDispatcher.setOwner(
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
    Role oldRole = null;
    try {
      oldRole = getRole(metalake, role);
    } catch (NoSuchRoleException e) {
      log.debug(e.getMessage());
    }
    boolean resultOfDeleteRole = dispatcher.deleteRole(metalake, role);
    if (resultOfDeleteRole && oldRole != null) {
      notifyRoleUserRelChange(((RoleEntity) oldRole).id());
    }
    return resultOfDeleteRole;
  }

  @Override
  public String[] listRoleNames(String metalake) throws NoSuchMetalakeException {
    return dispatcher.listRoleNames(metalake);
  }

  @Override
  public String[] listRoleNamesByObject(String metalake, MetadataObject object)
      throws NoSuchMetalakeException, NoSuchMetadataObjectException {
    return dispatcher.listRoleNamesByObject(metalake, object);
  }

  @Override
  public Role grantPrivilegeToRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchMetalakeException, NoSuchRoleException {
    Role grantedRole = dispatcher.grantPrivilegeToRole(metalake, role, object, privileges);
    notifyRoleUserRelChange(metalake, role);
    return grantedRole;
  }

  @Override
  public Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchMetalakeException, NoSuchRoleException {
    Role revokedRole = dispatcher.revokePrivilegesFromRole(metalake, role, object, privileges);
    notifyRoleUserRelChange(metalake, role);
    return revokedRole;
  }

  private static void notifyRoleUserRelChange(String metalake, List<String> roles) {
    GravitinoAuthorizer gravitinoAuthorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (gravitinoAuthorizer != null) {
      for (String role : roles) {
        gravitinoAuthorizer.handleRolePrivilegeChange(metalake, role);
      }
    }
  }

  private static void notifyRoleUserRelChange(String metalake, String role) {
    GravitinoAuthorizer gravitinoAuthorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (gravitinoAuthorizer != null) {
      gravitinoAuthorizer.handleRolePrivilegeChange(metalake, role);
    }
  }

  private static void notifyRoleUserRelChange(Long role) {
    GravitinoAuthorizer gravitinoAuthorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (gravitinoAuthorizer != null) {
      gravitinoAuthorizer.handleRolePrivilegeChange(role);
    }
  }
}
