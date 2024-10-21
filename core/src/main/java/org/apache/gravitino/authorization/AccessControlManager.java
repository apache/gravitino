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
package org.apache.gravitino.authorization;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.storage.IdGenerator;

/**
 * AccessControlManager is used for manage users, roles, grant information, this class is an
 * entrance class for tenant management. The operations will be protected by one lock.
 */
public class AccessControlManager implements AccessControlDispatcher {

  private final UserGroupManager userGroupManager;
  private final RoleManager roleManager;
  private final PermissionManager permissionManager;
  private final List<String> serviceAdmins;

  public AccessControlManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.roleManager = new RoleManager(store, idGenerator);
    this.userGroupManager = new UserGroupManager(store, idGenerator);
    this.permissionManager = new PermissionManager(store, roleManager);
    this.serviceAdmins = config.get(Configs.SERVICE_ADMINS);
  }

  @Override
  public User addUser(String metalake, String user)
      throws UserAlreadyExistsException, NoSuchMetalakeException {
    return userGroupManager.addUser(metalake, user);
  }

  @Override
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    return userGroupManager.removeUser(metalake, user);
  }

  @Override
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
    return userGroupManager.getUser(metalake, user);
  }

  @Override
  public String[] listUserNames(String metalake) throws NoSuchMetalakeException {
    return userGroupManager.listUserNames(metalake);
  }

  @Override
  public User[] listUsers(String metalake) throws NoSuchMetalakeException {
    return userGroupManager.listUsers(metalake);
  }

  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    return userGroupManager.addGroup(metalake, group);
  }

  @Override
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    return userGroupManager.removeGroup(metalake, group);
  }

  @Override
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
    return userGroupManager.getGroup(metalake, group);
  }

  @Override
  public Group[] listGroups(String metalake) throws NoSuchMetalakeException {
    return userGroupManager.listGroups(metalake);
  }

  @Override
  public String[] listGroupNames(String metalake) throws NoSuchMetalakeException {
    return userGroupManager.listGroupNames(metalake);
  }

  @Override
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    return permissionManager.grantRolesToUser(metalake, roles, user);
  }

  @Override
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    return permissionManager.grantRolesToGroup(metalake, roles, group);
  }

  @Override
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    return permissionManager.revokeRolesFromGroup(metalake, roles, group);
  }

  @Override
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    return permissionManager.revokeRolesFromUser(metalake, roles, user);
  }

  @Override
  public boolean isServiceAdmin(String user) {
    return serviceAdmins.contains(user);
  }

  @Override
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    return roleManager.createRole(metalake, role, properties, securableObjects);
  }

  @Override
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return roleManager.getRole(metalake, role);
  }

  @Override
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    return roleManager.deleteRole(metalake, role);
  }

  @Override
  public String[] listRoleNames(String metalake) throws NoSuchMetalakeException {
    return roleManager.listRoleNames(metalake);
  }

  @Override
  public String[] listRoleNamesByObject(String metalake, MetadataObject object)
      throws NoSuchMetalakeException, NoSuchMetadataObjectException {
    return roleManager.listRoleNamesByObject(metalake, object);
  }

  @Override
  public Role grantPrivilegeToRole(
      String metalake, String role, MetadataObject object, List<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return permissionManager.grantPrivilegesToRole(metalake, role, object, privileges);
  }

  @Override
  public Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, List<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return permissionManager.revokePrivilegesFromRole(metalake, role, object, privileges);
  }
}
