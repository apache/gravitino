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
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.MetadataObjectUtil;

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
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
        LockType.WRITE,
        () -> userGroupManager.addUser(metalake, user));
  }

  @Override
<<<<<<< HEAD
=======
  public List<BulkItemResult<User>> addUsers(String metalake, List<UserAdd> users)
      throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          List<BulkItemResult<User>> results = Lists.newArrayListWithCapacity(users.size());
          for (int index = 0; index < users.size(); index++) {
            UserAdd user = users.get(index);
            try {
              User addedUser = userGroupManager.addUser(metalake, user.name());
              results.add(BulkItemResult.success(index, user.name(), addedUser));
            } catch (Exception e) {
              results.add(BulkItemResult.failure(index, user.name(), e));
            }
          }
          return results;
        });
  }

  @Override
>>>>>>> 0dcc2ec16 ([#12841] refactor(core): Remove external_id and enabled from user and group metadata (#12842))
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
        LockType.WRITE,
        () -> userGroupManager.removeUser(metalake, user));
  }

  @Override
<<<<<<< HEAD
=======
  public List<BulkItemResult<String>> removeUsers(
      String metalake, List<String> users, Optional<Owner> metalakeOwner)
      throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          List<BulkItemResult<String>> results = Lists.newArrayListWithCapacity(users.size());
          for (int index = 0; index < users.size(); index++) {
            String user = users.get(index);
            try {
              ensureNotMetalakeOwner(metalakeOwner, metalake, user);
              boolean removed = userGroupManager.removeUser(metalake, user);
              if (!removed) {
                results.add(
                    BulkItemResult.failure(
                        index, user, new NoSuchUserException("User does not exist: %s", user)));
                continue;
              }
              results.add(BulkItemResult.success(index, user));
            } catch (Exception e) {
              results.add(BulkItemResult.failure(index, user, e));
            }
          }
          return results;
        });
  }

  @Override
>>>>>>> 0dcc2ec16 ([#12841] refactor(core): Remove external_id and enabled from user and group metadata (#12842))
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofUser(metalake, user),
        LockType.READ,
        () -> userGroupManager.getUser(metalake, user));
  }

  @Override
  public String[] listUserNames(String metalake) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
        LockType.READ,
        () -> userGroupManager.listUserNames(metalake));
  }

  @Override
  public User[] listUsers(String metalake) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofUserNamespace(metalake).levels()),
        LockType.READ,
        () -> userGroupManager.listUsers(metalake));
  }

  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
        LockType.WRITE,
        () -> userGroupManager.addGroup(metalake, group));
  }

  @Override
<<<<<<< HEAD
=======
  public List<BulkItemResult<Group>> addGroups(String metalake, List<GroupAdd> groups)
      throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          List<BulkItemResult<Group>> results = Lists.newArrayListWithCapacity(groups.size());
          for (int index = 0; index < groups.size(); index++) {
            GroupAdd group = groups.get(index);
            try {
              Group addedGroup = userGroupManager.addGroup(metalake, group.name());
              results.add(BulkItemResult.success(index, group.name(), addedGroup));
            } catch (Exception e) {
              results.add(BulkItemResult.failure(index, group.name(), e));
            }
          }
          return results;
        });
  }

  @Override
>>>>>>> 0dcc2ec16 ([#12841] refactor(core): Remove external_id and enabled from user and group metadata (#12842))
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
        LockType.WRITE,
        () -> userGroupManager.removeGroup(metalake, group));
  }

  @Override
<<<<<<< HEAD
=======
  public List<BulkItemResult<String>> removeGroups(
      String metalake, List<String> groups, Optional<Owner> metalakeOwner)
      throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          List<BulkItemResult<String>> results = Lists.newArrayListWithCapacity(groups.size());
          for (int index = 0; index < groups.size(); index++) {
            String group = groups.get(index);
            try {
              ensureNotMetalakeOwnerGroup(metalakeOwner, metalake, group);
              boolean removed = userGroupManager.removeGroup(metalake, group);
              if (!removed) {
                results.add(
                    BulkItemResult.failure(
                        index, group, new NoSuchGroupException("Group does not exist: %s", group)));
                continue;
              }
              results.add(BulkItemResult.success(index, group));
            } catch (Exception e) {
              results.add(BulkItemResult.failure(index, group, e));
            }
          }
          return results;
        });
  }

  @Override
>>>>>>> 0dcc2ec16 ([#12841] refactor(core): Remove external_id and enabled from user and group metadata (#12842))
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofGroup(metalake, group),
        LockType.READ,
        () -> userGroupManager.getGroup(metalake, group));
  }

  @Override
  public Group[] listGroups(String metalake) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
        LockType.READ,
        () -> userGroupManager.listGroups(metalake));
  }

  @Override
  public String[] listGroupNames(String metalake) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
        LockType.READ,
        () -> userGroupManager.listGroupNames(metalake));
  }

  @Override
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofUser(metalake, user),
        LockType.WRITE,
        () -> permissionManager.grantRolesToUser(metalake, roles, user));
  }

  @Override
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofGroup(metalake, group),
        LockType.WRITE,
        () -> permissionManager.grantRolesToGroup(metalake, roles, group));
  }

  @Override
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, IllegalRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofGroup(metalake, group),
        LockType.WRITE,
        () -> permissionManager.revokeRolesFromGroup(metalake, roles, group));
  }

  @Override
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, IllegalRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofUser(metalake, user),
        LockType.WRITE,
        () -> permissionManager.revokeRolesFromUser(metalake, roles, user));
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
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofRoleNamespace(metalake).levels()),
        LockType.WRITE,
        () -> roleManager.createRole(metalake, role, properties, securableObjects));
  }

  @Override
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofRole(metalake, role),
        LockType.READ,
        () -> roleManager.getRole(metalake, role));
  }

  @Override
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofRoleNamespace(metalake).levels()),
        LockType.WRITE,
        () -> roleManager.deleteRole(metalake, role));
  }

  @Override
  public String[] listRoleNames(String metalake) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(AuthorizationUtils.ofRoleNamespace(metalake).levels()),
        LockType.READ,
        () -> roleManager.listRoleNames(metalake));
  }

  @Override
  public String[] listRoleNamesByObject(String metalake, MetadataObject object)
      throws NoSuchMetalakeException, NoSuchMetadataObjectException {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
    return TreeLockUtils.doWithTreeLock(
        identifier, LockType.READ, () -> roleManager.listRoleNamesByObject(metalake, object));
  }

  @Override
  public Role grantPrivilegeToRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofRole(metalake, role),
        LockType.WRITE,
        () -> permissionManager.grantPrivilegesToRole(metalake, role, object, privileges));
  }

  @Override
  public Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofRole(metalake, role),
        LockType.WRITE,
        () -> permissionManager.revokePrivilegesFromRole(metalake, role, object, privileges));
  }

  @Override
  public Role overridePrivilegesInRole(
      String metalake, String role, List<SecurableObject> securableObjectsToOverride)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        AuthorizationUtils.ofRole(metalake, role),
        LockType.WRITE,
        () ->
            permissionManager.overridePrivilegesInRole(metalake, role, securableObjectsToOverride));
  }
}
