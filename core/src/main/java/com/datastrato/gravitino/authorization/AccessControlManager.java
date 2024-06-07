/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.Executable;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AccessControlManager is used for manage users, roles, admin, grant information, this class is an
 * entrance class for tenant management. This lock policy about this is as follows: First, admin
 * operations are prevented by one lock. Then, other operations are prevented by the other lock. For
 * non-admin operations, Gravitino doesn't choose metalake level lock. There are some reasons
 * mainly: First, the metalake can be renamed by users. It's hard to maintain a map with metalake as
 * the key. Second, the lock will be couped with life cycle of the metalake.
 */
public class AccessControlManager {

  private final UserGroupManager userGroupManager;
  private final AdminManager adminManager;
  private final RoleManager roleManager;
  private final PermissionManager permissionManager;
  private final Object adminOperationLock = new Object();
  private final Object nonAdminOperationLock = new Object();

  public AccessControlManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.roleManager = new RoleManager(store, idGenerator, config);
    this.adminManager = new AdminManager(store, idGenerator, config, roleManager);
    this.userGroupManager = new UserGroupManager(store, idGenerator, roleManager);
    this.permissionManager = new PermissionManager(store, roleManager);
  }

  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addUser(String metalake, String user)
      throws UserAlreadyExistsException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> userGroupManager.addUser(metalake, user));
  }

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return True if the User was successfully removed, false only when there's no such user,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String metalake, String user) throws NoSuchMetalakeException {
    return doWithNonAdminLock(() -> userGroupManager.removeUser(metalake, user));
  }

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  public User getUser(String metalake, String user)
      throws NoSuchUserException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> userGroupManager.getUser(metalake, user));
  }

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  public Group addGroup(String metalake, String group)
      throws GroupAlreadyExistsException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> userGroupManager.addGroup(metalake, group));
  }

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return True if the Group was successfully removed, false only when there's no such group,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  public boolean removeGroup(String metalake, String group) throws NoSuchMetalakeException {
    return doWithNonAdminLock(() -> userGroupManager.removeGroup(metalake, group));
  }

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  public Group getGroup(String metalake, String group)
      throws NoSuchGroupException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> userGroupManager.getGroup(metalake, group));
  }

  /**
   * Grant roles to a user.
   *
   * @param metalake The metalake of the User.
   * @param user The name of the User.
   * @param roles The names of the Role.
   * @return The User after granted.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  public User grantRolesToUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> permissionManager.grantRolesToUser(metalake, roles, user));
  }

  /**
   * Grant roles to a group.
   *
   * @param metalake The metalake of the Group.
   * @param group The name of the Group.
   * @param roles The names of the Role.
   * @return The Group after granted.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  public Group grantRolesToGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> permissionManager.grantRolesToGroup(metalake, roles, group));
  }

  /**
   * Revoke roles from a group.
   *
   * @param metalake The metalake of the Group.
   * @param group The name of the Group.
   * @param roles The name of the Role.
   * @return The Group after revoked.
   * @throws NoSuchGroupException If the Group with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  public Group revokeRolesFromGroup(String metalake, List<String> roles, String group)
      throws NoSuchGroupException, NoSuchRoleException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> permissionManager.revokeRolesFromGroup(metalake, roles, group));
  }

  /**
   * Revoke roles from a user.
   *
   * @param metalake The metalake of the User.
   * @param user The name of the User.
   * @param roles The name of the Role.
   * @return The User after revoked.
   * @throws NoSuchUserException If the User with the given name does not exist.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  public User revokeRolesFromUser(String metalake, List<String> roles, String user)
      throws NoSuchUserException, NoSuchRoleException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> permissionManager.revokeRolesFromUser(metalake, roles, user));
  }

  /**
   * Adds a new metalake admin.
   *
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a metalake admin with the same name already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addMetalakeAdmin(String user) throws UserAlreadyExistsException {
    return doWithAdminLock(() -> adminManager.addMetalakeAdmin(user));
  }

  /**
   * Removes a metalake admin. Only service admins can manage metalake admins.
   *
   * @param user The name of the User.
   * @return True if the User was successfully removed, false only when there's no such metalake
   *     admin, otherwise it will throw an exception.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeMetalakeAdmin(String user) {
    return doWithAdminLock(() -> adminManager.removeMetalakeAdmin(user));
  }

  /**
   * Judges whether the user is the service admin.
   *
   * @param user the name of the user
   * @return True if the user is service admin, otherwise false.
   */
  public boolean isServiceAdmin(String user) {
    return adminManager.isServiceAdmin(user);
  }

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param securableObjects The securable objects of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same name already exists.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      List<SecurableObject> securableObjects)
      throws RoleAlreadyExistsException, NoSuchMetalakeException {
    return doWithNonAdminLock(
        () -> roleManager.createRole(metalake, role, properties, securableObjects));
  }

  /**
   * Gets a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return The getting Role instance.
   * @throws NoSuchRoleException If the Role with the given name does not exist.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If getting the Role encounters storage issues.
   */
  public Role getRole(String metalake, String role)
      throws NoSuchRoleException, NoSuchMetalakeException {
    return doWithNonAdminLock(() -> roleManager.getRole(metalake, role));
  }

  /**
   * Deletes a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return True if the Role was successfully deleted, false only when there's no such role,
   *     otherwise it will throw an exception.
   * @throws NoSuchMetalakeException If the Metalake with the given name does not exist.
   * @throws RuntimeException If deleting the Role encounters storage issues.
   */
  public boolean deleteRole(String metalake, String role) throws NoSuchMetalakeException {
    return doWithNonAdminLock(() -> roleManager.deleteRole(metalake, role));
  }

  public List<RoleEntity> getRolesByUserFromMetalake(String metalake, String currentUser) {
    return doWithNonAdminLock(
        () -> {
          User user = getUser(metalake, currentUser);
          // TODO: get roles from the group
          return user.roles().stream()
              .map(role -> roleManager.getRole(metalake, role))
              .collect(Collectors.toList());
        });
  }

  @VisibleForTesting
  RoleManager getRoleManager() {
    return roleManager;
  }

  private <R, E extends Exception> R doWithNonAdminLock(Executable<R, E> executable) throws E {
    synchronized (nonAdminOperationLock) {
      return executable.execute();
    }
  }

  private <R, E extends Exception> R doWithAdminLock(Executable<R, E> executable) throws E {
    synchronized (adminOperationLock) {
      return executable.execute();
    }
  }
}
