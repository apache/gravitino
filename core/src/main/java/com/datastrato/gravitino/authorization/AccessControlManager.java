/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.Executable;
import java.util.List;
import java.util.Map;

/**
 * AccessControlManager is used for manage users, roles, admin, grant information, this class is an
 * entrance class for tenant management. This lock policy about this is as follows: First, admin
 * operations are prevented by one lock. Then, other operations are prevented by the other lock. For
 * non-admin operations, Gravitino doesn't choose metalake level lock. There are some reasons
 * mainly: First, the metalake can be renamed by users. It's hard to maintain a map with metalake as
 * the key. Second, the lock will be couped with life cycle of the metalake.
 */
public class AccessControlManager implements SupportsUserOperation, SupportsGroupOperation, SupportsAdminManagement, SupportsRoleManagement {

  private final UserGroupManager userGroupManager;
  private final AdminManager adminManager;
  private final RoleManager roleManager;
  private final Object adminOperationLock = new Object();
  private final Object nonAdminOperationLock = new Object();

  public AccessControlManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.userGroupManager = new UserGroupManager(store, idGenerator);
    this.adminManager = new AdminManager(store, idGenerator, config);
    this.roleManager = new RoleManager(store, idGenerator);
  }

  public User addUser(String metalake, String user) throws UserAlreadyExistsException {
    return doWithNonAdminLock(() -> userGroupManager.addUser(metalake, user));
  }

  public boolean removeUser(String metalake, String user) {
    return doWithNonAdminLock(() -> userGroupManager.removeUser(metalake, user));
  }

  public User getUser(String metalake, String user) throws NoSuchUserException {
    return doWithNonAdminLock(() -> userGroupManager.getUser(metalake, user));
  }

  public Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    return doWithNonAdminLock(() -> userGroupManager.addGroup(metalake, group));
  }

  public boolean removeGroup(String metalake, String group) {
    return doWithNonAdminLock(() -> userGroupManager.removeGroup(metalake, group));
  }

  public Group getGroup(String metalake, String group) throws NoSuchGroupException {
    return doWithNonAdminLock(() -> userGroupManager.getGroup(metalake, group));
  }

  public User addMetalakeAdmin(String user) {
    return doWithAdminLock(() -> adminManager.addMetalakeAdmin(user));
  }


  public boolean removeMetalakeAdmin(String user) {
    return doWithAdminLock(() -> adminManager.removeMetalakeAdmin(user));
  }

  /**
   * Judges whether the user is the service admin.
   *
   * @param user the name of the user
   * @return true, if the user is service admin, otherwise false.
   */
  public boolean isServiceAdmin(String user) {
    return adminManager.isServiceAdmin(user);
  }

  /**
   * Judges whether the user is the metalake admin.
   *
   * @param user the name of the user
   * @return true, if the user is metalake admin, otherwise false.
   */
  public boolean isMetalakeAdmin(String user) {
    return doWithAdminLock(() -> adminManager.isMetalakeAdmin(user));
  }

  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      NameIdentifier privilegeEntityIdentifier,
      Entity.EntityType privilegeEntityType,
      List<Privilege> privileges)
      throws RoleAlreadyExistsException {
    return doWithNonAdminLock(
        () ->
            roleManager.createRole(
                metalake,
                role,
                properties,
                privilegeEntityIdentifier,
                privilegeEntityType,
                privileges));
  }

  public Role loadRole(String metalake, String role) throws NoSuchRoleException {
    return doWithNonAdminLock(() -> roleManager.loadRole(metalake, role));
  }

  public boolean dropRole(String metalake, String role) {
    return doWithNonAdminLock(() -> roleManager.dropRole(metalake, role));
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
