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
import java.util.List;
import java.util.Map;

/**
 * AccessControlManager is used for manage users, roles, admin, grant information, this class is an
 * entrance class for tenant management.
 */
public class AccessControlManager {

  private final UserGroupManager userGroupManager;
  private final AdminManager adminManager;
  private final RoleManager roleManager;

  public AccessControlManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.userGroupManager = new UserGroupManager(store, idGenerator);
    this.adminManager = new AdminManager(store, idGenerator, config);
    this.roleManager = new RoleManager(store, idGenerator);
  }

  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param name The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addUser(String metalake, String name) throws UserAlreadyExistsException {
    return userGroupManager.addUser(metalake, name);
  }

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String metalake, String user) {
    return userGroupManager.removeUser(metalake, user);
  }

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given identifier does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  public User getUser(String metalake, String user) throws NoSuchUserException {
    return userGroupManager.getUser(metalake, user);
  }

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  public Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    return userGroupManager.addGroup(metalake, group);
  }

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  public boolean removeGroup(String metalake, String group) {
    return userGroupManager.removeGroup(metalake, group);
  }

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  public Group getGroup(String metalake, String group) throws NoSuchGroupException {
    return userGroupManager.getGroup(metalake, group);
  }

  /**
   * Adds a new metalake admin.
   *
   * @param user The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addMetalakeAdmin(String user) {
    return adminManager.addMetalakeAdmin(user);
  }

  /**
   * Removes a metalake admin.
   *
   * @param user The name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeMetalakeAdmin(String user) {
    return adminManager.removeMetalakeAdmin(user);
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
    return adminManager.isMetalakeAdmin(user);
  }

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param privilegeEntityIdentifier The privilege entity identifier of the Role.
   * @param privilegeEntityType The privilege entity type of the Role.
   * @param privileges The privileges of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same identifier already exists.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      NameIdentifier privilegeEntityIdentifier,
      Entity.EntityType privilegeEntityType,
      List<Privilege> privileges)
      throws RoleAlreadyExistsException {
    return roleManager.createRole(
        metalake, role, properties, privilegeEntityIdentifier, privilegeEntityType, privileges);
  }

  /**
   * Loads a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return The loading Role instance.
   * @throws NoSuchRoleException If the Role with the given identifier does not exist.
   * @throws RuntimeException If loading the Role encounters storage issues.
   */
  public Role loadRole(String metalake, String role) throws NoSuchRoleException {
    return roleManager.loadRole(metalake, role);
  }

  /**
   * Drops a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return `true` if the Role was successfully dropped, `false` otherwise.
   * @throws RuntimeException If dropping the User encounters storage issues.
   */
  public boolean dropRole(String metalake, String role) {
    return roleManager.dropRole(metalake, role);
  }
}
