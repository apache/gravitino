/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.List;

/**
 * Interface for authorization User and Group hooks operation of the underlying access control
 * system.
 */
public interface AuthorizationUserHook {
  /**
   * Add a new User to underlying access control system.
   *
   * @param user The user entity.
   * @return True if the add User was successfully added, false if the add User was failed.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  Boolean onAddUser(User user) throws RuntimeException;

  /**
   * Removes a User from underlying access control system.
   *
   * @param user The name of the User.
   * @return True if the User was successfully removed, false if the remove User was failed.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  Boolean onRemoveUser(String user) throws RuntimeException;

  /**
   * Gets a User from underlying access control system. Because User information already storage in
   * the Gravition, so we don't need to get the User from the underlying access control system. We
   * only need to check if the User exists in the underlying access control system.
   *
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  User onGetUser(String user) throws RuntimeException;

  /**
   * Adds a new Group to underlying access control system.
   *
   * @param group The name of the Group.
   * @return True if the add Group was successfully added, false if the add Group was failed.
   * @throws RuntimeException If adding the Group encounters storage issues.
   */
  Boolean onAddGroup(String group) throws RuntimeException;

  /**
   * Removes a Group from underlying access control system.
   *
   * @param group THe name of the Group.
   * @return True if the remove Group was successfully removed, false if the remove Group was
   *     failed.
   * @throws RuntimeException If removing the Group encounters storage issues.
   */
  Boolean onRemoveGroup(String group) throws RuntimeException;

  /**
   * Gets a Group from underlying access control system. Because Group information already storage
   * in the Gravition, so we don't need to get the Group from the underlying access control system.
   * We only need to check if the Group exists in the underlying access control system.
   *
   * @param group The name of the Group.
   * @return The getting Group instance.
   * @throws RuntimeException If getting the Group encounters storage issues.
   */
  Group onGetGroup(String group);

  /**
   * Grant roles to a user into underlying access control system.
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the Grant was successfully, false if the Grant was failed.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  Boolean onGrantRolesToUser(List<Role> roles, User user) throws RuntimeException;

  /**
   * Revoke roles from a user into underlying access control system.
   *
   * @param user The entity of the User.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, false if the revoke was failed.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  Boolean onRevokeRolesFromUser(List<Role> roles, User user) throws RuntimeException;

  /**
   * Grant roles to a group into underlying access control system.
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke was failed.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  Boolean onGrantRolesToGroup(List<Role> roles, Group group) throws RuntimeException;

  /**
   * Revoke roles from a group from underlying access control system.
   *
   * @param group The entity of the Group.
   * @param roles The entities of the Roles.
   * @return True if the revoke was successfully removed, False if the revoke was failed.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  Boolean onRevokeRolesFromGroup(List<Role> roles, Group group) throws RuntimeException;
}
