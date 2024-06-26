/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.List;
import java.util.function.Function;

/** authorization operations interfaces. */
public interface AuthorizationOperations {
  /**
   * Update a role.
   *
   * @param role The entity of the Role.
   * @param changes role changes apply to the role.
   * @return True if the update operation success; False otherwise.
   * @throws RuntimeException If update role encounters storage issues.
   */
  Boolean updateRole(Role role, RoleChange... changes) throws RuntimeException;

  /**
   * Drop roles.
   *
   * @param roles The entities of the Role.
   * @return True if the delete operation success; False otherwise.
   * @throws RuntimeException If delete role encounters storage issues.
   */
  Boolean deleteRoles(List<Role> roles) throws RuntimeException;

  /**
   * Grant roles to a user.
   *
   * @param roles The entities of the Role.
   * @param user The name of the User.
   * @return True if the roles are grant to a user successfully, False otherwise.
   * @throws RuntimeException If granting roles to a user encounters storage issues.
   */
  Boolean grantRolesToUser(List<Role> roles, String user) throws RuntimeException;

  /**
   * Grant roles to a group.
   *
   * @param roles The entities of the Role.
   * @param group The name of the Group.
   * @return True if the roles are grant to a group successfully, False otherwise.
   * @throws RuntimeException If granting roles to a group encounters storage issues.
   */
  Boolean grantRolesToGroup(List<Role> roles, String group) throws RuntimeException;

  /**
   * Revoke roles from a user.
   *
   * @param roles The entities of the Role.
   * @param user The name of the User.
   * @return True if the roles are revoke from user successfully, False otherwise.
   * @throws RuntimeException If revoking roles from a user encounters storage issues.
   */
  Boolean revokeRolesFromUser(List<Role> roles, String user) throws RuntimeException;

  /**
   * Revoke roles from a group.
   *
   * @param roles The entities of the Role.
   * @param group The name of the Group.
   * @return True if the roles are revoke from group successfully, False otherwise.
   * @throws RuntimeException If revoking roles from a group encounters storage issues.
   */
  Boolean revokeRolesFromGroup(List<Role> roles, String group) throws RuntimeException;

  /**
   * Chained call functions support complex authorization combinations.
   *
   * @param functions The authorization operations functions.
   * @return true if the all functions call successfully, false otherwise.
   */
  default <R> boolean runAuthorizationChain(List<Function<AuthorizationOperations, R>> functions) {
    try {
      for (Function<AuthorizationOperations, R> function : functions) {
        R r = function.apply(this);
        if (r instanceof Boolean && !((Boolean) r)) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
