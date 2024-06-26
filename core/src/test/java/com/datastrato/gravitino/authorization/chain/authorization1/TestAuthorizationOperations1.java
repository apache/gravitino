/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization1;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import java.util.List;

public class TestAuthorizationOperations1 implements AuthorizationOperations {
  public boolean updateRole1 = false;
  public boolean deleteRoles1 = false;
  public boolean grantRolesToUser1 = false;
  public boolean grantRolesToGroup1 = false;
  public boolean revokeRolesFromUser1 = false;
  public boolean revokeRolesFromGroup1 = false;

  @Override
  public Boolean updateRole(Role role, RoleChange... changes) throws RuntimeException {
    updateRole1 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean deleteRoles(List<Role> roles) throws RuntimeException {
    deleteRoles1 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean grantRolesToUser(List<Role> roles, String user) throws RuntimeException {
    grantRolesToUser1 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean grantRolesToGroup(List<Role> roles, String group) throws RuntimeException {
    grantRolesToGroup1 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean revokeRolesFromUser(List<Role> roles, String user) throws RuntimeException {
    revokeRolesFromUser1 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean revokeRolesFromGroup(List<Role> roles, String group) throws RuntimeException {
    revokeRolesFromGroup1 = true;
    return Boolean.TRUE;
  }
}
