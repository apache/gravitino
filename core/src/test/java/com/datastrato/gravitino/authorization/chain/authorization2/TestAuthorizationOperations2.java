/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization2;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import java.util.List;

public class TestAuthorizationOperations2 implements AuthorizationOperations {
  public boolean updateRole2 = false;
  public boolean deleteRoles2 = false;
  public boolean grantRolesToUser2 = false;
  public boolean grantRolesToGroup2 = false;
  public boolean revokeRolesFromUser2 = false;
  public boolean revokeRolesFromGroup2 = false;

  @Override
  public Boolean updateRole(Role role, RoleChange... changes) throws RuntimeException {
    updateRole2 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean deleteRoles(List<Role> roles) throws RuntimeException {
    deleteRoles2 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean grantRolesToUser(List<Role> roles, String user) throws RuntimeException {
    grantRolesToUser2 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean grantRolesToGroup(List<Role> roles, String group) throws RuntimeException {
    grantRolesToGroup2 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean revokeRolesFromUser(List<Role> roles, String user) throws RuntimeException {
    revokeRolesFromUser2 = true;
    return Boolean.TRUE;
  }

  @Override
  public Boolean revokeRolesFromGroup(List<Role> roles, String group) throws RuntimeException {
    revokeRolesFromGroup2 = true;
    return Boolean.TRUE;
  }
}
