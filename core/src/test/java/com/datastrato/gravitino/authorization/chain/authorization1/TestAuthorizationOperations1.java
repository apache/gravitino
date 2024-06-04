/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization1;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;

public class TestAuthorizationOperations1 implements AuthorizationOperations {
  public String roleName1;
  public String user1;
  public String group1;
  public boolean updateRole1 = false;

  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
    roleName1 = name;
    return null;
  }

  @Override
  public Boolean dropRole(Role role) throws UnsupportedOperationException {
    roleName1 = null;
    return false;
  }

  @Override
  public Boolean toUser(String userName) throws UnsupportedOperationException {
    user1 = userName;
    return false;
  }

  @Override
  public Boolean toGroup(String groupName) throws UnsupportedOperationException {
    group1 = groupName;
    return false;
  }

  @Override
  public Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException {
    updateRole1 = true;
    return null;
  }
}
