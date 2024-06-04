/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization2;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;

public class TestAuthorizationOperations2 implements AuthorizationOperations {
  public String roleName2;
  public String user2;
  public String group2;
  public boolean updateRole2 = false;

  @Override
  public Role createRole(String name) throws UnsupportedOperationException {
    roleName2 = name;
    return null;
  }

  @Override
  public Boolean dropRole(Role role) throws UnsupportedOperationException {
    roleName2 = null;
    return false;
  }

  @Override
  public Boolean toUser(String userName) throws UnsupportedOperationException {
    user2 = userName;
    return false;
  }

  @Override
  public Boolean toGroup(String groupName) throws UnsupportedOperationException {
    group2 = groupName;
    return false;
  }

  @Override
  public Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException {
    updateRole2 = true;
    return null;
  }
}
