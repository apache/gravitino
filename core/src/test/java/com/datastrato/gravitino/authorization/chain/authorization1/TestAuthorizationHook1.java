/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.chain.authorization1;

import com.datastrato.gravitino.authorization.AuthorizationHook;
import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.User;
import java.util.List;

public class TestAuthorizationHook1 implements AuthorizationHook {
  public boolean callOnCreateRole1 = false;

  @Override
  public Boolean onCreateRole(Role role, List<SecurableObject> securableObjects)
      throws RuntimeException {
    callOnCreateRole1 = true;
    return null;
  }

  @Override
  public Role onGetRole(String role) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onDeleteRole(Role role) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onUpdateRole(Role role, RoleChange... changes) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onAddUser(User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRemoveUser(String user) throws RuntimeException {
    return null;
  }

  @Override
  public User onGetUser(String user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onAddGroup(String group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRemoveGroup(String group) throws RuntimeException {
    return null;
  }

  @Override
  public Group onGetGroup(String group) {
    return null;
  }

  @Override
  public Boolean onGrantRolesToUser(List<Role> roles, User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRevokeRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGrantRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRevokeRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    return null;
  }
}
