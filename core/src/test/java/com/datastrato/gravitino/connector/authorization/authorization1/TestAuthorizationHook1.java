/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.connector.authorization.authorization1;

import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.RoleChange;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.connector.authorization.AuthorizationHook;
import java.io.IOException;
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
  public Boolean onCheckRole(String role) throws RuntimeException {
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
  public Boolean onCheckUser(String user) throws RuntimeException {
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
  public Boolean onCheckGroup(String group) {
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

  @Override
  public void close() throws IOException {}
}
