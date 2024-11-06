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
package org.apache.gravitino.connector.authorization.ranger;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;

public class TestRangerAuthorizationPlugin implements AuthorizationPlugin {
  public boolean callOnCreateRole1 = false;

  @Override
  public Boolean onRoleCreated(Role role) throws RuntimeException {
    callOnCreateRole1 = true;
    return null;
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onUserAdded(User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onUserRemoved(User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onUserAcquired(User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGroupAcquired(Group group) {
    return null;
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    return null;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    return null;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    return null;
  }
}
