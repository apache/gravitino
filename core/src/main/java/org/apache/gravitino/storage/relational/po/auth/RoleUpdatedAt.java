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
package org.apache.gravitino.storage.relational.po.auth;

/** Role identity and privilege-list staleness sentinel. */
public class RoleUpdatedAt {
  private long roleId;
  private String roleName;
  private long updatedAt;

  /** Required by MyBatis for result mapping. */
  public RoleUpdatedAt() {}

  public RoleUpdatedAt(long roleId, long updatedAt) {
    this.roleId = roleId;
    this.updatedAt = updatedAt;
  }

  public RoleUpdatedAt(long roleId, String roleName, long updatedAt) {
    this.roleId = roleId;
    this.roleName = roleName;
    this.updatedAt = updatedAt;
  }

  public long getRoleId() {
    return roleId;
  }

  public void setRoleId(long roleId) {
    this.roleId = roleId;
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }
}
