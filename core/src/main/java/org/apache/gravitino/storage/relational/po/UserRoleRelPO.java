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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class UserRoleRelPO {
  private Long userId;
  private Long roleId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getUserId() {
    return userId;
  }

  public Long getRoleId() {
    return roleId;
  }

  public String getAuditInfo() {
    return auditInfo;
  }

  public Long getCurrentVersion() {
    return currentVersion;
  }

  public Long getLastVersion() {
    return lastVersion;
  }

  public Long getDeletedAt() {
    return deletedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserRoleRelPO)) {
      return false;
    }
    UserRoleRelPO userRoleRelPO = (UserRoleRelPO) o;
    return Objects.equal(getUserId(), userRoleRelPO.getUserId())
        && Objects.equal(getRoleId(), userRoleRelPO.getRoleId())
        && Objects.equal(getAuditInfo(), userRoleRelPO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), userRoleRelPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), userRoleRelPO.getLastVersion())
        && Objects.equal(getDeletedAt(), userRoleRelPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getUserId(),
        getRoleId(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final UserRoleRelPO userRoleRelPO;

    private Builder() {
      userRoleRelPO = new UserRoleRelPO();
    }

    public Builder withUserId(Long userId) {
      userRoleRelPO.userId = userId;
      return this;
    }

    public Builder withRoleId(Long roleId) {
      userRoleRelPO.roleId = roleId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      userRoleRelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      userRoleRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      userRoleRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      userRoleRelPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(userRoleRelPO.userId != null, "User id is required");
      Preconditions.checkArgument(userRoleRelPO.roleId != null, "Role id is required");
      Preconditions.checkArgument(userRoleRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(
          userRoleRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(userRoleRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(userRoleRelPO.deletedAt != null, "Deleted at is required");
    }

    public UserRoleRelPO build() {
      validate();
      return userRoleRelPO;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
