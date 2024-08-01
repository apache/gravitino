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

public class RolePO {
  private Long roleId;
  private String roleName;
  private Long metalakeId;
  private String properties;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getRoleId() {
    return roleId;
  }

  public String getRoleName() {
    return roleName;
  }

  public Long getMetalakeId() {
    return metalakeId;
  }

  public String getProperties() {
    return properties;
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
    if (!(o instanceof RolePO)) {
      return false;
    }
    RolePO tablePO = (RolePO) o;
    return Objects.equal(getRoleId(), tablePO.getRoleId())
        && Objects.equal(getRoleName(), tablePO.getRoleName())
        && Objects.equal(getMetalakeId(), tablePO.getMetalakeId())
        && Objects.equal(getProperties(), tablePO.getProperties())
        && Objects.equal(getAuditInfo(), tablePO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), tablePO.getCurrentVersion())
        && Objects.equal(getLastVersion(), tablePO.getLastVersion())
        && Objects.equal(getDeletedAt(), tablePO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getRoleId(),
        getRoleName(),
        getMetalakeId(),
        getProperties(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final RolePO rolePO;

    private Builder() {
      rolePO = new RolePO();
    }

    public Builder withRoleId(Long roleId) {
      rolePO.roleId = roleId;
      return this;
    }

    public Builder withRoleName(String roleName) {
      rolePO.roleName = roleName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      rolePO.metalakeId = metalakeId;
      return this;
    }

    public Builder withProperties(String properties) {
      rolePO.properties = properties;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      rolePO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      rolePO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      rolePO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      rolePO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(rolePO.roleId != null, "Role id is required");
      Preconditions.checkArgument(rolePO.roleName != null, "Role name is required");
      Preconditions.checkArgument(rolePO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(rolePO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(rolePO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(rolePO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(rolePO.deletedAt != null, "Deleted at is required");
    }

    public RolePO build() {
      validate();
      return rolePO;
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
