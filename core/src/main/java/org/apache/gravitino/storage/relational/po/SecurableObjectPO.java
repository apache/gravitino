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

public class SecurableObjectPO {

  private Long roleId;
  private Long entityId;
  private String type;
  private String privilegeNames;
  private String privilegeConditions;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getRoleId() {
    return roleId;
  }

  public Long getEntityId() {
    return entityId;
  }

  public String getType() {
    return type;
  }

  public String getPrivilegeNames() {
    return privilegeNames;
  }

  public String getPrivilegeConditions() {
    return privilegeConditions;
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
    if (!(o instanceof SecurableObjectPO)) {
      return false;
    }
    SecurableObjectPO securableObjectPO = (SecurableObjectPO) o;
    return Objects.equal(getRoleId(), securableObjectPO.getRoleId())
        && Objects.equal(getEntityId(), securableObjectPO.getEntityId())
        && Objects.equal(getType(), securableObjectPO.getType())
        && Objects.equal(getPrivilegeConditions(), securableObjectPO.getPrivilegeConditions())
        && Objects.equal(getPrivilegeNames(), securableObjectPO.getPrivilegeNames())
        && Objects.equal(getCurrentVersion(), securableObjectPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), securableObjectPO.getLastVersion())
        && Objects.equal(getDeletedAt(), securableObjectPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getRoleId(),
        getEntityId(),
        getType(),
        getPrivilegeNames(),
        getPrivilegeConditions(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final SecurableObjectPO securableObjectPO;

    private Builder() {
      securableObjectPO = new SecurableObjectPO();
    }

    public Builder withRoleId(Long roleId) {
      securableObjectPO.roleId = roleId;
      return this;
    }

    public Builder withEntityId(long entityId) {
      securableObjectPO.entityId = entityId;
      return this;
    }

    public Builder withType(String type) {
      securableObjectPO.type = type;
      return this;
    }

    public Builder withPrivilegeNames(String privilegeNames) {
      securableObjectPO.privilegeNames = privilegeNames;
      return this;
    }

    public Builder withPrivilegeConditions(String privilegeConditions) {
      securableObjectPO.privilegeConditions = privilegeConditions;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      securableObjectPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      securableObjectPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      securableObjectPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(securableObjectPO.roleId != null, "Role id is required");
      Preconditions.checkArgument(securableObjectPO.type != null, "Type is required");
      Preconditions.checkArgument(
          securableObjectPO.privilegeNames != null, "Privilege names are required");
      Preconditions.checkArgument(
          securableObjectPO.privilegeConditions != null, "Privilege conditions are required");
      Preconditions.checkArgument(
          securableObjectPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(
          securableObjectPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(securableObjectPO.deletedAt != null, "Deleted at is required");
    }

    public SecurableObjectPO build() {
      validate();
      return securableObjectPO;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
