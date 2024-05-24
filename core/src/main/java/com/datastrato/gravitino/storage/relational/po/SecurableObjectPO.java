/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.po;

import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SecurableObjectPO {

  private Long roleId;
  private String fullName;
  private  MetadataObject.Type type;
  private String privilegeNames;
  private String privilegeConditions;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getRoleId() {
    return roleId;
  }

  public String getFullName() {
    return fullName;
  }

  public MetadataObject.Type getType() {
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
        && Objects.equal(getFullName(), securableObjectPO.getFullName())
        && Objects.equal(getType(), securableObjectPO.getType())
        && Objects.equal(getPrivilegeConditions(), securableObjectPO.privilegeConditions)
        && Objects.equal(getPrivilegeNames(), securableObjectPO.getPrivilegeNames())
        && Objects.equal(getCurrentVersion(), securableObjectPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), securableObjectPO.getLastVersion())
        && Objects.equal(getDeletedAt(), securableObjectPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getRoleId(),
        getFullName(),
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

    public Builder withFullName(String fullName) {
      securableObjectPO.fullName = fullName;
      return this;
    }

    public Builder withType(SecurableObject.Type type) {
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
      Preconditions.checkArgument(securableObjectPO.fullName != null, "Full name is required");
      Preconditions.checkArgument(securableObjectPO.type != null, "Type is required");
      Preconditions.checkArgument(
          securableObjectPO.privilegeNames != null, "Privilege names are required");
      Preconditions.checkArgument(
          securableObjectPO.privilegeConditions != null, "Priviege conditions are required");
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
