/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class GroupRoleRelPO {
  private Long groupId;
  private Long roleId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getGroupId() {
    return groupId;
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
    if (!(o instanceof GroupRoleRelPO)) {
      return false;
    }
    GroupRoleRelPO groupRoleRelPO = (GroupRoleRelPO) o;
    return Objects.equal(getGroupId(), groupRoleRelPO.getGroupId())
        && Objects.equal(getRoleId(), groupRoleRelPO.getRoleId())
        && Objects.equal(getAuditInfo(), groupRoleRelPO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), groupRoleRelPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), groupRoleRelPO.getLastVersion())
        && Objects.equal(getDeletedAt(), groupRoleRelPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getGroupId(),
        getRoleId(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final GroupRoleRelPO groupRoleRelPO;

    private Builder() {
      groupRoleRelPO = new GroupRoleRelPO();
    }

    public Builder withGroupId(Long groupId) {
      groupRoleRelPO.groupId = groupId;
      return this;
    }

    public Builder withRoleId(Long roleId) {
      groupRoleRelPO.roleId = roleId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      groupRoleRelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      groupRoleRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      groupRoleRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      groupRoleRelPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(groupRoleRelPO.groupId != null, "Group id is required");
      Preconditions.checkArgument(groupRoleRelPO.roleId != null, "Role id is required");
      Preconditions.checkArgument(groupRoleRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(
          groupRoleRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(groupRoleRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(groupRoleRelPO.deletedAt != null, "Deleted at is required");
    }

    public GroupRoleRelPO build() {
      validate();
      return groupRoleRelPO;
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
