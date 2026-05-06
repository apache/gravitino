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

public class IdpGroupPO {
  private Long groupId;
  private String groupName;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getGroupId() {
    return groupId;
  }

  public String getGroupName() {
    return groupName;
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
    if (!(o instanceof IdpGroupPO)) {
      return false;
    }
    IdpGroupPO tablePO = (IdpGroupPO) o;
    return Objects.equal(getGroupId(), tablePO.getGroupId())
        && Objects.equal(getGroupName(), tablePO.getGroupName())
        && Objects.equal(getAuditInfo(), tablePO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), tablePO.getCurrentVersion())
        && Objects.equal(getLastVersion(), tablePO.getLastVersion())
        && Objects.equal(getDeletedAt(), tablePO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getGroupId(),
        getGroupName(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final IdpGroupPO groupPO;

    private Builder() {
      groupPO = new IdpGroupPO();
    }

    public Builder withGroupId(Long groupId) {
      groupPO.groupId = groupId;
      return this;
    }

    public Builder withGroupName(String groupName) {
      groupPO.groupName = groupName;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      groupPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      groupPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      groupPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      groupPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(groupPO.groupId != null, "Group id is required");
      Preconditions.checkArgument(groupPO.groupName != null, "Group name is required");
      Preconditions.checkArgument(groupPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(groupPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(groupPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(groupPO.deletedAt != null, "Deleted at is required");
    }

    public IdpGroupPO build() {
      validate();
      return groupPO;
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
