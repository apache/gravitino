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

public class IdpGroupUserRelPO {
  private Long id;
  private Long groupId;
  private Long userId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getId() {
    return id;
  }

  public Long getGroupId() {
    return groupId;
  }

  public Long getUserId() {
    return userId;
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
    if (!(o instanceof IdpGroupUserRelPO)) {
      return false;
    }
    IdpGroupUserRelPO idpGroupUserRelPO = (IdpGroupUserRelPO) o;
    return Objects.equal(getId(), idpGroupUserRelPO.getId())
        && Objects.equal(getGroupId(), idpGroupUserRelPO.getGroupId())
        && Objects.equal(getUserId(), idpGroupUserRelPO.getUserId())
        && Objects.equal(getAuditInfo(), idpGroupUserRelPO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), idpGroupUserRelPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), idpGroupUserRelPO.getLastVersion())
        && Objects.equal(getDeletedAt(), idpGroupUserRelPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getId(),
        getGroupId(),
        getUserId(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final IdpGroupUserRelPO idpGroupUserRelPO;

    private Builder() {
      idpGroupUserRelPO = new IdpGroupUserRelPO();
    }

    public Builder withId(Long id) {
      idpGroupUserRelPO.id = id;
      return this;
    }

    public Builder withGroupId(Long groupId) {
      idpGroupUserRelPO.groupId = groupId;
      return this;
    }

    public Builder withUserId(Long userId) {
      idpGroupUserRelPO.userId = userId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      idpGroupUserRelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      idpGroupUserRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      idpGroupUserRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      idpGroupUserRelPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(idpGroupUserRelPO.id != null, "Relation id is required");
      Preconditions.checkArgument(idpGroupUserRelPO.groupId != null, "Group id is required");
      Preconditions.checkArgument(idpGroupUserRelPO.userId != null, "User id is required");
      Preconditions.checkArgument(idpGroupUserRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(
          idpGroupUserRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(
          idpGroupUserRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(idpGroupUserRelPO.deletedAt != null, "Deleted at is required");
    }

    public IdpGroupUserRelPO build() {
      validate();
      return idpGroupUserRelPO;
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
