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

public class UserPO {
  private Long userId;
  private String userName;
  private Long metalakeId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }

  public Long getMetalakeId() {
    return metalakeId;
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
    if (!(o instanceof UserPO)) {
      return false;
    }
    UserPO tablePO = (UserPO) o;
    return Objects.equal(getUserId(), tablePO.getUserId())
        && Objects.equal(getUserName(), tablePO.getUserName())
        && Objects.equal(getMetalakeId(), tablePO.getMetalakeId())
        && Objects.equal(getAuditInfo(), tablePO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), tablePO.getCurrentVersion())
        && Objects.equal(getLastVersion(), tablePO.getLastVersion())
        && Objects.equal(getDeletedAt(), tablePO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getUserId(),
        getUserName(),
        getMetalakeId(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final UserPO userPO;

    private Builder() {
      userPO = new UserPO();
    }

    public Builder withUserId(Long userId) {
      userPO.userId = userId;
      return this;
    }

    public Builder withUserName(String userName) {
      userPO.userName = userName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      userPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      userPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      userPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      userPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      userPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(userPO.userId != null, "User id is required");
      Preconditions.checkArgument(userPO.userName != null, "User name is required");
      Preconditions.checkArgument(userPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(userPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(userPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(userPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(userPO.deletedAt != null, "Deleted at is required");
    }

    public UserPO build() {
      validate();
      return userPO;
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
